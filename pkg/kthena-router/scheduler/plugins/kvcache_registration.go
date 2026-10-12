/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugins

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const (
	runtimeRegisterPath = "/kvcache/routers/register"

	defaultRuntimePort             = 9000
	defaultRegistrationIntervalSec = 30
	defaultRegistrationTTLSec      = 90

	// maxConcurrentRegistrations bounds the fan-out of one registration sweep
	// so that many unreachable pods cannot stretch a sweep past the TTL.
	maxConcurrentRegistrations = 16
)

// routerRegistrationRequest is the body posted to the runtime sidecar's
// /kvcache/routers/register endpoint. Kept in sync by hand with
// python/kthena/runtime/app.py.
type routerRegistrationRequest struct {
	RouterID string `json:"router_id"`
	Endpoint string `json:"endpoint"`
	// Generation identifies one router process. It changes when the router
	// container restarts (even with an unchanged pod name), so sidecars can
	// detect that the in-memory index was lost and push a fresh snapshot.
	Generation string `json:"generation"`
	TTLSeconds int    `json:"ttl_seconds"`
}

// kvRouterRegistrar periodically registers this router instance with the
// runtime sidecar of every known model-serving pod, so the sidecars can push
// KV cache events back. Registration doubles as a heartbeat: sidecars expire
// routers that stop re-registering within the TTL. The router in turn drops
// the blocks of a pod once it has not registered with it for that long, see
// dropLapsedOwners.
type kvRouterRegistrar struct {
	store       podLister
	index       *KVBlockMemoryIndex
	routerID    string
	generation  string // unique per router process, see routerRegistrationRequest
	endpoint    string // base URL the sidecar pushes events to, e.g. http://10.0.0.5:9080
	runtimePort int
	interval    time.Duration
	ttlSeconds  int
	client      *http.Client

	mu sync.Mutex
	// lastAttempt is when a registration was last sent to each pod, keyed by
	// cache owner identifier.
	lastAttempt map[string]time.Time
}

func newKVRouterRegistrar(store podLister, index *KVBlockMemoryIndex, routerID, endpoint string,
	runtimePort, intervalSeconds, ttlSeconds int) *kvRouterRegistrar {
	return &kvRouterRegistrar{
		store:       store,
		index:       index,
		routerID:    routerID,
		generation:  newProcessGeneration(),
		endpoint:    endpoint,
		runtimePort: runtimePort,
		interval:    time.Duration(intervalSeconds) * time.Second,
		ttlSeconds:  ttlSeconds,
		client:      &http.Client{Timeout: 3 * time.Second},
		lastAttempt: make(map[string]time.Time),
	}
}

// newProcessGeneration returns an identifier that is unique per router
// process, so a restarted container with the same pod name still triggers a
// snapshot from every sidecar.
func newProcessGeneration() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf)
}

// routerSelfEndpoint derives the base URL runtime sidecars should push KV
// events to. POD_IP must be injected via the downward API on the router
// deployment; without it memory mode cannot receive pushes.
func routerSelfEndpoint(eventsPort int) (string, error) {
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		return "", fmt.Errorf("POD_IP environment variable is not set")
	}
	return "http://" + net.JoinHostPort(podIP, strconv.Itoa(eventsPort)), nil
}

// routerInstanceID identifies this router replica in sidecar registries. Pod
// names are only unique within a namespace, so the namespace is included to
// keep routers from different namespaces from overwriting each other.
func routerInstanceID() string {
	name := os.Getenv("POD_NAME")
	if name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return "kthena-router"
		}
		name = hostname
	}
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		return namespace + "/" + name
	}
	return name
}

// run sweeps immediately and then on every tick until ctx is done.
func (r *kvRouterRegistrar) run(ctx context.Context) {
	r.sweep(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

// sweep registers with all known pods, then drops the blocks of pods whose
// registration has lapsed.
func (r *kvRouterRegistrar) sweep(ctx context.Context) {
	r.registerAll(ctx)
	r.dropLapsedOwners(time.Now())
}

func (r *kvRouterRegistrar) registerAll(ctx context.Context) {
	pods := r.store.GetAllPods()
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		registered int
	)
	// Bounded fan-out: registrations run concurrently so a sweep over many
	// pods without a reachable sidecar stays well below the TTL.
	sem := make(chan struct{}, maxConcurrentRegistrations)
	for podName, podInfo := range pods {
		pod := podInfo.GetPod()
		if pod == nil || pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(podName types.NamespacedName, podIP string) {
			defer wg.Done()
			defer func() { <-sem }()
			r.recordAttempt(cacheOwnerIdentifier(podName))
			if err := r.registerWithPod(ctx, podIP); err != nil {
				// Pods without a runtime sidecar (or not in memory mode) are expected
				// to fail; keep this quiet.
				klog.V(4).Infof("KVCacheAware: registration with pod %s (%s) failed: %v",
					podName, podIP, err)
				return
			}
			mu.Lock()
			registered++
			mu.Unlock()
		}(podName, pod.Status.PodIP)
	}
	wg.Wait()
	klog.V(4).Infof("KVCacheAware: registered with %d/%d runtime sidecars", registered, len(pods))
}

func (r *kvRouterRegistrar) recordAttempt(owner string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastAttempt[owner] = time.Now()
}

// registrationLapse is how long after the last registration attempt with a
// pod its sidecar has certainly expired this router: the TTL, plus the
// request timeout as the request may be received until then, plus one
// interval of slack for a sidecar that handles the request late.
func (r *kvRouterRegistrar) registrationLapse() time.Duration {
	return time.Duration(r.ttlSeconds)*time.Second + r.client.Timeout + r.interval
}

// dropLapsedOwners drops the blocks of every pod this router has not tried to
// register with within registrationLapse. A pod leaves the sweep when it
// leaves the datastore, either because it was deleted or because it is
// temporarily not Ready.
//
// Blocks are kept until the registration lapsed, not dropped as soon as the
// pod leaves: a sidecar only resends its full block set to a router that
// registers anew (or after a failed push), so a pod that is Ready again
// before its registration expired would otherwise stay without blocks. Once
// the registration has lapsed, a pod that comes back always gets a snapshot.
func (r *kvRouterRegistrar) dropLapsedOwners(now time.Time) {
	lapse := r.registrationLapse()
	var lapsed []string
	r.mu.Lock()
	for owner, last := range r.lastAttempt {
		if now.Sub(last) >= lapse {
			delete(r.lastAttempt, owner)
		}
	}
	for _, owner := range r.index.Owners() {
		if _, ok := r.lastAttempt[owner]; !ok {
			lapsed = append(lapsed, owner)
		}
	}
	r.mu.Unlock()

	for _, owner := range lapsed {
		r.index.RemoveOwner(owner)
		klog.V(4).Infof("KVCacheAware: dropped the blocks of pod %s, not registered with for %s", owner, lapse)
	}
}

func (r *kvRouterRegistrar) registerWithPod(ctx context.Context, podIP string) error {
	body, err := json.Marshal(routerRegistrationRequest{
		RouterID:   r.routerID,
		Endpoint:   r.endpoint,
		Generation: r.generation,
		TTLSeconds: r.ttlSeconds,
	})
	if err != nil {
		return err
	}

	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(r.runtimePort)) + runtimeRegisterPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
