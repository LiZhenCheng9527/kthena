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
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
)

func TestRouterSelfEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		podIP   string
		port    int
		want    string
		wantErr bool
	}{
		{
			name:  "IPv4 address",
			podIP: "10.0.0.5",
			port:  9080,
			want:  "http://10.0.0.5:9080",
		},
		{
			name:  "IPv6 address is bracketed",
			podIP: "fd00::1",
			port:  9080,
			want:  "http://[fd00::1]:9080",
		},
		{
			name:    "missing POD_IP",
			podIP:   "",
			port:    9080,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POD_IP", tt.podIP)
			got, err := routerSelfEndpoint(tt.port)
			if (err != nil) != tt.wantErr {
				t.Fatalf("routerSelfEndpoint() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("routerSelfEndpoint() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRouterInstanceID(t *testing.T) {
	tests := []struct {
		name      string
		podName   string
		namespace string
		want      string
	}{
		{
			name:      "namespace-qualified pod name",
			podName:   "kthena-router-0",
			namespace: "kthena-system",
			want:      "kthena-system/kthena-router-0",
		},
		{
			name:    "pod name only",
			podName: "kthena-router-0",
			want:    "kthena-router-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POD_NAME", tt.podName)
			t.Setenv("POD_NAMESPACE", tt.namespace)
			if got := routerInstanceID(); got != tt.want {
				t.Errorf("routerInstanceID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewProcessGeneration(t *testing.T) {
	g1 := newProcessGeneration()
	g2 := newProcessGeneration()
	if g1 == "" || g2 == "" {
		t.Fatal("generation must not be empty")
	}
	if g1 == g2 {
		t.Errorf("expected unique generations, got %q twice", g1)
	}
}

func newTestRegistrar(pods podLister, idx *KVBlockMemoryIndex, runtimePort int) *kvRouterRegistrar {
	return newKVRouterRegistrar(pods, idx, "router-0", "http://10.0.0.9:9080", runtimePort,
		defaultRegistrationIntervalSec, defaultRegistrationTTLSec)
}

func storeTestBlock(t *testing.T, idx *KVBlockMemoryIndex, owner string) {
	t.Helper()
	payload := KVEventPayload{
		PodIdentifier: owner,
		ModelName:     "qwen",
		Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}}},
	}
	if err := idx.Apply(&payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}
}

func TestKVRouterRegistrar_DropLapsedOwners(t *testing.T) {
	const owner = "pod-1.default"
	lapse := newTestRegistrar(nil, nil, 0).registrationLapse()
	now := time.Now()

	tests := []struct {
		name string
		// lastAttemptAgo is how long ago a registration was last sent to the
		// pod; zero means never.
		lastAttemptAgo time.Duration
		wantKept       bool
	}{
		{name: "registered within the lapse", lastAttemptAgo: lapse - time.Second, wantKept: true},
		{name: "registration lapsed", lastAttemptAgo: lapse, wantKept: false},
		{name: "never registered", wantKept: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := NewKVBlockMemoryIndex(0)
			storeTestBlock(t, idx, owner)
			r := newTestRegistrar(&fakePodLister{}, idx, 0)
			if tt.lastAttemptAgo > 0 {
				r.lastAttempt[owner] = now.Add(-tt.lastAttemptAgo)
			}

			r.dropLapsedOwners(now)

			if kept := slices.Contains(idx.Owners(), owner); kept != tt.wantKept {
				t.Errorf("blocks kept = %v, want %v", kept, tt.wantKept)
			}
			if _, tracked := r.lastAttempt[owner]; tracked != tt.wantKept {
				t.Errorf("attempt still tracked = %v, want %v", tracked, tt.wantKept)
			}
		})
	}
}

func TestKVRouterRegistrar_KeepsBlocksOfPodBrieflyNotReady(t *testing.T) {
	var registrations atomic.Int32
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == runtimeRegisterPath {
			registrations.Add(1)
		}
	}))
	defer sidecar.Close()
	sidecarURL, err := url.Parse(sidecar.URL)
	if err != nil {
		t.Fatalf("failed to parse sidecar URL: %v", err)
	}
	host, port, err := net.SplitHostPort(sidecarURL.Host)
	if err != nil {
		t.Fatalf("failed to split sidecar address: %v", err)
	}
	runtimePort, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("invalid sidecar port %q: %v", port, err)
	}

	pods := &fakePodLister{pods: map[types.NamespacedName]*datastore.PodInfo{}}
	pods.add("default", "pod-1", host)
	idx := NewKVBlockMemoryIndex(0)
	storeTestBlock(t, idx, "pod-1.default")
	r := newTestRegistrar(pods, idx, runtimePort)

	r.sweep(context.Background())
	if got := registrations.Load(); got != 1 {
		t.Fatalf("registrations = %d, want 1", got)
	}
	if got := idx.Owners(); len(got) != 1 {
		t.Fatalf("expected the blocks of a registered pod to be kept, got owners %v", got)
	}

	// Not Ready: the pod leaves the datastore and is no longer registered
	// with, but its sidecar may still consider this router registered and
	// would not resend its blocks when the pod is Ready again.
	delete(pods.pods, types.NamespacedName{Namespace: "default", Name: "pod-1"})
	r.sweep(context.Background())
	if got := registrations.Load(); got != 1 {
		t.Fatalf("registrations = %d, want 1", got)
	}
	if got := idx.Owners(); len(got) != 1 {
		t.Fatalf("expected the blocks to be kept while the registration is valid, got owners %v", got)
	}

	// Once the registration has lapsed, the sidecar treats the next one as new
	// and resends its blocks, so they can be dropped.
	r.dropLapsedOwners(time.Now().Add(r.registrationLapse()))
	if got := idx.Owners(); len(got) != 0 {
		t.Fatalf("expected the blocks to be dropped after the registration lapsed, got owners %v", got)
	}
}
