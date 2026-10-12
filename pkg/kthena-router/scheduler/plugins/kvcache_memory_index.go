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
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
)

// The in-memory KV index ("memory" index mode) replaces the shared Redis
// block matrix with a per-router-process index kept in sync by push:
//
//  1. The runtime sidecar next to each inference engine subscribes to the
//     engine's KV cache events, converts engine block hashes into
//     standardized token-block hashes, and keeps the authoritative
//     per-model block set for its pod
//     (python/kthena/runtime/memory_kv_manager.py).
//  2. Every router periodically registers with each runtime sidecar
//     (kvcache_registration.go), announcing its push endpoint, a TTL, and a
//     per-process generation.
//  3. Sidecars push incremental stored/removed/cleared deltas to all
//     registered routers, and push a full replace-style snapshot to a router
//     whose process is new (generation change or expired registration) or
//     whose previous push failed, so a router can always rebuild its index.
//  4. Each router applies these events to its KVBlockMemoryIndex and scores
//     pods by longest prefix match exactly like the Redis backend; a
//     periodic GC drops entries not refreshed within the freshness window.
//
// The push endpoint trusts nothing in the payload about who sent it: the
// sending pod is resolved from the connection's source IP against the pods
// tracked by the datastore, once per connection, so a caller can only write
// entries for itself.
// Store times are capped at the receive time and every pod's share of the
// index is bounded, so a misbehaving pod cannot pin entries past GC or grow
// router memory without limit.
//
// KV event types pushed by the runtime sidecar in memory sync mode.
// Kept in sync by hand with python/kthena/runtime/memory_kv_manager.py.
const (
	kvEventStored   = "stored"
	kvEventRemoved  = "removed"
	kvEventCleared  = "cleared"
	kvEventSnapshot = "snapshot"
)

// KVEvent is a single KV cache event carried in a push payload. All block
// hashes are standardized hashes computed by the runtime sidecar.
type KVEvent struct {
	Type        string   `json:"type"`
	BlockHashes []uint64 `json:"block_hashes,omitempty"`
	Timestamp   int64    `json:"timestamp,omitempty"`
	// Timestamps optionally carries one unix-second store time per entry of
	// BlockHashes. Snapshots use it to preserve original store times instead
	// of re-stamping every block at snapshot time, which would defeat the
	// engine-restart freshness filter.
	Timestamps []int64 `json:"timestamps,omitempty"`
}

// KVEventPayload is the body of POST /kvcache/events pushed by a runtime sidecar.
type KVEventPayload struct {
	PodIdentifier string    `json:"pod_identifier"`
	ModelName     string    `json:"model_name"`
	Events        []KVEvent `json:"events"`
}

// An owner is the pod identifier of the inference engine pod that holds a
// block in its KV cache, in the "<podName>.<namespace>" form — the same
// identifier the Redis backend stores as hash field names, so lookup results
// flow through the shared scoring path unchanged.
//
// ownerModelKey indexes per-owner-per-model block sets for removal and snapshots.
type ownerModelKey struct {
	owner string
	model string
}

// defaultMaxBlocksPerPod is the default upper bound of blocks a single pod may
// hold in the index across all models. It is far above the KV cache capacity
// of a single engine (1Mi blocks of 16 tokens is ~16.7M cached tokens) and only
// exists to bound router memory.
const defaultMaxBlocksPerPod = 1 << 20

// KVBlockMemoryIndex is an in-memory replacement for the Redis block matrix.
// It stores which pod (owner) currently caches which standardized token block
// hash, per model, together with the time the block was last stored.
type KVBlockMemoryIndex struct {
	mu sync.RWMutex
	// maxBlocksPerOwner caps ownerTotals; an owner at the cap evicts its
	// oldest blocks to store new ones, see evictOldestLocked.
	maxBlocksPerOwner int
	// model -> block hash -> owner(podName.namespace) -> unix seconds of last store
	blocks map[string]map[uint64]map[string]int64
	// reverse index for cleared/snapshot events and eviction:
	// owner+model -> block hash -> unix seconds of last store, same as in blocks
	ownerBlocks map[ownerModelKey]map[uint64]int64
	// owner -> number of blocks held across all models
	ownerTotals map[string]int
}

// NewKVBlockMemoryIndex creates an index that holds at most maxBlocksPerOwner
// blocks per pod; a non-positive value selects defaultMaxBlocksPerPod.
func NewKVBlockMemoryIndex(maxBlocksPerOwner int) *KVBlockMemoryIndex {
	if maxBlocksPerOwner <= 0 {
		maxBlocksPerOwner = defaultMaxBlocksPerPod
	}
	return &KVBlockMemoryIndex{
		maxBlocksPerOwner: maxBlocksPerOwner,
		blocks:            make(map[string]map[uint64]map[string]int64),
		ownerBlocks:       make(map[ownerModelKey]map[uint64]int64),
		ownerTotals:       make(map[string]int),
	}
}

// Apply updates the index with a pushed event payload.
func (idx *KVBlockMemoryIndex) Apply(payload *KVEventPayload) error {
	if payload.PodIdentifier == "" || payload.ModelName == "" {
		return fmt.Errorf("pod_identifier and model_name are required")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	now := time.Now().Unix()
	evicted := 0
	for _, event := range payload.Events {
		if len(event.Timestamps) > 0 && len(event.Timestamps) != len(event.BlockHashes) {
			return fmt.Errorf("timestamps length %d does not match block_hashes length %d",
				len(event.Timestamps), len(event.BlockHashes))
		}
		switch event.Type {
		case kvEventStored:
			evicted += idx.storeLocked(payload.ModelName, payload.PodIdentifier, event.BlockHashes, event.Timestamp, event.Timestamps, now)
		case kvEventRemoved:
			idx.removeLocked(payload.ModelName, payload.PodIdentifier, event.BlockHashes)
		case kvEventCleared:
			idx.clearLocked(payload.ModelName, payload.PodIdentifier)
		case kvEventSnapshot:
			// A snapshot replaces everything known about this owner+model.
			idx.clearLocked(payload.ModelName, payload.PodIdentifier)
			evicted += idx.storeLocked(payload.ModelName, payload.PodIdentifier, event.BlockHashes, event.Timestamp, event.Timestamps, now)
		default:
			return fmt.Errorf("unknown event type %q", event.Type)
		}
	}
	if evicted > 0 {
		klog.V(2).Infof("KVCacheAware: pod %s reached the limit of %d indexed blocks, evicted its %d oldest blocks",
			payload.PodIdentifier, idx.maxBlocksPerOwner, evicted)
	}
	return nil
}

// clampStoreTime returns the store time to record for a pushed block. Store
// times come from the pushing pod: one in the future would never be garbage
// collected and would defeat the restart freshness check, so it is capped at
// the receive time; a missing one defaults to it.
func clampStoreTime(ts, now int64) int64 {
	if ts <= 0 || ts > now {
		return now
	}
	return ts
}

// storeLocked records hashes as owned by owner and returns how many blocks of
// owner were evicted to make room for them.
func (idx *KVBlockMemoryIndex) storeLocked(model, owner string, hashes []uint64, timestamp int64, timestamps []int64, now int64) int {
	if len(hashes) == 0 {
		return 0
	}
	key := ownerModelKey{owner: owner, model: model}
	modelBlocks, owned := idx.ownerModelLocked(key)
	evicted := 0
	for i, hash := range hashes {
		ts := timestamp
		if len(timestamps) > 0 && timestamps[i] > 0 {
			ts = timestamps[i]
		}
		ts = clampStoreTime(ts, now)
		if _, held := owned[hash]; !held {
			if idx.ownerTotals[owner] >= idx.maxBlocksPerOwner {
				evicted += idx.evictOldestLocked(owner)
				// Eviction deletes the maps it empties, possibly these.
				modelBlocks, owned = idx.ownerModelLocked(key)
			}
			idx.ownerTotals[owner]++
		}
		owned[hash] = ts
		owners, ok := modelBlocks[hash]
		if !ok {
			owners = make(map[string]int64)
			modelBlocks[hash] = owners
		}
		owners[owner] = ts
	}
	return evicted
}

// ownerModelLocked returns the blocks of key.model and the blocks owned by
// key.owner in that model, creating the maps if needed.
func (idx *KVBlockMemoryIndex) ownerModelLocked(key ownerModelKey) (map[uint64]map[string]int64, map[uint64]int64) {
	modelBlocks, ok := idx.blocks[key.model]
	if !ok {
		modelBlocks = make(map[uint64]map[string]int64)
		idx.blocks[key.model] = modelBlocks
	}
	owned, ok := idx.ownerBlocks[key]
	if !ok {
		owned = make(map[uint64]int64)
		idx.ownerBlocks[key] = owned
	}
	return modelBlocks, owned
}

// evictOldestLocked makes room for new blocks of owner, which holds
// maxBlocksPerOwner blocks, by evicting its least recently stored blocks
// across all models. Blocks left over from a previous incarnation of the pod
// are older than its current ones, so they go first. A tenth of the limit is
// evicted at once, so the scan over the owner's blocks is not repeated for
// every new block. It returns the number of evicted blocks.
func (idx *KVBlockMemoryIndex) evictOldestLocked(owner string) int {
	n := max(1, idx.maxBlocksPerOwner/10)

	// The n oldest blocks are those stored before cutoff, plus atCutoff of
	// those stored at cutoff.
	counts := make(map[int64]int)
	for key, owned := range idx.ownerBlocks {
		if key.owner != owner {
			continue
		}
		for _, ts := range owned {
			counts[ts]++
		}
	}
	var cutoff int64
	atCutoff := n
	for _, ts := range slices.Sorted(maps.Keys(counts)) {
		cutoff = ts
		if counts[ts] >= atCutoff {
			break
		}
		atCutoff -= counts[ts]
	}

	evicted := 0
	for key, owned := range idx.ownerBlocks {
		if key.owner != owner {
			continue
		}
		modelBlocks := idx.blocks[key.model]
		for hash, ts := range owned {
			if ts > cutoff || (ts == cutoff && atCutoff == 0) {
				continue
			}
			if ts == cutoff {
				atCutoff--
			}
			delete(owned, hash)
			if owners, ok := modelBlocks[hash]; ok {
				delete(owners, owner)
				if len(owners) == 0 {
					delete(modelBlocks, hash)
				}
			}
			evicted++
		}
		if len(owned) == 0 {
			delete(idx.ownerBlocks, key)
		}
		if len(modelBlocks) == 0 {
			delete(idx.blocks, key.model)
		}
	}
	idx.releaseLocked(owner, evicted)
	return evicted
}

// releaseLocked accounts for count blocks no longer held by owner.
func (idx *KVBlockMemoryIndex) releaseLocked(owner string, count int) {
	if remaining := idx.ownerTotals[owner] - count; remaining > 0 {
		idx.ownerTotals[owner] = remaining
	} else {
		delete(idx.ownerTotals, owner)
	}
}

func (idx *KVBlockMemoryIndex) removeLocked(model, owner string, hashes []uint64) {
	modelBlocks := idx.blocks[model]
	key := ownerModelKey{owner: owner, model: model}
	owned := idx.ownerBlocks[key]
	for _, hash := range hashes {
		if _, held := owned[hash]; !held {
			continue
		}
		delete(owned, hash)
		idx.releaseLocked(owner, 1)
		if owners, ok := modelBlocks[hash]; ok {
			delete(owners, owner)
			if len(owners) == 0 {
				delete(modelBlocks, hash)
			}
		}
	}
	if len(owned) == 0 {
		delete(idx.ownerBlocks, key)
	}
	if len(modelBlocks) == 0 {
		delete(idx.blocks, model)
	}
}

func (idx *KVBlockMemoryIndex) clearLocked(model, owner string) {
	key := ownerModelKey{owner: owner, model: model}
	modelBlocks := idx.blocks[model]
	owned := idx.ownerBlocks[key]
	for hash := range owned {
		if owners, ok := modelBlocks[hash]; ok {
			delete(owners, owner)
			if len(owners) == 0 {
				delete(modelBlocks, hash)
			}
		}
	}
	idx.releaseLocked(owner, len(owned))
	delete(idx.ownerBlocks, key)
	if len(modelBlocks) == 0 {
		delete(idx.blocks, model)
	}
}

// GetBlockOwners returns, for each requested block hash that is present, the
// owners and the unix-second timestamps of their last store. The timestamps
// are returned as strings so the result can flow through the same freshness
// filtering (freshOwners) as the Redis backend.
func (idx *KVBlockMemoryIndex) GetBlockOwners(model string, hashes []uint64) map[uint64]map[string]string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[uint64]map[string]string)
	modelBlocks, ok := idx.blocks[model]
	if !ok {
		return result
	}
	for _, hash := range hashes {
		owners, ok := modelBlocks[hash]
		if !ok || len(owners) == 0 {
			continue
		}
		entries := make(map[string]string, len(owners))
		for owner, ts := range owners {
			entries[owner] = strconv.FormatInt(ts, 10)
		}
		result[hash] = entries
	}
	return result
}

// RemoveOwner drops every block owned by the given pod identifier across all
// models, e.g. once the pod is gone.
func (idx *KVBlockMemoryIndex) RemoveOwner(owner string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for key := range idx.ownerBlocks {
		if key.owner == owner {
			idx.clearLocked(key.model, key.owner)
		}
	}
}

// Owners returns the pod identifiers that hold at least one block.
func (idx *KVBlockMemoryIndex) Owners() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	owners := make([]string, 0, len(idx.ownerTotals))
	for owner := range idx.ownerTotals {
		owners = append(owners, owner)
	}
	return owners
}

// gcStaleEntries drops ownership entries older than freshDuration, mirroring
// the Redis backend GC so restarts or missed removal events cannot leave dead
// entries behind forever.
//
// The full map is traversed while holding the write lock, blocking concurrent
// lookups for the duration of the sweep. This is a deliberate simplicity
// trade-off: the index size is bounded by the aggregate engine KV cache
// capacity (one entry per cached block per pod), so a sweep is pure in-memory
// work finishing in milliseconds even at millions of entries, and it runs
// only once per kvCacheGCInterval (hourly). If the pause ever becomes a
// problem, the sweep can be made incremental, like the cursor-based SCAN GC
// of the Redis backend.
func (idx *KVBlockMemoryIndex) gcStaleEntries(freshDuration time.Duration) {
	cutoff := time.Now().Add(-freshDuration).Unix()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for model, modelBlocks := range idx.blocks {
		for hash, owners := range modelBlocks {
			for owner, ts := range owners {
				if ts < cutoff {
					delete(owners, owner)
					key := ownerModelKey{owner: owner, model: model}
					owned := idx.ownerBlocks[key]
					delete(owned, hash)
					idx.releaseLocked(owner, 1)
					// Remove emptied parent entries so replaced pods do not
					// leave empty maps behind indefinitely.
					if len(owned) == 0 {
						delete(idx.ownerBlocks, key)
					}
				}
			}
			if len(owners) == 0 {
				delete(modelBlocks, hash)
			}
		}
		if len(modelBlocks) == 0 {
			delete(idx.blocks, model)
		}
	}
}

// runGC periodically removes stale entries until ctx is done.
func (idx *KVBlockMemoryIndex) runGC(ctx context.Context, interval, freshDuration time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx.gcStaleEntries(freshDuration)
		}
	}
}

// kvEventsMaxBodyBytes caps the request body accepted on the KV events
// endpoint so oversized payloads cannot exhaust router memory.
const kvEventsMaxBodyBytes = 4 << 20 // 4 MiB

const (
	// kvEventSourceCacheTTL bounds how long the source IP to pod mapping is
	// reused before it is rebuilt from the datastore. Datastore pod events
	// invalidate the mapping right away; the TTL is only a safety net.
	kvEventSourceCacheTTL = 30 * time.Second
	// kvEventSourceMissRebuildInterval rate-limits rebuilds triggered by a
	// push from an unknown source IP, e.g. a pod that was just added.
	kvEventSourceMissRebuildInterval = time.Second
)

// podLister is the subset of datastore.Store used to resolve push sources.
type podLister interface {
	GetAllPods() map[types.NamespacedName]*datastore.PodInfo
	GetPodInfo(podName types.NamespacedName) *datastore.PodInfo
}

// kvEventSourceResolver maps the source IP of a KV events push to the pod that
// sent it. Only pods tracked by the datastore, i.e. selected by a ModelServer,
// are accepted.
type kvEventSourceResolver struct {
	pods podLister

	mu   sync.Mutex
	byIP map[string]types.NamespacedName
	// shared holds IPs used by more than one pod (e.g. hostNetwork pods on
	// the same node); pushes from them cannot be attributed and are rejected.
	shared      map[string]struct{}
	lastRebuild time.Time
	// stale makes the next lookup rebuild the mapping, see invalidate.
	stale bool
}

func newKVEventSourceResolver(pods podLister) *kvEventSourceResolver {
	return &kvEventSourceResolver{pods: pods}
}

// invalidate is the datastore Pod callback of the resolver. Any pod add,
// update or delete can change which pod an IP belongs to, or make an IP
// shared (e.g. a second hostNetwork pod on a node), so the cached mapping must
// not be used for the next push.
func (r *kvEventSourceResolver) invalidate(datastore.EventData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stale = true
}

// resolveOwner returns the owner identifier of the pod whose IP matches the
// host of remoteAddr.
func (r *kvEventSourceResolver) resolveOwner(remoteAddr string) (string, error) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", fmt.Errorf("invalid remote address %q", remoteAddr)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", fmt.Errorf("invalid remote address %q", remoteAddr)
	}
	key := ip.String()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stale || time.Since(r.lastRebuild) >= kvEventSourceCacheTTL {
		r.rebuildLocked()
	}
	pod := r.lookupLocked(key)
	if pod == nil && time.Since(r.lastRebuild) >= kvEventSourceMissRebuildInterval {
		r.rebuildLocked()
		pod = r.lookupLocked(key)
	}
	if pod != nil {
		return podCacheOwnerIdentifier(pod), nil
	}
	if _, ok := r.shared[key]; ok {
		return "", fmt.Errorf("source IP %s is shared by multiple pods", key)
	}
	return "", fmt.Errorf("source IP %s is not a known model-serving pod", key)
}

func (r *kvEventSourceResolver) lookupLocked(ip string) *datastore.PodInfo {
	name, ok := r.byIP[ip]
	if !ok {
		return nil
	}
	// Re-check the datastore so a cached entry of a deleted pod, or of a pod
	// whose IP was reassigned, is never matched.
	info := r.pods.GetPodInfo(name)
	if info == nil || !slices.Contains(podIPs(info.GetPod()), ip) {
		return nil
	}
	return info
}

func (r *kvEventSourceResolver) rebuildLocked() {
	byIP := make(map[string]types.NamespacedName)
	shared := make(map[string]struct{})
	for name, info := range r.pods.GetAllPods() {
		for _, ip := range podIPs(info.GetPod()) {
			if _, ok := shared[ip]; ok {
				continue
			}
			if other, ok := byIP[ip]; ok && other != name {
				delete(byIP, ip)
				shared[ip] = struct{}{}
				continue
			}
			byIP[ip] = name
		}
	}
	r.byIP = byIP
	r.shared = shared
	r.lastRebuild = time.Now()
	r.stale = false
}

// podIPs returns the normalized IPs assigned to pod.
func podIPs(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	var ips []string
	add := func(s string) {
		if ip := net.ParseIP(s); ip != nil {
			ips = append(ips, ip.String())
		}
	}
	add(pod.Status.PodIP)
	for _, podIP := range pod.Status.PodIPs {
		add(podIP.IP)
	}
	return ips
}

// kvEventsConnKey is the context key of a kvEventsConn.
type kvEventsConnKey struct{}

// kvEventsConn is the state of one connection to the KV events server.
type kvEventsConn struct {
	// owner is the pod the connection was attributed to, see kvEventsSource.
	owner atomic.Pointer[string]
}

// withKVEventsConn is the ConnContext hook of the KV events server.
func withKVEventsConn(ctx context.Context, _ net.Conn) context.Context {
	return context.WithValue(ctx, kvEventsConnKey{}, &kvEventsConn{})
}

// kvEventsSource returns the pod that sent r. A sidecar pushes over a
// long-lived keep-alive connection, and the peer of a TCP connection does not
// change while it is open, so the sending pod is resolved on the first push of
// a connection and reused for the following ones. A failed resolution is not
// kept: the next push on the connection retries it, e.g. once the datastore
// knows the pod.
func kvEventsSource(r *http.Request, resolveOwner func(remoteAddr string) (string, error)) (string, error) {
	conn, _ := r.Context().Value(kvEventsConnKey{}).(*kvEventsConn)
	if conn != nil {
		if owner := conn.owner.Load(); owner != nil {
			return *owner, nil
		}
	}
	// Use the TCP peer address, never forwarding headers (gin's ClientIP):
	// they are set by the caller and would let it choose its identity.
	owner, err := resolveOwner(r.RemoteAddr)
	if err != nil {
		return "", err
	}
	if conn != nil {
		conn.owner.Store(&owner)
	}
	return owner, nil
}

// kvEventsHandler handles POST /kvcache/events pushed by runtime sidecars.
// resolveOwner maps the connection's remote address to the sending pod, see
// kvEventsSource; the payload's pod_identifier must match it.
func kvEventsHandler(index *KVBlockMemoryIndex, resolveOwner func(remoteAddr string) (string, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		owner, err := kvEventsSource(c.Request, resolveOwner)
		if err != nil {
			klog.V(2).Infof("KVCacheAware: rejected KV events push from %s: %v", c.Request.RemoteAddr, err)
			c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, kvEventsMaxBodyBytes)
		var payload KVEventPayload
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload: " + err.Error()})
			return
		}
		if payload.PodIdentifier != owner {
			klog.V(2).Infof("KVCacheAware: rejected KV events push from %s: pod_identifier %q does not match sending pod %q",
				c.Request.RemoteAddr, payload.PodIdentifier, owner)
			c.JSON(http.StatusForbidden, gin.H{
				"error": fmt.Sprintf("pod_identifier %q does not match the sending pod %q", payload.PodIdentifier, owner),
			})
			return
		}
		if err := index.Apply(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		klog.V(4).Infof("KVCacheAware: applied %d pushed events from pod=%s model=%s",
			len(payload.Events), payload.PodIdentifier, payload.ModelName)
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}

// newKVEventsServer returns the HTTP server that receives pushed KV events
// from runtime sidecars on addr.
func newKVEventsServer(addr string, index *KVBlockMemoryIndex, resolveOwner func(remoteAddr string) (string, error)) *http.Server {
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.POST("/kvcache/events", kvEventsHandler(index, resolveOwner))

	return &http.Server{
		Addr:    addr,
		Handler: engine.Handler(),
		// Lets the handler attribute each connection to its pod once.
		ConnContext: withKVEventsConn,
		// Basic hardening against slow or stalled clients holding
		// connections open on this cluster-reachable endpoint.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// startKVEventsServer starts the HTTP listener that receives pushed KV events
// from runtime sidecars when the plugin runs in memory index mode. The listener
// is bound synchronously so a startup failure (e.g. the port is already in use)
// fails the router instead of leaving it running without an index feed.
func startKVEventsServer(port int, index *KVBlockMemoryIndex, resolveOwner func(remoteAddr string) (string, error)) {
	server := newKVEventsServer(fmt.Sprintf(":%d", port), index, resolveOwner)
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		klog.Fatalf("KVCacheAware: cannot listen on KV events port %d: %v", port, err)
	}
	go func() {
		klog.Infof("KVCacheAware: starting KV events server on %s", server.Addr)
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			klog.Errorf("KVCacheAware: KV events server failed: %v", err)
		}
	}()
}
