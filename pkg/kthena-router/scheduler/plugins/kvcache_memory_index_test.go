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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
)

func TestKVBlockMemoryIndex_Apply(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name        string
		payloads    []KVEventPayload
		wantErr     bool
		queryModel  string
		queryHashes []uint64
		wantOwners  map[uint64][]string
	}{
		{
			name: "stored blocks are queryable",
			payloads: []KVEventPayload{{
				PodIdentifier: "pod-1.default",
				ModelName:     "qwen",
				Events: []KVEvent{
					{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now},
				},
			}},
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2, 3},
			wantOwners: map[uint64][]string{
				1: {"pod-1.default"},
				2: {"pod-1.default"},
			},
		},
		{
			name: "removed blocks disappear",
			payloads: []KVEventPayload{
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events: []KVEvent{
						{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now},
						{Type: kvEventRemoved, BlockHashes: []uint64{1}},
					},
				},
			},
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2},
			wantOwners: map[uint64][]string{
				2: {"pod-1.default"},
			},
		},
		{
			name: "cleared drops only the clearing owner",
			payloads: []KVEventPayload{
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
				},
				{
					PodIdentifier: "pod-2.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
				},
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventCleared}},
				},
			},
			queryModel:  "qwen",
			queryHashes: []uint64{1},
			wantOwners: map[uint64][]string{
				1: {"pod-2.default"},
			},
		},
		{
			name: "snapshot replaces previous owner state",
			payloads: []KVEventPayload{
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now}},
				},
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventSnapshot, BlockHashes: []uint64{3}, Timestamp: now}},
				},
			},
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2, 3},
			wantOwners: map[uint64][]string{
				3: {"pod-1.default"},
			},
		},
		{
			name: "empty snapshot clears previous owner state",
			payloads: []KVEventPayload{
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now}},
				},
				{
					PodIdentifier: "pod-1.default",
					ModelName:     "qwen",
					Events:        []KVEvent{{Type: kvEventSnapshot, Timestamp: now}},
				},
			},
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "mismatched timestamps length is rejected",
			payloads: []KVEventPayload{{
				PodIdentifier: "pod-1.default",
				ModelName:     "qwen",
				Events: []KVEvent{{
					Type:        kvEventSnapshot,
					BlockHashes: []uint64{1, 2},
					Timestamps:  []int64{now},
				}},
			}},
			wantErr:     true,
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "models are isolated",
			payloads: []KVEventPayload{{
				PodIdentifier: "pod-1.default",
				ModelName:     "qwen",
				Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
			}},
			queryModel:  "other-model",
			queryHashes: []uint64{1},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "missing pod identifier is rejected",
			payloads: []KVEventPayload{{
				ModelName: "qwen",
				Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}}},
			}},
			wantErr:     true,
			queryModel:  "qwen",
			queryHashes: []uint64{1},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "unknown event type is rejected",
			payloads: []KVEventPayload{{
				PodIdentifier: "pod-1.default",
				ModelName:     "qwen",
				Events:        []KVEvent{{Type: "bogus", BlockHashes: []uint64{1}}},
			}},
			wantErr:     true,
			queryModel:  "qwen",
			queryHashes: []uint64{1},
			wantOwners:  map[uint64][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := NewKVBlockMemoryIndex(0)
			var gotErr error
			for i := range tt.payloads {
				if err := idx.Apply(&tt.payloads[i]); err != nil {
					gotErr = err
				}
			}
			if (gotErr != nil) != tt.wantErr {
				t.Fatalf("Apply() error = %v, wantErr = %v", gotErr, tt.wantErr)
			}

			got := idx.GetBlockOwners(tt.queryModel, tt.queryHashes)
			if len(got) != len(tt.wantOwners) {
				t.Fatalf("GetBlockOwners() = %v, want owners for %v", got, tt.wantOwners)
			}
			for hash, wantPods := range tt.wantOwners {
				entries, ok := got[hash]
				if !ok {
					t.Fatalf("expected hash %d in result, got %v", hash, got)
				}
				if len(entries) != len(wantPods) {
					t.Fatalf("hash %d: got owners %v, want %v", hash, entries, wantPods)
				}
				for _, pod := range wantPods {
					if _, ok := entries[pod]; !ok {
						t.Errorf("hash %d: expected owner %s, got %v", hash, pod, entries)
					}
				}
			}
		})
	}
}

func TestKVBlockMemoryIndex_SnapshotPreservesPerBlockTimestamps(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	tsOld := time.Now().Add(-2 * time.Hour).Unix()
	tsNew := time.Now().Unix()

	payload := KVEventPayload{
		PodIdentifier: "pod-1.default",
		ModelName:     "qwen",
		Events: []KVEvent{{
			Type:        kvEventSnapshot,
			BlockHashes: []uint64{1, 2},
			Timestamps:  []int64{tsOld, tsNew},
		}},
	}
	if err := idx.Apply(&payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}

	got := idx.GetBlockOwners("qwen", []uint64{1, 2})
	if want := strconv.FormatInt(tsOld, 10); got[1]["pod-1.default"] != want {
		t.Errorf("hash 1 timestamp = %s, want %s", got[1]["pod-1.default"], want)
	}
	if want := strconv.FormatInt(tsNew, 10); got[2]["pod-1.default"] != want {
		t.Errorf("hash 2 timestamp = %s, want %s", got[2]["pod-1.default"], want)
	}
}

func TestKVBlockMemoryIndex_RemoveOwner(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	now := time.Now().Unix()
	for _, payload := range []KVEventPayload{
		{
			PodIdentifier: "pod-1.default",
			ModelName:     "qwen",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now}},
		},
		{
			PodIdentifier: "pod-1.default",
			ModelName:     "llama",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{9}, Timestamp: now}},
		},
		{
			PodIdentifier: "pod-2.default",
			ModelName:     "qwen",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
		},
	} {
		p := payload
		if err := idx.Apply(&p); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}

	idx.RemoveOwner("pod-1.default")

	qwen := idx.GetBlockOwners("qwen", []uint64{1, 2})
	if len(qwen) != 1 {
		t.Fatalf("expected only hash 1 to remain for qwen, got %v", qwen)
	}
	if _, ok := qwen[1]["pod-2.default"]; !ok {
		t.Errorf("expected pod-2.default to remain owner of hash 1, got %v", qwen)
	}
	if llama := idx.GetBlockOwners("llama", []uint64{9}); len(llama) != 0 {
		t.Errorf("expected llama blocks of pod-1 to be gone, got %v", llama)
	}
}

func TestKVBlockMemoryIndex_GCStaleEntries(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	fresh := time.Now().Unix()
	stale := time.Now().Add(-25 * time.Hour).Unix()

	for _, payload := range []KVEventPayload{
		{
			PodIdentifier: "pod-fresh.default",
			ModelName:     "qwen",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: fresh}},
		},
		{
			PodIdentifier: "pod-stale.default",
			ModelName:     "qwen",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: stale}},
		},
	} {
		p := payload
		if err := idx.Apply(&p); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}

	idx.gcStaleEntries(kvCacheFieldFreshDuration)

	got := idx.GetBlockOwners("qwen", []uint64{1, 2})
	if len(got) != 1 {
		t.Fatalf("expected only hash 1 to survive GC, got %v", got)
	}
	if _, ok := got[1]["pod-fresh.default"]; !ok {
		t.Errorf("expected fresh owner to survive GC, got %v", got)
	}
	if _, ok := got[1]["pod-stale.default"]; ok {
		t.Errorf("expected stale owner to be removed, got %v", got)
	}
}

// TestKVBlockMemoryIndex_GCRemovesEmptyParentEntries guards against leaked
// empty owner/model maps: with repeated pod replacement every historical pod
// would otherwise leave an empty reverse-index entry behind forever.
func TestKVBlockMemoryIndex_GCRemovesEmptyParentEntries(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	stale := time.Now().Add(-25 * time.Hour).Unix()

	for _, payload := range []KVEventPayload{
		{
			PodIdentifier: "pod-1.default",
			ModelName:     "qwen",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: stale}},
		},
		{
			PodIdentifier: "pod-2.default",
			ModelName:     "qwen",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{2, 3}, Timestamp: stale}},
		},
		{
			PodIdentifier: "pod-3.default",
			ModelName:     "llama",
			Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{9}, Timestamp: stale}},
		},
	} {
		p := payload
		if err := idx.Apply(&p); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}

	idx.gcStaleEntries(kvCacheFieldFreshDuration)

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.blocks) != 0 {
		t.Errorf("expected forward index to be empty after GC expired everything, got %v", idx.blocks)
	}
	if len(idx.ownerBlocks) != 0 {
		t.Errorf("expected reverse index to be empty after GC expired everything, got %v", idx.ownerBlocks)
	}
	if len(idx.ownerTotals) != 0 {
		t.Errorf("expected per-owner totals to be empty after GC expired everything, got %v", idx.ownerTotals)
	}
}

func TestKVCacheAware_QueryMemoryForBlocks(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	now := time.Now().Unix()
	payload := KVEventPayload{
		PodIdentifier: "pod-1.default",
		ModelName:     "qwen",
		Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{111}, Timestamp: now}},
	}
	if err := idx.Apply(&payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}

	plugin := &KVCacheAware{
		indexMode:   kvCacheIndexModeMemory,
		memoryIndex: idx,
	}

	result, err := plugin.queryBlocks([]uint64{111, 222}, "qwen", nil)
	if err != nil {
		t.Fatalf("queryBlocks returned error: %v", err)
	}
	if len(result) != 1 || len(result[111]) != 1 || result[111][0] != "pod-1.default" {
		t.Fatalf("unexpected result: %v", result)
	}

	// Ownership written before the owning pod's containers started is dropped.
	result, err = plugin.queryBlocks([]uint64{111}, "qwen",
		map[string]int64{"pod-1.default": now + 10})
	if err != nil {
		t.Fatalf("queryBlocks returned error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected stale ownership to be dropped, got %v", result)
	}
}

func TestKVBlockMemoryIndex_ClampsStoreTime(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	before := time.Now().Unix()
	past := time.Now().Add(-time.Hour).Unix()
	future := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()

	payload := KVEventPayload{
		PodIdentifier: "pod-1.default",
		ModelName:     "qwen",
		Events: []KVEvent{
			{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: future},
			{Type: kvEventStored, BlockHashes: []uint64{2, 3, 4}, Timestamps: []int64{past, future, 0}},
			{Type: kvEventStored, BlockHashes: []uint64{5}},
		},
	}
	if err := idx.Apply(&payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}
	after := time.Now().Unix()

	got := idx.GetBlockOwners("qwen", []uint64{1, 2, 3, 4, 5})
	if want := strconv.FormatInt(past, 10); got[2]["pod-1.default"] != want {
		t.Errorf("hash 2: timestamp = %q, want past timestamp %q to be preserved", got[2]["pod-1.default"], want)
	}
	for _, hash := range []uint64{1, 3, 4, 5} {
		ts, err := strconv.ParseInt(got[hash]["pod-1.default"], 10, 64)
		if err != nil {
			t.Fatalf("hash %d: invalid timestamp %q", hash, got[hash]["pod-1.default"])
		}
		if ts < before || ts > after {
			t.Errorf("hash %d: timestamp = %d, want receive time in [%d, %d]", hash, ts, before, after)
		}
	}

	// A clamped future timestamp must still expire.
	idx.mu.Lock()
	for _, owners := range idx.blocks["qwen"] {
		owners["pod-1.default"] = time.Now().Add(-25 * time.Hour).Unix()
	}
	idx.mu.Unlock()
	idx.gcStaleEntries(kvCacheFieldFreshDuration)
	if got := idx.GetBlockOwners("qwen", []uint64{1, 2, 3, 4, 5}); len(got) != 0 {
		t.Errorf("expected all entries to be garbage collected, got %v", got)
	}
}

func TestKVBlockMemoryIndex_MaxBlocksPerOwner(t *testing.T) {
	idx := NewKVBlockMemoryIndex(3)
	now := time.Now().Unix()
	apply := func(owner, model string, events ...KVEvent) {
		t.Helper()
		p := KVEventPayload{PodIdentifier: owner, ModelName: model, Events: events}
		if err := idx.Apply(&p); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}
	total := func(owner string) int {
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		return idx.ownerTotals[owner]
	}
	stored := func(hashes ...uint64) KVEvent {
		return KVEvent{Type: kvEventStored, BlockHashes: hashes, Timestamp: now}
	}

	// The cap spans all models of an owner.
	apply("pod-1.default", "qwen", stored(1, 2))
	apply("pod-1.default", "llama", stored(3, 4))
	if got := idx.GetBlockOwners("llama", []uint64{3, 4}); len(got) != 1 || got[3] == nil {
		t.Fatalf("expected only hash 3 of llama to be stored, got %v", got)
	}
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals = %d, want 3", got)
	}

	// Re-storing held blocks does not consume capacity.
	apply("pod-1.default", "qwen", stored(1, 2))
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals after re-store = %d, want 3", got)
	}

	// Other owners have their own budget.
	apply("pod-2.default", "qwen", stored(1, 2, 3))
	if got := idx.GetBlockOwners("qwen", []uint64{3}); got[3]["pod-2.default"] == "" {
		t.Fatalf("expected pod-2 to store hash 3, got %v", got)
	}

	// Removing a block that is not held does not release capacity.
	apply("pod-1.default", "qwen", KVEvent{Type: kvEventRemoved, BlockHashes: []uint64{99}})
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals after removing unheld block = %d, want 3", got)
	}

	// Removal frees capacity.
	apply("pod-1.default", "qwen", KVEvent{Type: kvEventRemoved, BlockHashes: []uint64{1}})
	apply("pod-1.default", "llama", stored(4))
	if got := idx.GetBlockOwners("llama", []uint64{4}); got[4]["pod-1.default"] == "" {
		t.Fatalf("expected hash 4 to be stored after freeing capacity, got %v", got)
	}

	// A snapshot replaces the model's blocks, releasing their capacity first.
	apply("pod-1.default", "qwen", KVEvent{Type: kvEventSnapshot, BlockHashes: []uint64{7}, Timestamp: now})
	got := idx.GetBlockOwners("qwen", []uint64{2, 7})
	if _, ok := got[2]["pod-1.default"]; ok || got[7]["pod-1.default"] == "" {
		t.Fatalf("expected snapshot to leave only hash 7 for pod-1, got %v", got)
	}
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals after snapshot = %d, want 3", got)
	}

	apply("pod-1.default", "llama", KVEvent{Type: kvEventCleared})
	if got := total("pod-1.default"); got != 1 {
		t.Fatalf("ownerTotals after clear = %d, want 1", got)
	}

	idx.RemoveOwner("pod-1.default")
	idx.mu.RLock()
	_, ok := idx.ownerTotals["pod-1.default"]
	idx.mu.RUnlock()
	if ok {
		t.Fatalf("expected ownerTotals entry to be removed with the owner")
	}
}

func TestKVBlockMemoryIndex_FullOwnerLeavesNoEmptyEntries(t *testing.T) {
	idx := NewKVBlockMemoryIndex(1)
	p := KVEventPayload{
		PodIdentifier: "pod-1.default",
		ModelName:     "qwen",
		Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}}},
	}
	if err := idx.Apply(&p); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}
	p = KVEventPayload{
		PodIdentifier: "pod-1.default",
		ModelName:     "llama",
		Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{2}}},
	}
	if err := idx.Apply(&p); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if _, ok := idx.blocks["llama"]; ok {
		t.Errorf("expected no forward index entry for a model whose blocks were all dropped")
	}
	if _, ok := idx.ownerBlocks[ownerModelKey{owner: "pod-1.default", model: "llama"}]; ok {
		t.Errorf("expected no reverse index entry for a model whose blocks were all dropped")
	}
}

type fakePodLister struct {
	pods      map[types.NamespacedName]*datastore.PodInfo
	listCalls int
}

func (f *fakePodLister) GetAllPods() map[types.NamespacedName]*datastore.PodInfo {
	f.listCalls++
	pods := make(map[types.NamespacedName]*datastore.PodInfo, len(f.pods))
	for name, info := range f.pods {
		pods[name] = info
	}
	return pods
}

func (f *fakePodLister) GetPodInfo(name types.NamespacedName) *datastore.PodInfo {
	return f.pods[name]
}

func (f *fakePodLister) add(namespace, name string, ips ...string) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if len(ips) > 0 {
		pod.Status.PodIP = ips[0]
	}
	for _, ip := range ips {
		pod.Status.PodIPs = append(pod.Status.PodIPs, corev1.PodIP{IP: ip})
	}
	f.pods[types.NamespacedName{Namespace: namespace, Name: name}] = datastore.NewPodInfo(pod, "vLLM")
}

func TestKVEventSourceResolver_ResolveOwner(t *testing.T) {
	pods := &fakePodLister{pods: map[types.NamespacedName]*datastore.PodInfo{}}
	pods.add("default", "pod-a", "10.0.0.1")
	pods.add("ns-b", "pod-b", "10.0.0.2", "fd00::2")
	pods.add("default", "host-1", "192.168.0.10")
	pods.add("default", "host-2", "192.168.0.10")
	pods.add("default", "pending")
	resolver := newKVEventSourceResolver(pods)

	tests := []struct {
		name       string
		remoteAddr string
		wantOwner  string
		wantErr    bool
	}{
		{name: "ipv4 pod", remoteAddr: "10.0.0.1:41000", wantOwner: "pod-a.default"},
		{name: "secondary ipv6 pod IP", remoteAddr: "[fd00::2]:41000", wantOwner: "pod-b.ns-b"},
		{name: "ipv4-mapped ipv6 source", remoteAddr: "[::ffff:10.0.0.2]:41000", wantOwner: "pod-b.ns-b"},
		{name: "unknown source", remoteAddr: "10.9.9.9:41000", wantErr: true},
		{name: "IP shared by multiple pods", remoteAddr: "192.168.0.10:41000", wantErr: true},
		{name: "missing port", remoteAddr: "10.0.0.1", wantErr: true},
		{name: "not an IP", remoteAddr: "pod-a:41000", wantErr: true},
		{name: "empty", remoteAddr: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, err := resolver.resolveOwner(tt.remoteAddr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveOwner(%q) error = %v, wantErr %v", tt.remoteAddr, err, tt.wantErr)
			}
			if owner != tt.wantOwner {
				t.Fatalf("resolveOwner(%q) = %q, want %q", tt.remoteAddr, owner, tt.wantOwner)
			}
		})
	}
}

func TestKVEventSourceResolver_TracksPodChanges(t *testing.T) {
	pods := &fakePodLister{pods: map[types.NamespacedName]*datastore.PodInfo{}}
	pods.add("default", "pod-a", "10.0.0.1")
	resolver := newKVEventSourceResolver(pods)

	if owner, err := resolver.resolveOwner("10.0.0.1:1"); err != nil || owner != "pod-a.default" {
		t.Fatalf("resolveOwner() = %q, %v; want pod-a.default", owner, err)
	}

	// A deleted pod is rejected immediately, even while the cache is fresh.
	delete(pods.pods, types.NamespacedName{Namespace: "default", Name: "pod-a"})
	if owner, err := resolver.resolveOwner("10.0.0.1:1"); err == nil {
		t.Fatalf("expected deleted pod to be rejected, got %q", owner)
	}

	// Misses rebuild the cache at most once per kvEventSourceMissRebuildInterval.
	listCalls := pods.listCalls
	for range 3 {
		if _, err := resolver.resolveOwner("10.0.0.3:1"); err == nil {
			t.Fatalf("expected unknown source to be rejected")
		}
	}
	if pods.listCalls != listCalls {
		t.Fatalf("expected no rebuild within the miss rebuild interval, got %d extra", pods.listCalls-listCalls)
	}

	// A pod that reuses the IP is picked up once a rebuild is allowed again.
	pods.add("default", "pod-c", "10.0.0.1")
	resolver.mu.Lock()
	resolver.lastRebuild = time.Now().Add(-kvEventSourceMissRebuildInterval)
	resolver.mu.Unlock()
	if owner, err := resolver.resolveOwner("10.0.0.1:1"); err != nil || owner != "pod-c.default" {
		t.Fatalf("resolveOwner() = %q, %v; want pod-c.default", owner, err)
	}
}

func TestKVEventsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	idx := NewKVBlockMemoryIndex(0)
	owners := map[string]string{"10.0.0.1:41000": "pod-1.default"}
	resolveOwner := func(remoteAddr string) (string, error) {
		if owner, ok := owners[remoteAddr]; ok {
			return owner, nil
		}
		return "", fmt.Errorf("unknown source %s", remoteAddr)
	}
	engine := gin.New()
	engine.POST("/kvcache/events", kvEventsHandler(idx, resolveOwner))

	tests := []struct {
		name       string
		remoteAddr string
		forwarded  string
		body       any
		rawBody    string
		wantStatus int
	}{
		{
			name:       "valid payload",
			remoteAddr: "10.0.0.1:41000",
			body: KVEventPayload{
				PodIdentifier: "pod-1.default",
				ModelName:     "qwen",
				Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{42}, Timestamp: time.Now().Unix()}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown source",
			remoteAddr: "10.0.0.9:41000",
			forwarded:  "10.0.0.1",
			body: KVEventPayload{
				PodIdentifier: "pod-1.default",
				ModelName:     "qwen",
				Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{43}}},
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "pod identifier of another pod",
			remoteAddr: "10.0.0.1:41000",
			body: KVEventPayload{
				PodIdentifier: "pod-2.default",
				ModelName:     "qwen",
				Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{44}}},
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "invalid json",
			remoteAddr: "10.0.0.1:41000",
			rawBody:    "{not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing model name",
			remoteAddr: "10.0.0.1:41000",
			body: KVEventPayload{
				PodIdentifier: "pod-1.default",
				Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{45}}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			if tt.rawBody != "" {
				body = []byte(tt.rawBody)
			} else {
				var err error
				body, err = json.Marshal(tt.body)
				if err != nil {
					t.Fatalf("failed to marshal body: %v", err)
				}
			}
			req := httptest.NewRequest(http.MethodPost, "/kvcache/events", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.RemoteAddr = tt.remoteAddr
			if tt.forwarded != "" {
				req.Header.Set("X-Forwarded-For", tt.forwarded)
				req.Header.Set("X-Real-IP", tt.forwarded)
			}
			rec := httptest.NewRecorder()
			engine.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}

	got := idx.GetBlockOwners("qwen", []uint64{42, 43, 44, 45})
	if len(got) != 1 || got[42]["pod-1.default"] == "" {
		t.Fatalf("expected only the block of the valid payload to be stored, got %v", got)
	}
}

func TestKVCacheAware_OnPodDeleted(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	p := KVEventPayload{
		PodIdentifier: "pod-1.default",
		ModelName:     "qwen",
		Events:        []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}}},
	}
	if err := idx.Apply(&p); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}
	plugin := &KVCacheAware{indexMode: kvCacheIndexModeMemory, memoryIndex: idx}
	pod := types.NamespacedName{Namespace: "default", Name: "pod-1"}

	plugin.onPodDeleted(datastore.EventData{EventType: datastore.EventUpdate, Pod: pod})
	if got := idx.GetBlockOwners("qwen", []uint64{1}); len(got) != 1 {
		t.Fatalf("expected blocks to survive a pod update, got %v", got)
	}

	plugin.onPodDeleted(datastore.EventData{EventType: datastore.EventDelete, Pod: pod})
	if got := idx.GetBlockOwners("qwen", []uint64{1}); len(got) != 0 {
		t.Fatalf("expected blocks of the deleted pod to be removed, got %v", got)
	}
}
