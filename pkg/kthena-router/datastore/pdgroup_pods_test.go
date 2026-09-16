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

package datastore

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	aiv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
)

func TestPDGroup(t *testing.T) {
	store := New()

	// Create a ModelServer with PDGroup configuration
	modelServer := &aiv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "default",
		},
		Spec: aiv1alpha1.ModelServerSpec{
			WorkloadSelector: &aiv1alpha1.WorkloadSelector{
				PDGroup: &aiv1alpha1.PDGroup{
					GroupKey: "pd-group",
					DecodeLabels: map[string]string{
						"role": "decode",
					},
					PrefillLabels: map[string]string{
						"role": "prefill",
					},
				},
			},
		},
	}

	modelServerName := types.NamespacedName{
		Namespace: "default",
		Name:      "test-model",
	}

	// Add the ModelServer to store
	err := store.AddOrUpdateModelServer(modelServer, nil)
	if err != nil {
		t.Fatalf("Failed to add model server: %v", err)
	}

	// Create test pods
	decodePod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-a",
				"role":     "decode",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}

	decodePod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-pod-2",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-b",
				"role":     "decode",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
		},
	}

	prefillPod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prefill-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-a",
				"role":     "prefill",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.3",
		},
	}

	prefillPod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prefill-pod-2",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-b",
				"role":     "prefill",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.4",
		},
	}

	// Add pods to store
	err = store.AddOrUpdatePod(decodePod1, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add decode pod 1: %v", err)
	}

	err = store.AddOrUpdatePod(decodePod2, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add decode pod 2: %v", err)
	}

	err = store.AddOrUpdatePod(prefillPod1, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add prefill pod 1: %v", err)
	}

	err = store.AddOrUpdatePod(prefillPod2, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add prefill pod 2: %v", err)
	}

	// Test GetDecodePods
	decodePods, err := store.GetDecodePods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get decode pods: %v", err)
	}

	if len(decodePods) != 2 {
		t.Errorf("Expected 2 decode pods, got %d", len(decodePods))
	}

	// Test GetPrefillPods
	prefillPods, err := store.GetPrefillPods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get prefill pods: %v", err)
	}

	if len(prefillPods) != 2 {
		t.Errorf("Expected 2 prefill pods, got %d", len(prefillPods))
	}

	// Test GetPrefillPodsForDecodeGroup
	decodePod1Name := types.NamespacedName{
		Namespace: "default",
		Name:      "decode-pod-1",
	}

	matchingPrefillPods, err := store.GetPrefillPodsForDecodeGroup(modelServerName, decodePod1Name)
	if err != nil {
		t.Fatalf("Failed to get prefill pods for decode group: %v", err)
	}

	if len(matchingPrefillPods) != 1 {
		t.Errorf("Expected 1 prefill pod for decode group, got %d", len(matchingPrefillPods))
	}

	if len(matchingPrefillPods) > 0 && matchingPrefillPods[0].Pod.Name != prefillPod1.Name {
		t.Errorf("Expected prefill-pod-1, got %s", matchingPrefillPods[0].Pod.Name)
	}

	// Test with decode-pod-2 (group-b)
	decodePod2Name := types.NamespacedName{
		Namespace: "default",
		Name:      "decode-pod-2",
	}

	matchingPrefillPods2, err := store.GetPrefillPodsForDecodeGroup(modelServerName, decodePod2Name)
	if err != nil {
		t.Fatalf("Failed to get prefill pods for decode group: %v", err)
	}

	if len(matchingPrefillPods2) != 1 {
		t.Errorf("Expected 1 prefill pod for decode group, got %d", len(matchingPrefillPods2))
	}

	if len(matchingPrefillPods2) > 0 && matchingPrefillPods2[0].Pod.Name != "prefill-pod-2" {
		t.Errorf("Expected prefill-pod-2, got %s", matchingPrefillPods2[0].Pod.Name)
	}
}

func TestPDGroupPodRemoval(t *testing.T) {
	store := New()

	// Create a ModelServer with PDGroup configuration
	modelServer := &aiv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "default",
		},
		Spec: aiv1alpha1.ModelServerSpec{
			WorkloadSelector: &aiv1alpha1.WorkloadSelector{
				PDGroup: &aiv1alpha1.PDGroup{
					GroupKey: "pd-group",
					DecodeLabels: map[string]string{
						"role": "decode",
					},
					PrefillLabels: map[string]string{
						"role": "prefill",
					},
				},
			},
		},
	}

	modelServerName := types.NamespacedName{
		Namespace: "default",
		Name:      "test-model",
	}

	// Add the ModelServer to store
	err := store.AddOrUpdateModelServer(modelServer, nil)
	if err != nil {
		t.Fatalf("Failed to add model server: %v", err)
	}

	// Create and add a decode pod
	decodePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-pod",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-a",
				"role":     "decode",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}

	err = store.AddOrUpdatePod(decodePod, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add decode pod: %v", err)
	}

	// Verify pod is categorized
	decodePods, err := store.GetDecodePods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get decode pods: %v", err)
	}

	if len(decodePods) != 1 {
		t.Errorf("Expected 1 decode pod, got %d", len(decodePods))
	}

	// Remove the pod
	podName := types.NamespacedName{
		Namespace: "default",
		Name:      "decode-pod",
	}

	err = store.DeletePod(podName)
	if err != nil {
		t.Fatalf("Failed to delete pod: %v", err)
	}

	// Verify pod is removed from categorization
	decodePods, err = store.GetDecodePods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get decode pods after deletion: %v", err)
	}

	if len(decodePods) != 0 {
		t.Errorf("Expected 0 decode pods after deletion, got %d", len(decodePods))
	}
}

// Label updates must replace a Pod's PD classification while it still matches the ModelServer.
func TestPDGroupPodLabelUpdate(t *testing.T) {
	// The changing Pod starts in group-a with oldRole; group and role are its new labels.
	// Empty values remove the corresponding label. decode and prefill count only the
	// changing Pod, excluding the unchanged prefill peer added in every case.
	tests := []struct {
		name    string
		oldRole string
		group   string
		role    string
		decode  int
		prefill int
	}{
		{"move decode group", "decode", "group-b", "decode", 1, 0},
		{"move prefill group", "prefill", "group-b", "prefill", 0, 1},
		{"prefill to decode", "prefill", "group-a", "decode", 1, 0},
		{"decode to prefill", "decode", "group-a", "prefill", 0, 1},
		{"remove group", "decode", "", "decode", 0, 0},
		{"remove role", "prefill", "group-a", "", 0, 0},
		{"unchanged labels", "decode", "group-a", "decode", 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New().(*store)
			ms := newTestModelServerWithPDGroup("test-model", "default")
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			require.NoError(t, s.AddOrUpdateModelServer(ms, nil))
			pod := newTestPod("changing", "default", map[string]string{
				"app": ms.Name, "pd-group": "group-a", "role": tt.oldRole,
			})
			podName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
			// Keep a peer in the original group to check that reclassification preserves other Pods.
			peer := newTestPod("peer", "default", map[string]string{
				"app": ms.Name, "pd-group": "group-a", "role": "prefill",
			})
			require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
			require.NoError(t, s.AddOrUpdatePod(peer, []*aiv1alpha1.ModelServer{ms}))
			oldInfo := s.GetPodInfo(podName)

			// Update a copy so the datastore can use the stored labels to remove the old classification.
			updated := pod.DeepCopy()
			updated.Labels["pd-group"] = tt.group
			updated.Labels["role"] = tt.role
			if tt.group == "" {
				delete(updated.Labels, "pd-group")
			}
			if tt.role == "" {
				delete(updated.Labels, "role")
			}
			require.NoError(t, s.AddOrUpdatePod(updated, []*aiv1alpha1.ModelServer{ms}))
			assert.Same(t, oldInfo, s.GetPodInfo(podName), "updates must preserve PodInfo")
			decode, err := s.GetDecodePods(msName)
			require.NoError(t, err)
			assert.Len(t, decode, tt.decode)
			prefill, err := s.GetPrefillPods(msName)
			require.NoError(t, err)
			assert.Len(t, prefill, tt.prefill+1, "the unrelated prefill peer must remain")
			value, ok := s.modelServer.Load(msName)
			require.True(t, ok)
			msInfo := value.(*modelServer)
			assert.Len(t, msInfo.getPods(), 2, "the ModelServer selector still matches both pods")

			// Deletion uses the latest labels, so leftover membership in a previous group
			// would survive cleanup and keep the group map nonempty.
			require.NoError(t, s.DeletePod(podName))
			require.NoError(t, s.DeletePod(types.NamespacedName{Namespace: peer.Namespace, Name: peer.Name}))
			assert.Empty(t, msInfo.pdGroups, "deleting the pods must leave no stale group")
		})
	}
}

// A status-only update must not hide a healthy Pod from concurrent PD scheduling.
func TestPDGroupConcurrentPodUpdate(t *testing.T) {
	for _, role := range []string{"decode", "prefill"} {
		t.Run(role, func(t *testing.T) {
			s := New()
			ms := newTestModelServerWithPDGroup("test-model", "default")
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			require.NoError(t, s.AddOrUpdateModelServer(ms, nil))
			var updated *corev1.Pod
			for _, podRole := range []string{"decode", "prefill"} {
				pod := newTestPod(podRole, "default", map[string]string{
					"app": ms.Name, "pd-group": "group-a", "role": podRole,
				})
				require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
				if podRole == role {
					updated = pod
				}
			}
			start := make(chan struct{})
			errs := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 10000; i++ {
					pod := updated.DeepCopy()
					pod.ResourceVersion = strconv.Itoa(i + 1)
					if err := s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}); err != nil {
						errs <- err
						return
					}
				}
			}()
			defer wg.Wait()
			close(start)
			for i := 0; i < 10000; i++ {
				decode, err := s.GetDecodePods(msName)
				require.NoError(t, err)
				require.Len(t, decode, 1, "a status-only update must preserve the decode candidate")
				prefill, err := s.GetPrefillPodsForDecodeGroup(msName, decode[0].GetPodNamespacedName())
				require.NoError(t, err)
				require.Len(t, prefill, 1, "a status-only update must preserve the prefill candidate")
			}
			wg.Wait()
			select {
			case err := <-errs:
				require.NoError(t, err)
			default:
			}
		})
	}
}
