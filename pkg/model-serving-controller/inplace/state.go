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

package inplace

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

// State is stored in one Pod annotation. It is written in the same PATCH that
// changes the images, so a restarted controller can always tell an expected
// container restart from a crash, and can resume an unfinished update.
type State struct {
	// Revision is the ModelServing revision the Pod is being updated to.
	Revision string `json:"revision"`
	// StartedAt is used for the progress deadline.
	StartedAt metav1.Time `json:"startedAt"`
	// Baseline holds the restart count of every patched container before the patch.
	Baseline map[string]int32 `json:"baseline,omitempty"`
	// Done is set together with the revision label flip.
	Done bool `json:"done,omitempty"`
}

// StateOf returns the Pod's in-place update state, or nil when there is none.
func StateOf(pod *corev1.Pod) *State {
	raw, ok := pod.Annotations[workloadv1alpha1.InPlaceUpdateStateAnnotationKey]
	if !ok {
		return nil
	}
	state := &State{}
	if err := json.Unmarshal([]byte(raw), state); err != nil {
		return nil
	}
	return state
}

// InProgress reports whether an in-place update was started on the Pod and not yet finished.
func InProgress(pod *corev1.Pod) bool {
	state := StateOf(pod)
	return state != nil && !state.Done
}

// Completed reports whether every patched container has restarted with the new
// image and the Pod is Ready again. Checking restart counts guards against the
// stale Ready condition that remains until kubelet notices the image change.
func Completed(pod *corev1.Pod) bool {
	state := StateOf(pod)
	if state == nil || state.Done || !utils.IsPodRunningAndReady(pod) {
		return false
	}
	restarts := restartCounts(pod)
	for name, base := range state.Baseline {
		if restarts[name] <= base {
			return false
		}
	}
	return true
}

// Updating reports whether the Pod is mid-update: started, but not yet Completed.
func Updating(pod *corev1.Pod) bool {
	return InProgress(pod) && !Completed(pod)
}

// UnexpectedRestart replaces a plain "container restarted" check. Restarts
// caused by an in-place update are expected and must not trigger recovery.
func UnexpectedRestart(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
		return false
	}
	state := StateOf(pod)
	if state != nil && !state.Done {
		return false
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if status.RestartCount > 0 {
			return true
		}
	}
	for name, count := range restartCounts(pod) {
		expected := int32(0)
		if state != nil {
			if base, ok := state.Baseline[name]; ok {
				expected = base + 1
			}
		}
		if count > expected {
			return true
		}
	}
	return false
}

// TargetImages returns the images the Pod's containers should run in ms.
func TargetImages(ms *workloadv1alpha1.ModelServing, pod *corev1.Pod) map[string]string {
	role := findRole(ms, pod.Labels[workloadv1alpha1.RoleLabelKey])
	if role == nil {
		return nil
	}
	template := &role.EntryTemplate
	if pod.Labels[workloadv1alpha1.EntryLabelKey] != utils.Entry && role.WorkerTemplate != nil {
		template = role.WorkerTemplate
	}
	images := make(map[string]string, len(template.Spec.Containers))
	for _, container := range template.Spec.Containers {
		images[container.Name] = container.Image
	}
	return images
}

func restartCounts(pod *corev1.Pod) map[string]int32 {
	counts := make(map[string]int32, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		counts[status.Name] = status.RestartCount
	}
	return counts
}

// Start patches the images of pod and records the update state atomically.
// It is a no-op when the Pod is already being updated to revision. The
// resourceVersion precondition makes a concurrent double start conflict.
func Start(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, images map[string]string, revision string, now time.Time) (bool, error) {
	if state := StateOf(pod); state != nil && !state.Done && state.Revision == revision {
		return false, nil
	}
	state := State{Revision: revision, StartedAt: metav1.NewTime(now), Baseline: map[string]int32{}}
	restarts := restartCounts(pod)
	containers := make([]map[string]string, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		image, ok := images[container.Name]
		if !ok || image == container.Image {
			continue
		}
		state.Baseline[container.Name] = restarts[container.Name]
		containers = append(containers, map[string]string{"name": container.Name, "image": image})
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return false, err
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"resourceVersion": pod.ResourceVersion,
			"annotations":     map[string]string{workloadv1alpha1.InPlaceUpdateStateAnnotationKey: string(raw)},
		},
	}
	if len(containers) > 0 {
		patch["spec"] = map[string]interface{}{"containers": containers}
	}
	return true, applyPatch(ctx, client, pod, patch)
}

// Complete flips the revision label (and role template hash when roleHash is
// non-empty) and marks the state done. It must only run once every Pod of the
// update unit is Completed, so a unit never carries mixed revisions.
func Complete(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, revision, roleHash string) error {
	state := StateOf(pod)
	if state == nil {
		state = &State{Revision: revision, StartedAt: metav1.Now()}
	}
	state.Done = true
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	podLabels := map[string]string{workloadv1alpha1.RevisionLabelKey: revision}
	if roleHash != "" {
		podLabels[workloadv1alpha1.RoleTemplateHashLabelKey] = roleHash
	}
	return applyPatch(ctx, client, pod, map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels":      podLabels,
			"annotations": map[string]string{workloadv1alpha1.InPlaceUpdateStateAnnotationKey: string(raw)},
		},
	})
}

func applyPatch(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, patch map[string]interface{}) error {
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patch pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}
