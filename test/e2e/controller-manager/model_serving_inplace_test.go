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

package controller_manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	workload "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/inplace"
	"github.com/volcano-sh/kthena/test/e2e/utils"
)

const inPlaceUpdateTimeout = 3 * time.Minute

// podSnapshot records what identifies a Pod across an update. A Pod that is
// deleted and recreated keeps its name but gets a new UID.
type podSnapshot struct {
	uid  types.UID
	role string
}

func newInPlaceModelServing(name string, strategy workload.RolloutStrategyType) *workload.ModelServing {
	prefill := createRole("prefill", 2, 0)
	decode := createRole("decode", 1, 0)
	rollout := &workload.RolloutStrategy{
		Type:         strategy,
		UpdateMethod: workload.InPlaceUpdateMethod,
	}
	if strategy == workload.RoleRollingUpdate {
		prefill.MaxUnavailable = ptr.To(intstr.FromInt(1))
		decode.MaxUnavailable = ptr.To(intstr.FromInt(1))
	} else {
		rollout.RollingUpdateConfiguration = &workload.RollingUpdateConfiguration{MaxUnavailable: ptr.To(intstr.FromInt(1))}
	}
	ms := createBasicModelServing(name, 2, 0, prefill, decode)
	ms.Spec.RecoveryPolicy = workload.RoleRecreate
	ms.Spec.RolloutStrategy = rollout
	return ms
}

func snapshotPods(t *testing.T, ctx context.Context, kubeClient *kubernetes.Clientset, msName string) map[string]podSnapshot {
	t.Helper()
	pods, err := kubeClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{LabelSelector: modelServingLabelSelector(msName)})
	require.NoError(t, err)
	snapshot := make(map[string]podSnapshot, len(pods.Items))
	for _, pod := range pods.Items {
		snapshot[pod.Name] = podSnapshot{uid: pod.UID, role: pod.Labels[workload.RoleLabelKey]}
	}
	require.NotEmpty(t, snapshot)
	return snapshot
}

func listPodsByName(t *testing.T, ctx context.Context, kubeClient *kubernetes.Clientset, msName string) map[string]corev1.Pod {
	t.Helper()
	pods, err := kubeClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{LabelSelector: modelServingLabelSelector(msName)})
	require.NoError(t, err)
	byName := make(map[string]corev1.Pod, len(pods.Items))
	for _, pod := range pods.Items {
		byName[pod.Name] = pod
	}
	return byName
}

func isPodReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// waitForInPlaceUpdated waits until every Pod of the roles in updatedRoles runs
// image, is Ready, and has finished its in-place update (state done, no longer in progress).
func waitForInPlaceUpdated(t *testing.T, ctx context.Context, kubeClient *kubernetes.Clientset, msName, image string, updatedRoles ...string) {
	t.Helper()
	updated := map[string]bool{}
	for _, role := range updatedRoles {
		updated[role] = true
	}
	require.Eventually(t, func() bool {
		pods, err := kubeClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{LabelSelector: modelServingLabelSelector(msName)})
		if err != nil || len(pods.Items) == 0 {
			return false
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if !updated[pod.Labels[workload.RoleLabelKey]] {
				continue
			}
			state := inplace.StateOf(pod)
			if !isPodReady(*pod) || pod.Spec.Containers[0].Image != image || state == nil || !state.Done || inplace.InProgress(pod) {
				return false
			}
		}
		return true
	}, inPlaceUpdateTimeout, 2*time.Second, "pods of roles %v should be updated in place to %s", updatedRoles, image)
}

// assertNoPodRecreated fails when a Pod from before the update is gone or has a new UID.
func assertNoPodRecreated(t *testing.T, ctx context.Context, kubeClient *kubernetes.Clientset, msName string, before map[string]podSnapshot) {
	t.Helper()
	after := listPodsByName(t, ctx, kubeClient, msName)
	assert.Len(t, after, len(before), "the number of pods must not change")
	for name, snap := range before {
		pod, ok := after[name]
		if !assert.True(t, ok, "pod %s was deleted during the update", name) {
			continue
		}
		assert.Equal(t, snap.uid, pod.UID, "pod %s was recreated instead of updated in place", name)
	}
}

// assertContainerRestartedOnce checks that kubelet restarted the container for the new image, and only once.
func assertContainerRestartedOnce(t *testing.T, pod corev1.Pod) {
	t.Helper()
	require.NotEmpty(t, pod.Status.ContainerStatuses)
	assert.Equal(t, int32(1), pod.Status.ContainerStatuses[0].RestartCount, "pod %s should have restarted exactly once", pod.Name)
}

func assertRevisionsConverged(t *testing.T, ctx context.Context, kubeClient *kubernetes.Clientset, msName string, roles ...string) {
	t.Helper()
	wanted := map[string]bool{}
	for _, role := range roles {
		wanted[role] = true
	}
	revisions := map[string]bool{}
	for _, pod := range listPodsByName(t, ctx, kubeClient, msName) {
		if wanted[pod.Labels[workload.RoleLabelKey]] {
			revisions[pod.Labels[workload.RevisionLabelKey]] = true
		}
	}
	assert.Len(t, revisions, 1, "pods must not carry mixed revisions after the update, got %v", revisions)
}

func setImages(image string, roles ...string) func(*workload.ModelServing) {
	return func(ms *workload.ModelServing) {
		for i := range ms.Spec.Template.Roles {
			role := &ms.Spec.Template.Roles[i]
			if len(roles) > 0 && !contains(roles, role.Name) {
				continue
			}
			role.EntryTemplate.Spec.Containers[0].Image = image
		}
	}
}

func contains(items []string, item string) bool {
	for _, i := range items {
		if i == item {
			return true
		}
	}
	return false
}

func createInPlaceModelServing(t *testing.T, ctx context.Context, kthenaClient *clientset.Clientset, ms *workload.ModelServing) {
	t.Helper()
	waitForWebhookReady(t, ctx, kthenaClient, testNamespace)
	createAndWaitForModelServing(t, ctx, kthenaClient, ms)
}

// TestModelServingInPlaceUpdateServingGroup verifies that with ServingGroupRollingUpdate an
// image-only change patches the existing Pods of every ServingGroup instead of recreating them.
func TestModelServingInPlaceUpdateServingGroup(t *testing.T) {
	ctx, kthenaClient, kubeClient := setupControllerManagerE2ETest(t)
	ms := newInPlaceModelServing("test-inplace-servinggroup", workload.ServingGroupRollingUpdate)
	createInPlaceModelServing(t, ctx, kthenaClient, ms)

	before := snapshotPods(t, ctx, kubeClient, ms.Name)
	t.Logf("Updating image of all roles in place, %d pods before update", len(before))
	updateModelServingWithRetry(t, ctx, kthenaClient, ms.Name, setImages(nginxAlpineImage))

	waitForInPlaceUpdated(t, ctx, kubeClient, ms.Name, nginxAlpineImage, "prefill", "decode")
	utilsWaitReady(t, ctx, kthenaClient, ms.Name)

	assertNoPodRecreated(t, ctx, kubeClient, ms.Name, before)
	for _, pod := range listPodsByName(t, ctx, kubeClient, ms.Name) {
		assert.Equal(t, nginxAlpineImage, pod.Spec.Containers[0].Image)
		assertContainerRestartedOnce(t, pod)
	}
	assertRevisionsConverged(t, ctx, kubeClient, ms.Name, "prefill", "decode")
}

// TestModelServingInPlaceUpdateRole verifies that with RoleRollingUpdate only the Pods of the
// Role whose image changed are restarted, and that they are patched rather than recreated.
func TestModelServingInPlaceUpdateRole(t *testing.T) {
	ctx, kthenaClient, kubeClient := setupControllerManagerE2ETest(t)
	ms := newInPlaceModelServing("test-inplace-role", workload.RoleRollingUpdate)
	createInPlaceModelServing(t, ctx, kthenaClient, ms)

	before := snapshotPods(t, ctx, kubeClient, ms.Name)
	t.Log("Updating image of the prefill role in place")
	updateModelServingWithRetry(t, ctx, kthenaClient, ms.Name, setImages(nginxAlpineImage, "prefill"))

	waitForInPlaceUpdated(t, ctx, kubeClient, ms.Name, nginxAlpineImage, "prefill")
	utilsWaitReady(t, ctx, kthenaClient, ms.Name)

	assertNoPodRecreated(t, ctx, kubeClient, ms.Name, before)
	for name, pod := range listPodsByName(t, ctx, kubeClient, ms.Name) {
		switch before[name].role {
		case "prefill":
			assert.Equal(t, nginxAlpineImage, pod.Spec.Containers[0].Image)
			assertContainerRestartedOnce(t, pod)
		default:
			assert.Equal(t, nginxImage, pod.Spec.Containers[0].Image, "unchanged role must keep its image")
			assert.Equal(t, int32(0), pod.Status.ContainerStatuses[0].RestartCount, "unchanged role %s must not restart", name)
			assert.Nil(t, inplace.StateOf(&pod), "unchanged role %s must not be touched", name)
		}
	}
	assertRevisionsConverged(t, ctx, kubeClient, ms.Name, "prefill")
}

// TestModelServingInPlaceUpdateFallsBackToRecreate verifies that a change beyond container
// images is not applied in place: the Pods are deleted and recreated, as with Recreate.
func TestModelServingInPlaceUpdateFallsBackToRecreate(t *testing.T) {
	tests := []struct {
		name     string
		strategy workload.RolloutStrategyType
	}{
		{name: "ServingGroupRollingUpdate", strategy: workload.ServingGroupRollingUpdate},
		{name: "RoleRollingUpdate", strategy: workload.RoleRollingUpdate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, kthenaClient, kubeClient := setupControllerManagerE2ETest(t)
			ms := newInPlaceModelServing("test-inplace-fallback-"+map[workload.RolloutStrategyType]string{
				workload.ServingGroupRollingUpdate: "sg",
				workload.RoleRollingUpdate:         "role",
			}[tt.strategy], tt.strategy)
			createInPlaceModelServing(t, ctx, kthenaClient, ms)

			before := snapshotPods(t, ctx, kubeClient, ms.Name)
			updateModelServingWithRetry(t, ctx, kthenaClient, ms.Name, func(ms *workload.ModelServing) {
				container := &ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0]
				container.Image = nginxAlpineImage
				container.Env = append(container.Env, corev1.EnvVar{Name: "INPLACE_E2E", Value: "changed"})
			})

			updatedRole := ms.Spec.Template.Roles[0].Name
			require.Eventually(t, func() bool {
				for name, pod := range listPodsByName(t, ctx, kubeClient, ms.Name) {
					if before[name].role != updatedRole {
						continue
					}
					if pod.UID == before[name].uid || !isPodReady(pod) || pod.Spec.Containers[0].Image != nginxAlpineImage {
						return false
					}
				}
				return true
			}, inPlaceUpdateTimeout, 2*time.Second, "pods of role %s should be recreated", updatedRole)
			utilsWaitReady(t, ctx, kthenaClient, ms.Name)

			for name, pod := range listPodsByName(t, ctx, kubeClient, ms.Name) {
				if before[name].role == updatedRole {
					assert.NotEqual(t, before[name].uid, pod.UID, "pod %s should have been recreated", name)
					assert.Nil(t, inplace.StateOf(&pod), "a recreated pod has no in-place state")
				}
			}
		})
	}
}

func utilsWaitReady(t *testing.T, ctx context.Context, kthenaClient *clientset.Clientset, msName string) {
	t.Helper()
	utils.WaitForModelServingReady(t, ctx, kthenaClient, testNamespace, msName)
}
