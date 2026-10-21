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
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/ptr"

	workload "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	controllerutils "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	"github.com/volcano-sh/kthena/test/e2e/utils"
)

// TestModelServingRestartGracePeriodSeconds triggers a real container restart,
// rather than deleting a Pod (which bypasses the restart grace period).
func TestModelServingRestartGracePeriodSeconds(t *testing.T) {
	const graceSeconds int64 = 20
	for _, seconds := range []int64{0, graceSeconds} {
		t.Run(fmt.Sprintf("grace_%ds", seconds), func(t *testing.T) {
			ctx, kthenaClient, kubeClient := setupControllerManagerE2ETest(t)
			prefill := createRole("prefill", 1, 0)
			podSpec := &prefill.EntryTemplate.Spec
			podSpec.TerminationGracePeriodSeconds = ptr.To(int64(1))
			podSpec.Volumes = []corev1.Volume{{
				Name: "failure",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}}
			container := &podSpec.Containers[0]
			container.VolumeMounts = []corev1.VolumeMount{{Name: "failure", MountPath: "/failure"}}
			probe := &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "test ! -f /failure/trigger"}},
				},
				PeriodSeconds:    1,
				FailureThreshold: 1,
			}
			container.LivenessProbe = probe.DeepCopy()
			container.ReadinessProbe = probe.DeepCopy()

			ms := createBasicModelServing(fmt.Sprintf("test-restart-grace-%d", seconds), 1, 0,
				prefill, createRole("decode", 1, 0))
			ms.Spec.RecoveryPolicy = workload.ServingGroupRecreate
			ms.Spec.Template.RestartGracePeriodSeconds = ptr.To(seconds)
			createAndWaitForModelServing(t, ctx, kthenaClient, ms)

			selector := modelServingLabelSelector(ms.Name)
			initialPods, err := kubeClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
			require.NoError(t, err)
			require.Len(t, initialPods.Items, 2)
			originalUIDs := make(map[types.UID]bool)
			var target *corev1.Pod
			for i := range initialPods.Items {
				pod := &initialPods.Items[i]
				require.True(t, utils.IsPodReady(*pod), "Initial pod %s must be ready", pod.Name)
				originalUIDs[pod.UID] = true
				if controllerutils.GetRoleName(pod) == "prefill" {
					target = pod
				}
			}
			require.NotNil(t, target)
			groupName := target.Labels[workload.GroupNameLabelKey]
			require.NotEmpty(t, groupName)

			// Start watching before fault injection so even immediate deletion is observed.
			watchCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			watcher, err := kubeClient.CoreV1().Pods(testNamespace).Watch(watchCtx, metav1.ListOptions{
				LabelSelector:   selector,
				ResourceVersion: initialPods.ResourceVersion,
			})
			require.NoError(t, err)
			defer watcher.Stop()

			// emptyDir preserves the failure across container restarts, but not Pod
			// replacement. Thus the failed Pod cannot recover on its own, while its
			// replacement becomes healthy without modifying the ModelServing template.
			config, err := utils.GetKubeConfig()
			require.NoError(t, err)
			request := kubeClient.CoreV1().RESTClient().Post().Namespace(testNamespace).
				Resource("pods").Name(target.Name).SubResource("exec").VersionedParams(&corev1.PodExecOptions{
				Container: container.Name,
				Command:   []string{"touch", "/failure/trigger"},
				Stdout:    true,
				Stderr:    true,
			}, scheme.ParameterCodec)
			executor, err := remotecommand.NewSPDYExecutor(config, "POST", request.URL())
			require.NoError(t, err)
			var stdout, stderr bytes.Buffer
			injectedAt := time.Now()
			execCtx, execCancel := context.WithTimeout(ctx, utils.DefaultAPICallTimeout)
			err = executor.StreamWithContext(execCtx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
			execCancel()
			require.NoError(t, err, "Fault injection failed: %s", stderr.String())

			var failedAt time.Time
			deletionObserved := false
			for !deletionObserved {
				select {
				case <-watchCtx.Done():
					require.FailNow(t, "Timed out waiting for controller to delete the failed Pod")
				case event, ok := <-watcher.ResultChan():
					require.True(t, ok, "Pod watch closed before deletion")
					require.NotEqual(t, watch.Error, event.Type, "Pod watch failed: %v", event.Object)
					pod, ok := event.Object.(*corev1.Pod)
					if !ok || !originalUIDs[pod.UID] {
						continue
					}
					if pod.UID == target.UID && controllerutils.ContainerRestarted(pod) && failedAt.IsZero() {
						require.Len(t, pod.Status.ContainerStatuses, 1)
						terminated := pod.Status.ContainerStatuses[0].LastTerminationState.Terminated
						require.NotNil(t, terminated, "Expected the failed container's termination timestamp")
						failedAt = terminated.FinishedAt.Time
						require.False(t, failedAt.IsZero())
						require.False(t, utils.IsPodReady(*pod), "Failed Pod must remain unready")
						t.Logf("Container restart observed after %s", time.Since(injectedAt))
					}
					if pod.DeletionTimestamp == nil && event.Type != watch.Deleted {
						continue
					}
					require.False(t, failedAt.IsZero(), "Expected a container restart before Pod deletion")
					require.NotNil(t, pod.DeletionTimestamp)
					// Use Kubernetes timestamps, not watch delivery times. DeletionTimestamp
					// includes the Pod termination grace; subtract it to measure when deletion
					// was requested. Allow only the timestamps' one-second precision loss.
					deletionStartedAt := pod.DeletionTimestamp.Time
					if pod.DeletionGracePeriodSeconds != nil {
						deletionStartedAt = deletionStartedAt.Add(-time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second)
					}
					elapsed := deletionStartedAt.Sub(failedAt)
					require.GreaterOrEqual(t, elapsed, time.Duration(seconds)*time.Second-time.Second,
						"Pod %s was deleted before the configured grace period elapsed", pod.Name)
					if pod.UID == target.UID {
						if seconds == 0 {
							require.Less(t, elapsed, time.Duration(graceSeconds)*time.Second,
								"Zero grace period must not wait for the nonzero grace window")
						}
						t.Logf("Failed Pod deletion requested %s after container termination (restartGracePeriodSeconds=%d)", elapsed, seconds)
						deletionObserved = true
					}
				}
			}
			watcher.Stop()

			// Verify both the failed role and the healthy sibling were replaced.
			require.Eventually(t, func() bool {
				pods, err := kubeClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
				if err != nil || len(pods.Items) != len(originalUIDs) {
					return false
				}
				for _, pod := range pods.Items {
					if originalUIDs[pod.UID] || pod.DeletionTimestamp != nil || !utils.IsPodReady(pod) ||
						pod.Labels[workload.GroupNameLabelKey] != groupName {
						return false
					}
				}
				return true
			}, 3*time.Minute, time.Second, "Entire ServingGroup must be recreated and ready")
			utils.WaitForModelServingReady(t, ctx, kthenaClient, testNamespace, ms.Name)
		})
	}
}
