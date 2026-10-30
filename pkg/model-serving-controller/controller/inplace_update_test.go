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

package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	kthenafake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/inplace"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

const inPlaceTestGroup = "ms-0"

type inPlaceFixture struct {
	controller *ModelServingController
	kubeClient *kubefake.Clientset
	oldMS      *workloadv1alpha1.ModelServing
	newMS      *workloadv1alpha1.ModelServing
	oldRev     string
	newRev     string
}

func newInPlaceModelServing(image string, mutate func(*workloadv1alpha1.ModelServing)) *workloadv1alpha1.ModelServing {
	ms := createStandardModelServing("ms", 1, 1)
	ms.UID = types.UID("ms-uid")
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = image
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{
		Type:                       workloadv1alpha1.ServingGroupRollingUpdate,
		UpdateMethod:               workloadv1alpha1.InPlaceUpdateMethod,
		RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{MaxUnavailable: ptr.To(intstr.FromInt(1))},
	}
	if mutate != nil {
		mutate(ms)
	}
	return ms
}

// newInPlaceFixture stores a v1 ServingGroup whose only Pod runs oldMS, and returns
// a controller that is asked to roll it out to newMS.
func newInPlaceFixture(t *testing.T, newMutate func(*workloadv1alpha1.ModelServing), podMutate func(*corev1.Pod)) *inPlaceFixture {
	t.Helper()
	kubeClient := kubefake.NewSimpleClientset()
	controller, err := NewModelServingController(kubeClient, kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	controller.store = datastore.New()

	oldMS := newInPlaceModelServing("img:v1", nil)
	newMS := newInPlaceModelServing("img:v2", newMutate)
	f := &inPlaceFixture{
		controller: controller,
		kubeClient: kubeClient,
		oldMS:      oldMS,
		newMS:      newMS,
		oldRev:     utils.ModelServingRevision(oldMS),
		newRev:     utils.ModelServingRevision(newMS),
	}

	_, err = utils.CreateControllerRevision(context.Background(), kubeClient, oldMS, f.oldRev, oldMS.Spec.Template.Roles)
	require.NoError(t, err)

	name := utils.GetNamespaceName(oldMS)
	controller.store.AddServingGroup(name, 0, f.oldRev)
	require.NoError(t, controller.store.UpdateServingGroupStatus(name, inPlaceTestGroup, datastore.ServingGroupRunning))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ms-0-prefill-0-0",
			UID:       "pod-uid",
			Labels: map[string]string{
				workloadv1alpha1.ModelServingNameLabelKey: "ms",
				workloadv1alpha1.GroupNameLabelKey:        inPlaceTestGroup,
				workloadv1alpha1.RoleLabelKey:             "prefill",
				workloadv1alpha1.RoleIDKey:                "prefill-0",
				workloadv1alpha1.EntryLabelKey:            utils.Entry,
				workloadv1alpha1.RevisionLabelKey:         f.oldRev,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: workloadv1alpha1.SchemeGroupVersion.String(),
				Kind:       workloadv1alpha1.ModelServingKind.Kind,
				Name:       "ms",
				UID:        oldMS.UID,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers:    []corev1.Container{{Name: "prefill-container", Image: "img:v1"}},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "prefill-container", RestartCount: 0}},
		},
	}
	if podMutate != nil {
		podMutate(pod)
	}
	_, err = kubeClient.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, controller.podsInformer.GetIndexer().Add(pod))
	return f
}

// syncPodFromAPI refreshes the informer cache from the fake API server.
func (f *inPlaceFixture) syncPodFromAPI(t *testing.T) *corev1.Pod {
	t.Helper()
	pod, err := f.kubeClient.CoreV1().Pods("default").Get(context.Background(), "ms-0-prefill-0-0", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, f.controller.podsInformer.GetIndexer().Update(pod))
	return pod
}

func TestUpdateServingGroupInPlaceOrRecreate(t *testing.T) {
	tests := []struct {
		name          string
		newMutate     func(*workloadv1alpha1.ModelServing)
		podMutate     func(*corev1.Pod)
		wantInPlace   bool
		wantRecreated bool
	}{
		{
			name:        "image-only change is patched in place",
			wantInPlace: true,
		},
		{
			name: "non-image change is recreated",
			newMutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "A", Value: "B"}}
			},
			wantRecreated: true,
		},
		{
			name: "Recreate update method is recreated",
			newMutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.UpdateMethod = workloadv1alpha1.RecreateUpdateMethod
			},
			wantRecreated: true,
		},
		{
			name: "zero maxUnavailable is recreated",
			newMutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxUnavailable = ptr.To(intstr.FromInt(0))
			},
			wantRecreated: true,
		},
		{
			name:          "restartPolicy Never is recreated",
			podMutate:     func(pod *corev1.Pod) { pod.Spec.RestartPolicy = corev1.RestartPolicyNever },
			wantRecreated: true,
		},
		{
			name: "terminating pod waits",
			podMutate: func(pod *corev1.Pod) {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newInPlaceFixture(t, tt.newMutate, tt.podMutate)
			sg := datastore.ServingGroup{Name: inPlaceTestGroup, Revision: f.oldRev, Status: datastore.ServingGroupRunning}

			require.NoError(t, f.controller.updateServingGroup(context.Background(), f.newMS, sg))

			status := f.controller.store.GetServingGroupStatus(utils.GetNamespaceName(f.newMS), inPlaceTestGroup)
			pod, err := f.kubeClient.CoreV1().Pods("default").Get(context.Background(), "ms-0-prefill-0-0", metav1.GetOptions{})
			require.NoError(t, err)

			assert.Equal(t, tt.wantRecreated, status == datastore.ServingGroupDeleting)
			if !tt.wantInPlace {
				assert.Equal(t, "img:v1", pod.Spec.Containers[0].Image)
				assert.Nil(t, inplace.StateOf(pod))
				return
			}
			assert.Equal(t, "img:v2", pod.Spec.Containers[0].Image)
			assert.Equal(t, f.oldRev, utils.ObjectRevision(pod), "revision label flips only on completion")
			state := inplace.StateOf(pod)
			require.NotNil(t, state)
			assert.Equal(t, f.newRev, state.Revision)
			assert.Equal(t, map[string]int32{"prefill-container": 0}, state.Baseline)
			assert.Equal(t, datastore.ServingGroupCreating, status, "group must count against the unavailable budget")
		})
	}
}

func TestCompleteInPlaceUpdates(t *testing.T) {
	tests := []struct {
		name         string
		deadline     *int32
		startedAgo   time.Duration
		restartCount int32
		wantFlipped  bool
		wantDeleting bool
	}{
		{
			name:         "restarted and ready flips the revision",
			startedAgo:   time.Minute,
			restartCount: 1,
			wantFlipped:  true,
		},
		{
			name:         "stale ready without restart keeps waiting",
			startedAgo:   time.Minute,
			restartCount: 0,
		},
		{
			name:         "expired deadline falls back to recreate",
			deadline:     ptr.To[int32](60),
			startedAgo:   2 * time.Minute,
			restartCount: 0,
			wantDeleting: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newInPlaceFixture(t, func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.ProgressDeadlineSeconds = tt.deadline
			}, nil)
			ctx := context.Background()
			sg := datastore.ServingGroup{Name: inPlaceTestGroup, Revision: f.oldRev, Status: datastore.ServingGroupRunning}
			require.NoError(t, f.controller.updateServingGroup(ctx, f.newMS, sg))

			// Simulate a controller restart: only Pod state survives.
			pod := f.syncPodFromAPI(t)
			state := inplace.StateOf(pod)
			state.StartedAt = metav1.NewTime(time.Now().Add(-tt.startedAgo))
			pod = pod.DeepCopy()
			pod.Annotations[workloadv1alpha1.InPlaceUpdateStateAnnotationKey] = marshalState(t, state)
			pod.Status.ContainerStatuses[0].RestartCount = tt.restartCount
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			_, err := f.kubeClient.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{})
			require.NoError(t, err)
			f.syncPodFromAPI(t)

			require.NoError(t, f.controller.completeInPlaceUpdates(ctx, f.newMS, f.newRev))

			got, err := f.kubeClient.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFlipped, utils.ObjectRevision(got) == f.newRev)
			assert.Equal(t, tt.wantFlipped, !inplace.InProgress(got))
			status := f.controller.store.GetServingGroupStatus(utils.GetNamespaceName(f.newMS), inPlaceTestGroup)
			assert.Equal(t, tt.wantDeleting, status == datastore.ServingGroupDeleting)
			if tt.wantFlipped {
				revision, ok := f.controller.store.GetServingGroupRevision(utils.GetNamespaceName(f.newMS), inPlaceTestGroup)
				assert.True(t, ok)
				assert.Equal(t, f.newRev, revision)
				assert.False(t, inplace.UnexpectedRestart(got), "the expected restart must not trigger recovery")
			}
		})
	}
}

func TestSurgeDisabledWhileUpdatingInPlace(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*workloadv1alpha1.ModelServing)
		wantInUse bool
	}{
		{name: "InPlace with unavailable budget", wantInUse: true},
		{
			name: "InPlace without unavailable budget keeps surge path",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxUnavailable = ptr.To(intstr.FromInt(0))
			},
		},
		{
			name: "Recreate keeps surge path",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.UpdateMethod = workloadv1alpha1.RecreateUpdateMethod
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newInPlaceFixture(t, tt.mutate, nil)
			assert.Equal(t, tt.wantInUse, f.controller.updatesInPlace(f.newMS))
		})
	}
}

func TestUpdatePodDoesNotRecoverInPlaceUpdatingPod(t *testing.T) {
	f := newInPlaceFixture(t, nil, nil)
	ctx := context.Background()
	f.controller.initialSync.Store(true)
	require.NoError(t, f.controller.modelServingsInformer.GetIndexer().Add(f.newMS))

	sg := datastore.ServingGroup{Name: inPlaceTestGroup, Revision: f.oldRev, Status: datastore.ServingGroupRunning}
	require.NoError(t, f.controller.updateServingGroup(ctx, f.newMS, sg))
	pod := f.syncPodFromAPI(t)
	pod.Status.ContainerStatuses[0].RestartCount = 1
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}

	f.controller.updatePod(nil, pod)

	assert.Equal(t, datastore.ServingGroupCreating, f.controller.store.GetServingGroupStatus(utils.GetNamespaceName(f.newMS), inPlaceTestGroup))
	_, loaded := f.controller.graceMap.Load(getPodGracePeriodKey(pod))
	assert.False(t, loaded, "an expected restart must not enter the recovery grace period")
}

func marshalState(t *testing.T, state *inplace.State) string {
	t.Helper()
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	return string(raw)
}

func roleModeMutate(maxUnavailable int, extra func(*workloadv1alpha1.ModelServing)) func(*workloadv1alpha1.ModelServing) {
	return func(ms *workloadv1alpha1.ModelServing) {
		ms.Spec.RolloutStrategy.Type = workloadv1alpha1.RoleRollingUpdate
		ms.Spec.RolloutStrategy.RollingUpdateConfiguration = nil
		ms.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(maxUnavailable))
		if extra != nil {
			extra(ms)
		}
	}
}

func (f *inPlaceFixture) addRunningRole(t *testing.T) {
	t.Helper()
	name := utils.GetNamespaceName(f.oldMS)
	f.controller.store.AddRole(name, inPlaceTestGroup, "prefill", "prefill-0", f.oldRev, utils.CalRoleTemplateHash(f.oldMS.Spec.Template.Roles[0]))
	require.NoError(t, f.controller.store.UpdateRoleStatus(name, inPlaceTestGroup, "prefill", "prefill-0", datastore.RoleRunning))
}

func (f *inPlaceFixture) roleStatus() datastore.RoleStatus {
	return f.controller.store.GetRoleStatus(utils.GetNamespaceName(f.newMS), inPlaceTestGroup, "prefill", "prefill-0")
}

func TestUpdateRoleInPlaceOrRecreate(t *testing.T) {
	tests := []struct {
		name          string
		newMutate     func(*workloadv1alpha1.ModelServing)
		wantInPlace   bool
		wantRecreated bool
	}{
		{
			name:        "image-only change is patched in place",
			newMutate:   roleModeMutate(1, nil),
			wantInPlace: true,
		},
		{
			name: "non-image change is recreated",
			newMutate: roleModeMutate(1, func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "A", Value: "B"}}
			}),
			wantRecreated: true,
		},
		{
			name:          "role maxUnavailable 0 is recreated",
			newMutate:     roleModeMutate(0, nil),
			wantRecreated: true,
		},
		{
			name: "Recreate update method is recreated",
			newMutate: roleModeMutate(1, func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.UpdateMethod = workloadv1alpha1.RecreateUpdateMethod
			}),
			wantRecreated: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newInPlaceFixture(t, tt.newMutate, nil)
			f.addRunningRole(t)

			require.NoError(t, f.controller.updateRole(context.Background(), f.newMS, inPlaceTestGroup, "prefill", "prefill-0"))

			pod, err := f.kubeClient.CoreV1().Pods("default").Get(context.Background(), "ms-0-prefill-0-0", metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantRecreated, f.roleStatus() == datastore.RoleDeleting)
			if !tt.wantInPlace {
				assert.Equal(t, "img:v1", pod.Spec.Containers[0].Image)
				assert.Nil(t, inplace.StateOf(pod))
				return
			}
			assert.Equal(t, "img:v2", pod.Spec.Containers[0].Image)
			assert.Equal(t, f.oldRev, utils.ObjectRevision(pod))
			assert.True(t, inplace.InProgress(pod))
			assert.Equal(t, datastore.RoleCreating, f.roleStatus(), "role must count against its unavailable budget")
		})
	}
}

// TestManageRollingUpdateInPlaceEndToEnd drives the whole rollout through
// manageRollingUpdate: start, a stale reconcile, the container restart, and completion.
func TestManageRollingUpdateInPlaceEndToEnd(t *testing.T) {
	tests := []struct {
		name      string
		newMutate func(*workloadv1alpha1.ModelServing)
		roleMode  bool
	}{
		{name: "ServingGroupRollingUpdate"},
		{name: "RoleRollingUpdate", newMutate: roleModeMutate(1, nil), roleMode: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newInPlaceFixture(t, tt.newMutate, nil)
			ctx := context.Background()
			name := utils.GetNamespaceName(f.newMS)
			if tt.roleMode {
				f.addRunningRole(t)
			}

			require.NoError(t, f.controller.manageRollingUpdate(ctx, f.newMS, f.newRev))
			pod := f.syncPodFromAPI(t)
			assert.Equal(t, "img:v2", pod.Spec.Containers[0].Image)
			assert.True(t, inplace.InProgress(pod))
			resourceVersion := pod.ResourceVersion

			// Nothing has restarted yet: reconciling again must neither re-patch nor flip.
			require.NoError(t, f.controller.manageRollingUpdate(ctx, f.newMS, f.newRev))
			pod = f.syncPodFromAPI(t)
			assert.Equal(t, resourceVersion, pod.ResourceVersion)
			assert.Equal(t, f.oldRev, utils.ObjectRevision(pod))

			// kubelet restarts the container and the Pod becomes Ready again.
			pod = pod.DeepCopy()
			pod.Status.ContainerStatuses[0].RestartCount = 1
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			_, err := f.kubeClient.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{})
			require.NoError(t, err)
			f.syncPodFromAPI(t)

			require.NoError(t, f.controller.manageRollingUpdate(ctx, f.newMS, f.newRev))
			pod = f.syncPodFromAPI(t)
			assert.Equal(t, f.newRev, utils.ObjectRevision(pod))
			assert.False(t, inplace.InProgress(pod))
			assert.False(t, inplace.UnexpectedRestart(pod))
			if tt.roleMode {
				assert.Equal(t, utils.CalRoleTemplateHash(f.newMS.Spec.Template.Roles[0]), utils.ObjectRoleTemplateHash(pod))
				roles, err := f.controller.store.GetRoleList(name, inPlaceTestGroup, "prefill")
				require.NoError(t, err)
				require.Len(t, roles, 1)
				assert.Equal(t, f.newRev, roles[0].Revision)
			} else {
				revision, ok := f.controller.store.GetServingGroupRevision(name, inPlaceTestGroup)
				assert.True(t, ok)
				assert.Equal(t, f.newRev, revision)
			}
			assert.NotEqual(t, datastore.ServingGroupDeleting, f.controller.store.GetServingGroupStatus(name, inPlaceTestGroup))
		})
	}
}

func TestCompleteInPlaceUpdatesRoleDeadlineRecreatesRoleOnly(t *testing.T) {
	f := newInPlaceFixture(t, roleModeMutate(1, func(ms *workloadv1alpha1.ModelServing) {
		ms.Spec.RolloutStrategy.ProgressDeadlineSeconds = ptr.To[int32](60)
	}), nil)
	f.addRunningRole(t)
	ctx := context.Background()
	require.NoError(t, f.controller.updateRole(ctx, f.newMS, inPlaceTestGroup, "prefill", "prefill-0"))

	pod := f.syncPodFromAPI(t).DeepCopy()
	state := inplace.StateOf(pod)
	state.StartedAt = metav1.NewTime(time.Now().Add(-2 * time.Minute))
	pod.Annotations[workloadv1alpha1.InPlaceUpdateStateAnnotationKey] = marshalState(t, state)
	_, err := f.kubeClient.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.syncPodFromAPI(t)

	require.NoError(t, f.controller.completeInPlaceUpdates(ctx, f.newMS, f.newRev))

	assert.Equal(t, datastore.RoleDeleting, f.roleStatus())
	assert.NotEqual(t, datastore.ServingGroupDeleting, f.controller.store.GetServingGroupStatus(utils.GetNamespaceName(f.newMS), inPlaceTestGroup), "only the Role is recreated")
}
