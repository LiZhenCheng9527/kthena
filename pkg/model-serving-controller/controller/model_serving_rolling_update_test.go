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
	"fmt"
	"testing"

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
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

func TestDeleteOutdatedServingGroups(t *testing.T) {
	tests := []struct {
		name                     string
		rolloutStrategy          *workloadv1alpha1.RolloutStrategy
		maxScaleDown             int
		notRunningOutdatedGroups []datastore.ServingGroup
		runningOutdatedGroups    []datastore.ServingGroup
		expectedUpdateCount      int
	}{
		{
			name:                     "no groups to delete",
			rolloutStrategy:          &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate},
			maxScaleDown:             2,
			notRunningOutdatedGroups: []datastore.ServingGroup{},
			runningOutdatedGroups:    []datastore.ServingGroup{},
			expectedUpdateCount:      0,
		},
		{
			name: "delete not running groups only",
			rolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.ServingGroupRollingUpdate,
			},
			maxScaleDown: 2,
			notRunningOutdatedGroups: []datastore.ServingGroup{
				{Name: "test-group-0", Status: datastore.ServingGroupCreating, Revision: "v1"},
				{Name: "test-group-1", Status: datastore.ServingGroupCreating, Revision: "v1"},
			},
			runningOutdatedGroups: []datastore.ServingGroup{},
			expectedUpdateCount:   2,
		},
		{
			name:                     "delete running groups only",
			rolloutStrategy:          &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate},
			maxScaleDown:             1,
			notRunningOutdatedGroups: []datastore.ServingGroup{},
			runningOutdatedGroups: []datastore.ServingGroup{
				{Name: "test-group-0", Status: datastore.ServingGroupRunning, Revision: "v1"},
				{Name: "test-group-1", Status: datastore.ServingGroupRunning, Revision: "v1"},
			},
			expectedUpdateCount: 1,
		},
		{
			name: "delete mixed groups with limited maxScaleDown",
			rolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.ServingGroupRollingUpdate,
			},
			maxScaleDown: 2,
			notRunningOutdatedGroups: []datastore.ServingGroup{
				{Name: "test-group-0", Status: datastore.ServingGroupCreating, Revision: "v1"},
				{Name: "test-group-1", Status: datastore.ServingGroupCreating, Revision: "v1"},
				{Name: "test-group-2", Status: datastore.ServingGroupCreating, Revision: "v1"},
			},
			runningOutdatedGroups: []datastore.ServingGroup{
				{Name: "test-group-3", Status: datastore.ServingGroupRunning, Revision: "v1"},
			},
			expectedUpdateCount: 2,
		},
		{
			name:                     "nil rollout strategy defaults to servinggroup rolling update",
			maxScaleDown:             1,
			notRunningOutdatedGroups: []datastore.ServingGroup{},
			runningOutdatedGroups: []datastore.ServingGroup{
				{Name: "test-group-0", Status: datastore.ServingGroupRunning, Revision: "v1"},
				{Name: "test-group-1", Status: datastore.ServingGroupRunning, Revision: "v1"},
			},
			expectedUpdateCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubeClient := kubefake.NewSimpleClientset()
			modelServingClient := kthenafake.NewSimpleClientset()
			apiextensionsClient := apiextfake.NewSimpleClientset()

			controller, err := NewModelServingController(kubeClient, modelServingClient, nil, apiextensionsClient)
			assert.NoError(t, err)

			ms := &workloadv1alpha1.ModelServing{
				ObjectMeta: metav1.ObjectMeta{Name: "test-group", Namespace: "default"},
				Spec:       workloadv1alpha1.ModelServingSpec{RolloutStrategy: tt.rolloutStrategy},
			}

			controller.store = datastore.New()
			for _, group := range append(tt.notRunningOutdatedGroups, tt.runningOutdatedGroups...) {
				_, ordinal := utils.GetParentNameAndOrdinal(group.Name)
				controller.store.AddServingGroup(utils.GetNamespaceName(ms), ordinal, group.Revision)
				controller.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), group.Name, group.Status)
			}

			allOutdatedGroups := append(tt.runningOutdatedGroups, tt.notRunningOutdatedGroups...)
			groups := selectServingGroupsForRollingUpdate(tt.maxScaleDown, nil, allOutdatedGroups)
			result, err := controller.deleteOutdatedResourcesForRollingUpdate(context.Background(), ms, groups, "v1")

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedUpdateCount, result)
		})
	}
}

func TestSelectServingGroupsForRollingUpdate(t *testing.T) {
	groups := selectServingGroupsForRollingUpdate(
		2,
		[]datastore.ServingGroup{
			{Name: "test-group-1", Status: datastore.ServingGroupRolling},
		},
		[]datastore.ServingGroup{
			{Name: "test-group-3", Status: datastore.ServingGroupRunning},
			{Name: "test-group-4", Status: datastore.ServingGroupRunning},
			{Name: "test-group-0", Status: datastore.ServingGroupCreating},
			{Name: "test-group-2", Status: datastore.ServingGroupCreating},
		},
	)

	require.Len(t, groups, 2)
	assert.Equal(t, "test-group-1", groups[0].Name)
	assert.Equal(t, "test-group-2", groups[1].Name)
}

func TestSelectServingGroupsForRollingUpdateAlwaysContinuesRollingGroups(t *testing.T) {
	groups := selectServingGroupsForRollingUpdate(
		0,
		[]datastore.ServingGroup{
			{Name: "test-group-1", Status: datastore.ServingGroupRolling},
			{Name: "test-group-2", Status: datastore.ServingGroupRolling},
		},
		[]datastore.ServingGroup{{Name: "test-group-3", Status: datastore.ServingGroupRunning}},
	)

	require.Len(t, groups, 2)
	assert.Equal(t, "test-group-2", groups[0].Name)
	assert.Equal(t, "test-group-1", groups[1].Name)
}

func TestDeleteOutdatedRolesForRoleRollingUpdateWithMaxUnavailable(t *testing.T) {
	ns := "default"
	msName := "test-ms"
	groupName := "test-ms-0"
	oldRevision := "old-revision"
	newRevision := "new-revision"
	outdatedHash := "outdated-hash"

	tests := []struct {
		name              string
		maxUnavailable    *intstr.IntOrString
		statuses          []datastore.RoleStatus
		expectedDeletions int
	}{
		{
			name:              "unset deletes all outdated replicas",
			statuses:          []datastore.RoleStatus{datastore.RoleRunning, datastore.RoleRunning, datastore.RoleRunning, datastore.RoleRunning},
			expectedDeletions: 4,
		},
		{
			name:              "configured value limits running replica deletion",
			maxUnavailable:    ptr.To(intstr.FromInt(2)),
			statuses:          []datastore.RoleStatus{datastore.RoleRunning, datastore.RoleRunning, datastore.RoleRunning, datastore.RoleRunning},
			expectedDeletions: 2,
		},
		{
			name:              "already unavailable outdated replicas can be replaced",
			maxUnavailable:    ptr.To(intstr.FromInt(2)),
			statuses:          []datastore.RoleStatus{datastore.RoleRunning, datastore.RoleRunning, datastore.RoleCreating, datastore.RoleCreating},
			expectedDeletions: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubeClient := kubefake.NewSimpleClientset()
			modelServingClient := kthenafake.NewSimpleClientset()
			apiextensionsClient := apiextfake.NewSimpleClientset()
			controller, err := NewModelServingController(kubeClient, modelServingClient, nil, apiextensionsClient)
			require.NoError(t, err)
			controller.store = datastore.New()

			ms := &workloadv1alpha1.ModelServing{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: msName},
				Spec: workloadv1alpha1.ModelServingSpec{
					Replicas:        ptr.To[int32](1),
					RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
					Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
						Name:     "decode",
						Replicas: ptr.To[int32](4),
						RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
							MaxUnavailable: tt.maxUnavailable,
						},
					}}},
				},
			}

			nsn := utils.GetNamespaceName(ms)
			controller.store.AddServingGroup(nsn, 0, oldRevision)
			for i, status := range tt.statuses {
				roleID := fmt.Sprintf("decode-%d", i)
				controller.store.AddRole(nsn, groupName, "decode", roleID, oldRevision, outdatedHash)
				require.NoError(t, controller.store.UpdateRoleStatus(nsn, groupName, "decode", roleID, status))
			}

			_, err = controller.deleteOutdatedResourcesForRollingUpdate(
				context.Background(), ms,
				[]datastore.ServingGroup{{Name: groupName, Revision: oldRevision, Status: datastore.ServingGroupRunning}},
				newRevision,
			)
			require.NoError(t, err)

			deletions := 0
			for _, action := range kubeClient.Actions() {
				if action.Matches("delete-collection", "pods") {
					deletions++
				}
			}
			assert.Equal(t, tt.expectedDeletions, deletions)
		})
	}
}

func TestManageRoleRollingUpdateRespectsServingGroupMaxUnavailable(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	controller, err := NewModelServingController(kubeClient, kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	controller.store = datastore.New()

	maxUnavailable := intstr.FromInt(1)
	role := workloadv1alpha1.Role{
		Name:     "decode",
		Replicas: ptr.To[int32](1),
		EntryTemplate: workloadv1alpha1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "new-image"}}},
		},
	}
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ms", Namespace: "default"},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas: ptr.To[int32](4),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.RoleRollingUpdate,
				RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
					MaxUnavailable: &maxUnavailable,
				},
			},
			Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}},
		},
	}

	nsn := utils.GetNamespaceName(ms)
	for ordinal := 0; ordinal < 4; ordinal++ {
		groupName := utils.GenerateServingGroupName(ms.Name, ordinal)
		controller.store.AddServingGroup(nsn, ordinal, "old-revision")
		require.NoError(t, controller.store.UpdateServingGroupStatus(nsn, groupName, datastore.ServingGroupRunning))
		controller.store.AddRole(nsn, groupName, role.Name, "decode-0", "old-revision", "old-role-hash")
		require.NoError(t, controller.store.UpdateRoleStatus(nsn, groupName, role.Name, "decode-0", datastore.RoleRunning))
	}

	require.NoError(t, controller.manageRollingUpdate(context.Background(), ms, "new-revision"))

	deleteCollections := 0
	for _, action := range kubeClient.Actions() {
		if action.Matches("delete-collection", "pods") {
			deleteCollections++
		}
	}
	assert.Equal(t, 1, deleteCollections)
	assert.Equal(t, datastore.ServingGroupRolling, controller.store.GetServingGroupStatus(nsn, "test-ms-3"))
	for ordinal := 0; ordinal < 3; ordinal++ {
		assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, utils.GenerateServingGroupName(ms.Name, ordinal)))
	}

	// The in-progress group consumes the only ServingGroup-level budget slot.
	require.NoError(t, controller.manageRollingUpdate(context.Background(), ms, "new-revision"))
	deleteCollections = 0
	for _, action := range kubeClient.Actions() {
		if action.Matches("delete-collection", "pods") {
			deleteCollections++
		}
	}
	assert.Equal(t, 1, deleteCollections)
}

func TestManageRollingUpdateUnavailableOldGroupDoesNotConsumeBudget(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	controller, err := NewModelServingController(kubeClient, kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	controller.store = datastore.New()

	maxUnavailable := intstr.FromInt(1)
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ms", Namespace: "default"},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas: ptr.To[int32](4),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.ServingGroupRollingUpdate,
				RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
					MaxUnavailable: &maxUnavailable,
				},
			},
		},
	}

	nsn := utils.GetNamespaceName(ms)
	for ordinal := 0; ordinal < 4; ordinal++ {
		groupName := utils.GenerateServingGroupName(ms.Name, ordinal)
		controller.store.AddServingGroup(nsn, ordinal, "old-revision")
		status := datastore.ServingGroupRunning
		if ordinal == 0 {
			status = datastore.ServingGroupCreating
		}
		require.NoError(t, controller.store.UpdateServingGroupStatus(nsn, groupName, status))
	}

	require.NoError(t, controller.manageRollingUpdate(context.Background(), ms, "new-revision"))

	// The already unavailable old group is reclaimed first. It occupies this
	// reconcile's only selection slot, so no running group starts updating yet.
	assert.Equal(t, datastore.ServingGroupNotFound, controller.store.GetServingGroupStatus(nsn, "test-ms-0"))
	assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, "test-ms-1"))
	assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, "test-ms-2"))
	assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, "test-ms-3"))
}

func TestManageRollingUpdateUnavailableNewGroupConsumesBudget(t *testing.T) {
	controller, err := NewModelServingController(kubefake.NewSimpleClientset(), kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	controller.store = datastore.New()

	maxUnavailable := intstr.FromInt(1)
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ms", Namespace: "default"},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas: ptr.To[int32](4),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.ServingGroupRollingUpdate,
				RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
					MaxUnavailable: &maxUnavailable,
				},
			},
		},
	}

	nsn := utils.GetNamespaceName(ms)
	for ordinal := 0; ordinal < 4; ordinal++ {
		groupName := utils.GenerateServingGroupName(ms.Name, ordinal)
		revision := "old-revision"
		status := datastore.ServingGroupRunning
		if ordinal == 3 {
			revision = "new-revision"
			status = datastore.ServingGroupCreating
		}
		controller.store.AddServingGroup(nsn, ordinal, revision)
		require.NoError(t, controller.store.UpdateServingGroupStatus(nsn, groupName, status))
	}

	require.NoError(t, controller.manageRollingUpdate(context.Background(), ms, "new-revision"))

	for ordinal := 0; ordinal < 3; ordinal++ {
		assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, utils.GenerateServingGroupName(ms.Name, ordinal)))
	}
	assert.Equal(t, datastore.ServingGroupCreating, controller.store.GetServingGroupStatus(nsn, "test-ms-3"))
}

func TestManageRoleRollingUpdateRespectsServingGroupPartition(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	controller, err := NewModelServingController(kubeClient, kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	controller.store = datastore.New()

	partition := intstr.FromInt(3)
	maxUnavailable := intstr.FromInt(2)
	role := workloadv1alpha1.Role{Name: "decode", Replicas: ptr.To[int32](1)}
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ms", Namespace: "default"},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas: ptr.To[int32](4),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.RoleRollingUpdate,
				RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
					MaxUnavailable: &maxUnavailable,
					Partition:      &partition,
				},
			},
			Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}},
		},
	}

	nsn := utils.GetNamespaceName(ms)
	for ordinal := 0; ordinal < 4; ordinal++ {
		groupName := utils.GenerateServingGroupName(ms.Name, ordinal)
		controller.store.AddServingGroup(nsn, ordinal, "old-revision")
		require.NoError(t, controller.store.UpdateServingGroupStatus(nsn, groupName, datastore.ServingGroupRunning))
		controller.store.AddRole(nsn, groupName, role.Name, "decode-0", "old-revision", "old-role-hash")
		require.NoError(t, controller.store.UpdateRoleStatus(nsn, groupName, role.Name, "decode-0", datastore.RoleRunning))
	}

	require.NoError(t, controller.manageRollingUpdate(context.Background(), ms, "new-revision"))
	assert.Equal(t, datastore.ServingGroupRolling, controller.store.GetServingGroupStatus(nsn, "test-ms-3"))
	for ordinal := 0; ordinal < 3; ordinal++ {
		assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, utils.GenerateServingGroupName(ms.Name, ordinal)))
	}
}

func TestTryFinishRoleRollingUpdate(t *testing.T) {
	controller, err := NewModelServingController(kubefake.NewSimpleClientset(), kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	controller.store = datastore.New()

	role := workloadv1alpha1.Role{Name: "decode", Replicas: ptr.To[int32](1)}
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ms", Namespace: "default"},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas:        ptr.To[int32](1),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
			Template:        workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}},
		},
	}
	nsn := utils.GetNamespaceName(ms)
	groupName := "test-ms-0"
	roleHash := utils.CalRoleTemplateHash(role)
	controller.store.AddServingGroup(nsn, 0, "old-revision")
	controller.store.AddRole(nsn, groupName, role.Name, "decode-0", "new-revision", roleHash)
	require.NoError(t, controller.store.UpdateRoleStatus(nsn, groupName, role.Name, "decode-0", datastore.RoleRunning))
	require.NoError(t, controller.store.UpdateServingGroupStatus(nsn, groupName, datastore.ServingGroupRolling))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-0-entry",
			Namespace: ms.Namespace,
			Labels: map[string]string{
				workloadv1alpha1.GroupNameLabelKey: groupName,
				workloadv1alpha1.RoleLabelKey:      role.Name,
				workloadv1alpha1.RoleIDKey:         "decode-0",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	require.NoError(t, controller.podsInformer.GetIndexer().Add(pod))

	finished, rolesToDelete, err := controller.tryFinishRoleRollingUpdate(ms, groupName, "new-revision")
	require.NoError(t, err)
	assert.True(t, finished)
	assert.Empty(t, rolesToDelete)
	assert.Equal(t, datastore.ServingGroupRunning, controller.store.GetServingGroupStatus(nsn, groupName))
	revision, ok := controller.store.GetServingGroupRevision(nsn, groupName)
	assert.True(t, ok)
	assert.Equal(t, "new-revision", revision)
}

type failRunningStatusStore struct {
	datastore.Store
}

func (s *failRunningStatusStore) UpdateServingGroupStatus(modelServingName types.NamespacedName, groupName string, status datastore.ServingGroupStatus) error {
	if status == datastore.ServingGroupRunning {
		return fmt.Errorf("injected status update failure")
	}
	return s.Store.UpdateServingGroupStatus(modelServingName, groupName, status)
}

func TestTryFinishRoleRollingUpdateRollsBackAndRequeues(t *testing.T) {
	controller, err := NewModelServingController(kubefake.NewSimpleClientset(), kthenafake.NewSimpleClientset(), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	baseStore := datastore.New()
	controller.store = baseStore

	role := workloadv1alpha1.Role{Name: "decode", Replicas: ptr.To[int32](1)}
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ms", Namespace: "default"},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas:        ptr.To[int32](1),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
			Template:        workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}},
		},
	}
	nsn := utils.GetNamespaceName(ms)
	groupName := "test-ms-0"
	roleHash := utils.CalRoleTemplateHash(role)
	baseStore.AddServingGroup(nsn, 0, "old-revision")
	baseStore.AddRole(nsn, groupName, role.Name, "decode-0", "new-revision", roleHash)
	require.NoError(t, baseStore.UpdateRoleStatus(nsn, groupName, role.Name, "decode-0", datastore.RoleRunning))
	require.NoError(t, baseStore.UpdateServingGroupStatus(nsn, groupName, datastore.ServingGroupRolling))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-0-entry",
			Namespace: ms.Namespace,
			Labels: map[string]string{
				workloadv1alpha1.GroupNameLabelKey: groupName,
				workloadv1alpha1.RoleLabelKey:      role.Name,
				workloadv1alpha1.RoleIDKey:         "decode-0",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	require.NoError(t, controller.podsInformer.GetIndexer().Add(pod))
	controller.store = &failRunningStatusStore{Store: baseStore}

	finished, rolesToDelete, err := controller.tryFinishRoleRollingUpdate(ms, groupName, "new-revision")
	require.Error(t, err)
	assert.False(t, finished)
	assert.Empty(t, rolesToDelete)
	assert.Equal(t, datastore.ServingGroupRolling, baseStore.GetServingGroupStatus(nsn, groupName))
	revision, ok := baseStore.GetServingGroupRevision(nsn, groupName)
	assert.True(t, ok)
	assert.Equal(t, "old-revision", revision)
	assert.Equal(t, 1, controller.workqueue.Len())
}

func TestRolesToDeleteForRoleRollingUpdate(t *testing.T) {
	ns := "default"
	msName := "test-ms"
	groupName := "test-ms-0"
	oldRevision := "old-revision"

	newRole := func(name, image string, replicas int32, maxUnavailable *intstr.IntOrString) workloadv1alpha1.Role {
		return workloadv1alpha1.Role{
			Name:     name,
			Replicas: ptr.To(replicas),
			RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
				MaxUnavailable: maxUnavailable,
			},
			EntryTemplate: workloadv1alpha1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}},
			},
		}
	}
	newPartitionedRole := func(name, image string, replicas int32, maxUnavailable, partition *intstr.IntOrString) workloadv1alpha1.Role {
		role := newRole(name, image, replicas, maxUnavailable)
		role.RollingUpdateConfiguration.Partition = partition
		return role
	}

	addRole := func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing, roleName, roleID, roleTemplateHash string, status datastore.RoleStatus) {
		t.Helper()
		store.AddRole(utils.GetNamespaceName(ms), groupName, roleName, roleID, oldRevision, roleTemplateHash)
		require.NoError(t, store.UpdateRoleStatus(utils.GetNamespaceName(ms), groupName, roleName, roleID, status))
	}

	tests := []struct {
		name              string
		roles             []workloadv1alpha1.Role
		setupStore        func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing)
		expected          []roleToDelete
		expectedOutdated  bool
		expectErrContains string
	}{
		{
			name:  "empty serving group has no outdated roles",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 2, nil)},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
			},
		},
		{
			name:  "current hash roles are not deleted even when unavailable",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 2, nil)},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				hash := utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0])
				addRole(t, store, ms, "prefill", "prefill-0", hash, datastore.RoleCreating)
				addRole(t, store, ms, "prefill", "prefill-1", hash, datastore.RoleRunning)
			},
		},
		{
			name:  "nil maxUnavailable deletes every outdated role by descending ordinal",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 3, nil)},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				for i := 0; i < 3; i++ {
					addRole(t, store, ms, "prefill", fmt.Sprintf("prefill-%d", i), "old-hash", datastore.RoleRunning)
				}
			},
			expected:         []roleToDelete{{roleName: "prefill", roleID: "prefill-2"}, {roleName: "prefill", roleID: "prefill-1"}, {roleName: "prefill", roleID: "prefill-0"}},
			expectedOutdated: true,
		},
		{
			name:  "maxUnavailable limits deletion and prioritizes not running outdated roles",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 4, ptr.To(intstr.FromInt(2)))},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				addRole(t, store, ms, "prefill", "prefill-0", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-1", "old-hash", datastore.RoleCreating)
				addRole(t, store, ms, "prefill", "prefill-2", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-3", "old-hash", datastore.RoleCreating)
			},
			expected:         []roleToDelete{{roleName: "prefill", roleID: "prefill-3"}, {roleName: "prefill", roleID: "prefill-2"}},
			expectedOutdated: true,
		},
		{
			name:  "partition protects first sorted role instances rather than ordinal range",
			roles: []workloadv1alpha1.Role{newPartitionedRole("prefill", "nginx:latest", 3, ptr.To(intstr.FromInt(2)), ptr.To(intstr.FromInt(2)))},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				addRole(t, store, ms, "prefill", "prefill-0", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-2", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-5", "old-hash", datastore.RoleRunning)
			},
			expected:         []roleToDelete{{roleName: "prefill", roleID: "prefill-5"}},
			expectedOutdated: true,
		},
		{
			name:  "new unavailable roles consume maxUnavailable budget",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 4, ptr.To(intstr.FromInt(2)))},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				hash := utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0])
				addRole(t, store, ms, "prefill", "prefill-0", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-1", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-2", hash, datastore.RoleCreating)
				addRole(t, store, ms, "prefill", "prefill-3", hash, datastore.RoleRunning)
			},
			expected:         []roleToDelete{{roleName: "prefill", roleID: "prefill-1"}},
			expectedOutdated: true,
		},
		{
			name:  "deleting roles consume maxUnavailable budget",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 4, ptr.To(intstr.FromInt(2)))},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				hash := utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0])
				addRole(t, store, ms, "prefill", "prefill-0", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-1", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-2", "old-hash", datastore.RoleDeleting)
				addRole(t, store, ms, "prefill", "prefill-3", hash, datastore.RoleCreating)
			},
			expectedOutdated: true,
		},
		{
			name:  "roles removed from spec are deleted except already deleting roles",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 1, nil)},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				hash := utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0])
				addRole(t, store, ms, "prefill", "prefill-0", hash, datastore.RoleRunning)
				addRole(t, store, ms, "deprecated", "deprecated-0", "deprecated-hash", datastore.RoleRunning)
				addRole(t, store, ms, "deprecated", "deprecated-1", "deprecated-hash", datastore.RoleDeleting)
			},
			expected:         []roleToDelete{{roleName: "deprecated", roleID: "deprecated-0"}},
			expectedOutdated: true,
		},
		{
			name:  "invalid maxUnavailable leaves outdated roles pending without returning error",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 2, ptr.To(intstr.FromString("invalid")))},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				addRole(t, store, ms, "prefill", "prefill-0", "old-hash", datastore.RoleRunning)
				addRole(t, store, ms, "prefill", "prefill-1", "old-hash", datastore.RoleRunning)
			},
			expectedOutdated: true,
		},
		{
			name:  "missing roleTemplateHash without ControllerRevision is skipped",
			roles: []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 1, nil)},
			setupStore: func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) {
				t.Helper()
				store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
				addRole(t, store, ms, "prefill", "prefill-0", "", datastore.RoleRunning)
			},
		},
		{
			name:              "missing serving group returns error",
			roles:             []workloadv1alpha1.Role{newRole("prefill", "nginx:latest", 1, nil)},
			setupStore:        func(t *testing.T, store datastore.Store, ms *workloadv1alpha1.ModelServing) { t.Helper() },
			expectErrContains: "failed to get roles for ServingGroup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := &workloadv1alpha1.ModelServing{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: msName},
				Spec:       workloadv1alpha1.ModelServingSpec{Template: workloadv1alpha1.ServingGroup{Roles: tt.roles}},
			}
			store := datastore.New()
			tt.setupStore(t, store, ms)

			controller := &ModelServingController{store: store, kubeClientSet: kubefake.NewSimpleClientset()}
			rolesToDelete, hasOutdatedRoles, err := controller.rolesToDeleteForRoleRollingUpdate(
				ms, datastore.ServingGroup{Name: groupName, Revision: oldRevision, Status: datastore.ServingGroupRunning},
			)

			if tt.expectErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedOutdated, hasOutdatedRoles)
			assert.Equal(t, tt.expected, rolesToDelete)
		})
	}
}

func TestRolesToDeleteForRoleRollingUpdate_LegacyRoleTemplateHashFromControllerRevision(t *testing.T) {
	ns := "default"
	msName := "test-ms"
	groupName := "test-ms-0"
	oldRevision := "old-revision"
	roleName := "prefill"
	oldRole := workloadv1alpha1.Role{
		Name:     roleName,
		Replicas: ptr.To[int32](1),
		EntryTemplate: workloadv1alpha1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "nginx:old"}}},
		},
	}
	newRole := workloadv1alpha1.Role{
		Name:     roleName,
		Replicas: ptr.To[int32](1),
		EntryTemplate: workloadv1alpha1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "nginx:new"}}},
		},
	}
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: msName},
		Spec:       workloadv1alpha1.ModelServingSpec{Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{newRole}}},
	}

	kubeClient := kubefake.NewSimpleClientset()
	_, err := utils.CreateControllerRevision(context.TODO(), kubeClient, ms, oldRevision, []workloadv1alpha1.Role{oldRole})
	require.NoError(t, err)

	store := datastore.New()
	store.AddServingGroup(utils.GetNamespaceName(ms), 0, oldRevision)
	store.AddRole(utils.GetNamespaceName(ms), groupName, roleName, "prefill-0", oldRevision, "")
	require.NoError(t, store.UpdateRoleStatus(utils.GetNamespaceName(ms), groupName, roleName, "prefill-0", datastore.RoleRunning))

	controller := &ModelServingController{store: store, kubeClientSet: kubeClient}
	rolesToDelete, hasOutdatedRoles, err := controller.rolesToDeleteForRoleRollingUpdate(
		ms, datastore.ServingGroup{Name: groupName, Revision: oldRevision, Status: datastore.ServingGroupRunning},
	)

	require.NoError(t, err)
	assert.True(t, hasOutdatedRoles)
	assert.Equal(t, []roleToDelete{{roleName: roleName, roleID: "prefill-0"}}, rolesToDelete)
}
