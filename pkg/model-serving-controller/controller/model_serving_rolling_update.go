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

	"k8s.io/klog/v2"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

func isServingGroupRollingDeletion(ms *workloadv1alpha1.ModelServing, status datastore.ServingGroupStatus) bool {
	return status == datastore.ServingGroupRolling &&
		(ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate)
}

// manageRollingUpdate resolves the lifecycle aspect of an update, checking outdated sets,
// enforcing strict Unavailable quota constraints, and actively evicting ServingGroups
// or respective Role workloads falling outside the current partition to enforce the rollback/rollforward.
//
// Main processing steps:
//  1. Identify the boundary for the currently active rollout partition.
//  2. Filter outdated groups (mismatched revision) that are allowed to be updated.
//  3. For ServingGroupRollingUpdate, enforce the ServingGroup-level maxUnavailable budget.
//  4. For RoleRollingUpdate, update outdated roles using each Role's maxUnavailable budget.
func (c *ModelServingController) manageRollingUpdate(ctx context.Context, ms *workloadv1alpha1.ModelServing, revision string) error {
	servingGroupList, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	if err != nil {
		return fmt.Errorf("cannot get ServingGroupList from store, err:%v", err)
	}

	partition, _, _ := c.getPartition(modelServingRolloutConfig(ms), modelServingReplicas(ms))
	if partition > len(servingGroupList) {
		return nil
	}
	groupsAfterPartition := servingGroupList[partition:]

	// Separate groups by processing priority. Rolling groups are always continued;
	// new candidates are selected from not-running groups before running groups.
	var rollingGroups []datastore.ServingGroup
	var notRunningOutdatedGroups []datastore.ServingGroup
	var runningOutdatedGroups []datastore.ServingGroup
	newServingGroupUnavailableCount := 0
	for _, sg := range groupsAfterPartition {
		if sg.Status == datastore.ServingGroupRolling {
			rollingGroups = append(rollingGroups, sg)
			continue
		}

		outdated := sg.Revision != revision
		// The assumption here is that updates will proceed normally when the `role`'s partition is sorted in descending order.
		if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.Type == workloadv1alpha1.RoleRollingUpdate {
			_, hasOutdatedRoles, err := c.rolesToDeleteForRoleRollingUpdate(ms, sg)
			if err != nil {
				return err
			}
			outdated = hasOutdatedRoles
		}
		if !outdated {
			if sg.Status != datastore.ServingGroupRunning {
				newServingGroupUnavailableCount++
			}
			// not-outdated servingGroup only to count the unavailable ones
			continue
		}

		if sg.Status != datastore.ServingGroupRunning {
			notRunningOutdatedGroups = append(notRunningOutdatedGroups, sg)
		} else {
			runningOutdatedGroups = append(runningOutdatedGroups, sg)
		}
	}

	// Reference: https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/deployment/rolling.go#L102
	maxUnavailable, err := utils.GetMaxUnavailable(ms)
	if err != nil {
		return fmt.Errorf("failed to calculate maxUnavailable: %v", err)
	}
	minAvailable := modelServingReplicas(ms) - maxUnavailable
	// Only unavailable new-version groups consume the update budget. An unavailable
	// old-version group can be replaced without further reducing availability.
	maxScaleDown := len(servingGroupList) - minAvailable - newServingGroupUnavailableCount

	// Put not-running groups at the end so they are selected before running
	// groups when taking the last maxScaleDown candidates.
	var groupsToUpdate []datastore.ServingGroup
	allOutdatedGroups := append(runningOutdatedGroups, notRunningOutdatedGroups...)
	if maxScaleDown <= 0 {
		groupsToUpdate = rollingGroups
	} else {
		groupsToUpdate = selectServingGroupsForRollingUpdate(maxScaleDown, rollingGroups, allOutdatedGroups)
	}

	if len(groupsToUpdate) == 0 {
		klog.V(4).Infof("No ServingGroups can be updated for ModelServing %s/%s: replicas=%d, minAvailable=%d, newUnavailable=%d",
			ms.Namespace, ms.Name, len(servingGroupList), minAvailable, newServingGroupUnavailableCount)
		return nil
	}

	// Delete outdated groups or roles according to the selected rollout strategy.
	updateCount, err := c.deleteOutdatedResourcesForRollingUpdate(ctx, ms, groupsToUpdate, revision)
	if err != nil {
		return err
	}

	if updateCount > 0 {
		klog.V(4).Infof("Deleted %d outdated ServingGroups for ModelServing %s (partition=%d)", updateCount, ms.Name, partition)
	}
	return nil
}

// selectServingGroupsForRollingUpdate selects the groups processed in this
// reconcile. Existing Rolling groups are always continued. The outdated groups
// are ordered with running groups first and not-running groups last, so taking
// the tail prioritizes not-running groups and larger ordinals.
func selectServingGroupsForRollingUpdate(
	maxScaleDown int,
	rollingGroups []datastore.ServingGroup,
	outdatedGroups []datastore.ServingGroup,
) []datastore.ServingGroup {
	selected := make([]datastore.ServingGroup, 0, max(maxScaleDown, len(rollingGroups)))
	// Rolling groups represent updates already in progress and must not be
	// stranded when the current budget is exhausted or reduced.
	for i := len(rollingGroups) - 1; i >= 0; i-- {
		selected = append(selected, rollingGroups[i])
	}
	remaining := maxScaleDown - len(rollingGroups)
	if remaining <= 0 {
		return selected
	}
	start := max(0, len(outdatedGroups)-remaining)
	// outdatedGroups contains running groups first and not-running groups last.
	// Iterate backwards to process Rolling -> notRunning -> Running groups, with
	// larger ordinals first in each category.
	for i := len(outdatedGroups) - 1; i >= start; i-- {
		selected = append(selected, outdatedGroups[i])
	}
	return selected
}

// deleteOutdatedResourcesForRollingUpdate deletes outdated resources during rolling update
// for the groups selected by manageRollingUpdate.
func (c *ModelServingController) deleteOutdatedResourcesForRollingUpdate(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groups []datastore.ServingGroup,
	revision string,
) (int, error) {
	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate {
		return c.deleteOutdatedServingGroups(ctx, ms, groups)
	}

	return c.deleteOutdatedRoles(ctx, ms, groups, revision)
}

// deleteOutdatedServingGroups deletes outdated ServingGroups
// for `ServingGroupRollingUpdate`.
func (c *ModelServingController) deleteOutdatedServingGroups(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groups []datastore.ServingGroup,
) (int, error) {
	updateCount := 0

	for _, sg := range groups {
		if err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), sg.Name, datastore.ServingGroupRolling); err != nil {
			return updateCount, fmt.Errorf("failed to set ServingGroup %s status to Rolling: %v", sg.Name, err)
		}
		klog.V(2).Infof("ServingGroup %s will be terminated for update (status=%s)", sg.Name, sg.Status)
		if err := c.deleteServingGroup(ctx, ms, sg.Name); err != nil {
			return updateCount, err
		}
		updateCount++
	}

	return updateCount, nil
}

// deleteOutdatedRoles deletes outdated Roles for `RoleRollingUpdate`.
func (c *ModelServingController) deleteOutdatedRoles(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groups []datastore.ServingGroup,
	revision string,
) (int, error) {
	updateCount := 0

	for _, sg := range groups {
		finished, rolesToDelete, err := c.tryFinishRoleRollingUpdate(ms, sg.Name, revision)
		if err != nil {
			return updateCount, err
		}
		if finished {
			c.enqueueModelServing(ms)
			continue
		}
		if len(rolesToDelete) == 0 {
			continue
		}
		if err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), sg.Name, datastore.ServingGroupRolling); err != nil {
			return updateCount, fmt.Errorf("failed to set ServingGroup %s status to Rolling: %v", sg.Name, err)
		}
		for _, role := range rolesToDelete {
			klog.V(2).Infof("Role %s/%s in ServingGroup %s will be terminated for update", role.roleName, role.roleID, sg.Name)
			c.DeleteRole(ctx, ms, sg.Name, role.roleName, role.roleID)
		}
		updateCount++
	}

	return updateCount, nil
}

// tryFinishRoleRollingUpdate returns the Roles that can be updated in this
// reconcile. If no actionable outdated Role remains, it changes a Rolling
// ServingGroup to Running after every Role is ready.
func (c *ModelServingController) tryFinishRoleRollingUpdate(ms *workloadv1alpha1.ModelServing, groupName, revision string) (bool, []roleToDelete, error) {
	groupRevision, _ := c.store.GetServingGroupRevision(utils.GetNamespaceName(ms), groupName)
	rolesToDelete, hasOutdatedRoles, err := c.rolesToDeleteForRoleRollingUpdate(ms, datastore.ServingGroup{Name: groupName, Revision: groupRevision})
	if err != nil {
		return false, nil, err
	}
	if hasOutdatedRoles {
		return false, rolesToDelete, nil
	}

	if c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), groupName) != datastore.ServingGroupRolling {
		return false, nil, nil
	}

	// The role has been successfully upgraded. However, it is not yet running.
	ready, err := c.checkServingGroupReady(ms, groupName)
	if err != nil {
		return false, nil, err
	}
	if !ready {
		return false, nil, nil
	}

	var revisionErr, statusErr error
	defer func() {
		if revisionErr == nil && statusErr == nil {
			return
		}

		// Keep the ServingGroup in its pre-completion state so the next
		// reconcile can retry both updates as one logical operation.
		if err := c.store.UpdateServingGroupRevision(utils.GetNamespaceName(ms), groupName, groupRevision); err != nil {
			klog.Errorf("failed to roll back ServingGroup %s revision: %v", groupName, err)
		}
		if err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), groupName, datastore.ServingGroupRolling); err != nil {
			klog.Errorf("failed to roll back ServingGroup %s status to Rolling: %v", groupName, err)
		}
		c.enqueueModelServing(ms)
	}()

	revisionErr = c.store.UpdateServingGroupRevision(utils.GetNamespaceName(ms), groupName, revision)
	if revisionErr != nil {
		return false, nil, fmt.Errorf("failed to update ServingGroup %s revision: %v", groupName, revisionErr)
	}
	statusErr = c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), groupName, datastore.ServingGroupRunning)
	if statusErr != nil {
		return false, nil, fmt.Errorf("failed to set ServingGroup %s status to Running: %v", groupName, statusErr)
	}
	klog.V(2).Infof("Role rolling update completed for ServingGroup %s", groupName)
	return true, nil, nil
}

type roleToDelete struct {
	roleName string
	roleID   string
}

func (c *ModelServingController) rolesToDeleteForRoleRollingUpdate(ms *workloadv1alpha1.ModelServing, sg datastore.ServingGroup) ([]roleToDelete, bool, error) {
	roleSpecByName := make(map[string]workloadv1alpha1.Role, len(ms.Spec.Template.Roles))
	for _, role := range ms.Spec.Template.Roles {
		roleSpecByName[role.Name] = role
	}

	allRoles, err := c.store.GetRolesByGroup(utils.GetNamespaceName(ms), sg.Name)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get roles for ServingGroup %s: %v", sg.Name, err)
	}

	var rolesToDelete []roleToDelete
	hasOutdatedRoles := false
	for _, roleSpec := range ms.Spec.Template.Roles {
		roleList, err := c.store.GetRoleList(utils.GetNamespaceName(ms), sg.Name, roleSpec.Name)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get roles for ServingGroup %s, role %s: %v", sg.Name, roleSpec.Name, err)
		}

		partition, _, partitionErr := c.getPartition(&roleSpec.RollingUpdateConfiguration, roleReplicas(roleSpec))
		if partitionErr != nil {
			return nil, false, fmt.Errorf("failed to parse partition for role %s: %v", roleSpec.Name, partitionErr)
		}
		// roleList is sorted by ordinal. Partition protects the first N Role
		// instances, regardless of gaps in their ordinal values.
		rolesAfterPartition := roleList[min(partition, len(roleList)):]
		outdatedRoles, newUnavailable := c.outdatedRoles(ms, sg, roleSpec, rolesAfterPartition)
		if len(outdatedRoles) == 0 {
			continue
		}
		hasOutdatedRoles = true
		maxScaleDown, err := calMaxScaleDown(roleSpec, outdatedRoles, len(roleList), newUnavailable)
		if err != nil {
			klog.Errorf("failed to calculate maxScaleDown for role %s in ServingGroup %s: %v", roleSpec.Name, sg.Name, err)
		}

		selectedRoles, err := selectOutdatedRolesToDelete(roleSpec.Name, outdatedRoles, maxScaleDown)
		if err != nil {
			return nil, false, err
		}
		rolesToDelete = append(rolesToDelete, selectedRoles...)
	}

	// handle the case when there are roles whose roleSpec has been deleted in the new revision. Those roles should be deleted directly since they are all outdated.
	for roleName, roles := range allRoles {
		if _, ok := roleSpecByName[roleName]; ok {
			continue
		}
		for roleID, role := range roles {
			if role.Status == datastore.RoleDeleting {
				continue
			}
			hasOutdatedRoles = true
			rolesToDelete = append(rolesToDelete, roleToDelete{roleName: roleName, roleID: roleID})
		}
	}

	return rolesToDelete, hasOutdatedRoles, nil
}

func (c *ModelServingController) outdatedRoles(ms *workloadv1alpha1.ModelServing, sg datastore.ServingGroup, roleSpec workloadv1alpha1.Role, roleList []datastore.Role) ([]datastore.Role, int) {
	expectedHash := utils.CalRoleTemplateHash(roleSpec)
	outdatedRoles := make([]datastore.Role, 0, len(roleList))
	// record the number of roles that is in rollingupdate but not ready yet.
	newUnavailable := 0
	for _, role := range roleList {
		if role.Status == datastore.RoleDeleting {
			newUnavailable++
			continue
		}
		observedHash, ok := c.resolveRoleTemplateHashForComparison(ms, sg, roleSpec.Name, role)
		if !ok {
			klog.Warningf("skip outdated check for role %s/%s in ServingGroup %s because roleTemplateHash is missing and cannot be inferred", roleSpec.Name, role.Name, sg.Name)
			continue
		}
		if observedHash != expectedHash {
			outdatedRoles = append(outdatedRoles, role)
			continue
		}
		if role.Status != datastore.RoleRunning {
			newUnavailable++
		}
	}
	return outdatedRoles, newUnavailable
}

func selectOutdatedRolesToDelete(roleName string, outdatedRoles []datastore.Role, maxScaleDown int) ([]roleToDelete, error) {
	rolesToDelete := make([]roleToDelete, 0, min(len(outdatedRoles), maxScaleDown))
	for i := len(outdatedRoles) - 1; i >= 0 && maxScaleDown > 0; i-- {
		role := outdatedRoles[i]
		rolesToDelete = append(rolesToDelete, roleToDelete{roleName: roleName, roleID: role.Name})
		maxScaleDown--
	}
	return rolesToDelete, nil
}

func calMaxScaleDown(role workloadv1alpha1.Role, outdatedRoles []datastore.Role, allReplicas, newUnavailable int) (int, error) {
	maxUnavailable, configured, err := utils.GetMaxUnavailableForRole(role)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate maxUnavailable for role %s: %v", role.Name, err)
	}
	if !configured {
		return len(outdatedRoles), nil
	}
	expectedReplicas := 1
	if role.Replicas != nil {
		expectedReplicas = int(*role.Replicas)
	}
	minAvailable := expectedReplicas - maxUnavailable
	if minAvailable < 0 {
		minAvailable = 0
	}
	maxScaleDown := allReplicas - minAvailable - newUnavailable
	if maxScaleDown < 0 {
		maxScaleDown = 0
	}
	return maxScaleDown, nil
}
