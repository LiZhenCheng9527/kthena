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
	"cmp"
	"context"
	"fmt"
	"slices"

	"k8s.io/klog/v2"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

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
	// Separate outdated groups into two categories: not-running and running
	// We prioritize updating not-running outdated groups first
	var notRunningOutdatedGroups []datastore.ServingGroup
	var runningOutdatedGroups []datastore.ServingGroup
	if partition >= len(servingGroupList) {
		// All servingGroups are protected by partition, so we should not update any group. Return directly.
		return nil
	}
	groupsAfterPartition := servingGroupList[partition:]

	newServingGroupUnavailableCount := 0
	for _, sg := range groupsAfterPartition {
		if sg.Status != datastore.ServingGroupRunning {
			if sg.Revision == revision {
				newServingGroupUnavailableCount++
			} else {
				notRunningOutdatedGroups = append(notRunningOutdatedGroups, sg)
			}
		} else if sg.Revision != revision {
			runningOutdatedGroups = append(runningOutdatedGroups, sg)
		}
	}

	maxScaleDown := 0
	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate {
		maxUnavailable, err := utils.GetMaxUnavailable(ms)
		if err != nil {
			return fmt.Errorf("failed to calculate maxUnavailable: %v", err)
		}

		// TODO(hzxuzhonghu): reuse calMaxScaleDown

		// Calculate the minimum number of available ServingGroups required
		// Refer to https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/deployment/rolling.go
		// Check if we can scale down. We can scale down in the following 2 cases:
		// * Some old servingGroups are unhealthy, we could safely scale down those unhealthy servingGroups
		//   since that won't further increase unavailability.
		// * New servingGroup has scaled up and its replicas become ready, then we can scale down old servingGroups
		//   in a further step.
		minAvailable := int(*ms.Spec.Replicas) - maxUnavailable
		maxScaleDown = len(servingGroupList) - minAvailable - newServingGroupUnavailableCount
		if maxScaleDown <= 0 {
			klog.V(4).Infof("No ServingGroups can be updated for ModelServing %s/%s: maxScaleDown=%d",
				ms.Namespace, ms.Name, maxScaleDown)
			return nil
		}
	}

	// Delete outdated groups or roles according to the selected rollout strategy.
	updateCount, err := c.deleteOutdatedResourcesForRollingUpdate(ctx, ms, maxScaleDown, notRunningOutdatedGroups, runningOutdatedGroups, revision)
	if err != nil {
		return err
	}

	if updateCount > 0 {
		klog.V(4).Infof("Deleted %d outdated ServingGroups for ModelServing %s (partition=%d)", updateCount, ms.Name, partition)
	}
	return nil
}

// deleteOutdatedResourcesForRollingUpdate deletes outdated resources during rolling update
// respecting maxScaleDown constraints.
func (c *ModelServingController) deleteOutdatedResourcesForRollingUpdate(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	maxScaleDown int,
	notRunningOutdatedGroups []datastore.ServingGroup,
	runningOutdatedGroups []datastore.ServingGroup,
	revision string,
) (int, error) {
	// Combine all outdated groups.
	// Delete in descending order by sequence number. Prioritise deletion of servingGroups in notRunning status.
	// Therefore, servingGroups in notRunning status should be placed at the end.
	allOutdatedGroups := append(runningOutdatedGroups, notRunningOutdatedGroups...)

	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate {
		return c.deleteOutdatedServingGroups(ctx, ms, maxScaleDown, allOutdatedGroups)
	}

	return c.deleteOutdatedRoles(ctx, ms, allOutdatedGroups, revision)
}

// deleteOutdatedServingGroups deletes outdated ServingGroups
// for `ServingGroupRollingUpdate`.
func (c *ModelServingController) deleteOutdatedServingGroups(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	maxScaleDown int,
	groups []datastore.ServingGroup,
) (int, error) {
	updateCount := 0

	// Iterate from end to start to delete largest ordinals first.
	for i := len(groups) - 1; i >= 0 && updateCount < maxScaleDown; i-- {
		sg := groups[i]
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

	// Iterate from end to start to delete largest ordinals first.
	for i := len(groups) - 1; i >= 0; i-- {
		sg := groups[i]
		rolesToDelete, hasOutdatedRoles, err := c.rolesToDeleteForRoleRollingUpdate(ms, sg)
		if err != nil {
			return updateCount, err
		}
		if !hasOutdatedRoles {
			c.updateServingGroupRevisionIfNoOutdatedRoles(ms, sg.Name, revision)
			continue
		}
		if len(rolesToDelete) == 0 {
			continue
		}
		for _, role := range rolesToDelete {
			klog.V(2).Infof("Role %s/%s in ServingGroup %s will be terminated for update", role.roleName, role.roleID, sg.Name)
			c.DeleteRole(ctx, ms, sg.Name, role.roleName, role.roleID)
		}
		updateCount++
	}

	return updateCount, nil
}

func (c *ModelServingController) updateServingGroupRevisionIfNoOutdatedRoles(ms *workloadv1alpha1.ModelServing, groupName, revision string) {
	if err := c.store.UpdateServingGroupRevision(utils.GetNamespaceName(ms), groupName, revision); err != nil {
		klog.Errorf("failed to update ServingGroup %s revision: %v", groupName, err)
		return
	}
	klog.V(2).Infof("Updated ServingGroup %s revision to latest: %s", groupName, revision)
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

		outdatedRoles, newUnavailable := c.outdatedRoles(ms, sg, roleSpec, roleList)
		partition, partitionConfigured, partitionErr := c.getPartition(&roleSpec.RollingUpdateConfiguration, roleReplicas(roleSpec))
		if partitionErr != nil {
			return nil, false, fmt.Errorf("failed to parse partition for role %s: %v", roleSpec.Name, partitionErr)
		}
		if partitionConfigured && partition > 0 && len(outdatedRoles) > 0 {
			protected := make(map[string]struct{}, partition)
			for _, role := range roleList {
				_, ordinal := utils.GetParentNameAndOrdinal(role.Name)
				if ordinal < partition {
					protected[role.Name] = struct{}{}
				}
			}
			filtered := outdatedRoles[:0]
			for _, r := range outdatedRoles {
				if _, ok := protected[r.Name]; ok {
					continue
				}
				filtered = append(filtered, r)
			}
			outdatedRoles = filtered
		}
		if len(outdatedRoles) == 0 {
			if partitionConfigured && partition > 0 {
				expectedHash := utils.CalRoleTemplateHash(roleSpec)
				for _, role := range roleList {
					_, ordinal := utils.GetParentNameAndOrdinal(role.Name)
					if ordinal >= partition {
						continue
					}
					if role.Status == datastore.RoleDeleting {
						continue
					}
					observedHash, ok := c.resolveRoleTemplateHashForComparison(ms, sg, roleSpec.Name, role)
					if ok && observedHash != expectedHash {
						hasOutdatedRoles = true
						break
					}
				}
			}
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
		} else if role.Status != datastore.RoleRunning {
			newUnavailable++
		}
	}

	slices.SortFunc(outdatedRoles, func(a, b datastore.Role) int {
		if a.Status != b.Status {
			if a.Status != datastore.RoleRunning {
				return -1
			}
			return 1
		}
		_, aOrdinal := utils.GetParentNameAndOrdinal(a.Name)
		_, bOrdinal := utils.GetParentNameAndOrdinal(b.Name)
		return cmp.Compare(bOrdinal, aOrdinal)
	})
	return outdatedRoles, newUnavailable
}

func selectOutdatedRolesToDelete(roleName string, outdatedRoles []datastore.Role, maxScaleDown int) ([]roleToDelete, error) {
	rolesToDelete := make([]roleToDelete, 0, len(outdatedRoles))
	for _, role := range outdatedRoles {
		if maxScaleDown == 0 {
			break
		}
		maxScaleDown--
		rolesToDelete = append(rolesToDelete, roleToDelete{roleName: roleName, roleID: role.Name})
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
