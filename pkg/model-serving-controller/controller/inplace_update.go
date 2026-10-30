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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/inplace"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

const defaultInPlaceProgressDeadline = 1800 * time.Second

// inPlaceDecision is the outcome of evaluating one update unit.
type inPlaceDecision int

const (
	// decisionInPlace means only container images differ and the unit can be patched.
	decisionInPlace inPlaceDecision = iota
	// decisionWait means the unit cannot be touched this round (e.g. a Pod is terminating).
	decisionWait
	// decisionRecreate means the unit must follow the regular delete-and-recreate path.
	decisionRecreate
)

// updatesInPlace reports whether the rolling update of ms may use in-place updates.
// When it is false the controller behaves exactly as for the Recreate method.
func (c *ModelServingController) updatesInPlace(ms *workloadv1alpha1.ModelServing) bool {
	if inplace.UpdateMethodOf(ms) != workloadv1alpha1.InPlaceUpdateMethod {
		return false
	}
	chain, err := c.buildPluginChain(ms)
	if err != nil {
		klog.Warningf("cannot build plugin chain of ModelServing %s/%s, fall back to recreate: %v", ms.Namespace, ms.Name, err)
		return false
	}
	if name, unsupported := chain.PluginWithoutInPlaceUpdate(); unsupported {
		klog.V(2).Infof("plugin %s of ModelServing %s/%s does not support in-place update, fall back to recreate", name, ms.Namespace, ms.Name)
		return false
	}
	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate {
		// With no unavailable budget an in-place update could never start. Recreate uses surge instead.
		maxUnavailable, err := utils.GetMaxUnavailable(ms)
		return err == nil && maxUnavailable > 0
	}
	return true
}

// roleUpdatesInPlace is updatesInPlace for RoleRollingUpdate, where each Role owns its budget.
func (c *ModelServingController) roleUpdatesInPlace(ms *workloadv1alpha1.ModelServing, roleName string) bool {
	if !c.updatesInPlace(ms) {
		return false
	}
	for _, role := range ms.Spec.Template.Roles {
		if role.Name != roleName {
			continue
		}
		maxUnavailable, configured, err := utils.GetMaxUnavailableForRole(role)
		return err == nil && (!configured || maxUnavailable > 0)
	}
	return false
}

// updateServingGroup replaces deleting an outdated ServingGroup. It patches the
// images in place when that is possible and otherwise recreates the group.
func (c *ModelServingController) updateServingGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, sg datastore.ServingGroup) error {
	if !c.updatesInPlace(ms) {
		return c.deleteServingGroup(ctx, ms, sg.Name)
	}
	pods, err := c.getPodsByIndex(GroupNameKey, fmt.Sprintf("%s/%s", ms.Namespace, sg.Name))
	if err != nil {
		return err
	}
	revision := utils.ModelServingRevision(ms)
	decision, reason := c.decideInPlace(ctx, ms, pods, revision, func(oldMS *workloadv1alpha1.ModelServing, _ *corev1.Pod) bool {
		same, err := inplace.OnlyImagesChanged(oldMS, ms)
		return err == nil && same
	})
	switch decision {
	case decisionWait:
		return nil
	case decisionRecreate:
		c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "InPlaceUpdateFallback",
			fmt.Sprintf("ServingGroup %s is recreated instead of updated in place: %s", sg.Name, reason))
		return c.deleteServingGroup(ctx, ms, sg.Name)
	}
	return c.startInPlaceUpdate(ctx, ms, sg.Name, pods, revision)
}

// updateRole is updateServingGroup for a single Role replica.
func (c *ModelServingController) updateRole(ctx context.Context, ms *workloadv1alpha1.ModelServing, groupName, roleName, roleID string) error {
	if !c.roleUpdatesInPlace(ms, roleName) {
		c.DeleteRole(ctx, ms, groupName, roleName, roleID)
		return nil
	}
	pods, err := c.getPodsByIndex(RoleIDKey, fmt.Sprintf("%s/%s/%s/%s", ms.Namespace, groupName, roleName, roleID))
	if err != nil {
		return err
	}
	revision := utils.ModelServingRevision(ms)
	decision, reason := c.decideInPlace(ctx, ms, pods, revision, func(oldMS *workloadv1alpha1.ModelServing, pod *corev1.Pod) bool {
		return inplace.OnlyRoleImagesChanged(oldMS, ms, utils.GetRoleName(pod))
	})
	switch decision {
	case decisionWait:
		return nil
	case decisionRecreate:
		c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "InPlaceUpdateFallback",
			fmt.Sprintf("Role %s/%s in ServingGroup %s is recreated instead of updated in place: %s", roleName, roleID, groupName, reason))
		c.DeleteRole(ctx, ms, groupName, roleName, roleID)
		return nil
	}
	return c.startInPlaceUpdate(ctx, ms, groupName, pods, revision)
}

// decideInPlace checks every Pod of an update unit. imagesOnly compares the
// template the Pod was created from (its labelled revision) with the desired one.
func (c *ModelServingController) decideInPlace(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	pods []*corev1.Pod,
	revision string,
	imagesOnly func(oldMS *workloadv1alpha1.ModelServing, pod *corev1.Pod) bool,
) (inPlaceDecision, string) {
	if len(pods) == 0 {
		return decisionRecreate, "no pods found"
	}
	sources := map[string]*workloadv1alpha1.ModelServing{}
	for _, pod := range pods {
		if utils.IsPodTerminating(pod) {
			return decisionWait, ""
		}
		if pod.Spec.RestartPolicy != corev1.RestartPolicyAlways {
			return decisionRecreate, fmt.Sprintf("pod %s restartPolicy is not Always", pod.Name)
		}
		podRevision := utils.ObjectRevision(pod)
		if podRevision == revision {
			continue
		}
		oldMS, ok := sources[podRevision]
		if !ok {
			var err error
			if oldMS, err = c.modelServingAtRevision(ctx, ms, podRevision); err != nil {
				return decisionRecreate, fmt.Sprintf("cannot read revision %s: %v", podRevision, err)
			}
			sources[podRevision] = oldMS
		}
		if !imagesOnly(oldMS, pod) {
			return decisionRecreate, "changes other than container images"
		}
	}
	return decisionInPlace, ""
}

func (c *ModelServingController) modelServingAtRevision(ctx context.Context, ms *workloadv1alpha1.ModelServing, revision string) (*workloadv1alpha1.ModelServing, error) {
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	if err != nil {
		return nil, err
	}
	if cr == nil {
		return nil, fmt.Errorf("ControllerRevision not found")
	}
	return utils.ModelServingAtRevision(ms, cr)
}

// startInPlaceUpdate patches every Pod of the unit and marks it unavailable at once,
// so the rollout budget accounts for the unit before the kubelet has restarted anything.
func (c *ModelServingController) startInPlaceUpdate(ctx context.Context, ms *workloadv1alpha1.ModelServing, groupName string, pods []*corev1.Pod, revision string) error {
	// Persist the target template so a later recovery never depends on the mutable spec.
	if _, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, ms, revision, ms.Spec.Template.Roles); err != nil {
		return err
	}
	for _, pod := range pods {
		if utils.ObjectRevision(pod) == revision && !inplace.InProgress(pod) {
			continue
		}
		started, err := inplace.Start(ctx, c.kubeClientSet, pod, inplace.TargetImages(ms, pod), revision, time.Now())
		if err != nil {
			return err
		}
		if started {
			klog.V(2).Infof("Pod %s/%s is updated in place to revision %s", pod.Namespace, pod.Name, revision)
		}
		if err := c.markPodUnavailable(ms, groupName, pod); err != nil {
			return err
		}
	}
	c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "InPlaceUpdateStarted",
		fmt.Sprintf("Updating images in place in ServingGroup %s to revision %s", groupName, revision))
	return nil
}

// inPlaceUnit is the set of Pods that move to the new revision together.
type inPlaceUnit struct {
	groupName string
	// roleName and roleID are empty for a ServingGroup unit.
	roleName string
	roleID   string
	pods     []*corev1.Pod
}

// completeInPlaceUpdates finishes in-place updates. It is level-triggered: it only
// reads Pod state, so it is safe to run after a controller restart or repeatedly.
func (c *ModelServingController) completeInPlaceUpdates(ctx context.Context, ms *workloadv1alpha1.ModelServing, revision string) error {
	if inplace.UpdateMethodOf(ms) != workloadv1alpha1.InPlaceUpdateMethod {
		return nil
	}
	groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	if err != nil {
		return nil
	}
	roleMode := ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.Type == workloadv1alpha1.RoleRollingUpdate
	for _, sg := range groups {
		if sg.Status == datastore.ServingGroupDeleting {
			continue
		}
		pods, err := c.getPodsByIndex(GroupNameKey, fmt.Sprintf("%s/%s", ms.Namespace, sg.Name))
		if err != nil {
			return err
		}
		for _, unit := range splitInPlaceUnits(ms, sg.Name, pods, roleMode) {
			if err := c.reconcileInPlaceUnit(ctx, ms, unit, revision); err != nil {
				return err
			}
		}
	}
	return nil
}

func splitInPlaceUnits(ms *workloadv1alpha1.ModelServing, groupName string, pods []*corev1.Pod, roleMode bool) []*inPlaceUnit {
	byKey := map[string]*inPlaceUnit{}
	var units []*inPlaceUnit
	for _, pod := range pods {
		if !utils.IsOwnedByModelServingWithUID(pod, ms.UID) {
			continue
		}
		key, roleName, roleID := groupName, "", ""
		if roleMode {
			roleName, roleID = utils.GetRoleName(pod), utils.GetRoleID(pod)
			key = roleName + "/" + roleID
		}
		unit, ok := byKey[key]
		if !ok {
			unit = &inPlaceUnit{groupName: groupName, roleName: roleName, roleID: roleID}
			byKey[key] = unit
			units = append(units, unit)
		}
		unit.pods = append(unit.pods, pod)
	}
	return units
}

func (c *ModelServingController) reconcileInPlaceUnit(ctx context.Context, ms *workloadv1alpha1.ModelServing, unit *inPlaceUnit, revision string) error {
	var earliest time.Time
	inProgress := false
	for _, pod := range unit.pods {
		if utils.IsPodTerminating(pod) {
			return nil
		}
		state := inplace.StateOf(pod)
		if state == nil || state.Done {
			continue
		}
		inProgress = true
		if state.Revision != revision {
			// The desired revision moved on; the regular rollout re-patches this unit.
			return nil
		}
		if earliest.IsZero() || state.StartedAt.Time.Before(earliest) {
			earliest = state.StartedAt.Time
		}
	}
	if !inProgress {
		return nil
	}

	if !unitCompleted(unit.pods, revision) {
		return c.enforceInPlaceDeadline(ctx, ms, unit, earliest)
	}

	for _, pod := range unit.pods {
		if !inplace.InProgress(pod) {
			continue
		}
		if err := inplace.Complete(ctx, c.kubeClientSet, pod, revision, c.roleTemplateHashOf(ms, pod)); err != nil {
			return err
		}
	}
	c.recordInPlaceRevision(ms, unit, revision)
	c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "InPlaceUpdateCompleted",
		fmt.Sprintf("In-place update of ServingGroup %s to revision %s completed", unit.groupName, revision))
	c.enqueueModelServing(ms)
	return nil
}

// unitCompleted is true once every Pod has restarted with its new image and is Ready.
// Pods that were never part of the update must already carry the target revision.
func unitCompleted(pods []*corev1.Pod, revision string) bool {
	for _, pod := range pods {
		if inplace.InProgress(pod) {
			if !inplace.Completed(pod) {
				return false
			}
			continue
		}
		if utils.ObjectRevision(pod) != revision {
			return false
		}
	}
	return true
}

func (c *ModelServingController) enforceInPlaceDeadline(ctx context.Context, ms *workloadv1alpha1.ModelServing, unit *inPlaceUnit, startedAt time.Time) error {
	deadline := defaultInPlaceProgressDeadline
	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.ProgressDeadlineSeconds != nil {
		deadline = time.Duration(*ms.Spec.RolloutStrategy.ProgressDeadlineSeconds) * time.Second
	}
	remaining := time.Until(startedAt.Add(deadline))
	if remaining > 0 {
		c.enqueueModelServingAfter(ms, remaining)
		return nil
	}
	c.emitRoleStatusEvent(ms, corev1.EventTypeWarning, "InPlaceUpdateFallback",
		fmt.Sprintf("In-place update of ServingGroup %s exceeded progress deadline %s, recreating", unit.groupName, deadline))
	if unit.roleName == "" {
		return c.deleteServingGroup(ctx, ms, unit.groupName)
	}
	c.DeleteRole(ctx, ms, unit.groupName, unit.roleName, unit.roleID)
	return nil
}

func (c *ModelServingController) roleTemplateHashOf(ms *workloadv1alpha1.ModelServing, pod *corev1.Pod) string {
	roleName := utils.GetRoleName(pod)
	for _, role := range ms.Spec.Template.Roles {
		if role.Name == roleName {
			return utils.CalRoleTemplateHash(role)
		}
	}
	return ""
}

// recordInPlaceRevision brings the in-memory store in line with the flipped Pod labels.
func (c *ModelServingController) recordInPlaceRevision(ms *workloadv1alpha1.ModelServing, unit *inPlaceUnit, revision string) {
	name := utils.GetNamespaceName(ms)
	seen := map[string]bool{}
	for _, pod := range unit.pods {
		roleName, roleID := utils.GetRoleName(pod), utils.GetRoleID(pod)
		key := roleName + "/" + roleID
		if seen[key] {
			continue
		}
		seen[key] = true
		if err := c.store.UpdateRoleRevision(name, unit.groupName, roleName, roleID, revision, c.roleTemplateHashOf(ms, pod)); err != nil {
			klog.Warningf("failed to update revision of role %s/%s in ServingGroup %s: %v", roleName, roleID, unit.groupName, err)
		}
	}
	if unit.roleName == "" {
		if err := c.store.UpdateServingGroupRevision(name, unit.groupName, revision); err != nil {
			klog.Warningf("failed to update revision of ServingGroup %s: %v", unit.groupName, err)
		}
	}
}
