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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

func newEligibilityModelServing() *workloadv1alpha1.ModelServing {
	podTemplate := func() workloadv1alpha1.PodTemplateSpec {
		return workloadv1alpha1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{Name: "download", Image: "example.com/downloader:v1"}},
				Containers: []corev1.Container{{
					Name:  "engine",
					Image: "example.com/engine:v1",
					Env:   []corev1.EnvVar{{Name: "MODEL", Value: "m1"}},
				}},
			},
		}
	}
	workerTemplate := podTemplate()
	return &workloadv1alpha1.ModelServing{
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas:      ptr.To[int32](2),
			SchedulerName: "volcano",
			Template: workloadv1alpha1.ServingGroup{
				Roles: []workloadv1alpha1.Role{{
					Name:           "decode",
					Replicas:       ptr.To[int32](1),
					EntryTemplate:  podTemplate(),
					WorkerReplicas: 1,
					WorkerTemplate: &workerTemplate,
				}},
			},
		},
	}
}

func withPrefill(ms *workloadv1alpha1.ModelServing) {
	ms.Spec.Template.Roles = append(ms.Spec.Template.Roles, *ms.Spec.Template.Roles[0].DeepCopy())
	ms.Spec.Template.Roles[1].Name = "prefill"
}

func setEntryImage(ms *workloadv1alpha1.ModelServing, role int, image string) {
	ms.Spec.Template.Roles[role].EntryTemplate.Spec.Containers[0].Image = image
}

func TestOnlyImagesChanged(t *testing.T) {
	tests := []struct {
		name   string
		base   func(ms *workloadv1alpha1.ModelServing)
		mutate func(ms *workloadv1alpha1.ModelServing)
		want   bool
	}{
		{
			name: "image change on entry and worker",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				setEntryImage(ms, 0, "example.com/engine:v2")
				ms.Spec.Template.Roles[0].WorkerTemplate.Spec.Containers[0].Image = "example.com/engine:v2"
			},
			want: true,
		},
		{
			name: "scaling and rollout fields change with the image",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Replicas = ptr.To[int32](4)
				ms.Spec.RecoveryPolicy = workloadv1alpha1.NoneRestartPolicy
				ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{
					Type:         workloadv1alpha1.ServingGroupRollingUpdate,
					UpdateMethod: workloadv1alpha1.InPlaceUpdateMethod,
				}
				ms.Spec.Template.GangPolicy = &workloadv1alpha1.GangPolicy{MinRoleReplicas: map[string]int32{"decode": 1}}
				role := &ms.Spec.Template.Roles[0]
				role.Replicas = ptr.To[int32](3)
				role.MaxUnavailable = ptr.To(intstr.FromInt32(2))
				setEntryImage(ms, 0, "example.com/engine:v2")
			},
			want: true,
		},
		{
			name: "changes that render the same Pods",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.SchedulerName = ""
				entry := &ms.Spec.Template.Roles[0].EntryTemplate
				entry.Spec.Containers[0].Env = append(entry.Spec.Containers[0].Env,
					corev1.EnvVar{Name: workloadv1alpha1.EntryAddressEnv, Value: "stale"})
				entry.Metadata = &workloadv1alpha1.Metadata{Labels: map[string]string{}}
				setEntryImage(ms, 0, "example.com/engine:v2")
			},
			want: true,
		},
		{
			name: "Roles reordered with an image change",
			base: withPrefill,
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				roles := ms.Spec.Template.Roles
				roles[0], roles[1] = roles[1], roles[0]
				setEntryImage(ms, 1, "example.com/engine:v2")
			},
			want: true,
		},
		{
			name: "environment change",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Env[0].Value = "m2"
			},
		},
		{
			name: "init container image change",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.InitContainers[0].Image = "example.com/downloader:v2"
			},
		},
		{
			name: "restartPolicy change with the image",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
				setEntryImage(ms, 0, "example.com/engine:v2")
			},
		},
		{
			name: "Pod template label change",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Metadata = &workloadv1alpha1.Metadata{Labels: map[string]string{"tier": "gold"}}
			},
		},
		{
			name: "container renamed",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Name = "server"
			},
		},
		{
			name: "Role added",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				withPrefill(ms)
			},
		},
		{
			name: "Role removed",
			base: withPrefill,
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles = ms.Spec.Template.Roles[:1]
			},
		},
		{
			name: "workerReplicas change",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].WorkerReplicas = 2
			},
		},
		{
			name: "worker template removed",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].WorkerReplicas = 0
				ms.Spec.Template.Roles[0].WorkerTemplate = nil
			},
		},
		{
			name: "scheduler change",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.SchedulerName = "default-scheduler"
			},
		},
		{
			name: "plugin change",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Plugins = []workloadv1alpha1.PluginSpec{{
					Name:   "demo",
					Type:   workloadv1alpha1.PluginTypeBuiltIn,
					Config: &apiextensionsv1.JSON{Raw: []byte(`{}`)},
				}}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldMS := newEligibilityModelServing()
			if tt.base != nil {
				tt.base(oldMS)
			}
			newMS := oldMS.DeepCopy()
			tt.mutate(newMS)
			got, err := OnlyImagesChanged(oldMS, newMS)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOnlyRoleImagesChanged(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(ms *workloadv1alpha1.ModelServing)
		roleName string
		want     bool
	}{
		{
			name: "image change in the Role",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				setEntryImage(ms, 0, "example.com/engine:v2")
			},
			roleName: "decode",
			want:     true,
		},
		{
			name: "scaling fields of the Role change with the image",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](3)
				ms.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt32(2))
				setEntryImage(ms, 0, "example.com/engine:v2")
			},
			roleName: "decode",
			want:     true,
		},
		{
			name: "changes in another Role and outside the Roles",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.SchedulerName = "default-scheduler"
				ms.Spec.Template.Roles[1].EntryTemplate.Spec.Containers[0].Env[0].Value = "m2"
				setEntryImage(ms, 0, "example.com/engine:v2")
			},
			roleName: "decode",
			want:     true,
		},
		{
			name: "environment change in the Role",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Env[0].Value = "m2"
			},
			roleName: "decode",
		},
		{
			name: "workerReplicas change in the Role",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles[0].WorkerReplicas = 2
			},
			roleName: "decode",
		},
		{
			name: "Role removed",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.Roles = ms.Spec.Template.Roles[:1]
			},
			roleName: "prefill",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldMS := newEligibilityModelServing()
			withPrefill(oldMS)
			newMS := oldMS.DeepCopy()
			tt.mutate(newMS)
			assert.Equal(t, tt.want, OnlyRoleImagesChanged(oldMS, newMS, tt.roleName))
		})
	}
}

func TestPlanUpdate(t *testing.T) {
	podOf := func(role string, entry bool) *corev1.Pod {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{workloadv1alpha1.RoleLabelKey: role}}}
		if entry {
			pod.Labels[workloadv1alpha1.EntryLabelKey] = "true"
		}
		return pod
	}
	source := newEligibilityModelServing()
	withPrefill(source)

	t.Run("changed images of each template", func(t *testing.T) {
		target := source.DeepCopy()
		setEntryImage(target, 0, "example.com/engine:v2")
		plan := PlanUpdate(source, target, "rev-1", "rev-2", "")
		require.Nil(t, plan.Unsupported)
		assert.Equal(t, map[string]string{"engine": "example.com/engine:v2"}, plan.ImagesForPod(podOf("decode", true)))
		assert.Empty(t, plan.ImagesForPod(podOf("decode", false)), "the worker template did not change")
		assert.Empty(t, plan.ImagesForPod(podOf("prefill", true)), "another Role did not change")
	})

	t.Run("only the planned Role", func(t *testing.T) {
		target := source.DeepCopy()
		setEntryImage(target, 0, "example.com/engine:v2")
		target.Spec.Template.Roles[1].EntryTemplate.Spec.Containers[0].Env[0].Value = "m2"
		plan := PlanUpdate(source, target, "rev-1", "rev-2", "decode")
		require.Nil(t, plan.Unsupported, "a change in another Role does not matter")
		assert.Equal(t, map[string]string{"engine": "example.com/engine:v2"}, plan.ImagesForPod(podOf("decode", true)))
	})

	t.Run("unsupported change", func(t *testing.T) {
		target := source.DeepCopy()
		target.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Env[0].Value = "m2"
		plan := PlanUpdate(source, target, "rev-1", "rev-2", "")
		require.NotNil(t, plan.Unsupported)
		assert.Equal(t, ReasonIneligible, plan.Unsupported.Reason)
		assert.Contains(t, plan.Unsupported.Message, "revision rev-2 changes fields other than regular-container images from revision rev-1")
	})
}
