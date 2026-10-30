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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

func podWithState(t *testing.T, state *State, ready bool, restarts int32) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", RestartCount: restarts}},
		},
	}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	if state != nil {
		raw, err := json.Marshal(state)
		require.NoError(t, err)
		pod.Annotations[workloadv1alpha1.InPlaceUpdateStateAnnotationKey] = string(raw)
	}
	return pod
}

func TestStatePredicates(t *testing.T) {
	started := &State{Revision: "v2", Baseline: map[string]int32{"main": 2}}
	done := &State{Revision: "v2", Baseline: map[string]int32{"main": 2}, Done: true}

	tests := []struct {
		name                  string
		state                 *State
		ready                 bool
		restarts              int32
		wantInProgress        bool
		wantCompleted         bool
		wantUpdating          bool
		wantUnexpectedRestart bool
	}{
		{name: "no state, no restarts"},
		{name: "no state, crash loop", restarts: 1, wantUnexpectedRestart: true},
		{name: "started, stale ready before restart", state: started, ready: true, restarts: 2, wantInProgress: true, wantUpdating: true},
		{name: "started, restarting", state: started, restarts: 3, wantInProgress: true, wantUpdating: true},
		{name: "started, restarted and ready", state: started, ready: true, restarts: 3, wantInProgress: true, wantCompleted: true},
		{name: "done, only the expected restart", state: done, ready: true, restarts: 3},
		{name: "done, crashed afterwards", state: done, ready: true, restarts: 4, wantUnexpectedRestart: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := podWithState(t, tt.state, tt.ready, tt.restarts)
			assert.Equal(t, tt.wantInProgress, InProgress(pod))
			assert.Equal(t, tt.wantCompleted, Completed(pod))
			assert.Equal(t, tt.wantUpdating, Updating(pod))
			assert.Equal(t, tt.wantUnexpectedRestart, UnexpectedRestart(pod))
		})
	}
}

func TestUnexpectedRestartInitContainer(t *testing.T) {
	pod := podWithState(t, nil, true, 0)
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", RestartCount: 1}}
	assert.True(t, UnexpectedRestart(pod))

	pod.Status.Phase = corev1.PodSucceeded
	assert.False(t, UnexpectedRestart(pod))
}

func TestTargetImages(t *testing.T) {
	template := func(image string) workloadv1alpha1.PodTemplateSpec {
		return workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}}
	}
	worker := template("worker:v2")
	ms := &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{Template: workloadv1alpha1.ServingGroup{
		Roles: []workloadv1alpha1.Role{{Name: "r", EntryTemplate: template("entry:v2"), WorkerTemplate: &worker}},
	}}}

	tests := []struct {
		name   string
		labels map[string]string
		want   map[string]string
	}{
		{name: "entry pod", labels: map[string]string{workloadv1alpha1.RoleLabelKey: "r", workloadv1alpha1.EntryLabelKey: "true"}, want: map[string]string{"main": "entry:v2"}},
		{name: "worker pod", labels: map[string]string{workloadv1alpha1.RoleLabelKey: "r"}, want: map[string]string{"main": "worker:v2"}},
		{name: "unknown role", labels: map[string]string{workloadv1alpha1.RoleLabelKey: "gone"}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: tt.labels}}
			assert.Equal(t, tt.want, TargetImages(ms, pod))
		})
	}
}
