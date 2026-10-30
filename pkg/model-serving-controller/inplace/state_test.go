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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

func podWithState(t *testing.T, state *State, ready bool, status corev1.ContainerStatus) *corev1.Pod {
	t.Helper()
	status.Name = "main"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{status},
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
	// "main" was patched to v2 while running container c1 with image id1, after one earlier update restart.
	started := &State{
		Revision: "v2",
		Patched:  map[string]PatchedContainer{"main": {Image: "registry/app:v2", PrevContainerID: "c1", PrevImageID: "id1"}},
		Expected: map[string]int32{"main": 2},
	}
	done := &State{Revision: "v2", Expected: map[string]int32{"main": 2}, Done: true}
	old := corev1.ContainerStatus{RestartCount: 1, ContainerID: "c1", Image: "registry/app:v1", ImageID: "id1"}
	updated := corev1.ContainerStatus{RestartCount: 2, ContainerID: "c2", Image: "registry/app:v2", ImageID: "id2"}

	tests := []struct {
		name                  string
		state                 *State
		ready                 bool
		status                corev1.ContainerStatus
		wantInProgress        bool
		wantCompleted         bool
		wantUpdating          bool
		wantUnexpectedRestart bool
	}{
		{name: "no state, no restarts"},
		{name: "no state, crash loop", status: corev1.ContainerStatus{RestartCount: 1}, wantUnexpectedRestart: true},
		{name: "started, stale ready before kubelet notices", state: started, ready: true, status: old, wantInProgress: true, wantUpdating: true},
		{name: "started, restarted but not ready", state: started, status: updated, wantInProgress: true, wantUpdating: true},
		{name: "started, restarted and ready", state: started, ready: true, status: updated, wantInProgress: true, wantCompleted: true},
		{
			name: "started, old image crashed and restarted", state: started, ready: true, wantInProgress: true, wantUpdating: true,
			status: corev1.ContainerStatus{RestartCount: 2, ContainerID: "c2", Image: "registry/app:v1", ImageID: "id1"},
		},
		{
			name: "started, runtime reports normalized name", state: started, ready: true, wantInProgress: true, wantCompleted: true,
			status: corev1.ContainerStatus{RestartCount: 2, ContainerID: "c2", Image: "registry/app:v2", ImageID: "id2"},
		},
		{
			name: "started, runtime reports a digest instead of the name", state: started, ready: true, wantInProgress: true, wantCompleted: true,
			status: corev1.ContainerStatus{RestartCount: 2, ContainerID: "c2", Image: "sha256:abcd", ImageID: "id2"},
		},
		{
			name: "started, crashed again after the update restart", state: started, status: corev1.ContainerStatus{RestartCount: 3, ContainerID: "c3", Image: "registry/app:v2", ImageID: "id2"},
			wantInProgress: true, wantUpdating: true, wantUnexpectedRestart: true,
		},
		{
			name: "first start after fixing a bad image has no restart", wantInProgress: true, wantCompleted: true, ready: true,
			state:  &State{Revision: "v2", Patched: map[string]PatchedContainer{"main": {Image: "app:v2"}}, Expected: map[string]int32{"main": 1}},
			status: corev1.ContainerStatus{ContainerID: "c1", Image: "docker.io/library/app:v2", ImageID: "id2"},
		},
		{name: "done, only the expected restarts", state: done, ready: true, status: updated},
		{name: "done, crashed afterwards", state: done, ready: true, status: corev1.ContainerStatus{RestartCount: 3}, wantUnexpectedRestart: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := podWithState(t, tt.state, tt.ready, tt.status)
			assert.Equal(t, tt.wantInProgress, InProgress(pod))
			assert.Equal(t, tt.wantCompleted, Completed(pod))
			assert.Equal(t, tt.wantUpdating, Updating(pod))
			assert.Equal(t, tt.wantUnexpectedRestart, UnexpectedRestart(pod))
		})
	}
}

func TestStartKeepsExpectedRestartsAcrossUpdates(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "uid", ResourceVersion: "7"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "a", Image: "a:v2"}, {Name: "b", Image: "b:v1"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "a", RestartCount: 1, ContainerID: "a1", ImageID: "ia2"},
			{Name: "b", ContainerID: "b0", ImageID: "ib1"},
		}},
	}
	pod.Annotations = map[string]string{workloadv1alpha1.InPlaceUpdateStateAnnotationKey: `{"revision":"v2","startedAt":null,"expected":{"a":1},"done":true}`}
	client := kubefake.NewSimpleClientset(pod)

	started, err := Start(context.Background(), client, pod, map[string]string{"a": "a:v2", "b": "b:v2"}, "v3", time.Now())
	require.NoError(t, err)
	require.True(t, started)

	got, err := client.CoreV1().Pods("ns").Get(context.Background(), "p", metav1.GetOptions{})
	require.NoError(t, err)
	state := StateOf(got)
	require.NotNil(t, state)
	assert.Equal(t, map[string]int32{"a": 1, "b": 1}, state.Expected, "an earlier update restart stays expected")
	assert.Equal(t, map[string]PatchedContainer{"b": {Image: "b:v2", PrevContainerID: "b0", PrevImageID: "ib1"}}, state.Patched)
	assert.Equal(t, "b:v2", got.Spec.Containers[1].Image)
}

func TestUnexpectedRestartInitContainer(t *testing.T) {
	pod := podWithState(t, nil, true, corev1.ContainerStatus{})
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

func TestStateOfMigratesLegacyAnnotation(t *testing.T) {
	annotate := func(raw string) *corev1.Pod {
		pod := podWithState(t, nil, true, corev1.ContainerStatus{RestartCount: 1, ContainerID: "c2", Image: "app:v2", ImageID: "id2"})
		pod.Annotations[workloadv1alpha1.InPlaceUpdateStateAnnotationKey] = raw
		return pod
	}
	tests := []struct {
		name           string
		raw            string
		wantInProgress bool
		wantCompleted  bool
		wantUnexpected bool
		wantRevision   string
	}{
		{
			name:         "finished update keeps its expected restart",
			raw:          `{"version":1,"phase":"Done","targetRevision":"v2","baseline":{"main":{"containerID":"c2","restartCount":1}}}`,
			wantRevision: "v2",
		},
		{
			name:           "unfinished update that already restarted completes",
			raw:            `{"version":1,"phase":"Patched","targetRevision":"v2","baseline":{"main":{"containerID":"c1","restartCount":0}},"patched":{"main":{"image":"app:v2","imageID":"id1"}}}`,
			wantInProgress: true, wantCompleted: true, wantRevision: "v2",
		},
		{
			name:           "unfinished update whose container has not restarted keeps waiting",
			raw:            `{"version":1,"phase":"Patched","targetRevision":"v2","baseline":{"main":{"containerID":"c2","restartCount":1}},"patched":{"main":{"image":"app:v2","imageID":"id2"}}}`,
			wantInProgress: true, wantRevision: "v2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := annotate(tt.raw)
			state := StateOf(pod)
			require.NotNil(t, state)
			assert.Equal(t, tt.wantRevision, state.Revision)
			assert.Equal(t, tt.wantInProgress, InProgress(pod))
			assert.Equal(t, tt.wantCompleted, Completed(pod))
			assert.Equal(t, tt.wantUnexpected, UnexpectedRestart(pod))
		})
	}
}
