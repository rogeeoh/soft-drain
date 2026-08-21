/*
Copyright 2026.

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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

const fixtureHash = "5449d4d8c8"

func rsFixture() *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "aaa-5449d4d8c8",
			Namespace: "default",
			Labels:    map[string]string{"app": "aaa", LabelPodTemplateHash: fixtureHash},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "aaa", Controller: ptr.To(true),
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "aaa", LabelPodTemplateHash: fixtureHash},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "nginx:1.15"}},
				},
			},
		},
	}
}

func TestBuildReplacement(t *testing.T) {
	rs := rsFixture()
	pod := buildReplacement(rs, "3f2a-uid")

	if pod.GenerateName != "aaa-5449d4d8c8-" {
		t.Errorf("generateName = %q, want %q", pod.GenerateName, "aaa-5449d4d8c8-")
	}
	if pod.Namespace != "default" {
		t.Errorf("namespace = %q", pod.Namespace)
	}
	if _, ok := pod.Labels[LabelPodTemplateHash]; ok {
		t.Error("pod-template-hash must be removed from replacement labels")
	}
	if pod.Labels["app"] != "aaa" {
		t.Errorf("app label = %q", pod.Labels["app"])
	}
	if pod.Labels[LabelReplaces] != "3f2a-uid" {
		t.Errorf("replaces label = %q", pod.Labels[LabelReplaces])
	}
	// cost left on a replacement makes that Pod die first in every scale-down after adoption
	if _, ok := pod.Annotations[AnnotationPodDeletionCost]; ok {
		t.Error("replacement must not carry pod-deletion-cost")
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != "nginx:1.15" {
		t.Errorf("spec not copied from rs template: %+v", pod.Spec)
	}
	// a polluted source template makes the next round decide on a template without the hash
	if rs.Spec.Template.Labels[LabelPodTemplateHash] != fixtureHash {
		t.Error("buildReplacement must not mutate rs.Spec.Template")
	}
}

func TestBuildReplacementNilMaps(t *testing.T) {
	rs := rsFixture()
	rs.Spec.Template.Labels = nil
	rs.Spec.Template.Annotations = nil
	pod := buildReplacement(rs, "uid")
	if pod.Labels[LabelReplaces] != "uid" {
		t.Errorf("replaces label = %q", pod.Labels[LabelReplaces])
	}
}

func TestDeploymentHealthy(t *testing.T) {
	base := func() *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Generation: 3},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(2))},
			Status: appsv1.DeploymentStatus{
				ObservedGeneration: 3,
				Replicas:           2,
				UpdatedReplicas:    2,
				AvailableReplicas:  2,
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*appsv1.Deployment)
		want   bool
	}{
		{"healthy", func(d *appsv1.Deployment) {}, true},
		{"stale observedGeneration", func(d *appsv1.Deployment) { d.Status.ObservedGeneration = 2 }, false},
		// a maxUnavailable: 0 rollout is caught by this term alone
		{"rollout in progress", func(d *appsv1.Deployment) { d.Status.Replicas = 3 }, false},
		{"below desired availability", func(d *appsv1.Deployment) { d.Status.AvailableReplicas = 1 }, false},
		// the >= boundary — catches a regression that wrongly tightens it to ==
		{"newer observedGeneration", func(d *appsv1.Deployment) { d.Status.ObservedGeneration = 4 }, true},
		{"more available than desired", func(d *appsv1.Deployment) { d.Status.AvailableReplicas = 3 }, true},
		{"nil replicas defaults to 1", func(d *appsv1.Deployment) {
			d.Spec.Replicas = nil
			d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.AvailableReplicas = 1, 1, 1
		}, true},
		{"nil replicas with nothing available", func(d *appsv1.Deployment) {
			d.Spec.Replicas = nil
			d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.AvailableReplicas = 0, 0, 0
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := base()
			tt.mutate(d)
			if got := deploymentHealthy(d); got != tt.want {
				t.Errorf("deploymentHealthy = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidReplacement(t *testing.T) {
	now := metav1.Now()
	base := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelReplaces: "uid"}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		}
	}
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
		want   bool
	}{
		{"valid running", func(p *corev1.Pod) {}, true},
		{"valid pending", func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending }, true},
		{"no replaces label", func(p *corev1.Pod) { delete(p.Labels, LabelReplaces) }, false},
		{"already adopted", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", Controller: ptr.To(true),
			}}
		}, false},
		// a non-controller ownerRef is not adoption — only a controller ownerRef counts
		{"non-controller ownerRef", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", Controller: ptr.To(false),
			}}
		}, true},
		{"failed", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }, false},
		{"succeeded", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }, false},
		{"terminating", func(p *corev1.Pod) { p.DeletionTimestamp = &now }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := base()
			tt.mutate(p)
			if got := validReplacement(p); got != tt.want {
				t.Errorf("validReplacement = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPodReady(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}}}
	if podReady(pod) {
		t.Error("PodReady=False must not be ready")
	}
	pod.Status.Conditions[1].Status = corev1.ConditionTrue
	if !podReady(pod) {
		t.Error("PodReady=True must be ready")
	}
	// when a node goes NotReady the kubelet really does produce Unknown
	pod.Status.Conditions[1].Status = corev1.ConditionUnknown
	if podReady(pod) {
		t.Error("PodReady=Unknown must not be ready")
	}
	if podReady(&corev1.Pod{}) {
		t.Error("pod without conditions must not be ready")
	}
}

func TestReplicaSetRef(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", Controller: ptr.To(true),
		}},
	}}
	if ref := replicaSetRef(pod); ref == nil || ref.Name != "rs" {
		t.Errorf("replicaSetRef = %+v", ref)
	}
	sts := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "StatefulSet", Name: "sts", Controller: ptr.To(true),
		}},
	}}
	if replicaSetRef(sts) != nil {
		t.Error("StatefulSet owner must not match")
	}
	nonController := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", Controller: ptr.To(false),
		}},
	}}
	if replicaSetRef(nonController) != nil {
		t.Error("non-controller ownerRef must not match")
	}
	if replicaSetRef(&corev1.Pod{}) != nil {
		t.Error("ownerless pod must not match")
	}
}

func TestDraining(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"false", false},
		{"", false},
	}
	for _, tt := range tests {
		node := &corev1.Node{}
		if tt.value != "" {
			node.Labels = map[string]string{LabelDrain: tt.value}
		}
		if got := draining(node); got != tt.want {
			t.Errorf("draining(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestOwnedByDeployment(t *testing.T) {
	rs := func(refs ...metav1.OwnerReference) *appsv1.ReplicaSet {
		return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{OwnerReferences: refs}}
	}
	if !ownedByDeployment(rs(metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: "Deployment", Name: "d", Controller: ptr.To(true),
	})) {
		t.Error("Deployment controller ref must match")
	}
	if ownedByDeployment(rs(metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: "Deployment", Name: "d", Controller: ptr.To(false),
	})) {
		t.Error("non-controller ref must not match")
	}
	if ownedByDeployment(rs(metav1.OwnerReference{
		APIVersion: "example.com/v1", Kind: "Deployment", Name: "d", Controller: ptr.To(true),
	})) {
		t.Error("same-kind CRD with different apiVersion must not match")
	}
	if ownedByDeployment(rs()) {
		t.Error("ownerless ReplicaSet must not match")
	}
}

func TestMergePatch(t *testing.T) {
	p := mergePatch(map[string]any{
		"metadata": map[string]any{"labels": map[string]any{"keep": "v", "remove": nil}},
	})
	if p.Type() != types.MergePatchType {
		t.Errorf("patch type = %v, want %v", p.Type(), types.MergePatchType)
	}
	data, err := p.Data(nil)
	if err != nil {
		t.Fatal(err)
	}
	// nil must serialize to JSON null for the merge patch to delete the key
	want := `{"metadata":{"labels":{"keep":"v","remove":null}}}`
	if string(data) != want {
		t.Errorf("patch = %s, want %s", data, want)
	}
}

func TestDrainActive(t *testing.T) {
	node := func(labels map[string]string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: labels}}
	}
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"no labels", nil, false},
		{"draining", map[string]string{LabelDrain: "true"}, true},
		{"in progress", map[string]string{LabelDrain: "true", LabelState: StateInProgress}, true},
		// a Complete node is one a human is about to reboot, so it stays banned
		{"complete", map[string]string{LabelDrain: "true", LabelState: StateComplete}, true},
		// a Cancelled node is an ordinary uncordoned node
		{"cancelled", map[string]string{LabelDrain: "true", LabelState: StateCancelled}, false},
		{"wrong label value", map[string]string{LabelDrain: "false"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := drainActive(node(tt.labels)); got != tt.want {
				t.Errorf("drainActive = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortPodsByAge(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	pods := []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "b", CreationTimestamp: t1}},
		{ObjectMeta: metav1.ObjectMeta{Name: "c", CreationTimestamp: t0}},
		{ObjectMeta: metav1.ObjectMeta{Name: "a", CreationTimestamp: t0}},
	}
	sortPodsByAge(pods)
	got := pods[0].Name + pods[1].Name + pods[2].Name
	if got != "acb" {
		t.Errorf("order = %q, want %q", got, "acb")
	}
}

func TestTemplatesEqualIgnoreHash(t *testing.T) {
	deployTpl := func() *corev1.PodTemplateSpec {
		return &corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "aaa"}},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "nginx:1.15"}},
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(rsTpl *corev1.PodTemplateSpec)
		want   bool
	}{
		// the same generation even though the rs template carries the hash
		{"same generation", func(*corev1.PodTemplateSpec) {}, true},
		{"image changed", func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Containers[0].Image = "nginx:1.16"
		}, false},
		// rollout restart changes only the template annotation (restartedAt)
		{"rollout restart", func(tpl *corev1.PodTemplateSpec) {
			tpl.Annotations = map[string]string{"kubectl.kubernetes.io/restartedAt": "2026-08-16T00:00:00Z"}
		}, false},
		{"env added", func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "X", Value: "1"}}
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rsTpl := deployTpl()
			rsTpl.Labels[LabelPodTemplateHash] = fixtureHash
			tt.mutate(rsTpl)
			if got := templatesEqualIgnoreHash(deployTpl(), rsTpl); got != tt.want {
				t.Errorf("templatesEqualIgnoreHash = %v, want %v", got, tt.want)
			}
			// if the comparison pollutes the source, later rounds decide differently
			if rsTpl.Labels[LabelPodTemplateHash] != fixtureHash {
				t.Error("comparison must not mutate its inputs")
			}
		})
	}
}
