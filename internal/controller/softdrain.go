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
	"context"
	"encoding/json"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	LabelDrain    = "soft-drain.io/drain"
	LabelState    = "soft-drain.io/state"
	LabelReplaces = "soft-drain.io/replaces"

	AnnotationCordoned        = "soft-drain.io/cordoned-by-controller"
	AnnotationPodDeletionCost = "controller.kubernetes.io/pod-deletion-cost"

	// int32 최솟값. 이 값을 쓰는 게 우리뿐이라 값이 이것이면 우리가 붙인 것이다.
	PodDeletionCost = "-2147483648"

	LabelPodTemplateHash = appsv1.DefaultDeploymentUniqueLabelKey

	StateInProgress = "InProgress"
	StateComplete   = "Complete"
	StateCancelled  = "Cancelled"
)

func draining(node *corev1.Node) bool {
	return node.Labels[LabelDrain] == "true"
}

// drainActive는 그 노드의 drain이 실제로 진행 중인지 본다. Cancelled 노드는
// uncordon된 보통 노드다 — 대체 Pod을 유지할 이유도, 착지를 막을 이유도 없다.
func drainActive(node *corev1.Node) bool {
	return draining(node) && node.Labels[LabelState] != StateCancelled
}

// replicaSetRef는 Pod의 controller가 apps/v1 ReplicaSet일 때 그 참조를 준다.
func replicaSetRef(pod *corev1.Pod) *metav1.OwnerReference {
	ref := metav1.GetControllerOf(pod)
	if ref == nil || ref.Kind != "ReplicaSet" || ref.APIVersion != "apps/v1" {
		return nil
	}
	return ref
}

func ownedByDeployment(rs *appsv1.ReplicaSet) bool {
	ref := metav1.GetControllerOf(rs)
	return ref != nil && ref.Kind == "Deployment" && ref.APIVersion == "apps/v1"
}

// validReplacement는 DESIGN.md 3단계의 "있는 것" 판정이다.
func validReplacement(pod *corev1.Pod) bool {
	return pod.Labels[LabelReplaces] != "" &&
		metav1.GetControllerOf(pod) == nil &&
		pod.Status.Phase != corev1.PodFailed &&
		pod.Status.Phase != corev1.PodSucceeded &&
		pod.DeletionTimestamp == nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// deploymentHealthy는 DESIGN.md 4단계의 Healthy(D) 판정이다.
// replicas == updatedReplicas 항이 "Pod을 가진 ReplicaSet이 하나뿐"을 잡는다.
func deploymentHealthy(d *appsv1.Deployment) bool {
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.Replicas == d.Status.UpdatedReplicas &&
		d.Status.AvailableReplicas >= ptr.Deref(d.Spec.Replicas, 1)
}

// buildReplacement는 rs.spec.template에서 대체 Pod을 만든다.
// 살아 있는 Pod을 베끼면 nodeName과 webhook이 넣은 사이드카가 따라온다.
func buildReplacement(rs *appsv1.ReplicaSet, targetUID types.UID) *corev1.Pod {
	tpl := rs.Spec.Template.DeepCopy()

	labels := tpl.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	// rs.spec.template.metadata.labels에는 hash가 이미 들어 있다.
	// hash가 있으면 ReplicaSet이 Pending인 Pod을 데려가서 그 Pod부터 지운다.
	delete(labels, LabelPodTemplateHash)
	labels[LabelReplaces] = string(targetUID)

	// pod-deletion-cost는 타깃에만 쓴다. 대체 Pod에 남으면 입양 후
	// 다음 스케일다운마다 그 Pod이 1순위로 죽는다.
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: rs.Name + "-",
			Namespace:    rs.Namespace,
			Labels:       labels,
			Annotations:  tpl.Annotations,
		},
		Spec: tpl.Spec,
	}
}

func sortPodsByAge(pods []*corev1.Pod) {
	sort.Slice(pods, func(i, j int) bool {
		if !pods[i].CreationTimestamp.Equal(&pods[j].CreationTimestamp) {
			return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
		}
		return pods[i].Name < pods[j].Name
	})
}

func mergePatch(doc map[string]any) client.Patch {
	raw, err := json.Marshal(doc)
	if err != nil {
		panic(err) // map[string]any 리터럴만 들어오므로 도달 불가
	}
	return client.RawPatch(types.MergePatchType, raw)
}

// deleteReplacement는 읽었던 UID와 resourceVersion을 preconditions로 걸어 지운다.
// 판정과 삭제 사이에 hash가 붙어 ReplicaSet이 데려간 Pod이면 resourceVersion이
// 바뀌어 삭제가 거부된다(deleted=false). 다음 라운드가 다시 판정한다.
func deleteReplacement(ctx context.Context, c client.Writer, pod *corev1.Pod) (deleted bool, err error) {
	err = c.Delete(ctx, pod, client.Preconditions{UID: &pod.UID, ResourceVersion: &pod.ResourceVersion})
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err), apierrors.IsConflict(err):
		return false, nil
	default:
		return false, err
	}
}
