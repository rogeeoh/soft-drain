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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	LabelDrain    = "soft-drain.com/drain"
	LabelState    = "soft-drain.com/state"
	LabelReplaces = "soft-drain.com/replaces"

	AnnotationCordoned        = "soft-drain.com/cordoned-by-controller"
	AnnotationPodDeletionCost = "controller.kubernetes.io/pod-deletion-cost"

	// The int32 minimum. Nothing else writes this value, so a Pod carrying it is ours.
	PodDeletionCost = "-2147483648"

	LabelPodTemplateHash = appsv1.DefaultDeploymentUniqueLabelKey

	StateInProgress = "InProgress"
	StateComplete   = "Complete"
	StateCancelled  = "Cancelled"
)

func draining(node *corev1.Node) bool {
	return node.Labels[LabelDrain] == "true"
}

// drainActive reports whether the node's drain is actually running. A Cancelled node
// is an ordinary uncordoned node — no reason to keep replacements or to ban landings.
func drainActive(node *corev1.Node) bool {
	return draining(node) && node.Labels[LabelState] != StateCancelled
}

// replicaSetRef returns the reference when the Pod's controller is an apps/v1 ReplicaSet.
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

// validReplacement is the "does exist" test of DESIGN.md step 3.
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

// deploymentHealthy is the Healthy(D) test of DESIGN.md step 4.
// The replicas == updatedReplicas term catches "only one ReplicaSet has Pods".
func deploymentHealthy(d *appsv1.Deployment) bool {
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.Replicas == d.Status.UpdatedReplicas &&
		d.Status.AvailableReplicas >= ptr.Deref(d.Spec.Replicas, 1)
}

// templatesEqualIgnoreHash is the same comparison as the Deployment controller's EqualIgnoreHash.
// Equal but for the pod-template-hash label means rs is that Deployment's current generation.
func templatesEqualIgnoreHash(a, b *corev1.PodTemplateSpec) bool {
	a2, b2 := a.DeepCopy(), b.DeepCopy()
	delete(a2.Labels, LabelPodTemplateHash)
	delete(b2.Labels, LabelPodTemplateHash)
	return apiequality.Semantic.DeepEqual(a2, b2)
}

// buildReplacement builds a replacement Pod from rs.spec.template.
// Copying the living Pod would bring nodeName and webhook-injected sidecars along.
func buildReplacement(rs *appsv1.ReplicaSet, targetUID types.UID) *corev1.Pod {
	tpl := rs.Spec.Template.DeepCopy()

	labels := tpl.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	// rs.spec.template.metadata.labels already carries the hash.
	// With the hash the ReplicaSet adopts a Pending Pod and deletes that one first.
	delete(labels, LabelPodTemplateHash)
	labels[LabelReplaces] = string(targetUID)

	// pod-deletion-cost is for targets only. Left on a replacement, that Pod dies first
	// in every scale-down after adoption.
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
		panic(err) // unreachable: only map[string]any literals are passed in
	}
	return client.RawPatch(types.MergePatchType, raw)
}

// deleteReplacement deletes with the UID and resourceVersion we read as preconditions.
// If the hash was attached in between and a ReplicaSet took the Pod, the resourceVersion
// changed and the deletion is rejected (deleted=false). The next round decides again.
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
