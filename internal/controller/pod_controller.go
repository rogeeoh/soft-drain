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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// PodReconciler는 대체 Pod을 키로 하는 회수 경로다 (DESIGN.md 3단계).
// ReplicaSet prune으로 타깃이 사라지면 노드 순회로는 대체 Pod을 쳐다볼 일이 없어서
// 대체 Pod 자체에서 출발하는 경로가 따로 있어야 한다. 판정은 같다.
type PodReconciler struct {
	client.Client
	// Reader는 API 서버를 직접 읽는다.
	Reader client.Reader
}

// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete

func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := r.Reader.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	uid := pod.Labels[LabelReplaces]
	if uid == "" || pod.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	// controller ownerRef가 있는 Pod은 지우지 않는다
	if metav1.GetControllerOf(pod) != nil {
		return ctrl.Result{}, nil
	}

	needed, err := r.replacementNeeded(ctx, pod, types.UID(uid))
	if err != nil {
		return ctrl.Result{}, err
	}
	if needed {
		// 취소나 타깃 eviction은 이 Pod의 이벤트를 만들지 않으므로 주기적으로 다시 판정한다
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	deleted, err := deleteReplacement(ctx, r.Client, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if deleted {
		log.Info("Deleted replacement Pod without a live target", "pod", req.String())
	}
	return ctrl.Result{}, nil
}

// replacementNeeded는 타깃이 살아서 drain 중인 노드에 있는지 본다.
func (r *PodReconciler) replacementNeeded(ctx context.Context, repl *corev1.Pod, uid types.UID) (bool, error) {
	// 죽은 대체 Pod은 Ready가 될 수도 입양될 수도 없다
	if repl.Status.Phase == corev1.PodFailed || repl.Status.Phase == corev1.PodSucceeded {
		return false, nil
	}

	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.InNamespace(repl.Namespace)); err != nil {
		return false, err
	}
	var tgt *corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].UID == uid {
			tgt = &pods.Items[i]
			break
		}
	}
	if tgt == nil || tgt.DeletionTimestamp != nil ||
		tgt.Status.Phase == corev1.PodFailed || tgt.Status.Phase == corev1.PodSucceeded {
		return false, nil
	}
	if tgt.Spec.NodeName == "" {
		return false, nil
	}

	node := &corev1.Node{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: tgt.Spec.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	// Cancelled 노드의 drain은 끝난 것이다 — 대체 Pod을 유지할 이유가 없다
	return drainActive(node), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	hasReplaces := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetLabels()[LabelReplaces] != ""
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}, builder.WithPredicates(hasReplaces)).
		Named("pod").
		Complete(r)
}
