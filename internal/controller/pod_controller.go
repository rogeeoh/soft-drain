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

	appsv1 "k8s.io/api/apps/v1"
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

// PodReconciler is the reclamation path keyed on the replacement Pod (DESIGN.md step 3).
// When a pruned ReplicaSet takes the targets with it, the node traversal never looks at
// the replacements again, so a path starting from the replacement itself is required.
// The decision is the same.
type PodReconciler struct {
	client.Client
	// Reader reads straight from the API server.
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
	// Pods with a controller ownerRef are never deleted
	if metav1.GetControllerOf(pod) != nil {
		return ctrl.Result{}, nil
	}

	needed, err := r.replacementNeeded(ctx, pod, types.UID(uid))
	if err != nil {
		return ctrl.Result{}, err
	}
	if needed {
		// Cancellation and target eviction produce no event for this Pod, so re-decide periodically
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	deleted, err := deleteReplacement(ctx, r.Client, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if deleted {
		log.Info("Deleted replacement Pod no longer needed by its ReplicaSet", "pod", req.String())
	}
	return ctrl.Result{}, nil
}

// replacementNeeded reports whether this Pod is within its ReplicaSet's replacement
// count — one per live target on a counted draining node, the oldest kept first
// (DESIGN.md step 3). The rule is the node round's, re-derived from the Pod's side.
func (r *PodReconciler) replacementNeeded(ctx context.Context, repl *corev1.Pod, rsUID types.UID) (bool, error) {
	// A dead replacement can neither become Ready nor be adopted
	if repl.Status.Phase == corev1.PodFailed || repl.Status.Phase == corev1.PodSucceeded {
		return false, nil
	}

	// The label carries a UID, and there is no Get by UID — the namespace list finds it.
	rss := &appsv1.ReplicaSetList{}
	if err := r.Reader.List(ctx, rss, client.InNamespace(repl.Namespace)); err != nil {
		return false, err
	}
	var rs *appsv1.ReplicaSet
	for i := range rss.Items {
		if rss.Items[i].UID == rsUID {
			rs = &rss.Items[i]
			break
		}
	}
	// a target's ReplicaSet is owned by a Deployment — the same definition the node round uses
	if rs == nil || !ownedByDeployment(rs) {
		return false, nil
	}

	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.InNamespace(repl.Namespace)); err != nil {
		return false, err
	}
	nodeCounted := map[string]bool{}
	live := 0
	var peers []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if ref := replicaSetRef(p); ref != nil && ref.UID == rsUID {
			if p.DeletionTimestamp != nil || p.Spec.NodeName == "" ||
				p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
				continue
			}
			counted, err := r.nodeCounted(ctx, nodeCounted, p.Spec.NodeName)
			if err != nil {
				return false, err
			}
			if counted {
				live++
			}
			continue
		}
		if p.Labels[LabelReplaces] == string(rsUID) && validReplacement(p) {
			peers = append(peers, p)
		}
	}
	if live == 0 {
		return false, nil
	}
	sortPodsByAge(peers)
	for i, p := range peers {
		if p.UID == repl.UID {
			return i < live, nil
		}
	}
	// gone from the list between the Get and the List — deleting resolves to NotFound
	return false, nil
}

// nodeCounted reports whether targets on that node are counted (drainCounting), with a
// per-call cache.
func (r *PodReconciler) nodeCounted(ctx context.Context, cache map[string]bool, name string) (bool, error) {
	if c, ok := cache[name]; ok {
		return c, nil
	}
	node := &corev1.Node{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			cache[name] = false
			return false, nil
		}
		return false, err
	}
	cache[name] = drainCounting(node)
	return cache[name], nil
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
