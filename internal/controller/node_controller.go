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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NodeReconciler empties nodes carrying the drain label (DESIGN.md "What the controller does").
type NodeReconciler struct {
	client.Client
	// Reader reads straight from the API server. The cache only tells us when to look again.
	Reader   client.Reader
	Recorder events.EventRecorder
}

// target is a Pod on the node whose owner is a ReplicaSet whose owner is a Deployment.
type target struct {
	pod *corev1.Pod
	rs  *appsv1.ReplicaSet
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := r.Reader.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !draining(node) {
		return ctrl.Result{}, r.restore(ctx, node)
	}
	return r.drain(ctx, node)
}

// restore takes back what we left on a node whose drain label is gone.
// Replacements are collected by PodReconciler's rule: delete unless the target is on a draining node.
func (r *NodeReconciler) restore(ctx context.Context, node *corev1.Node) error {
	if node.Labels[LabelState] == "" && node.Annotations[AnnotationCordoned] == "" {
		return nil
	}
	log := logf.FromContext(ctx)

	if err := r.sweepDeletionCost(ctx, node.Name); err != nil {
		return err
	}

	doc := map[string]any{
		"metadata": map[string]any{
			"labels":      map[string]any{LabelState: nil},
			"annotations": map[string]any{AnnotationCordoned: nil},
		},
	}
	if node.Annotations[AnnotationCordoned] == "true" && node.Spec.Unschedulable {
		doc["spec"] = map[string]any{"unschedulable": false}
	}
	if err := r.Patch(ctx, node, mergePatch(doc)); err != nil {
		return err
	}
	log.Info("Restored node after drain label removal", "node", node.Name)
	return nil
}

// sweepDeletionCost removes the pod-deletion-cost we wrote on the node's Pods.
func (r *NodeReconciler) sweepDeletionCost(ctx context.Context, nodeName string) error {
	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		// delete it only when the value is exactly ours
		if pod.Annotations[AnnotationPodDeletionCost] != PodDeletionCost {
			continue
		}
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{AnnotationPodDeletionCost: nil}},
		})
		if err := r.Patch(ctx, pod, patch); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// cancel lets go of a node uncordoned mid-drain. The cordon is the premise of this
// approach, and without it there is no point continuing. We do not re-cordon and fight
// the human. The annotation goes too — the cordon it recorded is already gone by their
// hand, and a cordon they place later must not be mistaken for ours by restore.
func (r *NodeReconciler) cancel(ctx context.Context, node *corev1.Node) error {
	if err := r.sweepDeletionCost(ctx, node.Name); err != nil {
		return err
	}
	patch := mergePatch(map[string]any{
		"metadata": map[string]any{
			"labels":      map[string]any{LabelState: StateCancelled},
			"annotations": map[string]any{AnnotationCordoned: nil},
		},
	})
	if err := r.Patch(ctx, node, patch); err != nil {
		return err
	}
	r.Recorder.Eventf(node, nil, corev1.EventTypeWarning, "DrainCancelled", "CancelDrain",
		"Cancelled drain because the node was uncordoned")
	logf.FromContext(ctx).Info("Cancelled drain because the node was uncordoned", "node", node.Name)
	return nil
}

func (r *NodeReconciler) drain(ctx context.Context, node *corev1.Node) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Cancelled is a latch. Nothing happens until the drain label is removed.
	if node.Labels[LabelState] == StateCancelled {
		return ctrl.Result{}, nil
	}

	// InProgress and Complete are only ever set after confirming the cordon. If the node
	// is no longer unschedulable, someone uncordoned it — mid-drain the premise that
	// guaranteed termination is gone, and after completion a human lifted our cordon and
	// decided to use the node again. Either way we let go.
	state := node.Labels[LabelState]
	if (state == StateInProgress || state == StateComplete) && !node.Spec.Unschedulable {
		return ctrl.Result{}, r.cancel(ctx, node)
	}

	// Complete is a latch. While the cordon holds, we do not act.
	if state == StateComplete {
		return ctrl.Result{}, nil
	}

	// 1. marking the node — the annotation is written only when we actually changed the value
	if !node.Spec.Unschedulable {
		patch := mergePatch(map[string]any{
			"spec":     map[string]any{"unschedulable": true},
			"metadata": map[string]any{"annotations": map[string]any{AnnotationCordoned: "true"}},
		})
		if err := r.Patch(ctx, node, patch); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Cordoned node", "node", node.Name)
	}

	targets, err := r.collectTargets(ctx, node)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 6. completion — terminating targets count too
	if len(targets) == 0 {
		return ctrl.Result{}, r.complete(ctx, node)
	}

	if node.Labels[LabelState] != StateInProgress {
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{LabelState: StateInProgress}},
		})
		if err := r.Patch(ctx, node, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.markTargets(ctx, targets); err != nil {
		return ctrl.Result{}, err
	}

	replsByUID, err := r.collectReplacements(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	retry, err := r.reconcileReplacements(ctx, node, targets, replsByUID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if retry {
		// The world changed between the decision and the deletion. Rather than carry a stale
		// list into hand-over, end the round. The next round decides again from scratch.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err := r.handOver(ctx, node, targets, replsByUID); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// complete sets Complete. The cordoned-by-controller annotation stays —
// the cordon is still ours, and restore takes it back when the drain label goes.
func (r *NodeReconciler) complete(ctx context.Context, node *corev1.Node) error {
	if node.Labels[LabelState] == StateComplete {
		return nil
	}
	patch := mergePatch(map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{LabelState: StateComplete},
		},
	})
	if err := r.Patch(ctx, node, patch); err != nil {
		return err
	}
	r.Recorder.Eventf(node, nil, corev1.EventTypeNormal, "DrainComplete", "Drain",
		"All Deployment Pods left the node")
	logf.FromContext(ctx).Info("Drain complete", "node", node.Name)
	return nil
}

// markTargets writes pod-deletion-cost on the targets (DESIGN.md step 2).
func (r *NodeReconciler) markTargets(ctx context.Context, targets []target) error {
	for _, t := range targets {
		if t.pod.Annotations[AnnotationPodDeletionCost] == PodDeletionCost {
			continue
		}
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{AnnotationPodDeletionCost: PodDeletionCost}},
		})
		if err := r.Patch(ctx, t.pod, patch); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// reconcileReplacements matches the set that should exist against the set that does
// (DESIGN.md step 3). Too few, create; a surplus on the same target, delete. Replacements
// seated on a draining node or superseded by a rollout are deleted here too — neither can
// reach adoption. If such a deletion is rejected by its preconditions, a retry ends the
// round, keeping a stale list out of hand-over so the next round decides from fresh state.
func (r *NodeReconciler) reconcileReplacements(ctx context.Context, node *corev1.Node, targets []target, replsByUID map[string][]*corev1.Pod) (retry bool, err error) {
	log := logf.FromContext(ctx)
	drainingNodes := map[string]bool{node.Name: true}
	deploys := map[types.NamespacedName]*appsv1.Deployment{}
	for _, t := range targets {
		// A terminating target already has the ReplicaSet making its own replacement.
		// Any replacement left over is deleted by the reclamation path (PodReconciler), same rule.
		if t.pod.DeletionTimestamp != nil {
			continue
		}
		uid := string(t.pod.UID)

		// A replacement seated on a draining node is deleted regardless of readiness.
		var kept []*corev1.Pod
		for _, repl := range replsByUID[uid] {
			landed, err := r.nodeDraining(ctx, drainingNodes, repl.Spec.NodeName)
			if err != nil {
				return false, err
			}
			if !landed {
				kept = append(kept, repl)
				continue
			}
			deleted, err := deleteReplacement(ctx, r.Client, repl)
			if err != nil {
				return false, err
			}
			if !deleted {
				return true, nil
			}
			r.Recorder.Eventf(node, repl, corev1.EventTypeWarning, "ReplacementOnDrainingNode",
				"DeleteReplacementPod",
				"Deleted replacement Pod %s/%s that landed on draining node %s",
				repl.Namespace, repl.Name, repl.Spec.NodeName)
			log.Info("Deleted replacement Pod that landed on draining node",
				"pod", repl.Namespace+"/"+repl.Name, "landedOn", repl.Spec.NodeName, "node", node.Name)
		}
		replsByUID[uid] = kept

		// If a rollout is performing the target's migration instead, delete the replacement
		// and create none until the generation comes back.
		superseded, err := r.supersededByRollout(ctx, deploys, t.rs)
		if err != nil {
			return false, err
		}
		if superseded {
			for _, repl := range kept {
				deleted, err := deleteReplacement(ctx, r.Client, repl)
				if err != nil {
					return false, err
				}
				if !deleted {
					return true, nil
				}
				r.Recorder.Eventf(node, repl, corev1.EventTypeNormal, "ReplacementSuperseded",
					"DeleteReplacementPod",
					"Deleted replacement Pod %s/%s superseded by a rollout of its Deployment",
					repl.Namespace, repl.Name)
				log.Info("Deleted replacement Pod superseded by a rollout",
					"pod", repl.Namespace+"/"+repl.Name, "target", t.pod.Namespace+"/"+t.pod.Name, "node", node.Name)
			}
			replsByUID[uid] = nil
			continue
		}

		switch {
		case len(kept) == 0:
			repl := buildReplacement(t.rs, t.pod.UID)
			if err := r.Create(ctx, repl); err != nil {
				r.Recorder.Eventf(node, nil, corev1.EventTypeWarning, "ReplacementCreateRejected",
					"CreateReplacementPod",
					"Failed to create replacement Pod for %s/%s: %v", t.pod.Namespace, t.pod.Name, err)
				log.Error(err, "Failed to create replacement Pod",
					"target", t.pod.Namespace+"/"+t.pod.Name, "node", node.Name)
				continue
			}
			log.Info("Created replacement Pod",
				"pod", repl.Namespace+"/"+repl.Name, "target", t.pod.Namespace+"/"+t.pod.Name, "node", node.Name)
		case len(kept) > 1:
			// Keep only the oldest one. The trim exists so this round's hand-over does not
			// look again at a Pod it just deleted.
			for _, extra := range kept[1:] {
				if _, err := deleteReplacement(ctx, r.Client, extra); err != nil {
					return false, err
				}
			}
			replsByUID[uid] = kept[:1]
		}
	}
	return false, nil
}

// handOver attaches the hash to a Ready replacement so the ReplicaSet takes it (DESIGN.md step 4).
// The decision is per Deployment, and whichever is ready hands over first.
func (r *NodeReconciler) handOver(ctx context.Context, node *corev1.Node, targets []target, replsByUID map[string][]*corev1.Pod) error {
	log := logf.FromContext(ctx)
	healthyDeploys := map[types.NamespacedName]bool{}
	for _, t := range targets {
		if t.pod.DeletionTimestamp != nil {
			continue
		}
		// replacements seated on a draining node were already deleted by reconcileReplacements
		existing := replsByUID[string(t.pod.UID)]
		if len(existing) == 0 {
			continue
		}
		repl := existing[0]
		if !podReady(repl) {
			continue
		}

		healthy, err := r.deployHealthy(ctx, healthyDeploys, t.rs)
		if err != nil {
			return err
		}
		if !healthy {
			continue
		}

		// The hash is read from the target's ReplicaSet; during a rollout that may not be the current RS.
		hash := t.rs.Labels[LabelPodTemplateHash]
		if hash == "" {
			// An RS created by a Deployment always has the hash label. Reaching here is abnormal, so leave a trace.
			log.Info("Skipped handover because ReplicaSet has no pod-template-hash label",
				"replicaset", t.rs.Namespace+"/"+t.rs.Name, "target", t.pod.Namespace+"/"+t.pod.Name)
			continue
		}
		// One patch attaches the hash and removes replaces. Ownership passes to the ReplicaSet here.
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{
				LabelPodTemplateHash: hash,
				LabelReplaces:        nil,
			}},
		})
		if err := r.Patch(ctx, repl, patch); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		log.Info("Handed replacement Pod over to ReplicaSet",
			"pod", repl.Namespace+"/"+repl.Name, "target", t.pod.Namespace+"/"+t.pod.Name, "node", node.Name)
	}
	return nil
}

func (r *NodeReconciler) collectTargets(ctx context.Context, node *corev1.Node) ([]target, error) {
	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return nil, err
	}
	rsCache := map[types.NamespacedName]*appsv1.ReplicaSet{}
	var targets []target
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		ref := replicaSetRef(pod)
		if ref == nil {
			continue
		}
		key := types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}
		rs, ok := rsCache[key]
		if !ok {
			rs = &appsv1.ReplicaSet{}
			if err := r.Reader.Get(ctx, key, rs); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return nil, err
			}
			rsCache[key] = rs
		}
		if !ownedByDeployment(rs) {
			continue
		}
		targets = append(targets, target{pod: pod, rs: rs})
	}
	return targets, nil
}

func (r *NodeReconciler) collectReplacements(ctx context.Context) (map[string][]*corev1.Pod, error) {
	repls := &corev1.PodList{}
	if err := r.Reader.List(ctx, repls, client.HasLabels{LabelReplaces}); err != nil {
		return nil, err
	}
	byUID := map[string][]*corev1.Pod{}
	for i := range repls.Items {
		p := &repls.Items[i]
		if !validReplacement(p) {
			continue
		}
		byUID[p.Labels[LabelReplaces]] = append(byUID[p.Labels[LabelReplaces]], p)
	}
	for _, pods := range byUID {
		sortPodsByAge(pods)
	}
	return byUID, nil
}

func (r *NodeReconciler) nodeDraining(ctx context.Context, cache map[string]bool, name string) (bool, error) {
	if name == "" {
		return false, nil
	}
	if d, ok := cache[name]; ok {
		return d, nil
	}
	n := &corev1.Node{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: name}, n); err != nil {
		if apierrors.IsNotFound(err) {
			cache[name] = false
			return false, nil
		}
		return false, err
	}
	// A Cancelled node is an ordinary open node, so landing there is fine. A Complete node
	// is one a human is about to reboot, so it stays banned.
	cache[name] = drainActive(n)
	return cache[name], nil
}

// supersededByRollout reports whether a rollout is performing the target's migration (DESIGN.md step 3).
// A paused rollout does not actually move, so it does not apply even if the templates differ.
func (r *NodeReconciler) supersededByRollout(ctx context.Context, cache map[types.NamespacedName]*appsv1.Deployment, rs *appsv1.ReplicaSet) (bool, error) {
	ref := metav1.GetControllerOf(rs)
	if ref == nil {
		// unreachable: collectTargets only passes RSes that cleared ownedByDeployment
		return false, nil
	}
	key := types.NamespacedName{Namespace: rs.Namespace, Name: ref.Name}
	d, ok := cache[key]
	if !ok {
		d = &appsv1.Deployment{}
		if err := r.Reader.Get(ctx, key, d); err != nil {
			if apierrors.IsNotFound(err) {
				// If the Deployment is gone the targets go soon after; that path does the reclamation
				d = nil
			} else {
				return false, err
			}
		}
		cache[key] = d
	}
	if d == nil || d.Spec.Paused {
		return false, nil
	}
	return !templatesEqualIgnoreHash(&d.Spec.Template, &rs.Spec.Template), nil
}

func (r *NodeReconciler) deployHealthy(ctx context.Context, cache map[types.NamespacedName]bool, rs *appsv1.ReplicaSet) (bool, error) {
	ref := metav1.GetControllerOf(rs)
	if ref == nil {
		// unreachable: collectTargets only passes RSes that cleared ownedByDeployment
		return false, nil
	}
	key := types.NamespacedName{Namespace: rs.Namespace, Name: ref.Name}
	if h, ok := cache[key]; ok {
		return h, nil
	}
	d := &appsv1.Deployment{}
	if err := r.Reader.Get(ctx, key, d); err != nil {
		if apierrors.IsNotFound(err) {
			cache[key] = false
			return false, nil
		}
		return false, err
	}
	cache[key] = deploymentHealthy(d)
	return cache[key], nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	marked := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetLabels()[LabelDrain] != "" ||
			o.GetLabels()[LabelState] != "" ||
			o.GetAnnotations()[AnnotationCordoned] != ""
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}, builder.WithPredicates(marked)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.nodesForPod)).
		Named("node").
		Complete(r)
}

// nodesForPod turns a Pod event into the nodes to wake. A replacement lives on another
// node, and the Pod alone does not say which drain it belongs to, so every draining node wakes.
func (r *NodeReconciler) nodesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	log := logf.FromContext(ctx)
	if pod.Labels[LabelReplaces] != "" {
		nodes := &corev1.NodeList{}
		if err := r.List(ctx, nodes, client.MatchingLabels{LabelDrain: "true"}); err != nil {
			// If this path fails, progress depends on the periodic requeue alone, so leave a trace
			log.Error(err, "Failed to list draining nodes for replacement Pod event",
				"pod", pod.Namespace+"/"+pod.Name)
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(nodes.Items))
		for i := range nodes.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: nodes.Items[i].Name}})
		}
		return reqs
	}
	if pod.Spec.NodeName == "" {
		return nil
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to get node for Pod event", "node", pod.Spec.NodeName)
		}
		return nil
	}
	if !draining(node) && node.Labels[LabelState] == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: node.Name}}}
}
