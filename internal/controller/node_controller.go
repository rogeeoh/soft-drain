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

// NodeReconciler는 drain 라벨이 붙은 노드를 비운다 (DESIGN.md "컨트롤러가 하는 일").
type NodeReconciler struct {
	client.Client
	// Reader는 API 서버를 직접 읽는다. 캐시는 다시 볼 때를 알려주는 데만 쓴다.
	Reader   client.Reader
	Recorder events.EventRecorder
}

// target은 그 노드 위에서 owner가 ReplicaSet이고 그 ReplicaSet의 owner가 Deployment인 Pod이다.
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

// restore는 drain 라벨이 사라진 노드에서 우리가 남긴 것을 걷는다.
// 대체 Pod은 PodReconciler의 판정(타깃이 drain 노드에 없으면 지운다)이 걷는다.
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

// sweepDeletionCost는 노드 위 Pod에서 우리가 박은 pod-deletion-cost를 걷는다.
func (r *NodeReconciler) sweepDeletionCost(ctx context.Context, nodeName string) error {
	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		// 값이 정확히 우리 것일 때만 지운다
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

// cancel은 drain 중 uncordon된 노드에서 손을 뗀다. cordon은 이 방식의 전제라
// 전제가 사라지면 계속할 의미가 없다. 다시 cordon해서 사람과 싸우지 않는다.
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

	// Cancelled는 래치다. drain 라벨이 걷힐 때까지 관여하지 않는다.
	if node.Labels[LabelState] == StateCancelled {
		return ctrl.Result{}, nil
	}

	// InProgress와 Complete는 cordon을 확인한 뒤에만 붙는다. 그런데 unschedulable이
	// 아니라면 누군가 uncordon한 것이다 — 진행 중이면 종료를 보장하던 전제가
	// 사라졌고, 끝난 뒤면 cordon 소유권을 넘겨받은 사람이 노드를 다시 쓰기로 한
	// 것이다. 어느 쪽이든 관여를 접는다.
	state := node.Labels[LabelState]
	if (state == StateInProgress || state == StateComplete) && !node.Spec.Unschedulable {
		return ctrl.Result{}, r.cancel(ctx, node)
	}

	// Complete는 래치다. cordon이 유지되는 동안 관여하지 않는다.
	if state == StateComplete {
		return ctrl.Result{}, nil
	}

	// 1. 노드 마킹 — 우리가 실제로 값을 바꿨을 때만 어노테이션을 단다
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

	// 6. 완료 — terminating 타깃도 센다
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
	if err := r.reconcileReplacements(ctx, node, targets, replsByUID); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.handOver(ctx, node, targets, replsByUID); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// complete는 어노테이션을 지워 cordon 소유권을 사람에게 넘기고 Complete를 붙인다.
func (r *NodeReconciler) complete(ctx context.Context, node *corev1.Node) error {
	if node.Labels[LabelState] == StateComplete {
		return nil
	}
	patch := mergePatch(map[string]any{
		"metadata": map[string]any{
			"labels":      map[string]any{LabelState: StateComplete},
			"annotations": map[string]any{AnnotationCordoned: nil},
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

// markTargets는 타깃에 pod-deletion-cost를 박는다 (DESIGN.md 2단계).
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

// reconcileReplacements는 있어야 할 집합과 있는 집합을 맞춘다 (DESIGN.md 3단계).
// 모자라면 만들고, 같은 타깃에 남으면 지운다.
func (r *NodeReconciler) reconcileReplacements(ctx context.Context, node *corev1.Node, targets []target, replsByUID map[string][]*corev1.Pod) error {
	log := logf.FromContext(ctx)
	for _, t := range targets {
		// terminating 타깃은 ReplicaSet이 이미 스스로 대체를 만들고 있다.
		// 남아 있던 대체 Pod은 회수 경로(PodReconciler)가 같은 판정으로 지운다.
		if t.pod.DeletionTimestamp != nil {
			continue
		}
		existing := replsByUID[string(t.pod.UID)]
		switch {
		case len(existing) == 0:
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
		case len(existing) > 1:
			// 제일 오래된 하나만 남긴다. 트림은 이 라운드의 넘기기가 방금 지운
			// Pod을 다시 보지 않게 하기 위한 것이다.
			for _, extra := range existing[1:] {
				if _, err := deleteReplacement(ctx, r.Client, extra); err != nil {
					return err
				}
			}
			replsByUID[string(t.pod.UID)] = existing[:1]
		}
	}
	return nil
}

// handOver는 Ready인 대체 Pod에 hash를 붙여 ReplicaSet이 데려가게 한다 (DESIGN.md 4단계).
// Deployment마다 따로 판정하고 준비된 것부터 넘긴다.
func (r *NodeReconciler) handOver(ctx context.Context, node *corev1.Node, targets []target, replsByUID map[string][]*corev1.Pod) error {
	log := logf.FromContext(ctx)
	drainingNodes := map[string]bool{node.Name: true}
	healthyDeploys := map[types.NamespacedName]bool{}
	for _, t := range targets {
		if t.pod.DeletionTimestamp != nil {
			continue
		}
		existing := replsByUID[string(t.pod.UID)]
		if len(existing) == 0 {
			continue
		}
		repl := existing[0]
		if !podReady(repl) {
			continue
		}

		landedOnDraining, err := r.nodeDraining(ctx, drainingNodes, repl.Spec.NodeName)
		if err != nil {
			return err
		}
		if landedOnDraining {
			// 넘겨봐야 그 노드에 타깃이 하나 더 생긴다. hash가 없어 아직 누구의
			// 자식도 아니므로 지우고, 다음 라운드가 cordon을 피해 새로 만든다.
			deleted, err := deleteReplacement(ctx, r.Client, repl)
			if err != nil {
				return err
			}
			if deleted {
				r.Recorder.Eventf(node, repl, corev1.EventTypeWarning, "ReplacementOnDrainingNode",
					"DeleteReplacementPod",
					"Deleted replacement Pod %s/%s that landed on draining node %s",
					repl.Namespace, repl.Name, repl.Spec.NodeName)
				log.Info("Deleted replacement Pod that landed on draining node",
					"pod", repl.Namespace+"/"+repl.Name, "landedOn", repl.Spec.NodeName, "node", node.Name)
			}
			continue
		}

		healthy, err := r.deployHealthy(ctx, healthyDeploys, t.rs)
		if err != nil {
			return err
		}
		if !healthy {
			continue
		}

		// hash는 타깃의 ReplicaSet에서 읽는다. 롤아웃 중이면 Deployment의 현재 RS가 아닐 수 있다.
		hash := t.rs.Labels[LabelPodTemplateHash]
		if hash == "" {
			// Deployment가 만든 RS에는 항상 hash 라벨이 있다. 여기 오면 비정상이므로 흔적을 남긴다.
			log.Info("Skipped handover because ReplicaSet has no pod-template-hash label",
				"replicaset", t.rs.Namespace+"/"+t.rs.Name, "target", t.pod.Namespace+"/"+t.pod.Name)
			continue
		}
		// patch 하나로 hash를 붙이고 replaces를 뗀다. 이 순간 소유가 ReplicaSet으로 넘어간다.
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
	// Cancelled 노드는 열린 보통 노드라 착지해도 된다. Complete 노드는 사람이
	// 리부팅하러 갈 노드라 여전히 막는다.
	cache[name] = drainActive(n)
	return cache[name], nil
}

func (r *NodeReconciler) deployHealthy(ctx context.Context, cache map[types.NamespacedName]bool, rs *appsv1.ReplicaSet) (bool, error) {
	ref := metav1.GetControllerOf(rs)
	if ref == nil {
		// collectTargets가 ownedByDeployment를 통과한 RS만 넘기므로 도달하지 않는다
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

// nodesForPod은 Pod 이벤트를 깨어날 노드로 바꾼다. 대체 Pod은 다른 노드에 떠 있어서
// 어느 drain의 것인지 Pod만으로는 알 수 없으므로 drain 중인 노드 전부를 깨운다.
func (r *NodeReconciler) nodesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	log := logf.FromContext(ctx)
	if pod.Labels[LabelReplaces] != "" {
		nodes := &corev1.NodeList{}
		if err := r.List(ctx, nodes, client.MatchingLabels{LabelDrain: "true"}); err != nil {
			// 이 경로가 실패하면 진행이 주기적 requeue에만 의존하므로 흔적을 남긴다
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
