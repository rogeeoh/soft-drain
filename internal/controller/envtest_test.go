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

// envtest에는 kube-controller-manager, scheduler, kubelet이 없다.
// 여기서는 우리 컨트롤러가 API 서버에 쓰는 것만 검증한다 (CLAUDE.md 테스트 3층).
// - ReplicaSet 입양·삭제·스케줄링은 일어나지 않으므로 오브젝트를 전부 손으로 만든다.
// - Pod 삭제는 kubelet이 없어 terminating에 머무니 deletionTimestamp로 단언한다.

import (
	"fmt"
	"slices"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeRecorder struct {
	mu      sync.Mutex
	reasons []string
}

func (f *fakeRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reasons = append(f.reasons, reason)
}

func (f *fakeRecorder) has(reason string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.reasons, reason)
}

var seq int

func uniq(prefix string) string {
	seq++
	return fmt.Sprintf("%s-%d", prefix, seq)
}

const testHash = "abc1234"

type fixture struct {
	node   *corev1.Node
	deploy *appsv1.Deployment
	rs     *appsv1.ReplicaSet
	target *corev1.Pod
	rec    *fakeRecorder
	r      *NodeReconciler
}

func nodeReq(node *corev1.Node) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: node.Name}}
}

func podReq(pod *corev1.Pod) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}}
}

func createNode(name string, labels map[string]string) *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	Expect(k8sClient.Create(ctx, node)).To(Succeed())
	return node
}

func simpleContainer() []corev1.Container {
	return []corev1.Container{{Name: "app", Image: "nginx:1.15"}}
}

// createWorkload는 Deployment → ReplicaSet → 타깃 Pod 사슬을 손으로 만든다.
func createWorkload(name, nodeName string) (*appsv1.Deployment, *appsv1.ReplicaSet, *corev1.Pod) {
	appLabels := map[string]string{"app": name}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: appLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
				Spec:       corev1.PodSpec{Containers: simpleContainer()},
			},
		},
	}
	Expect(k8sClient.Create(ctx, deploy)).To(Succeed())

	rsLabels := map[string]string{"app": name, LabelPodTemplateHash: testHash}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-" + testHash,
			Namespace: "default",
			Labels:    rsLabels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: deploy.Name, UID: deploy.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: rsLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: rsLabels},
				Spec:       corev1.PodSpec{Containers: simpleContainer()},
			},
		},
	}
	Expect(k8sClient.Create(ctx, rs)).To(Succeed())

	target := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rs.Name + "-target",
			Namespace: "default",
			Labels:    rsLabels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: simpleContainer()},
	}
	Expect(k8sClient.Create(ctx, target)).To(Succeed())
	return deploy, rs, target
}

func setupFixture() *fixture {
	name := uniq("wl")
	node := createNode(uniq("drain-node"), map[string]string{LabelDrain: "true"})
	deploy, rs, target := createWorkload(name, node.Name)
	rec := &fakeRecorder{}
	return &fixture{
		node: node, deploy: deploy, rs: rs, target: target, rec: rec,
		r: &NodeReconciler{Client: k8sClient, Reader: k8sClient, Recorder: rec},
	}
}

// createReplacement는 스케줄까지 끝난 대체 Pod을 흉내 낸다. envtest에는
// 스케줄러가 없어 nodeName을 생성 시점에 박는다.
func createReplacement(f *fixture, nodeName string, ready bool) *corev1.Pod {
	repl := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      uniq(f.rs.Name),
			Namespace: "default",
			Labels: map[string]string{
				"app":         f.deploy.Name,
				LabelReplaces: string(f.target.UID),
			},
		},
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: simpleContainer()},
	}
	Expect(k8sClient.Create(ctx, repl)).To(Succeed())
	if ready {
		repl.Status = corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		}
		Expect(k8sClient.Status().Update(ctx, repl)).To(Succeed())
	}
	return repl
}

func setDeployHealthy(deploy *appsv1.Deployment) {
	fresh := &appsv1.Deployment{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(deploy), fresh)).To(Succeed())
	fresh.Status = appsv1.DeploymentStatus{
		ObservedGeneration: fresh.Generation,
		Replicas:           1,
		UpdatedReplicas:    1,
		ReadyReplicas:      1,
		AvailableReplicas:  1,
	}
	Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
}

func getNode(name string) *corev1.Node {
	node := &corev1.Node{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, node)).To(Succeed())
	return node
}

func getPod(name string) *corev1.Pod {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, pod)).To(Succeed())
	return pod
}

func listReplacements(targetUID types.UID) []corev1.Pod {
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.MatchingLabels{LabelReplaces: string(targetUID)})).To(Succeed())
	return pods.Items
}

var _ = Describe("NodeReconciler", func() {
	It("cordon하고 cost를 박고 대체 Pod을 만든다", func() {
		f := setupFixture()

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		node := getNode(f.node.Name)
		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations[AnnotationCordoned]).To(Equal("true"))
		Expect(node.Labels[LabelState]).To(Equal(StateInProgress))

		target := getPod(f.target.Name)
		Expect(target.Annotations[AnnotationPodDeletionCost]).To(Equal(PodDeletionCost))

		repls := listReplacements(f.target.UID)
		Expect(repls).To(HaveLen(1))
		repl := repls[0]
		Expect(repl.Name).To(HavePrefix(f.rs.Name + "-"))
		Expect(repl.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(repl.Labels["app"]).To(Equal(f.deploy.Name))
		Expect(repl.Annotations).NotTo(HaveKey(AnnotationPodDeletionCost))
		Expect(repl.Spec.Containers[0].Image).To(Equal("nginx:1.15"))
	})

	It("Ready인 대체 Pod을 Healthy Deployment에서 넘긴다", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		setDeployHealthy(f.deploy)

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// patch 하나로 hash가 붙고 replaces가 떨어져 소유가 ReplicaSet으로 넘어간다
		got := getPod(repl.Name)
		Expect(got.Labels[LabelPodTemplateHash]).To(Equal(testHash))
		Expect(got.Labels).NotTo(HaveKey(LabelReplaces))
	})

	It("Deployment가 Healthy가 아니면 넘기지 않는다", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		// 롤아웃 중: replicas != updatedReplicas
		fresh := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		fresh.Status = appsv1.DeploymentStatus{
			ObservedGeneration: fresh.Generation,
			Replicas:           2,
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
			AvailableReplicas:  1,
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getPod(repl.Name)
		Expect(got.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(got.Labels[LabelReplaces]).To(Equal(string(f.target.UID)))
	})

	It("drain 노드에 앉은 Ready 대체 Pod은 지우고 다음 라운드에 새로 만든다", func() {
		f := setupFixture()
		repl := createReplacement(f, f.node.Name, true)
		setDeployHealthy(f.deploy)

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getPod(repl.Name)
		Expect(got.DeletionTimestamp).NotTo(BeNil())
		Expect(got.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(f.rec.has("ReplacementOnDrainingNode")).To(BeTrue())

		// terminating은 있는 것으로 세지 않으므로 다음 라운드가 새로 만든다
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		var live int
		for _, p := range listReplacements(f.target.UID) {
			if p.DeletionTimestamp == nil {
				live++
			}
		}
		Expect(live).To(Equal(1))
	})

	It("판정과 삭제 사이에 hash가 붙은 Pod은 지우지 않는다", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)

		// 삭제자(PodReconciler)가 읽어둔 시점의 복사본
		stale := repl.DeepCopy()

		// 그 사이 넘기기가 patch 하나로 hash를 붙이고 replaces를 뗀다
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{
				LabelPodTemplateHash: testHash,
				LabelReplaces:        nil,
			}},
		})
		Expect(k8sClient.Patch(ctx, repl, patch)).To(Succeed())

		// stale 복사본으로 삭제 시도 — resourceVersion precondition이 거부한다
		deleted, err := deleteReplacement(ctx, k8sClient, stale)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeFalse())
		Expect(getPod(repl.Name).DeletionTimestamp).To(BeNil())
	})

	It("넘겼는데 타깃이 살아남으면 다음 라운드가 새 대체 Pod을 만든다", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		setDeployHealthy(f.deploy)

		// 1라운드: 넘기기까지 간다
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		handed := getPod(repl.Name)
		Expect(handed.Labels[LabelPodTemplateHash]).To(Equal(testHash))

		// envtest에는 RS 컨트롤러가 없어 타깃이 지워지지 않는다 — 넘기는 순간
		// replicas가 올라 초과분이 증설분에 흡수된 것과 같은 상태다.
		// 2라운드: "타깃은 그대로인데 대신할 Pod이 없다"를 보고 하나 더 만든다.
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		var live []corev1.Pod
		for _, p := range listReplacements(f.target.UID) {
			if p.DeletionTimestamp == nil {
				live = append(live, p)
			}
		}
		Expect(live).To(HaveLen(1))
		Expect(live[0].Name).NotTo(Equal(repl.Name))
		// 넘긴 Pod은 건드리지 않았다
		Expect(getPod(repl.Name).Labels).NotTo(HaveKey(LabelReplaces))
	})

	It("타깃이 없으면 Complete를 붙이고 cordon 소유권을 넘긴다", func() {
		rec := &fakeRecorder{}
		r := &NodeReconciler{Client: k8sClient, Reader: k8sClient, Recorder: rec}
		node := createNode(uniq("empty-node"), map[string]string{LabelDrain: "true"})

		_, err := r.Reconcile(ctx, nodeReq(node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(node.Name)
		Expect(got.Spec.Unschedulable).To(BeTrue())
		Expect(got.Labels[LabelState]).To(Equal(StateComplete))
		Expect(got.Annotations).NotTo(HaveKey(AnnotationCordoned))
		Expect(rec.has("DrainComplete")).To(BeTrue())
	})

	It("Complete 래치: 그 후 타깃이 생겨도 관여하지 않는다", func() {
		f := setupFixture()
		// Complete 상태를 만든다: 타깃을 먼저 지우고(0 grace로 즉시) reconcile
		Expect(k8sClient.Delete(ctx, f.target, client.GracePeriodSeconds(0))).To(Succeed())
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Labels[LabelState]).To(Equal(StateComplete))

		// tolerate 워크로드가 뒤늦게 앉은 상황
		_, _, late := createWorkload(uniq("late"), f.node.Name)
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		Expect(getNode(f.node.Name).Labels[LabelState]).To(Equal(StateComplete))
		Expect(getPod(late.Name).Annotations).NotTo(HaveKey(AnnotationPodDeletionCost))
		Expect(listReplacements(late.UID)).To(BeEmpty())
	})

	It("drain 중 uncordon되면 취소한다", func() {
		f := setupFixture()
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// 사람이 uncordon
		node := getNode(f.node.Name)
		node.Spec.Unschedulable = false
		Expect(k8sClient.Update(ctx, node)).To(Succeed())

		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(f.node.Name)
		Expect(got.Labels[LabelState]).To(Equal(StateCancelled))
		Expect(got.Annotations).NotTo(HaveKey(AnnotationCordoned))
		Expect(got.Spec.Unschedulable).To(BeFalse())
		Expect(getPod(f.target.Name).Annotations).NotTo(HaveKey(AnnotationPodDeletionCost))
		Expect(f.rec.has("DrainCancelled")).To(BeTrue())

		// 래치: 다시 돌려도 cordon하지 않는다
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Spec.Unschedulable).To(BeFalse())
	})

	It("drain 라벨이 사라지면 되돌린다", func() {
		f := setupFixture()
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		node := getNode(f.node.Name)
		delete(node.Labels, LabelDrain)
		Expect(k8sClient.Update(ctx, node)).To(Succeed())

		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(f.node.Name)
		Expect(got.Spec.Unschedulable).To(BeFalse())
		Expect(got.Labels).NotTo(HaveKey(LabelState))
		Expect(got.Annotations).NotTo(HaveKey(AnnotationCordoned))
		Expect(getPod(f.target.Name).Annotations).NotTo(HaveKey(AnnotationPodDeletionCost))
	})

	It("사람이 미리 cordon한 노드에는 어노테이션을 달지 않는다", func() {
		rec := &fakeRecorder{}
		r := &NodeReconciler{Client: k8sClient, Reader: k8sClient, Recorder: rec}
		node := createNode(uniq("pre-cordoned"), map[string]string{LabelDrain: "true"})
		node.Spec.Unschedulable = true
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		_, _, target := createWorkload(uniq("wl"), node.Name)
		_ = target

		_, err := r.Reconcile(ctx, nodeReq(node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(node.Name)
		Expect(got.Annotations).NotTo(HaveKey(AnnotationCordoned))
		Expect(got.Labels[LabelState]).To(Equal(StateInProgress))
	})
})

var _ = Describe("PodReconciler", func() {
	newReconciler := func() *PodReconciler {
		return &PodReconciler{Client: k8sClient, Reader: k8sClient}
	}

	It("타깃이 없는 대체 Pod을 지운다", func() {
		repl := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      uniq("orphan"),
				Namespace: "default",
				Labels:    map[string]string{LabelReplaces: "00000000-dead-beef-0000-000000000000"},
			},
			Spec: corev1.PodSpec{Containers: simpleContainer()},
		}
		Expect(k8sClient.Create(ctx, repl)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, podReq(repl))
		Expect(err).NotTo(HaveOccurred())
		// 스케줄 전 Pod은 kubelet 확인이 필요 없어 즉시 사라진다
		gone := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: repl.Name}, &corev1.Pod{})
		Expect(apierrors.IsNotFound(gone)).To(BeTrue())
	})

	It("타깃이 drain 노드에 살아 있으면 유지하고 재판정을 예약한다", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		res, err := newReconciler().Reconcile(ctx, podReq(repl))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(30 * time.Second))
		Expect(getPod(repl.Name).DeletionTimestamp).To(BeNil())
	})

	It("Cancelled 노드의 대체 Pod은 지운다", func() {
		f := setupFixture()
		node := getNode(f.node.Name)
		node.Labels[LabelState] = StateCancelled
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		_, err := newReconciler().Reconcile(ctx, podReq(repl))
		Expect(err).NotTo(HaveOccurred())
		Expect(getPod(repl.Name).DeletionTimestamp).NotTo(BeNil())
	})

	It("controller ownerRef가 있는 Pod은 건드리지 않는다", func() {
		f := setupFixture()
		adopted := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      uniq("adopted"),
				Namespace: "default",
				Labels:    map[string]string{LabelReplaces: string(f.target.UID)},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "ReplicaSet", Name: f.rs.Name, UID: f.rs.UID,
					Controller: ptr.To(true),
				}},
			},
			Spec: corev1.PodSpec{Containers: simpleContainer()},
		}
		Expect(k8sClient.Create(ctx, adopted)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, podReq(adopted))
		Expect(err).NotTo(HaveOccurred())
		Expect(getPod(adopted.Name).DeletionTimestamp).To(BeNil())
	})
})
