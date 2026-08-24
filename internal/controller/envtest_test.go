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

// envtest has no kube-controller-manager, scheduler or kubelet.
// Only what our controller writes to the API server is verified here (CLAUDE.md, the three test layers).
// - ReplicaSet adoption, deletion and scheduling never happen, so every object is built by hand.
// - Pod deletion stays terminating without a kubelet, so it is asserted through deletionTimestamp.

import (
	"context"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
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

// conflictOnDelete simulates the interleaving where the Pod changes between the
// decision and the deletion, so the preconditions reject it.
type conflictOnDelete struct{ client.Client }

func (c *conflictOnDelete) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, obj.GetName(),
		fmt.Errorf("the object has been modified"))
}

// the rollout specs push the generation forward by changing the template to this image
const rolledImage = "nginx:1.16"

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
	// Rounds now walk every drain-labeled node, so a node left labeled would leak this
	// spec's state into every later spec's reconcile. Strip the labels when the spec ends.
	DeferCleanup(func() {
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{LabelDrain: nil, LabelState: nil}},
		})
		Expect(client.IgnoreNotFound(k8sClient.Patch(ctx, node, patch))).To(Succeed())
	})
	return node
}

func simpleContainer() []corev1.Container {
	return []corev1.Container{{Name: "app", Image: "nginx:1.15"}}
}

// createWorkload builds the Deployment → ReplicaSet → target Pod chain by hand.
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

// createReplacement simulates a replacement that is already scheduled. envtest has no
// scheduler, so nodeName is written at creation.
func createReplacement(f *fixture, nodeName string, ready bool) *corev1.Pod {
	repl := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      uniq(f.rs.Name),
			Namespace: "default",
			Labels: map[string]string{
				"app":         f.deploy.Name,
				LabelReplaces: string(f.rs.UID),
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

// createReplacementNamed is createReplacement with a caller-chosen name, for specs where
// the age order must be deterministic — equal creation timestamps fall back to the name.
func createReplacementNamed(f *fixture, name, nodeName string, ready bool) *corev1.Pod {
	repl := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				"app":         f.deploy.Name,
				LabelReplaces: string(f.rs.UID),
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

func listReplacements(rsUID types.UID) []corev1.Pod {
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.MatchingLabels{LabelReplaces: string(rsUID)})).To(Succeed())
	return pods.Items
}

func liveReplacements(rsUID types.UID) []corev1.Pod {
	var live []corev1.Pod
	for _, p := range listReplacements(rsUID) {
		if p.DeletionTimestamp == nil {
			live = append(live, p)
		}
	}
	return live
}

// addTarget seats one more Pod of the fixture's ReplicaSet on the given node.
func addTarget(f *fixture, nodeName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      uniq(f.rs.Name + "-target"),
			Namespace: "default",
			Labels:    map[string]string{"app": f.deploy.Name, LabelPodTemplateHash: testHash},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: f.rs.Name, UID: f.rs.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: simpleContainer()},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	return pod
}

var _ = Describe("NodeReconciler", func() {
	It("cordons, stamps the cost, creates the replacement", func() {
		f := setupFixture()

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		node := getNode(f.node.Name)
		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations[AnnotationCordoned]).To(Equal("true"))
		Expect(node.Labels[LabelState]).To(Equal(StateInProgress))

		target := getPod(f.target.Name)
		Expect(target.Annotations[AnnotationPodDeletionCost]).To(Equal(PodDeletionCost))

		repls := listReplacements(f.rs.UID)
		Expect(repls).To(HaveLen(1))
		repl := repls[0]
		Expect(repl.Name).To(HavePrefix(f.rs.Name + "-"))
		Expect(repl.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(repl.Labels["app"]).To(Equal(f.deploy.Name))
		Expect(repl.Annotations).NotTo(HaveKey(AnnotationPodDeletionCost))
		Expect(repl.Spec.Containers[0].Image).To(Equal("nginx:1.15"))
	})

	It("hands a Ready replacement over when the Deployment is healthy", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		setDeployHealthy(f.deploy)

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// one patch attaches the hash and removes replaces, passing ownership to the ReplicaSet
		got := getPod(repl.Name)
		Expect(got.Labels[LabelPodTemplateHash]).To(Equal(testHash))
		Expect(got.Labels).NotTo(HaveKey(LabelReplaces))
	})

	It("does not hand over while the Deployment is unhealthy", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		// mid-rollout: replicas != updatedReplicas
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
		Expect(got.Labels[LabelReplaces]).To(Equal(string(f.rs.UID)))
	})

	It("deletes a replacement that landed on a draining node and recreates it in the same round", func() {
		f := setupFixture()
		repl := createReplacement(f, f.node.Name, true)
		setDeployHealthy(f.deploy)

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getPod(repl.Name)
		Expect(got.DeletionTimestamp).NotTo(BeNil())
		Expect(got.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(f.rec.has("ReplacementOnDrainingNode")).To(BeTrue())

		// terminating does not count as existing, so the round that deleted it creates a new one at once
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(1))
	})

	It("deletes a pending replacement once its landing node starts draining", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		// if the node it landed on is fine, wait for Ready
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getPod(repl.Name).DeletionTimestamp).To(BeNil())

		// once that node is draining there is no reason to wait for Ready
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{LabelDrain: "true"}},
		})
		Expect(k8sClient.Patch(ctx, other, patch)).To(Succeed())
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		Expect(getPod(repl.Name).DeletionTimestamp).NotTo(BeNil())
		Expect(f.rec.has("ReplacementOnDrainingNode")).To(BeTrue())
	})

	It("retires the replacement and pauses creation while a rollout supersedes the target", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		// with the image changed the target's RS is not the current generation
		fresh := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		fresh.Spec.Template.Spec.Containers[0].Image = rolledImage
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getPod(repl.Name).DeletionTimestamp).NotTo(BeNil())
		Expect(f.rec.has("ReplacementSuperseded")).To(BeTrue())

		// while stale, nothing is created again — the migration belongs to the rollout
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(liveReplacements(f.rs.UID)).To(BeEmpty())

		// it resumes once the generation comes back
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		fresh.Spec.Template.Spec.Containers[0].Image = "nginx:1.15"
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(1))
	})

	It("keeps the replacement when the template changed but the Deployment is paused", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		fresh := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		fresh.Spec.Paused = true
		fresh.Spec.Template.Spec.Containers[0].Image = rolledImage
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// paused means the rollout does not actually move — replacements are maintained as usual
		Expect(getPod(repl.Name).DeletionTimestamp).To(BeNil())
		Expect(f.rec.has("ReplacementSuperseded")).To(BeFalse())
	})

	It("folds the round instead of handing over when a landing deletion is rejected", func() {
		f := setupFixture()
		f.r.Client = &conflictOnDelete{Client: k8sClient}
		repl := createReplacement(f, f.node.Name, true)
		setDeployHealthy(f.deploy)

		res, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// a stale list whose deletion was rejected never reaches hand-over — the round ends and decides again
		got := getPod(repl.Name)
		Expect(got.DeletionTimestamp).To(BeNil())
		Expect(got.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(res.RequeueAfter).To(Equal(time.Second))
	})

	It("prefers retirement over handover for a Ready replacement of a superseded target", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		setDeployHealthy(f.deploy)

		fresh := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		fresh.Spec.Template.Spec.Containers[0].Image = rolledImage
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// deletion, not adoption, even if it was Ready — Healthy(D) blocks hand-over for the
		// whole rollout, so this replacement can never reach adoption
		got := getPod(repl.Name)
		Expect(got.DeletionTimestamp).NotTo(BeNil())
		Expect(got.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(f.rec.has("ReplacementSuperseded")).To(BeTrue())
	})

	It("refuses to delete a Pod that gained the hash between judgment and deletion", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)

		// the copy as the deleter (PodReconciler) read it
		stale := repl.DeepCopy()

		// meanwhile the hand-over attaches the hash and removes replaces in one patch
		patch := mergePatch(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{
				LabelPodTemplateHash: testHash,
				LabelReplaces:        nil,
			}},
		})
		Expect(k8sClient.Patch(ctx, repl, patch)).To(Succeed())

		// deleting with the stale copy — the resourceVersion precondition rejects it
		deleted, err := deleteReplacement(ctx, k8sClient, stale)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeFalse())
		Expect(getPod(repl.Name).DeletionTimestamp).To(BeNil())
	})

	It("creates a fresh replacement next round when the target survives a handover", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, true)
		setDeployHealthy(f.deploy)

		// round 1: goes through hand-over
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		handed := getPod(repl.Name)
		Expect(handed.Labels[LabelPodTemplateHash]).To(Equal(testHash))

		// envtest has no RS controller, so the target is not deleted — the same state as
		// replicas going up at the moment of hand-over and the surplus being absorbed.
		// round 2: sees "the target is still there and nothing stands in for it" and creates one more.
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		live := liveReplacements(f.rs.UID)
		Expect(live).To(HaveLen(1))
		Expect(live[0].Name).NotTo(Equal(repl.Name))
		// the handed-over Pod was left untouched
		Expect(getPod(repl.Name).Labels).NotTo(HaveKey(LabelReplaces))
	})

	It("keeps one shared set per ReplicaSet across draining nodes", func() {
		f := setupFixture()
		second := createNode(uniq("drain-node"), map[string]string{LabelDrain: "true"})
		away := addTarget(f, second.Name)

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// one round reconciles the shared set: two targets, two replacements
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(2))
		// the other node's target is marked by the same round — the write is idempotent
		Expect(getPod(away.Name).Annotations[AnnotationPodDeletionCost]).To(Equal(PodDeletionCost))

		// counting per node would double the set; another round changes nothing
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(2))
	})

	It("does not count targets on a Complete node", func() {
		f := setupFixture()
		done := createNode(uniq("drain-node"), map[string]string{LabelDrain: "true", LabelState: StateComplete})
		// Complete is only ever set after confirming the cordon
		done.Spec.Unschedulable = true
		Expect(k8sClient.Update(ctx, done)).To(Succeed())
		addTarget(f, done.Name)

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// the Complete node is latched — its late arrival raises no count
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(1))
	})

	It("trims the youngest replacement when a target goes away", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		gone := addTarget(f, f.node.Name)
		older := createReplacementNamed(f, f.rs.Name+"-repl-a", other.Name, false)
		younger := createReplacementNamed(f, f.rs.Name+"-repl-b", other.Name, false)

		// two targets, two replacements — balanced, nothing to do
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(2))

		// the ReplicaSet picked this target — with no pairing, only the count changes
		Expect(k8sClient.Delete(ctx, gone)).To(Succeed())
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// the set shrinks from the end with the least readiness work
		Expect(getPod(younger.Name).DeletionTimestamp).NotTo(BeNil())
		Expect(getPod(older.Name).DeletionTimestamp).To(BeNil())
	})

	It("holds creation while the Deployment reports more Pods than it wants", func() {
		f := setupFixture()
		fresh := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		// the state right after a hand-over: the adoption is in, the deletion still owed
		fresh.Status = appsv1.DeploymentStatus{
			ObservedGeneration: fresh.Generation,
			Replicas:           2,
			UpdatedReplicas:    2,
			ReadyReplicas:      2,
			AvailableReplicas:  2,
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		// marking went ahead; creation held
		Expect(getPod(f.target.Name).Annotations[AnnotationPodDeletionCost]).To(Equal(PodDeletionCost))
		Expect(liveReplacements(f.rs.UID)).To(BeEmpty())

		// the count returned to spec — the deletion landed — and creation resumes
		setDeployHealthy(f.deploy)
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(liveReplacements(f.rs.UID)).To(HaveLen(1))
	})

	It("hands over every Ready member up to the live target count", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		addTarget(f, f.node.Name)
		ra := createReplacementNamed(f, f.rs.Name+"-repl-a", other.Name, true)
		rb := createReplacementNamed(f, f.rs.Name+"-repl-b", other.Name, true)

		fresh := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.deploy), fresh)).To(Succeed())
		fresh.Status = appsv1.DeploymentStatus{
			ObservedGeneration: fresh.Generation,
			Replicas:           2,
			UpdatedReplicas:    2,
			ReadyReplicas:      2,
			AvailableReplicas:  2,
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// both are within the live target count, so both go
		for _, name := range []string{ra.Name, rb.Name} {
			got := getPod(name)
			Expect(got.Labels[LabelPodTemplateHash]).To(Equal(testHash))
			Expect(got.Labels).NotTo(HaveKey(LabelReplaces))
		}
	})

	It("deletes the excess Ready replacement instead of handing it over", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		older := createReplacementNamed(f, f.rs.Name+"-repl-a", other.Name, true)
		excess := createReplacementNamed(f, f.rs.Name+"-repl-b", other.Name, true)
		setDeployHealthy(f.deploy)

		// one live target, two Ready members: one adoption beyond the target count would
		// make the ReplicaSet delete a Pod at cost 0, so the trim must win over hand-over
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		handed := getPod(older.Name)
		Expect(handed.Labels[LabelPodTemplateHash]).To(Equal(testHash))
		Expect(handed.Labels).NotTo(HaveKey(LabelReplaces))

		trimmed := getPod(excess.Name)
		Expect(trimmed.DeletionTimestamp).NotTo(BeNil())
		Expect(trimmed.Labels).NotTo(HaveKey(LabelPodTemplateHash))
	})

	It("a rejected trim drops the Pod from the round without folding it", func() {
		f := setupFixture()
		f.r.Client = &conflictOnDelete{Client: k8sClient}
		other := createNode(uniq("other-node"), nil)
		older := createReplacementNamed(f, f.rs.Name+"-repl-a", other.Name, true)
		rejected := createReplacementNamed(f, f.rs.Name+"-repl-b", other.Name, true)
		setDeployHealthy(f.deploy)

		res, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// unlike the landing and rollout deletions, a rejected trim does not end the round
		Expect(res.RequeueAfter).NotTo(Equal(time.Second))

		// the survivor of the trim went through hand-over as usual
		handed := getPod(older.Name)
		Expect(handed.Labels[LabelPodTemplateHash]).To(Equal(testHash))
		Expect(handed.Labels).NotTo(HaveKey(LabelReplaces))

		// the changed Pod merely dropped out of the round's list: alive, not handed over
		got := getPod(rejected.Name)
		Expect(got.DeletionTimestamp).To(BeNil())
		Expect(got.Labels).NotTo(HaveKey(LabelPodTemplateHash))
		Expect(got.Labels[LabelReplaces]).To(Equal(string(f.rs.UID)))
	})

	It("attaches Complete and keeps the cordon annotation when no targets remain", func() {
		rec := &fakeRecorder{}
		r := &NodeReconciler{Client: k8sClient, Reader: k8sClient, Recorder: rec}
		node := createNode(uniq("empty-node"), map[string]string{LabelDrain: "true"})

		_, err := r.Reconcile(ctx, nodeReq(node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(node.Name)
		Expect(got.Spec.Unschedulable).To(BeTrue())
		Expect(got.Labels[LabelState]).To(Equal(StateComplete))
		// the cordon is still ours — the annotation comes off when the label does
		Expect(got.Annotations).To(HaveKeyWithValue(AnnotationCordoned, "true"))
		Expect(rec.has("DrainComplete")).To(BeTrue())
	})

	It("release after Complete uncordons the controller's cordon", func() {
		rec := &fakeRecorder{}
		r := &NodeReconciler{Client: k8sClient, Reader: k8sClient, Recorder: rec}
		node := createNode(uniq("done-node"), map[string]string{LabelDrain: "true"})

		// an empty node reaches cordon and Complete in the first round
		_, err := r.Reconcile(ctx, nodeReq(node))
		Expect(err).NotTo(HaveOccurred())
		got := getNode(node.Name)
		Expect(got.Labels[LabelState]).To(Equal(StateComplete))
		Expect(got.Annotations).To(HaveKeyWithValue(AnnotationCordoned, "true"))

		delete(got.Labels, LabelDrain)
		Expect(k8sClient.Update(ctx, got)).To(Succeed())

		_, err = r.Reconcile(ctx, nodeReq(node))
		Expect(err).NotTo(HaveOccurred())

		got = getNode(node.Name)
		Expect(got.Spec.Unschedulable).To(BeFalse())
		Expect(got.Labels).NotTo(HaveKey(LabelState))
		Expect(got.Annotations).NotTo(HaveKey(AnnotationCordoned))
	})

	It("release leaves a cordon the human re-applied after cancelling", func() {
		f := setupFixture()
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// a human uncordons — it folds into Cancelled and the annotation is deleted
		node := getNode(f.node.Name)
		node.Spec.Unschedulable = false
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Labels[LabelState]).To(Equal(StateCancelled))

		// a human re-cordons for another reason, then the label is removed
		node = getNode(f.node.Name)
		node.Spec.Unschedulable = true
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		node = getNode(f.node.Name)
		delete(node.Labels, LabelDrain)
		Expect(k8sClient.Update(ctx, node)).To(Succeed())

		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(f.node.Name)
		// a cordon that is not our record is not lifted
		Expect(got.Spec.Unschedulable).To(BeTrue())
		Expect(got.Labels).NotTo(HaveKey(LabelState))
		Expect(got.Annotations).NotTo(HaveKey(AnnotationCordoned))
	})

	It("Complete latches: later targets are left alone", func() {
		f := setupFixture()
		// reach Complete: delete the target first (0 grace, immediate) and reconcile
		Expect(k8sClient.Delete(ctx, f.target, client.GracePeriodSeconds(0))).To(Succeed())
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Labels[LabelState]).To(Equal(StateComplete))

		// a tolerating workload seated late
		_, lateRS, late := createWorkload(uniq("late"), f.node.Name)
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		Expect(getNode(f.node.Name).Labels[LabelState]).To(Equal(StateComplete))
		Expect(getPod(late.Name).Annotations).NotTo(HaveKey(AnnotationPodDeletionCost))
		Expect(listReplacements(lateRS.UID)).To(BeEmpty())
	})

	It("cancels when uncordoned mid-drain", func() {
		f := setupFixture()
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		// a human uncordons
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

		// latch: another round does not cordon again
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Spec.Unschedulable).To(BeFalse())
	})

	It("folds involvement when uncordoned after Complete", func() {
		f := setupFixture()
		Expect(k8sClient.Delete(ctx, f.target, client.GracePeriodSeconds(0))).To(Succeed())
		_, err := f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Labels[LabelState]).To(Equal(StateComplete))

		// a human lifts our cordon and decides to use the node again
		node := getNode(f.node.Name)
		node.Spec.Unschedulable = false
		Expect(k8sClient.Update(ctx, node)).To(Succeed())

		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())

		got := getNode(f.node.Name)
		Expect(got.Labels[LabelState]).To(Equal(StateCancelled))
		Expect(got.Spec.Unschedulable).To(BeFalse())
		Expect(f.rec.has("DrainCancelled")).To(BeTrue())

		// folded into Cancelled, so the landing check does not block this node either
		Expect(drainActive(got)).To(BeFalse())

		// latch: another round does not cordon again
		_, err = f.r.Reconcile(ctx, nodeReq(f.node))
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(f.node.Name).Spec.Unschedulable).To(BeFalse())
	})

	It("restores the node when the drain label disappears", func() {
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

	It("adds no annotation to a node a human cordoned first", func() {
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

	It("deletes a replacement whose ReplicaSet is gone", func() {
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
		// an unscheduled Pod needs no kubelet confirmation, so it disappears immediately
		gone := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: repl.Name}, &corev1.Pod{})
		Expect(apierrors.IsNotFound(gone)).To(BeTrue())
	})

	It("keeps a replacement whose target lives on a draining node and requeues", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		res, err := newReconciler().Reconcile(ctx, podReq(repl))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(30 * time.Second))
		Expect(getPod(repl.Name).DeletionTimestamp).To(BeNil())
	})

	It("deletes replacements of a Cancelled node", func() {
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

	It("lets go of the surplus tail and keeps the oldest within the count", func() {
		f := setupFixture()
		other := createNode(uniq("other-node"), nil)
		older := createReplacementNamed(f, f.rs.Name+"-repl-a", other.Name, false)
		younger := createReplacementNamed(f, f.rs.Name+"-repl-b", other.Name, false)

		// one live target: the oldest is kept, the tail deletes itself
		_, err := newReconciler().Reconcile(ctx, podReq(younger))
		Expect(err).NotTo(HaveOccurred())
		Expect(getPod(younger.Name).DeletionTimestamp).NotTo(BeNil())

		res, err := newReconciler().Reconcile(ctx, podReq(older))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(30 * time.Second))
		Expect(getPod(older.Name).DeletionTimestamp).To(BeNil())
	})

	It("lets go of a replacement whose only target sits on a Complete node", func() {
		f := setupFixture()
		// the drain finished and latched; a target seated late on that node is not acted on,
		// so the Pod-side count must exclude it too
		node := getNode(f.node.Name)
		node.Labels[LabelState] = StateComplete
		node.Spec.Unschedulable = true
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		other := createNode(uniq("other-node"), nil)
		repl := createReplacement(f, other.Name, false)

		_, err := newReconciler().Reconcile(ctx, podReq(repl))
		Expect(err).NotTo(HaveOccurred())
		Expect(getPod(repl.Name).DeletionTimestamp).NotTo(BeNil())
	})

	It("never touches a Pod with a controller ownerRef", func() {
		f := setupFixture()
		adopted := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      uniq("adopted"),
				Namespace: "default",
				Labels:    map[string]string{LabelReplaces: string(f.rs.UID)},
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
