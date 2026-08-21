//go:build e2e
// +build e2e

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

package e2e

// e2e is the only layer where the kcm and the scheduler actually run. This is where the
// full loop — ReplicaSet adoption and surplus deletion included — is seen converging.
// Only scenarios that can be induced deterministically, without timing races, live here.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rogeeoh/soft-drain/test/utils"
)

const namespace = "soft-drain-system"

func kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

func mustKubectl(args ...string) string {
	out, err := kubectl(args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "kubectl %v failed: %s", args, out)
	return out
}

func applyYAML(y string) {
	f := filepath.Join(GinkgoT().TempDir(), "obj.yaml")
	ExpectWithOffset(1, os.WriteFile(f, []byte(y), 0o600)).To(Succeed())
	mustKubectl("apply", "-f", f)
}

type workload struct {
	name         string
	replicas     int    // 0 means 1
	pin          string // non-empty pins to that node with a nodeSelector
	recreate     bool   // Recreate strategy (the rollout deletes the old Pod at once)
	tolerate     bool   // node.kubernetes.io/unschedulable toleration
	antiAffinity bool   // required podAntiAffinity(hostname) — spread one per node
	readyDelay   int    // readiness initialDelaySeconds (0 means 1)
	probeFile    string // non-empty adds a hostPath file exec probe — Ready only on the node holding the file
	pvc          string // non-empty mounts this PVC
}

func deployYAML(w workload) string {
	replicas := w.replicas
	if replicas == 0 {
		replicas = 1
	}
	strategy := ""
	if w.recreate {
		strategy = "  strategy:\n    type: Recreate\n"
	}
	podExtras := ""
	if w.pin != "" {
		podExtras += fmt.Sprintf("      nodeSelector:\n        kubernetes.io/hostname: %s\n", w.pin)
	}
	if w.tolerate {
		podExtras += "      tolerations:\n" +
			"      - key: node.kubernetes.io/unschedulable\n" +
			"        operator: Exists\n" +
			"        effect: NoSchedule\n"
	}
	if w.antiAffinity {
		podExtras += fmt.Sprintf("      affinity:\n"+
			"        podAntiAffinity:\n"+
			"          requiredDuringSchedulingIgnoredDuringExecution:\n"+
			"          - labelSelector:\n"+
			"              matchLabels:\n"+
			"                app: %s\n"+
			"            topologyKey: kubernetes.io/hostname\n", w.name)
	}
	containerExtras := ""
	switch {
	case w.probeFile != "":
		podExtras += "      volumes:\n" +
			"      - name: marker\n" +
			"        hostPath:\n" +
			"          path: /var/lib/sd-e2e\n" +
			"          type: DirectoryOrCreate\n"
		containerExtras += "        volumeMounts:\n" +
			"        - name: marker\n" +
			"          mountPath: /marker\n"
	case w.pvc != "":
		podExtras += fmt.Sprintf("      volumes:\n"+
			"      - name: data\n"+
			"        persistentVolumeClaim:\n"+
			"          claimName: %s\n", w.pvc)
		containerExtras += "        volumeMounts:\n" +
			"        - name: data\n" +
			"          mountPath: /data\n"
	}
	delay := w.readyDelay
	if delay == 0 {
		delay = 1
	}
	probe := fmt.Sprintf("        readinessProbe:\n"+
		"          httpGet:\n"+
		"            path: /\n"+
		"            port: 80\n"+
		"          initialDelaySeconds: %d\n"+
		"          periodSeconds: 2\n", delay)
	if w.probeFile != "" {
		probe = fmt.Sprintf("        readinessProbe:\n"+
			"          exec:\n"+
			"            command: [\"cat\", %q]\n"+
			"          initialDelaySeconds: 1\n"+
			"          periodSeconds: 2\n", w.probeFile)
	}
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: default
spec:
%s  replicas: %d
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
%s      containers:
      - name: app
        image: %s
%s%s`, w.name, strategy, replicas, w.name, w.name, podExtras, workloadImage, containerExtras, probe)
}

func pvcYAML(name string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: default
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 100Mi
`, name)
}

func pdbYAML(name, app string) string {
	return fmt.Sprintf(`apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: %s
  namespace: default
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: %s
`, name, app)
}

func statefulSetYAML(name, node string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: default
spec:
  clusterIP: None
  selector:
    app: %s
  ports:
  - port: 80
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: default
spec:
  serviceName: %s
  replicas: 1
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      nodeSelector:
        kubernetes.io/hostname: %s
      containers:
      - name: app
        image: %s
`, name, name, name, name, name, name, node, workloadImage)
}

func jobYAML(name, node string) string {
	return fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: default
spec:
  template:
    metadata:
      labels:
        app: %s
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: %s
      containers:
      - name: app
        image: %s
        command: ["sleep", "3600"]
`, name, name, node, workloadImage)
}

func serviceYAML(app string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: default
spec:
  selector:
    app: %s
  ports:
  - port: 80
    targetPort: 80
`, app, app)
}

func nakedPodYAML(name, node string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
  - name: app
    image: %s
`, name, node, workloadImage)
}

func nodeStateLabel(node string) string {
	out, _ := kubectl("get", "node", node, "-o", `jsonpath={.metadata.labels.soft-drain\.com/state}`)
	return strings.TrimSpace(out)
}

func nodeUnschedulable(node string) string {
	out, _ := kubectl("get", "node", node, "-o", "jsonpath={.spec.unschedulable}")
	return strings.TrimSpace(out)
}

// podsOf returns every Pod of that app, original or replacement.
func podsOf(app string) []string {
	out := mustKubectl("get", "pods", "-n", "default", "-l", "app="+app,
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)
	return nonEmptyLines(out)
}

func nodeOfPod(name string) string {
	return strings.TrimSpace(mustKubectl("get", "pod", "-n", "default", name,
		"-o", "jsonpath={.spec.nodeName}"))
}

func podPhase(name string) string {
	return strings.TrimSpace(mustKubectl("get", "pod", "-n", "default", name,
		"-o", "jsonpath={.status.phase}"))
}

func podCost(name string) string {
	out, _ := kubectl("get", "pod", "-n", "default", name,
		"-o", `jsonpath={.metadata.annotations.controller\.kubernetes\.io/pod-deletion-cost}`)
	return strings.TrimSpace(out)
}

// replacementPods counts only that app's replacements. Counting cluster-wide would be
// polluted by leftovers from another spec or an earlier run.
func replacementPods(app string) []string {
	out := mustKubectl("get", "pods", "-n", "default", "-l", "soft-drain.com/replaces,app="+app,
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)
	return nonEmptyLines(out)
}

func nonEmptyLines(s string) []string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, strings.TrimSpace(l))
		}
	}
	return lines
}

func allWorkers() []string {
	out := mustKubectl("get", "nodes",
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)
	var workers []string
	for _, n := range nonEmptyLines(out) {
		if strings.Contains(n, "worker") {
			workers = append(workers, n)
		}
	}
	return workers
}

// pickWorker picks one schedulable worker node.
func pickWorker() string {
	out := mustKubectl("get", "nodes",
		"-o", `jsonpath={range .items[*]}{.metadata.name} {.spec.unschedulable}{"\n"}{end}`)
	for _, l := range nonEmptyLines(out) {
		fields := strings.Fields(l)
		if strings.Contains(fields[0], "worker") && len(fields) == 1 {
			return fields[0]
		}
	}
	Fail("no schedulable worker node found")
	return ""
}

// cleanup is best-effort. The last spec's DeferCleanup can run after AfterAll (which
// undeploys the controller), so it takes things back itself instead of relying on restore.
func cleanupDrainNode(node string) {
	_, _ = kubectl("label", "node", node, "soft-drain.com/drain-")
	_, _ = kubectl("label", "node", node, "soft-drain.com/state-")
	_, _ = kubectl("annotate", "node", node, "soft-drain.com/cordoned-by-controller-")
	_, _ = kubectl("uncordon", node)
}

func cleanupApp(app string) {
	_, _ = kubectl("delete", "deployment", "-n", "default", app, "--ignore-not-found", "--wait=false")
	// An ownerless replacement is not collected by deleting the Deployment. Delete it here
	// so nothing is left behind once the controller is gone.
	_, _ = kubectl("delete", "pods", "-n", "default", "-l", "app="+app, "--ignore-not-found", "--wait=false")
}

func cleanupDrain(node, app string) {
	cleanupDrainNode(node)
	cleanupApp(app)
}

// deployPackedOnNode cordons the other workers briefly so the whole workload lands on
// that node. Unlike a nodeSelector pin, replacements are free to go to another node.
func deployPackedOnNode(w workload, node string) {
	var others []string
	for _, o := range allWorkers() {
		if o != node {
			others = append(others, o)
		}
	}
	for _, o := range others {
		mustKubectl("cordon", o)
	}
	defer func() {
		for _, o := range others {
			_, _ = kubectl("uncordon", o)
		}
	}()
	applyYAML(deployYAML(w))
	mustKubectl("rollout", "status", "-n", "default", "deploy/"+w.name, "--timeout=180s")
}

const controllerDeploy = "deploy/soft-drain-controller-manager"

// startPinnedDrain creates a workload pinned to a node and starts a drain, driving it to
// the state "InProgress with one Pending replacement".
func startPinnedDrain(app, worker string, w workload) (origPod string) {
	applyYAML(deployYAML(w))
	mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
	origPod = podsOf(app)[0]

	mustKubectl("label", "node", worker, "soft-drain.com/drain=true")
	EventuallyWithOffset(1, func(g Gomega) {
		g.Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
		repls := replacementPods(app)
		g.Expect(repls).To(HaveLen(1))
		g.Expect(podPhase(repls[0])).To(Equal("Pending"))
	}, 2*time.Minute, 3*time.Second).Should(Succeed())
	return origPod
}

type availabilityMonitor struct {
	stop       chan struct{}
	done       chan struct{}
	mu         sync.Mutex
	violations []string
}

// watchAvailability samples each Deployment's availableReplicas and the Service Endpoints
// periodically, recording any moment either falls below the intended count.
func watchAvailability(apps map[string]int) *availabilityMonitor {
	m := &availabilityMonitor{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(m.done)
		for {
			select {
			case <-m.stop:
				return
			default:
			}
			for app, want := range apps {
				out, err := kubectl("get", "deploy", "-n", "default", app,
					"-o", "jsonpath={.status.availableReplicas}")
				if err == nil {
					n, _ := strconv.Atoi(strings.TrimSpace(out))
					if n < want {
						m.record(fmt.Sprintf("%s availableReplicas=%d, want >= %d", app, n, want))
					}
				}
				eps, err := kubectl("get", "endpoints", "-n", "default", app,
					"-o", "jsonpath={.subsets[*].addresses[*].ip}")
				if err == nil && strings.TrimSpace(eps) == "" {
					m.record(app + " has no ready endpoint")
				}
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()
	return m
}

func (m *availabilityMonitor) record(v string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.violations = append(m.violations, time.Now().Format("15:04:05.000")+" "+v)
}

func (m *availabilityMonitor) finish() []string {
	close(m.stop)
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.violations
}

var _ = Describe("soft-drain", Ordered, func() {
	BeforeAll(func() {
		By("creating manager namespace")
		_, _ = kubectl("create", "ns", namespace)

		By("deploying the controller-manager")
		_, err := utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("waiting for the controller-manager to be ready")
		mustKubectl("rollout", "status", "-n", namespace, "deploy/soft-drain-controller-manager",
			"--timeout=120s")
	})

	AfterAll(func() {
		By("undeploying the controller-manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))
		_, _ = kubectl("delete", "ns", namespace, "--ignore-not-found")
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		logs, _ := kubectl("logs", "-n", namespace, "deploy/soft-drain-controller-manager", "--tail=100")
		_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s\n", logs)
		nodes, _ := kubectl("get", "nodes", "-o", "wide")
		_, _ = fmt.Fprintf(GinkgoWriter, "Nodes:\n%s\n", nodes)
		pods, _ := kubectl("get", "pods", "-A", "-o", "wide")
		_, _ = fmt.Fprintf(GinkgoWriter, "Pods:\n%s\n", pods)
	})

	It("moves the workload to another node and attaches Complete", Label("shard-a"), func() {
		const app = "sd-happy"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")

		origPod := podsOf(app)[0]
		srcNode := nodeOfPod(origPod)
		DeferCleanup(func() { cleanupDrain(srcNode, app) })

		By("labeling the node")
		mustKubectl("label", "node", srcNode, "soft-drain.com/drain=true")

		By("waiting for Complete")
		Eventually(func() string { return nodeStateLabel(srcNode) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))

		// the node stays cordoned
		Expect(nodeUnschedulable(srcNode)).To(Equal("true"))

		By("verifying the workload moved")
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(pods[0]).NotTo(Equal(origPod))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(srcNode))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			// adopted by the ReplicaSet: the hash is there and replaces is gone
			labels := mustKubectl("get", "pod", "-n", "default", pods[0], "-o", "jsonpath={.metadata.labels}")
			g.Expect(labels).To(ContainSubstring("pod-template-hash"))
			g.Expect(labels).NotTo(ContainSubstring("soft-drain.com/replaces"))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("moving several Deployments at once never drops availability below spec", Label("shard-a"), func() {
		apps := map[string]int{
			"sd-multi-1": 1, "sd-multi-2": 1, "sd-multi-3": 1, "sd-multi-4": 1, "sd-multi-5": 1,
			"sd-multi-ha": 2,
		}
		for app, replicas := range apps {
			applyYAML(deployYAML(workload{name: app, replicas: replicas}))
			applyYAML(serviceYAML(app))
		}
		for app := range apps {
			mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		}
		srcNode := nodeOfPod(podsOf("sd-multi-1")[0])
		DeferCleanup(func() {
			cleanupDrainNode(srcNode)
			for app := range apps {
				cleanupApp(app)
				_, _ = kubectl("delete", "service", "-n", "default", app, "--ignore-not-found")
			}
		})

		By("watching availability and draining the node")
		monitor := watchAvailability(apps)
		mustKubectl("label", "node", srcNode, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(srcNode) },
			5*time.Minute, 3*time.Second).Should(Equal("Complete"))

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped during drain:\n%s", strings.Join(violations, "\n"))

		By("verifying no Deployment pod remains on the drained node")
		for app := range apps {
			for _, p := range podsOf(app) {
				Expect(nodeOfPod(p)).NotTo(Equal(srcNode), "pod %s of %s is still on %s", p, app, srcNode)
			}
		}
	})

	It("waits in Pending with nowhere to go, restores when the label is removed", Label("shard-a"), func() {
		const app = "sd-pending"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		// not every worker can be blocked, so a node pin manufactures "nowhere to go"
		origPod := startPinnedDrain(app, worker, workload{name: app, pin: worker})

		Expect(podCost(origPod)).To(Equal("-2147483648"))

		By("removing the label")
		mustKubectl("label", "node", worker, "soft-drain.com/drain-")

		By("waiting for the node and the workload to be restored")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(BeEmpty())
			g.Expect(nodeUnschedulable(worker)).To(BeEmpty())
			g.Expect(replacementPods(app)).To(BeEmpty())
			g.Expect(podCost(origPod)).To(BeEmpty())
		}, 2*time.Minute, 3*time.Second).Should(Succeed())

		// the original Pod was left untouched
		Expect(podsOf(app)).To(Equal([]string{origPod}))
	})

	It("uncordon cancels and leaves Cancelled", Label("shard-a"), func() {
		const app = "sd-cancel"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		origPod := startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("uncordoning the node")
		mustKubectl("uncordon", worker)

		By("waiting for Cancelled")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(Equal("Cancelled"))
			g.Expect(nodeUnschedulable(worker)).To(BeEmpty())
			g.Expect(podCost(origPod)).To(BeEmpty())
		}, 90*time.Second, 3*time.Second).Should(Succeed())

		By("waiting for replacement pods to be reclaimed")
		Eventually(func() []string { return replacementPods(app) },
			2*time.Minute, 3*time.Second).Should(BeEmpty())

		// the original Pod was left untouched
		Expect(podsOf(app)).To(Equal([]string{origPod}))
	})

	It("converges on the new template when a rolling update lands mid-drain", Label("shard-b"), func() {
		const app = "sd-rolling"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]
		srcNode := nodeOfPod(origPod)
		DeferCleanup(func() { cleanupDrain(srcNode, app) })

		mustKubectl("label", "node", srcNode, "soft-drain.com/drain=true")

		By("triggering a rolling update mid-drain")
		mustKubectl("patch", "deployment", "-n", "default", app, "--type=merge",
			"-p", `{"spec":{"template":{"metadata":{"labels":{"rollout":"v2"}}}}}`)

		By("waiting for convergence")
		Eventually(func() string { return nodeStateLabel(srcNode) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(srcNode))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			v := strings.TrimSpace(mustKubectl("get", "pod", "-n", "default", pods[0],
				"-o", "jsonpath={.metadata.labels.rollout}"))
			g.Expect(v).To(Equal("v2"))
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 3*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("the replacement follows when a rollout prunes its target", Label("shard-b"), func() {
		const app = "sd-prune"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker, recreate: true})

		// Recreate deletes the old Pod (the target) at once. The new Pod is pinned to the
		// cordoned node and stays mis-scheduled Pending, so it is not a target.
		By("triggering a Recreate rollout mid-drain")
		mustKubectl("patch", "deployment", "-n", "default", app, "--type=merge",
			"-p", `{"spec":{"template":{"metadata":{"labels":{"rollout":"v2"}}}}}`)

		By("waiting for the replacement to follow the target away")
		Eventually(func(g Gomega) {
			g.Expect(replacementPods(app)).To(BeEmpty())
			g.Expect(nodeStateLabel(worker)).To(Equal("Complete"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("a template change mid-drain retires the stale replacement and the rollout moves the Pod", Label("shard-a"), func() {
		const app = "sd-supersede"
		applyYAML(deployYAML(workload{name: app, readyDelay: 45}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]
		srcNode := nodeOfPod(origPod)
		DeferCleanup(func() { cleanupDrain(srcNode, app) })

		mustKubectl("label", "node", srcNode, "soft-drain.com/drain=true")
		var stale string
		Eventually(func(g Gomega) {
			repls := replacementPods(app)
			g.Expect(repls).To(HaveLen(1))
			stale = repls[0]
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("changing the template while the replacement is far from Ready")
		mustKubectl("patch", "deployment", "-n", "default", app, "--type=merge",
			"-p", `{"spec":{"template":{"metadata":{"labels":{"rollout":"v2"}}}}}`)

		// reclaimed long before the 45s readiness elapses — waiting for Ready would hang here
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "pod", "-n", "default", stale,
				"-o", "jsonpath={.metadata.deletionTimestamp}")
			if err != nil {
				// only a Pod that is fully gone passes — a transient API error is not a pass
				g.Expect(err.Error()).To(ContainSubstring("NotFound"))
				return
			}
			g.Expect(strings.TrimSpace(out)).NotTo(BeEmpty())
		}, 40*time.Second, 2*time.Second).Should(Succeed())

		By("the rollout finishes the move and the drain completes without a new replacement")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(srcNode)).To(Equal("Complete"))
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(srcNode))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 4*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("a pending replacement retires the moment its landing node starts draining", Label("shard-b"), func() {
		const app = "sd-landing"
		applyYAML(deployYAML(workload{name: app, readyDelay: 45}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		srcNode := nodeOfPod(podsOf(app)[0])

		// make the landing deterministic: keep only land and hand-cordon the other workers
		var land string
		var parked []string
		for _, w := range allWorkers() {
			switch {
			case w == srcNode:
			case land == "":
				land = w
			default:
				parked = append(parked, w)
			}
		}
		Expect(land).NotTo(BeEmpty())
		for _, w := range parked {
			mustKubectl("cordon", w)
		}
		DeferCleanup(func() {
			for _, w := range parked {
				_, _ = kubectl("uncordon", w)
			}
			cleanupDrainNode(land)
			cleanupDrain(srcNode, app)
		})

		mustKubectl("label", "node", srcNode, "soft-drain.com/drain=true")
		var repl string
		Eventually(func(g Gomega) {
			repls := replacementPods(app)
			g.Expect(repls).To(HaveLen(1))
			g.Expect(nodeOfPod(repls[0])).To(Equal(land))
			repl = repls[0]
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("draining the node the replacement landed on")
		mustKubectl("label", "node", land, "soft-drain.com/drain=true")

		// reclaimed long before the 45s readiness elapses
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "pod", "-n", "default", repl,
				"-o", "jsonpath={.metadata.deletionTimestamp}")
			if err != nil {
				// only a Pod that is fully gone passes — a transient API error is not a pass
				g.Expect(err.Error()).To(ContainSubstring("NotFound"))
				return
			}
			g.Expect(strings.TrimSpace(out)).NotTo(BeEmpty())
		}, 40*time.Second, 2*time.Second).Should(Succeed())

		By("a fresh replacement exists with nowhere to go")
		Eventually(func(g Gomega) {
			var live []string
			for _, p := range replacementPods(app) {
				out, err := kubectl("get", "pod", "-n", "default", p,
					"-o", "jsonpath={.metadata.deletionTimestamp}")
				if err == nil && strings.TrimSpace(out) == "" {
					live = append(live, p)
				}
			}
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0]).NotTo(Equal(repl))
			// even if it briefly lands on land and is reclaimed, it converges to Pending
			g.Expect(podPhase(live[0])).To(Equal("Pending"))
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("uncordoning a parked worker lets the drain converge")
		mustKubectl("uncordon", parked[0])
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(srcNode)).To(Equal("Complete"))
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).To(Equal(parked[0]))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 4*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("the replacement follows when the Deployment is deleted mid-drain", Label("shard-a"), func() {
		const app = "sd-delete"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("deleting the deployment mid-drain")
		mustKubectl("delete", "deployment", "-n", "default", app)

		Eventually(func(g Gomega) {
			g.Expect(replacementPods(app)).To(BeEmpty())
			g.Expect(nodeStateLabel(worker)).To(Equal("Complete"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("scale-up mid-drain still means one replacement per target", Label("shard-b"), func() {
		const app = "sd-scaleup"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("scaling up mid-drain")
		mustKubectl("scale", "deployment", "-n", "default", app, "--replicas=3")

		// the new Pods are pinned to the cordoned node and stay mis-scheduled Pending — not targets.
		// Creating by replicas instead of "one per target" would over-create right here.
		Consistently(func() []string { return replacementPods(app) },
			45*time.Second, 5*time.Second).Should(HaveLen(1))
		Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
	})

	It("scale to zero mid-drain removes replacements and completes", Label("shard-b"), func() {
		const app = "sd-scalezero"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("scaling to zero mid-drain")
		mustKubectl("scale", "deployment", "-n", "default", app, "--replicas=0")

		Eventually(func(g Gomega) {
			g.Expect(replacementPods(app)).To(BeEmpty())
			g.Expect(nodeStateLabel(worker)).To(Equal("Complete"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("draining every worker deadlocks, restoring one unblocks", Label("shard-a"), func() {
		const app = "sd-alldrain"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		src := nodeOfPod(podsOf(app)[0])
		workers := allWorkers()
		DeferCleanup(func() {
			for _, w := range workers {
				cleanupDrainNode(w)
			}
			cleanupApp(app)
		})

		// block the empty workers first. Labelling src first would let the replacement schedule
		// onto a worker not yet cordoned and finish without ever deadlocking.
		By("cordoning off every other worker first")
		for _, w := range workers {
			if w != src {
				mustKubectl("label", "node", w, "soft-drain.com/drain=true")
			}
		}
		Eventually(func(g Gomega) {
			for _, w := range workers {
				if w != src {
					g.Expect(nodeStateLabel(w)).To(Equal("Complete"))
				}
			}
		}, 2*time.Minute, 3*time.Second).Should(Succeed())

		By("draining the workload's node")
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		By("waiting for the stuck state")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(Equal("InProgress"))
			repls := replacementPods(app)
			g.Expect(repls).To(HaveLen(1))
			g.Expect(podPhase(repls[0])).To(Equal("Pending"))
		}, 3*time.Minute, 3*time.Second).Should(Succeed())

		// give back one Complete empty worker. Removing just the label makes the controller take
		// back the cordon it placed — that behaviour is what breaks the deadlock here.
		var freed string
		for _, w := range workers {
			if w != src {
				freed = w
				break
			}
		}
		By("freeing one completed worker")
		mustKubectl("label", "node", freed, "soft-drain.com/drain-")

		By("waiting for the stuck drain to complete")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(Equal("Complete"))
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).To(Equal(freed))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 3*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("a node a human cordoned first keeps human cordon ownership at the end", Label("shard-a"), func() {
		const app = "sd-precordon"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		src := nodeOfPod(podsOf(app)[0])
		DeferCleanup(func() { cleanupDrain(src, app) })

		By("cordoning first, then labeling")
		mustKubectl("cordon", src)
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		Eventually(func() string { return nodeStateLabel(src) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))

		// not a cordon we placed, so there is no annotation
		ann, _ := kubectl("get", "node", src,
			"-o", `jsonpath={.metadata.annotations.soft-drain\.com/cordoned-by-controller}`)
		Expect(strings.TrimSpace(ann)).To(BeEmpty())

		By("removing the label keeps the human's cordon")
		mustKubectl("label", "node", src, "soft-drain.com/drain-")
		Eventually(func() string { return nodeStateLabel(src) },
			60*time.Second, 2*time.Second).Should(BeEmpty())
		Expect(nodeUnschedulable(src)).To(Equal("true"))
	})

	It("Pods not owned by a Deployment are untouched", Label("shard-d"), func() {
		const naked = "sd-naked"
		worker := pickWorker()
		DeferCleanup(func() {
			_, _ = kubectl("delete", "pod", "-n", "default", naked, "--ignore-not-found", "--wait=false")
			cleanupDrainNode(worker)
		})

		applyYAML(nakedPodYAML(naked, worker))
		mustKubectl("wait", "--for=condition=Ready", "pod/"+naked, "-n", "default", "--timeout=120s")

		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// a naked pod is not a target, so the node goes Complete right away
		Eventually(func() string { return nodeStateLabel(worker) },
			2*time.Minute, 3*time.Second).Should(Equal("Complete"))

		Expect(podPhase(naked)).To(Equal("Running"))
		Expect(nodeOfPod(naked)).To(Equal(worker))
		Expect(podCost(naked)).To(BeEmpty())
	})

	It("two drains in a row each move and finish", Label("shard-c"), func() {
		const app = "sd-redrain"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		first := nodeOfPod(podsOf(app)[0])
		DeferCleanup(func() {
			for _, w := range allWorkers() {
				cleanupDrainNode(w)
			}
			cleanupApp(app)
		})

		By("draining the first node")
		mustKubectl("label", "node", first, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(first) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))

		var second string
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			second = nodeOfPod(pods[0])
			g.Expect(second).NotTo(Equal(first))
		}, 60*time.Second, 3*time.Second).Should(Succeed())

		By("restoring the first node and draining the second")
		mustKubectl("label", "node", first, "soft-drain.com/drain-")
		mustKubectl("uncordon", first)
		mustKubectl("label", "node", second, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(second) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))

		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(second))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("a workload tolerating unschedulable loops on landed replacements", Label("shard-d"), func() {
		const app = "sd-tolerate"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		applyYAML(deployYAML(workload{name: app, pin: worker, tolerate: true}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]

		// start clean so events with the same reason from an earlier spec or run do not pollute it
		_, _ = kubectl("delete", "events", "-n", "default",
			"--field-selector", "reason=ReplacementOnDrainingNode", "--ignore-not-found")

		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// a replacement that gets through the cordon onto the same node is deleted once Ready,
		// and the next round creates another. The repeating Warning Events are the evidence.
		By("waiting for the landing-deletion loop to leave evidence")
		Eventually(func(g Gomega) {
			out, _ := kubectl("get", "events", "-n", "default",
				"--field-selector", "reason=ReplacementOnDrainingNode", "-o", "name")
			g.Expect(nonEmptyLines(out)).NotTo(BeEmpty())
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		// it cannot be moved, so this stays InProgress and the original Pod lives
		Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
		Expect(podsOf(app)).To(ContainElement(origPod))
	})

	// ─── multiple replicas ───

	It("r=3 packed on one node moves all three without downtime", Label("shard-d"), func() {
		const app = "sd-pack"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		deployPackedOnNode(workload{name: app, replicas: 3}, worker)

		monitor := watchAvailability(map[string]int{app: 3})
		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(worker) },
			4*time.Minute, 3*time.Second).Should(Equal("Complete"))

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped during drain:\n%s", strings.Join(violations, "\n"))

		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(3))
			for _, p := range pods {
				g.Expect(nodeOfPod(p)).NotTo(Equal(worker))
				g.Expect(podPhase(p)).To(Equal("Running"))
			}
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("with r=3 spread out only the draining node's Pod moves, the rest keep their names", Label("shard-d"), func() {
		const app = "sd-spread"
		applyYAML(deployYAML(workload{name: app, replicas: 3, antiAffinity: true}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")

		pods := podsOf(app)
		Expect(pods).To(HaveLen(3))
		src := nodeOfPod(pods[0])
		DeferCleanup(func() { cleanupDrain(src, app) })
		untouched := map[string]string{} // name -> node
		for _, p := range pods[1:] {
			untouched[p] = nodeOfPod(p)
		}

		monitor := watchAvailability(map[string]int{app: 3})
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(src) },
			4*time.Minute, 3*time.Second).Should(Equal("Complete"))

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped during drain:\n%s", strings.Join(violations, "\n"))

		// the Pod on the other node is identical down to its name (the object) — no wrong Pod was killed
		for name, node := range untouched {
			Expect(podPhase(name)).To(Equal("Running"))
			Expect(nodeOfPod(name)).To(Equal(node))
		}
	})

	It("draining both nodes of an r=2 Deployment at once stays zero-downtime", Label("shard-d"), func() {
		const app = "sd-straddle"
		applyYAML(deployYAML(workload{name: app, replicas: 2, antiAffinity: true}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")

		pods := podsOf(app)
		Expect(pods).To(HaveLen(2))
		nodeA, nodeB := nodeOfPod(pods[0]), nodeOfPod(pods[1])
		DeferCleanup(func() {
			cleanupDrainNode(nodeA)
			cleanupDrain(nodeB, app)
		})

		monitor := watchAvailability(map[string]int{app: 2})
		mustKubectl("label", "node", nodeA, "soft-drain.com/drain=true")
		mustKubectl("label", "node", nodeB, "soft-drain.com/drain=true")

		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(nodeA)).To(Equal("Complete"))
			g.Expect(nodeStateLabel(nodeB)).To(Equal("Complete"))
		}, 5*time.Minute, 3*time.Second).Should(Succeed())

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped during drain:\n%s", strings.Join(violations, "\n"))

		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				g.Expect(nodeOfPod(p)).NotTo(BeElementOf(nodeA, nodeB))
				g.Expect(podPhase(p)).To(Equal("Running"))
			}
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("saturated antiAffinity leaves no seat and blocks in Pending", Label("shard-b"), func() {
		const app = "sd-full"
		workers := allWorkers()
		applyYAML(deployYAML(workload{name: app, replicas: len(workers), antiAffinity: true}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")

		src := nodeOfPod(podsOf(app)[0])
		DeferCleanup(func() { cleanupDrain(src, app) })

		mustKubectl("label", "node", src, "soft-drain.com/drain=true")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(Equal("InProgress"))
			repls := replacementPods(app)
			g.Expect(repls).To(HaveLen(1))
			g.Expect(podPhase(repls[0])).To(Equal("Pending"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())

		// the arithmetic of "bring it up first, delete it later" in general — it stays blocked
		Consistently(func() string { return nodeStateLabel(src) },
			30*time.Second, 5*time.Second).Should(Equal("InProgress"))

		mustKubectl("label", "node", src, "soft-drain.com/drain-")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(BeEmpty())
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	// ─── disruptive actors ───

	It("a controller restart mid-drain resumes without duplicates", Label("shard-c"), func() {
		const app = "sd-restart"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("restarting the controller mid-drain")
		mustKubectl("rollout", "restart", "-n", namespace, controllerDeploy)
		mustKubectl("rollout", "status", "-n", namespace, controllerDeploy, "--timeout=120s")

		// memoryless, so the decision after a restart is the same — no replacement is added or lost
		Consistently(func(g Gomega) {
			g.Expect(replacementPods(app)).To(HaveLen(1))
			g.Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		// the restarted controller handles the cancel too
		mustKubectl("label", "node", worker, "soft-drain.com/drain-")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(BeEmpty())
			g.Expect(nodeUnschedulable(worker)).To(BeEmpty())
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("labels applied while the controller is down are handled once it returns", Label("shard-c"), func() {
		const app = "sd-offline"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		src := nodeOfPod(podsOf(app)[0])
		DeferCleanup(func() {
			_, _ = kubectl("scale", "-n", namespace, controllerDeploy, "--replicas=1")
			cleanupDrain(src, app)
		})

		By("scaling the controller down to zero")
		mustKubectl("scale", "-n", namespace, controllerDeploy, "--replicas=0")
		mustKubectl("wait", "--for=delete", "pod", "-l", "control-plane=controller-manager",
			"-n", namespace, "--timeout=60s")

		By("labeling while nobody is watching")
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")
		Consistently(func() string { return nodeStateLabel(src) },
			10*time.Second, 2*time.Second).Should(BeEmpty())

		By("scaling the controller back up")
		mustKubectl("scale", "-n", namespace, controllerDeploy, "--replicas=1")
		mustKubectl("rollout", "status", "-n", namespace, controllerDeploy, "--timeout=120s")

		// a watch is level, not edge — it converges from the current state with no missed events
		Eventually(func() string { return nodeStateLabel(src) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(src))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("draining the controller's own node migrates itself and converges", Label("shard-c"), func() {
		managerPods := func() []string {
			out := mustKubectl("get", "pods", "-n", namespace, "-l", "control-plane=controller-manager",
				"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)
			return nonEmptyLines(out)
		}
		pods := managerPods()
		Expect(pods).To(HaveLen(1))
		src := strings.TrimSpace(mustKubectl("get", "pod", "-n", namespace, pods[0],
			"-o", "jsonpath={.spec.nodeName}"))
		Expect(src).To(ContainSubstring("worker"))
		DeferCleanup(func() { cleanupDrainNode(src) })

		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		// Complete can only be set after the old controller Pod object is gone, and by then the
		// old instance is already dead — proof that the migrated instance took over the leader
		// lease and kept the loop running.
		Eventually(func() string { return nodeStateLabel(src) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))

		Eventually(func(g Gomega) {
			pods := managerPods()
			g.Expect(pods).To(HaveLen(1))
			node := strings.TrimSpace(mustKubectl("get", "pod", "-n", namespace, pods[0],
				"-o", "jsonpath={.spec.nodeName}"))
			g.Expect(node).NotTo(Equal(src))
			repl, _ := kubectl("get", "pods", "-n", namespace, "-l", "soft-drain.com/replaces",
				"-o", "jsonpath={.items[*].metadata.name}")
			g.Expect(strings.TrimSpace(repl)).To(BeEmpty())
			g.Expect(strings.TrimSpace(mustKubectl("get", "deploy", "-n", namespace,
				strings.TrimPrefix(controllerDeploy, "deploy/"),
				"-o", "jsonpath={.status.availableReplicas}"))).To(Equal("1"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("a hand-deleted replacement is recreated next round", Label("shard-a"), func() {
		const app = "sd-meddle"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		first := replacementPods(app)[0]
		By("deleting the replacement by hand")
		mustKubectl("delete", "pod", "-n", "default", first, "--wait=false")

		Eventually(func(g Gomega) {
			repls := replacementPods(app)
			g.Expect(repls).To(HaveLen(1))
			g.Expect(repls[0]).NotTo(Equal(first))
		}, 90*time.Second, 3*time.Second).Should(Succeed())
		Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
	})

	It("a hand-removed cost annotation is stamped again", Label("shard-b"), func() {
		const app = "sd-cost"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		origPod := startPinnedDrain(app, worker, workload{name: app, pin: worker})

		Expect(podCost(origPod)).To(Equal("-2147483648"))
		By("removing the cost annotation by hand")
		mustKubectl("annotate", "pod", "-n", "default", origPod,
			"controller.kubernetes.io/pod-deletion-cost-")

		Eventually(func() string { return podCost(origPod) },
			60*time.Second, 3*time.Second).Should(Equal("-2147483648"))
	})

	It("a paused Deployment never receives a handover even with a Ready replacement", Label("shard-b"), func() {
		const app = "sd-paused"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]
		src := nodeOfPod(origPod)
		DeferCleanup(func() {
			_, _ = kubectl("rollout", "resume", "-n", "default", "deploy/"+app)
			cleanupDrain(src, app)
		})

		// pause plus a template change pins Healthy(D) to false
		By("pausing the deployment with a pending template change")
		mustKubectl("rollout", "pause", "-n", "default", "deploy/"+app)
		mustKubectl("patch", "deployment", "-n", "default", app, "--type=merge",
			"-p", `{"spec":{"template":{"metadata":{"labels":{"rollout":"v2"}}}}}`)

		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		// the replacement must stay, replaces label and all, even once it is Ready
		By("verifying the handover is deferred")
		Eventually(func() []string { return replacementPods(app) },
			2*time.Minute, 3*time.Second).Should(HaveLen(1))
		Consistently(func(g Gomega) {
			g.Expect(replacementPods(app)).To(HaveLen(1))
			g.Expect(podPhase(origPod)).To(Equal("Running"))
			g.Expect(nodeStateLabel(src)).To(Equal("InProgress"))
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		By("resuming the rollout")
		mustKubectl("rollout", "resume", "-n", "default", "deploy/"+app)

		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(Equal("Complete"))
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(src))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			v := strings.TrimSpace(mustKubectl("get", "pod", "-n", "default", pods[0],
				"-o", "jsonpath={.metadata.labels.rollout}"))
			g.Expect(v).To(Equal("v2"))
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 3*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("label flapping converges on the last declaration", Label("shard-a"), func() {
		const app = "sd-flap"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		src := nodeOfPod(podsOf(app)[0])
		DeferCleanup(func() { cleanupDrain(src, app) })

		By("flapping the label")
		for i := 0; i < 3; i++ {
			mustKubectl("label", "node", src, "soft-drain.com/drain=true", "--overwrite")
			time.Sleep(2 * time.Second)
			mustKubectl("label", "node", src, "soft-drain.com/drain-")
			time.Sleep(2 * time.Second)
		}
		mustKubectl("label", "node", src, "soft-drain.com/drain=true", "--overwrite")

		Eventually(func() string { return nodeStateLabel(src) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(src))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	// ─── workload variety ───

	It("a forever-unready replacement never costs the original its life", Label("shard-d"), func() {
		const app = "sd-noready"
		worker := pickWorker()
		DeferCleanup(func() {
			cleanupDrain(worker, app)
			_, _ = utils.Run(exec.Command("docker", "exec", worker, "rm", "-rf", "/var/lib/sd-e2e"))
		})

		// the marker file exists only on this node — a replacement can never be Ready wherever it lands
		By("planting the readiness marker on one node only")
		_, err := utils.Run(exec.Command("docker", "exec", worker,
			"sh", "-c", "mkdir -p /var/lib/sd-e2e && touch /var/lib/sd-e2e/ready"))
		Expect(err).NotTo(HaveOccurred())

		deployPackedOnNode(workload{name: app, probeFile: "/marker/ready"}, worker)
		origPod := podsOf(app)[0]

		monitor := watchAvailability(map[string]int{app: 1})
		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// a replacement appears, and since it never goes Ready the hand-over never happens
		Eventually(func() []string { return replacementPods(app) },
			2*time.Minute, 3*time.Second).Should(HaveLen(1))
		Consistently(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
			g.Expect(podPhase(origPod)).To(Equal("Running"))
			g.Expect(replacementPods(app)).To(HaveLen(1))
		}, 45*time.Second, 5*time.Second).Should(Succeed())

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped while replacement never became ready:\n%s", strings.Join(violations, "\n"))
	})

	It("a slow-starting workload is moved without downtime after waiting for Ready", Label("shard-d"), func() {
		const app = "sd-slow"
		applyYAML(deployYAML(workload{name: app, readyDelay: 20}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		src := nodeOfPod(podsOf(app)[0])
		DeferCleanup(func() { cleanupDrain(src, app) })

		monitor := watchAvailability(map[string]int{app: 1})
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(src) },
			4*time.Minute, 3*time.Second).Should(Equal("Complete"))

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped during slow-start drain:\n%s", strings.Join(violations, "\n"))
	})

	It("a local-PVC workload stalls as documented instead of breaking", Label("shard-d"), func() {
		const app = "sd-pvc"
		applyYAML(pvcYAML("sd-data"))
		applyYAML(deployYAML(workload{name: app, pvc: "sd-data"}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]
		src := nodeOfPod(origPod)
		DeferCleanup(func() {
			cleanupDrain(src, app)
			_, _ = kubectl("delete", "pvc", "-n", "default", "sd-data", "--ignore-not-found", "--wait=false")
		})

		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		// the PV's node affinity points at the cordoned node, so the replacement is Pending forever
		Eventually(func(g Gomega) {
			repls := replacementPods(app)
			g.Expect(repls).To(HaveLen(1))
			g.Expect(podPhase(repls[0])).To(Equal("Pending"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
		Consistently(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(Equal("InProgress"))
			g.Expect(podPhase(origPod)).To(Equal("Running"))
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		mustKubectl("label", "node", src, "soft-drain.com/drain-")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(BeEmpty())
			g.Expect(replacementPods(app)).To(BeEmpty())
			g.Expect(podPhase(origPod)).To(Equal("Running"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("StatefulSets, Jobs and naked Pods are untouched and Complete still lands", Label("shard-d"), func() {
		worker := pickWorker()
		DeferCleanup(func() {
			_, _ = kubectl("delete", "statefulset", "-n", "default", "sd-sts", "--ignore-not-found", "--wait=false")
			_, _ = kubectl("delete", "service", "-n", "default", "sd-sts", "--ignore-not-found")
			_, _ = kubectl("delete", "job", "-n", "default", "sd-job", "--ignore-not-found", "--wait=false")
			_, _ = kubectl("delete", "pod", "-n", "default", "sd-naked", "--ignore-not-found", "--wait=false")
			cleanupDrainNode(worker)
		})

		By("planting non-Deployment workloads on the node")
		applyYAML(statefulSetYAML("sd-sts", worker))
		applyYAML(jobYAML("sd-job", worker))
		applyYAML(nakedPodYAML("sd-naked", worker))
		mustKubectl("rollout", "status", "-n", "default", "statefulset/sd-sts", "--timeout=120s")
		mustKubectl("wait", "--for=condition=Ready", "pod", "-l", "job-name=sd-job",
			"-n", "default", "--timeout=120s")
		mustKubectl("wait", "--for=condition=Ready", "pod/sd-naked", "-n", "default", "--timeout=120s")

		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// none of the three is a target, so the node goes Complete right away
		Eventually(func() string { return nodeStateLabel(worker) },
			2*time.Minute, 3*time.Second).Should(Equal("Complete"))

		for _, pod := range []string{"sd-sts-0", "sd-naked"} {
			Expect(podPhase(pod)).To(Equal("Running"))
			Expect(nodeOfPod(pod)).To(Equal(worker))
			Expect(podCost(pod)).To(BeEmpty())
		}
		jobPods := nonEmptyLines(mustKubectl("get", "pods", "-n", "default", "-l", "job-name=sd-job",
			"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`))
		Expect(jobPods).To(HaveLen(1))
		Expect(podPhase(jobPods[0])).To(Equal("Running"))
		Expect(podCost(jobPods[0])).To(BeEmpty())
	})

	It("a PDB cannot block it since eviction is never used, still zero-downtime", Label("shard-d"), func() {
		const app = "sd-pdb"
		applyYAML(deployYAML(workload{name: app, replicas: 2, antiAffinity: true}))
		applyYAML(pdbYAML("sd-pdb", app))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")

		pods := podsOf(app)
		Expect(pods).To(HaveLen(2))
		src := nodeOfPod(pods[0])
		DeferCleanup(func() {
			_, _ = kubectl("delete", "pdb", "-n", "default", "sd-pdb", "--ignore-not-found")
			cleanupDrain(src, app)
		})

		monitor := watchAvailability(map[string]int{app: 2})
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(src) },
			4*time.Minute, 3*time.Second).Should(Equal("Complete"))

		violations := monitor.finish()
		Expect(violations).To(BeEmpty(),
			"availability dropped during drain with PDB:\n%s", strings.Join(violations, "\n"))

		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				g.Expect(podPhase(p)).To(Equal("Running"))
			}
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("the kubectl plugin path equals the raw label path, start to release", Label("shard-c"), func() {
		const app = "sd-plugin"
		plugin := filepath.Join(GinkgoT().TempDir(), "kubectl-soft_drain")
		out, err := utils.Run(exec.Command("go", "build", "-o", plugin,
			"github.com/rogeeoh/soft-drain/cmd/kubectl-soft_drain"))
		Expect(err).NotTo(HaveOccurred(), out)

		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })

		By("starting a stuck drain with --wait=false")
		applyYAML(deployYAML(workload{name: app, pin: worker}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		out, err = utils.Run(exec.Command(plugin, worker, "--wait=false"))
		Expect(err).NotTo(HaveOccurred(), out)
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
			g.Expect(replacementPods(app)).To(HaveLen(1))
		}, 60*time.Second, 2*time.Second).Should(Succeed())

		By("cancelling via uncordon, then re-draining through the plugin")
		mustKubectl("uncordon", worker)
		Eventually(func() string { return nodeStateLabel(worker) },
			60*time.Second, 2*time.Second).Should(Equal("Cancelled"))
		// the plugin removes and re-adds the Cancelled latch — a re-drain is just a new drain
		out, err = utils.Run(exec.Command(plugin, worker, "--wait=false", "--timeout", "2m"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring("clearing it first"))
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
			g.Expect(replacementPods(app)).To(HaveLen(1))
		}, 60*time.Second, 2*time.Second).Should(Succeed())

		By("printing the version without touching the cluster")
		out, err = utils.Run(exec.Command(plugin, "version"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(strings.TrimSpace(out)).To(HavePrefix("kubectl-soft_drain"))

		By("checking status output, human and machine")
		out, err = utils.Run(exec.Command(plugin, "status"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring(worker))
		Expect(out).To(ContainSubstring("InProgress"))
		out, err = utils.Run(exec.Command(plugin, "status", worker, "-o", "json"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring(`"state": "InProgress"`))
		Expect(out).To(ContainSubstring(`"ready": false`))

		By("releasing")
		out, err = utils.Run(exec.Command(plugin, "release", worker, "--timeout", "2m"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(nodeStateLabel(worker)).To(BeEmpty())
		Expect(nodeUnschedulable(worker)).To(BeEmpty())
		Eventually(func() []string { return replacementPods(app) },
			60*time.Second, 2*time.Second).Should(BeEmpty())
		out, err = utils.Run(exec.Command(plugin, "status"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).NotTo(ContainSubstring(worker))

		By("draining and releasing two empty nodes in one call")
		var two []string
		for _, o := range allWorkers() {
			if o != worker && len(two) < 2 {
				two = append(two, o)
			}
		}
		Expect(two).To(HaveLen(2))
		DeferCleanup(func() {
			for _, o := range two {
				cleanupDrainNode(o)
			}
		})
		out, err = utils.Run(exec.Command(plugin, two[0], two[1], "--timeout", "2m"))
		Expect(err).NotTo(HaveOccurred(), out)
		for _, o := range two {
			Expect(nodeStateLabel(o)).To(Equal("Complete"))
		}
		out, err = utils.Run(exec.Command(plugin, "release", two[0], two[1], "--timeout", "2m"))
		Expect(err).NotTo(HaveOccurred(), out)
		for _, o := range two {
			Expect(nodeStateLabel(o)).To(BeEmpty())
			// release also takes back the cordon we placed — the restore patch is atomic, so an
			// empty state means the uncordon is done too
			Expect(nodeUnschedulable(o)).To(BeEmpty())
		}

		By("draining to completion in blocking mode")
		applyYAML(deployYAML(workload{name: app})) // unpinned — there is somewhere to go now
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		var src string
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			src = nodeOfPod(pods[0])
		}, 60*time.Second, 2*time.Second).Should(Succeed())
		DeferCleanup(func() { cleanupDrainNode(src) })

		out, err = utils.Run(exec.Command(plugin, src, "--timeout", "3m"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring("drained"))
		Expect(nodeStateLabel(src)).To(Equal("Complete"))
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(src))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("uncordon after Complete folds involvement and reopens the node for landings", Label("shard-c"), func() {
		const app = "sd-reopen"
		workers := allWorkers()
		Expect(len(workers)).To(BeNumerically(">=", 3))
		reopened, src := workers[0], workers[1]
		DeferCleanup(func() {
			cleanupDrainNode(reopened)
			cleanupDrain(src, app)
		})

		By("draining an empty node to Complete, then uncordoning it")
		mustKubectl("label", "node", reopened, "soft-drain.com/drain=true")
		Eventually(func() string { return nodeStateLabel(reopened) },
			2*time.Minute, 2*time.Second).Should(Equal("Complete"))
		mustKubectl("uncordon", reopened)
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(reopened)).To(Equal("Cancelled"))
			g.Expect(nodeUnschedulable(reopened)).To(BeEmpty())
		}, 60*time.Second, 2*time.Second).Should(Succeed())

		By("draining a workload whose replacement can only land on the reopened node")
		deployPackedOnNode(workload{name: app}, src)
		for _, o := range workers {
			if o != reopened && o != src {
				mustKubectl("cordon", o)
				DeferCleanup(func(node string) func() {
					return func() { _, _ = kubectl("uncordon", node) }
				}(o))
			}
		}
		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		// under the pre-fold rule the replacement would be deleted as fast as it lands and never finish
		Eventually(func() string { return nodeStateLabel(src) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).To(Equal(reopened))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})
})
