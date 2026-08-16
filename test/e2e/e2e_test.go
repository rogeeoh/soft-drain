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

// e2e는 kcm과 스케줄러가 실제로 도는 유일한 층이다. 여기서 처음으로
// ReplicaSet 입양과 초과분 삭제까지 포함한 전체 루프의 수렴을 본다.
// 타이밍 경합 없이 결정론적으로 유도할 수 있는 시나리오만 담는다.

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
	replicas     int    // 0이면 1
	pin          string // 비어 있지 않으면 그 노드에 nodeSelector로 고정
	recreate     bool   // Recreate 전략 (롤아웃이 옛 Pod을 즉시 지운다)
	tolerate     bool   // node.kubernetes.io/unschedulable toleration
	antiAffinity bool   // required podAntiAffinity(hostname) — 노드당 하나로 확산
	readyDelay   int    // readiness initialDelaySeconds (0이면 1)
	probeFile    string // 비어 있지 않으면 hostPath 파일 exec probe — 파일이 있는 노드에서만 Ready
	pvc          string // 비어 있지 않으면 이 PVC를 마운트
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

// podsOf는 원본이든 대체든 그 앱의 Pod 전부를 준다.
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

// replacementPods는 그 앱의 대체 Pod만 센다. 클러스터 전역으로 세면
// 다른 스펙이나 이전 실행이 남긴 것에 오염된다.
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

// pickWorker는 스케줄 가능한 워커 노드 하나를 고른다.
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

// cleanup은 best-effort다. 마지막 스펙의 DeferCleanup은 AfterAll(컨트롤러
// undeploy) 뒤에 돌 수 있어 컨트롤러의 restore에 기대지 않고 직접 걷는다.
func cleanupDrainNode(node string) {
	_, _ = kubectl("label", "node", node, "soft-drain.com/drain-")
	_, _ = kubectl("label", "node", node, "soft-drain.com/state-")
	_, _ = kubectl("annotate", "node", node, "soft-drain.com/cordoned-by-controller-")
	_, _ = kubectl("uncordon", node)
}

func cleanupApp(app string) {
	_, _ = kubectl("delete", "deployment", "-n", "default", app, "--ignore-not-found", "--wait=false")
	// 주인 없는 대체 Pod은 Deployment 삭제로 걷히지 않는다. 컨트롤러가 이미
	// 내려간 뒤에도 남지 않도록 직접 지운다.
	_, _ = kubectl("delete", "pods", "-n", "default", "-l", "app="+app, "--ignore-not-found", "--wait=false")
}

func cleanupDrain(node, app string) {
	cleanupDrainNode(node)
	cleanupApp(app)
}

// deployPackedOnNode는 다른 워커를 잠시 cordon해서 워크로드 전체를 그 노드에
// 앉힌다. nodeSelector 고정과 달리 대체 Pod은 자유롭게 다른 노드로 갈 수 있다.
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

// startPinnedDrain은 노드에 고정된 워크로드를 만들고 drain을 걸어
// "InProgress + Pending 대체 Pod 1개" 상태까지 끌고 간다.
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

// watchAvailability는 각 Deployment의 availableReplicas와 Service Endpoints를
// 주기적으로 샘플링해 의도한 개수 밑으로 떨어진 순간을 기록한다.
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

	It("워크로드를 다른 노드로 옮기고 Complete를 붙인다", func() {
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

		// 노드는 cordon된 채로 남는다
		Expect(nodeUnschedulable(srcNode)).To(Equal("true"))

		By("verifying the workload moved")
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(pods[0]).NotTo(Equal(origPod))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(srcNode))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
			// ReplicaSet에 입양됐다: hash는 있고 replaces는 없다
			labels := mustKubectl("get", "pod", "-n", "default", pods[0], "-o", "jsonpath={.metadata.labels}")
			g.Expect(labels).To(ContainSubstring("pod-template-hash"))
			g.Expect(labels).NotTo(ContainSubstring("soft-drain.com/replaces"))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("여러 Deployment를 한꺼번에 옮겨도 가용 개수가 의도 밑으로 떨어지지 않는다", func() {
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

	It("갈 곳이 없으면 Pending으로 기다리고, 라벨을 걷으면 되돌린다", func() {
		const app = "sd-pending"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		// 모든 워커를 막을 수는 없으니 노드 고정으로 "갈 곳 없음"을 만든다
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

		// 원래 Pod은 건드리지 않았다
		Expect(podsOf(app)).To(Equal([]string{origPod}))
	})

	It("uncordon하면 취소하고 Cancelled를 남긴다", func() {
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

		// 원래 Pod은 건드리지 않았다
		Expect(podsOf(app)).To(Equal([]string{origPod}))
	})

	It("drain 중 롤링업데이트가 나도 새 템플릿으로 수렴한다", func() {
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

	It("롤아웃이 타깃을 지우면 대체 Pod도 따라 사라진다", func() {
		const app = "sd-prune"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker, recreate: true})

		// Recreate가 옛 Pod(타깃)을 즉시 지운다. 새 Pod은 cordon된 노드에
		// 고정돼 미스케줄 Pending이므로 타깃이 아니다.
		By("triggering a Recreate rollout mid-drain")
		mustKubectl("patch", "deployment", "-n", "default", app, "--type=merge",
			"-p", `{"spec":{"template":{"metadata":{"labels":{"rollout":"v2"}}}}}`)

		By("waiting for the replacement to follow the target away")
		Eventually(func(g Gomega) {
			g.Expect(replacementPods(app)).To(BeEmpty())
			g.Expect(nodeStateLabel(worker)).To(Equal("Complete"))
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("drain 중 Deployment가 지워져도 대체 Pod이 따라 사라진다", func() {
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

	It("drain 중 스케일업이 나도 대체 Pod은 타깃마다 하나다", func() {
		const app = "sd-scaleup"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("scaling up mid-drain")
		mustKubectl("scale", "deployment", "-n", "default", app, "--replicas=3")

		// 새 Pod들은 cordon된 노드에 고정돼 미스케줄 Pending — 타깃이 아니다.
		// "타깃마다 하나"가 아니라 replicas 기준으로 만들었다면 여기서 초과 생성된다.
		Consistently(func() []string { return replacementPods(app) },
			45*time.Second, 5*time.Second).Should(HaveLen(1))
		Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
	})

	It("drain 중 스케일 0이 되면 대체 Pod도 사라지고 Complete가 된다", func() {
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

	It("모든 워커를 drain하면 막히고, 하나를 되돌리면 풀린다", func() {
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

		// 빈 워커들을 먼저 막는다. src를 먼저 라벨하면 대체 Pod이 아직 cordon
		// 안 된 워커에 스케줄돼 교착 없이 그냥 끝나버린다.
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

		// Complete된 빈 워커 하나를 사람이 되돌린다. Complete는 cordon 소유권을
		// 사람에게 넘겼으므로 라벨 제거만으로는 uncordon되지 않는다.
		var freed string
		for _, w := range workers {
			if w != src {
				freed = w
				break
			}
		}
		By("freeing one completed worker")
		mustKubectl("label", "node", freed, "soft-drain.com/drain-")
		mustKubectl("uncordon", freed)

		By("waiting for the stuck drain to complete")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(Equal("Complete"))
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).To(Equal(freed))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 3*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("사람이 미리 cordon한 노드는 끝나도 cordon 소유권이 사람에게 있다", func() {
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

		// 우리가 건 cordon이 아니므로 어노테이션이 없다
		ann, _ := kubectl("get", "node", src,
			"-o", `jsonpath={.metadata.annotations.soft-drain\.com/cordoned-by-controller}`)
		Expect(strings.TrimSpace(ann)).To(BeEmpty())

		By("removing the label keeps the human's cordon")
		mustKubectl("label", "node", src, "soft-drain.com/drain-")
		Eventually(func() string { return nodeStateLabel(src) },
			60*time.Second, 2*time.Second).Should(BeEmpty())
		Expect(nodeUnschedulable(src)).To(Equal("true"))
	})

	It("Deployment 소속이 아닌 Pod은 건드리지 않는다", func() {
		const naked = "sd-naked"
		worker := pickWorker()
		DeferCleanup(func() {
			_, _ = kubectl("delete", "pod", "-n", "default", naked, "--ignore-not-found", "--wait=false")
			cleanupDrainNode(worker)
		})

		applyYAML(nakedPodYAML(naked, worker))
		mustKubectl("wait", "--for=condition=Ready", "pod/"+naked, "-n", "default", "--timeout=120s")

		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// naked pod은 타깃이 아니므로 노드는 곧바로 Complete가 된다
		Eventually(func() string { return nodeStateLabel(worker) },
			2*time.Minute, 3*time.Second).Should(Equal("Complete"))

		Expect(podPhase(naked)).To(Equal("Running"))
		Expect(nodeOfPod(naked)).To(Equal(worker))
		Expect(podCost(naked)).To(BeEmpty())
	})

	It("연달아 두 번 drain해도 매번 옮기고 끝난다", func() {
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

	It("unschedulable을 tolerate하는 워크로드는 착지한 대체 Pod을 지우며 반복한다", func() {
		const app = "sd-tolerate"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		applyYAML(deployYAML(workload{name: app, pin: worker, tolerate: true}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]

		// 이전 스펙이나 실행이 남긴 같은 reason의 이벤트에 오염되지 않게 지우고 시작한다
		_, _ = kubectl("delete", "events", "-n", "default",
			"--field-selector", "reason=ReplacementOnDrainingNode", "--ignore-not-found")

		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// 대체 Pod이 cordon을 뚫고 같은 노드에 앉아 Ready가 되면 지워지고,
		// 다음 라운드가 다시 만든다. 반복 Warning Event가 그 증거다.
		By("waiting for the landing-deletion loop to leave evidence")
		Eventually(func(g Gomega) {
			out, _ := kubectl("get", "events", "-n", "default",
				"--field-selector", "reason=ReplacementOnDrainingNode", "-o", "name")
			g.Expect(nonEmptyLines(out)).NotTo(BeEmpty())
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		// 옮기지 못하므로 InProgress에 머물고, 원래 Pod은 산다
		Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
		Expect(podsOf(app)).To(ContainElement(origPod))
	})

	// ─── 다중 replicas ───

	It("r=3이 한 노드에 몰려 있어도 셋 다 무중단으로 옮긴다", func() {
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

	It("확산된 r=3에서 drain 노드의 Pod만 움직이고 나머지는 이름까지 그대로다", func() {
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

		// 다른 노드의 Pod은 이름(오브젝트)까지 그대로다 — 엉뚱한 Pod을 안 죽였다
		for name, node := range untouched {
			Expect(podPhase(name)).To(Equal("Running"))
			Expect(nodeOfPod(name)).To(Equal(node))
		}
	})

	It("한 Deployment(r=2)가 걸친 두 노드를 동시에 drain해도 무중단이다", func() {
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

	It("antiAffinity가 포화되면 자리가 없어 Pending으로 막힌다", func() {
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

		// "먼저 띄우고 나중에 지운다" 방식 전체의 산술 — 막힌 채 유지된다
		Consistently(func() string { return nodeStateLabel(src) },
			30*time.Second, 5*time.Second).Should(Equal("InProgress"))

		mustKubectl("label", "node", src, "soft-drain.com/drain-")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(src)).To(BeEmpty())
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	// ─── 방해 행위자 ───

	It("drain 중 컨트롤러가 재시작해도 이어서 하고 중복 생성이 없다", func() {
		const app = "sd-restart"
		worker := pickWorker()
		DeferCleanup(func() { cleanupDrain(worker, app) })
		startPinnedDrain(app, worker, workload{name: app, pin: worker})

		By("restarting the controller mid-drain")
		mustKubectl("rollout", "restart", "-n", namespace, controllerDeploy)
		mustKubectl("rollout", "status", "-n", namespace, controllerDeploy, "--timeout=120s")

		// 무기억이라 재기동 후에도 같은 판정 — 대체 Pod이 늘지도 줄지도 않는다
		Consistently(func(g Gomega) {
			g.Expect(replacementPods(app)).To(HaveLen(1))
			g.Expect(nodeStateLabel(worker)).To(Equal("InProgress"))
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		// 재기동한 컨트롤러가 취소도 처리한다
		mustKubectl("label", "node", worker, "soft-drain.com/drain-")
		Eventually(func(g Gomega) {
			g.Expect(nodeStateLabel(worker)).To(BeEmpty())
			g.Expect(nodeUnschedulable(worker)).To(BeEmpty())
			g.Expect(replacementPods(app)).To(BeEmpty())
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("컨트롤러가 죽어 있는 동안 붙인 라벨도 살아나면 처리한다", func() {
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

		// watch는 edge가 아니라 level이다 — 놓친 이벤트 없이 현재 상태에서 수렴한다
		Eventually(func() string { return nodeStateLabel(src) },
			3*time.Minute, 3*time.Second).Should(Equal("Complete"))
		Eventually(func(g Gomega) {
			pods := podsOf(app)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(nodeOfPod(pods[0])).NotTo(Equal(src))
			g.Expect(podPhase(pods[0])).To(Equal("Running"))
		}, 60*time.Second, 3*time.Second).Should(Succeed())
	})

	It("컨트롤러 자신이 있는 노드를 drain해도 스스로 이주하고 수렴한다", func() {
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

		// Complete는 옛 컨트롤러 Pod 오브젝트가 사라진 뒤에만 붙을 수 있고,
		// 그 시점에 옛 인스턴스는 이미 죽어 있다 — 이주한 새 인스턴스가
		// 리더 리스를 이어받아 루프를 계속한다는 증명이다.
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

	It("사용자가 대체 Pod을 지우면 다음 라운드가 다시 만든다", func() {
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

	It("사용자가 cost 어노테이션을 지우면 다시 박는다", func() {
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

	It("paused Deployment는 Ready인 대체 Pod이 있어도 넘기지 않는다", func() {
		const app = "sd-paused"
		applyYAML(deployYAML(workload{name: app}))
		mustKubectl("rollout", "status", "-n", "default", "deploy/"+app, "--timeout=180s")
		origPod := podsOf(app)[0]
		src := nodeOfPod(origPod)
		DeferCleanup(func() {
			_, _ = kubectl("rollout", "resume", "-n", "default", "deploy/"+app)
			cleanupDrain(src, app)
		})

		// pause + 템플릿 변경으로 Healthy(D)를 거짓으로 고정한다
		By("pausing the deployment with a pending template change")
		mustKubectl("rollout", "pause", "-n", "default", "deploy/"+app)
		mustKubectl("patch", "deployment", "-n", "default", app, "--type=merge",
			"-p", `{"spec":{"template":{"metadata":{"labels":{"rollout":"v2"}}}}}`)

		mustKubectl("label", "node", src, "soft-drain.com/drain=true")

		// 대체 Pod이 Ready가 되어도 replaces 라벨을 단 채 남아 있어야 한다
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

	It("라벨을 붙였다 뗐다 반복해도 마지막 선언대로 수렴한다", func() {
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

	// ─── 워크로드 다양성 ───

	It("대체 Pod이 영영 Ready가 못 되면 원본을 절대 죽이지 않는다", func() {
		const app = "sd-noready"
		worker := pickWorker()
		DeferCleanup(func() {
			cleanupDrain(worker, app)
			_, _ = utils.Run(exec.Command("docker", "exec", worker, "rm", "-rf", "/var/lib/sd-e2e"))
		})

		// 이 노드에만 marker 파일을 만든다 — 대체 Pod은 어디에 앉든 Ready가 못 된다
		By("planting the readiness marker on one node only")
		_, err := utils.Run(exec.Command("docker", "exec", worker,
			"sh", "-c", "mkdir -p /var/lib/sd-e2e && touch /var/lib/sd-e2e/ready"))
		Expect(err).NotTo(HaveOccurred())

		deployPackedOnNode(workload{name: app, probeFile: "/marker/ready"}, worker)
		origPod := podsOf(app)[0]

		monitor := watchAvailability(map[string]int{app: 1})
		mustKubectl("label", "node", worker, "soft-drain.com/drain=true")

		// 대체 Pod이 생기고, Ready가 못 되니 넘기기가 영영 일어나지 않는다
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

	It("기동이 느린 워크로드도 Ready를 기다렸다가 무중단으로 옮긴다", func() {
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

	It("로컬 PVC 워크로드는 문서의 한계대로 멈추지, 부서지지 않는다", func() {
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

		// PV의 노드 어피니티가 cordon된 노드를 가리켜 대체 Pod이 영영 Pending이다
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

	It("StatefulSet, Job, naked Pod은 건드리지 않고 Complete가 된다", func() {
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

		// 셋 다 타깃이 아니므로 노드는 곧바로 Complete가 된다
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

	It("PDB가 있어도 eviction을 안 쓰므로 막히지 않고 무중단으로 끝난다", func() {
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

	It("kubectl 플러그인으로 걸고 취소해도 라벨 경로와 같다", func() {
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

		By("cancelling with --cancel")
		out, err = utils.Run(exec.Command(plugin, worker, "--cancel", "--timeout", "2m"))
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(nodeStateLabel(worker)).To(BeEmpty())
		Expect(nodeUnschedulable(worker)).To(BeEmpty())
		Eventually(func() []string { return replacementPods(app) },
			60*time.Second, 2*time.Second).Should(BeEmpty())

		By("draining to completion in blocking mode")
		applyYAML(deployYAML(workload{name: app})) // 고정 해제 — 이제 갈 곳이 있다
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
})
