// kubectl-soft_drain은 kubectl 플러그인이다 (kubectl soft-drain 으로 호출).
// 쓰는 것은 drain 라벨 하나뿐이고 나머지는 읽기다.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"

	sd "github.com/rogeeoh/soft-drain/internal/controller"
)

const pollInterval = 2 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	flags := pflag.NewFlagSet("kubectl-soft_drain", pflag.ExitOnError)
	cancelDrain := flags.Bool("cancel", false, "remove the drain label and wait for the node to be restored")
	waitDone := flags.Bool("wait", true, "wait until the drain completes")
	timeout := flags.Duration("timeout", 0,
		"give up waiting after this duration (0 = wait forever; the drain itself keeps running)")
	cfg := genericclioptions.NewConfigFlags(true)
	cfg.AddFlags(flags)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: kubectl soft-drain NODE [--cancel] [--wait=false] [--timeout=30m]\n\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		flags.Usage()
		return errors.New("exactly one node name is required")
	}
	node := flags.Args()[0]

	restCfg, err := cfg.ToRESTConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	if *cancelDrain {
		return runCancel(ctx, cs, node, *waitDone)
	}
	return runDrain(ctx, cs, node, *waitDone)
}

func runDrain(ctx context.Context, cs kubernetes.Interface, node string, waitDone bool) error {
	n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("node %q not found", node)
	}
	if err != nil {
		return err
	}

	if n.Labels[sd.LabelDrain] == "true" {
		switch n.Labels[sd.LabelState] {
		case sd.StateComplete:
			fmt.Printf("node/%s is already drained (state=Complete)\n", node)
			return nil
		case sd.StateCancelled:
			// Cancelled는 래치다 — 라벨을 걷어 복원시킨 뒤에만 다시 걸 수 있다.
			fmt.Printf("node/%s has a cancelled drain; clearing it first\n", node)
			if err := setDrainLabel(ctx, cs, node, false); err != nil {
				return err
			}
			if err := waitForState(ctx, cs, node, ""); err != nil {
				return err
			}
		default:
			fmt.Printf("node/%s is already being drained; waiting\n", node)
		}
	}

	if err := setDrainLabel(ctx, cs, node, true); err != nil {
		return err
	}
	fmt.Printf("node/%s labeled for soft-drain\n", node)

	if !waitDone {
		fmt.Printf("not waiting; check with: kubectl get node %s -L soft-drain.com/state\n", node)
		return nil
	}
	return watchDrain(ctx, cs, node)
}

func watchDrain(ctx context.Context, cs kubernetes.Interface, node string) error {
	seenTargets := map[types.UID]string{}
	replReady := map[string]bool{}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				diagnose(cs, node, seenTargets)
				return errors.New("timed out waiting; the drain keeps running (label remains)")
			}
			return errors.New("interrupted; the drain keeps running (label remains)")
		case <-ticker.C:
		}

		n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			continue
		}
		switch n.Labels[sd.LabelState] {
		case sd.StateComplete:
			fmt.Printf("node/%s drained (all Deployment pods moved; node remains cordoned)\n", node)
			return nil
		case sd.StateCancelled:
			return errors.New("drain was cancelled (node was uncordoned)")
		}

		targets, err := markedPods(ctx, cs, node)
		if err != nil {
			continue
		}
		current := map[types.UID]bool{}
		for _, p := range targets {
			current[p.UID] = true
			if _, ok := seenTargets[p.UID]; !ok {
				seenTargets[p.UID] = p.Namespace + "/" + p.Name
				fmt.Printf("moving pod %s\n", seenTargets[p.UID])
			}
		}
		for uid, name := range seenTargets {
			if name != "" && !current[uid] {
				fmt.Printf("pod %s moved\n", name)
				seenTargets[uid] = ""
			}
		}

		for _, p := range replacementsFor(ctx, cs, seenTargets) {
			key := p.Namespace + "/" + p.Name
			if ready := podIsReady(&p); ready && !replReady[key] {
				replReady[key] = true
				fmt.Printf("replacement %s ready on %s\n", key, p.Spec.NodeName)
			} else if _, known := replReady[key]; !known {
				replReady[key] = false
				fmt.Printf("replacement %s created (for %s)\n", key, seenTargets[types.UID(p.Labels[sd.LabelReplaces])])
			}
		}
	}
}

// diagnose는 "막혔을 때 보는 법"의 자동화다 — Pending 대체 Pod의 스케줄러
// 메시지를 그대로 보여준다.
func diagnose(cs kubernetes.Interface, node string, seenTargets map[types.UID]string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remaining, err := markedPods(ctx, cs, node)
	if err != nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%d pod(s) still on node/%s\n", len(remaining), node)
	for _, p := range replacementsFor(ctx, cs, seenTargets) {
		if podIsReady(&p) {
			continue
		}
		msg := "not ready"
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status != corev1.ConditionTrue {
				msg = c.Message
			}
		}
		fmt.Fprintf(os.Stderr, "replacement %s/%s: %s\n", p.Namespace, p.Name, msg)
	}
}

func runCancel(ctx context.Context, cs kubernetes.Interface, node string, waitDone bool) error {
	n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("node %q not found", node)
	}
	if err != nil {
		return err
	}
	if n.Labels[sd.LabelDrain] == "" && n.Labels[sd.LabelState] == "" {
		fmt.Printf("node/%s has no soft-drain to cancel\n", node)
		return nil
	}
	if err := setDrainLabel(ctx, cs, node, false); err != nil {
		return err
	}
	fmt.Printf("node/%s drain label removed\n", node)
	if !waitDone {
		return nil
	}
	if err := waitForState(ctx, cs, node, ""); err != nil {
		return err
	}
	fmt.Printf("node/%s restored\n", node)
	return nil
}

func setDrainLabel(ctx context.Context, cs kubernetes.Interface, node string, on bool) error {
	value := `"true"`
	if !on {
		value = "null"
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%s}}}`, sd.LabelDrain, value)
	_, err := cs.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

func waitForState(ctx context.Context, cs kubernetes.Interface, node, want string) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up waiting for state %q on node/%s: %w", want, node, ctx.Err())
		case <-ticker.C:
		}
		n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			continue
		}
		if n.Labels[sd.LabelState] == want {
			return nil
		}
	}
}

// markedPods는 노드 위에서 컨트롤러가 cost를 박은 Pod을 준다. 값이
// -2147483648이면 우리가 붙인 것이다 — 그 값을 쓰는 게 우리뿐이다.
func markedPods(ctx context.Context, cs kubernetes.Interface, node string) ([]corev1.Pod, error) {
	list, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		return nil, err
	}
	var marked []corev1.Pod
	for _, p := range list.Items {
		if p.Annotations[sd.AnnotationPodDeletionCost] == sd.PodDeletionCost {
			marked = append(marked, p)
		}
	}
	return marked, nil
}

func replacementsFor(ctx context.Context, cs kubernetes.Interface, targets map[types.UID]string) []corev1.Pod {
	list, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: sd.LabelReplaces,
	})
	if err != nil {
		return nil
	}
	var repls []corev1.Pod
	for _, p := range list.Items {
		if _, ours := targets[types.UID(p.Labels[sd.LabelReplaces])]; ours {
			repls = append(repls, p)
		}
	}
	return repls
}

func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
