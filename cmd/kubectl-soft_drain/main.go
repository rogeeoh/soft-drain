// kubectl-soft_drain is a kubectl plugin (invoked as kubectl soft-drain).
// The only thing it writes is the drain label; everything else is a read.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	sd "github.com/rogeeoh/soft-drain/internal/controller"
)

const pollInterval = 2 * time.Second

// version is injected at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	err := run()
	if err == nil {
		return
	}
	// Ctrl-C is a detach, not a failure — the label stays and the work continues.
	// Exit code 130 (128+SIGINT) reports only "did not run to completion", as is conventional.
	if errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "interrupted; the operation keeps running on the cluster (labels remain)")
		fmt.Fprintln(os.Stderr, "check with: kubectl soft-drain status")
		os.Exit(130)
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func run() error {
	flags := pflag.NewFlagSet("kubectl-soft_drain", pflag.ExitOnError)
	waitDone := flags.Bool("wait", true, "wait until the operation completes")
	timeout := flags.Duration("timeout", 0,
		"give up waiting after this duration (0 = wait forever; the operation itself keeps running)")
	output := flags.StringP("output", "o", "", "status output format: json or yaml")
	// A nil field is not registered as a flag. Only the connection selectors
	// (--kubeconfig, --context) are kept — namespaces and advanced connection flags mean
	// nothing to this plugin.
	cfg := genericclioptions.NewConfigFlags(true)
	cfg.CacheDir = nil
	cfg.ClusterName = nil
	cfg.AuthInfoName = nil
	cfg.Namespace = nil
	cfg.APIServer = nil
	cfg.TLSServerName = nil
	cfg.Insecure = nil
	cfg.CertFile = nil
	cfg.KeyFile = nil
	cfg.CAFile = nil
	cfg.BearerToken = nil
	cfg.Impersonate = nil
	cfg.ImpersonateUID = nil
	cfg.ImpersonateGroup = nil
	cfg.ImpersonateUserExtra = nil
	cfg.Timeout = nil
	cfg.DisableCompression = nil
	cfg.AddFlags(flags)
	flags.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage:
  kubectl soft-drain NODE...           start soft drains and watch until Complete
  kubectl soft-drain status [NODE...]  show nodes under soft-drain (-o json|yaml)
  kubectl soft-drain release NODE...   remove drain labels and wait for restore
  kubectl soft-drain version           print the plugin version

"status", "release" and "version" are reserved words. kubectl uncordon also cancels an
in-flight drain but leaves the label and a Cancelled latch; release removes
the label and restores the node fully.

`)
		flags.PrintDefaults()
	}
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := flags.Args()

	if len(args) == 0 {
		flags.Usage()
		return nil
	}
	// version must answer without a cluster — checked before the client is built.
	if args[0] == "version" {
		fmt.Printf("kubectl-soft_drain %s\n", version)
		return nil
	}

	restCfg, err := cfg.ToRESTConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx := sigCtx
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	var err2 error
	switch args[0] {
	case "status":
		err2 = runStatus(ctx, cs, args[1:], *output)
	case "release":
		if len(args) < 2 {
			flags.Usage()
			return errors.New("release takes at least one node name")
		}
		err2 = runRelease(ctx, cs, args[1:], *waitDone)
	default:
		if *output != "" {
			return errors.New("-o is only valid with status")
		}
		err2 = runDrain(ctx, cs, args, *waitDone)
	}
	// Wherever it was interrupted, a signal means a detach, not a failure.
	if err2 != nil && sigCtx.Err() != nil {
		return context.Canceled
	}
	return err2
}

type podRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type replacementStatus struct {
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	Ready            bool   `json:"ready"`
	Node             string `json:"node,omitempty"`
	SchedulerMessage string `json:"schedulerMessage,omitempty"`
}

type nodeStatus struct {
	Node         string              `json:"node"`
	State        string              `json:"state,omitempty"`
	Drain        bool                `json:"drain"`
	Targets      []podRef            `json:"targets"`
	Replacements []replacementStatus `json:"replacements"`
}

// runStatus reports on the nodes soft-drain is managing. Reads only.
// A machine that just needs node names should query labels, as usual:
// kubectl get nodes -l soft-drain.com/state=Complete -o name
func runStatus(ctx context.Context, cs kubernetes.Interface, filter []string, output string) error {
	if output != "" && output != "json" && output != "yaml" {
		return fmt.Errorf("unsupported output format %q (json or yaml)", output)
	}
	wanted := map[string]bool{}
	for _, f := range filter {
		wanted[f] = true
	}
	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	var rows []nodeStatus
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if len(wanted) > 0 && !wanted[n.Name] {
			continue
		}
		if n.Labels[sd.LabelDrain] == "" && n.Labels[sd.LabelState] == "" {
			continue
		}
		row, err := statusRow(ctx, cs, n)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Node < rows[j].Node })

	switch output {
	case "json":
		out, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	case "yaml":
		out, err := yaml.Marshal(rows)
		if err != nil {
			return err
		}
		fmt.Print(string(out))
		return nil
	}

	if len(rows) == 0 {
		fmt.Println("No nodes under soft-drain.")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(w, "NODE\tSTATE\tTARGETS\tREPLACEMENTS")
	for _, row := range rows {
		state := row.State
		if state == "" {
			state = "-"
		}
		ready, pending := 0, 0
		for _, r := range row.Replacements {
			if r.Ready {
				ready++
			} else {
				pending++
			}
		}
		repl := "-"
		if ready+pending > 0 {
			repl = fmt.Sprintf("%d Ready, %d Pending", ready, pending)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", row.Node, state, len(row.Targets), repl)
	}
	return w.Flush()
}

func statusRow(ctx context.Context, cs kubernetes.Interface, n *corev1.Node) (nodeStatus, error) {
	row := nodeStatus{
		Node:         n.Name,
		State:        n.Labels[sd.LabelState],
		Drain:        n.Labels[sd.LabelDrain] == "true",
		Targets:      []podRef{},
		Replacements: []replacementStatus{},
	}
	targets, err := markedPods(ctx, cs, n.Name)
	if err != nil {
		return row, err
	}
	uids := map[types.UID]string{}
	for _, p := range targets {
		uids[p.UID] = p.Namespace + "/" + p.Name
		row.Targets = append(row.Targets, podRef{Namespace: p.Namespace, Name: p.Name})
	}
	for _, p := range replacementsFor(ctx, cs, uids) {
		r := replacementStatus{
			Namespace: p.Namespace,
			Name:      p.Name,
			Ready:     podIsReady(&p),
			Node:      p.Spec.NodeName,
		}
		if !r.Ready {
			for _, c := range p.Status.Conditions {
				if c.Type == corev1.PodScheduled && c.Status != corev1.ConditionTrue {
					r.SchedulerMessage = c.Message
				}
			}
		}
		row.Replacements = append(row.Replacements, r)
	}
	return row, nil
}

func runDrain(ctx context.Context, cs kubernetes.Interface, nodes []string, waitDone bool) error {
	var pending []string
	for _, node := range nodes {
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
				continue
			case sd.StateCancelled:
				// Cancelled is a latch — the label must be removed to restore before it can be set again.
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
		pending = append(pending, node)
	}

	if !waitDone {
		fmt.Printf("not waiting; check with: kubectl soft-drain status\n")
		return nil
	}
	if len(pending) == 0 {
		return nil
	}
	return watchDrains(ctx, cs, pending)
}

func watchDrains(ctx context.Context, cs kubernetes.Interface, nodes []string) error {
	seenTargets := map[types.UID]string{}
	replReady := map[string]bool{}
	pending := map[string]bool{}
	for _, n := range nodes {
		pending[n] = true
	}
	cancelled := false
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				for node := range pending {
					diagnose(cs, node, seenTargets)
				}
				return errors.New("timed out waiting; the drains keep running (labels remain)")
			}
			return ctx.Err()
		case <-ticker.C:
		}

		for node := range pending {
			n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
			if err != nil {
				continue
			}
			switch n.Labels[sd.LabelState] {
			case sd.StateComplete:
				fmt.Printf("node/%s drained (all Deployment pods moved; node remains cordoned)\n", node)
				delete(pending, node)
				continue
			case sd.StateCancelled:
				fmt.Fprintf(os.Stderr, "node/%s drain was cancelled (node was uncordoned)\n", node)
				cancelled = true
				delete(pending, node)
				continue
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
	if cancelled {
		return errors.New("some drains were cancelled")
	}
	return nil
}

// diagnose automates "reading a stuck node" — it shows the scheduler's message for
// Pending replacements verbatim.
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

// runRelease removes the label. On a drain in progress that is a cancel, on a Complete
// node it ends management — underneath it is the same action.
func runRelease(ctx context.Context, cs kubernetes.Interface, nodes []string, waitDone bool) error {
	var pending []string
	for _, node := range nodes {
		n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("node %q not found", node)
		}
		if err != nil {
			return err
		}
		if n.Labels[sd.LabelDrain] == "" && n.Labels[sd.LabelState] == "" {
			fmt.Printf("node/%s is not under soft-drain\n", node)
			continue
		}
		if err := setDrainLabel(ctx, cs, node, false); err != nil {
			return err
		}
		fmt.Printf("node/%s drain label removed\n", node)
		pending = append(pending, node)
	}
	if !waitDone {
		return nil
	}
	for _, node := range pending {
		if err := waitForState(ctx, cs, node, ""); err != nil {
			return err
		}
		fmt.Printf("node/%s restored\n", node)
	}
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

// markedPods returns the Pods on the node the controller wrote a cost on. A value of
// -2147483648 is ours — nothing else writes it.
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
