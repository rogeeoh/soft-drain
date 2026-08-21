# soft-drain working rules

This file holds working rules only. The design lives entirely in DESIGN.md.

## SSOT

- **DESIGN.md is the only design norm.** If the code disagrees with it, the code is wrong. If
  the design itself looks wrong, report it to the user rather than editing DESIGN.md. DESIGN.md
  is modified only with the user's approval.
- **DESIGN.ko.md is a translation, not a norm.** A design change touches both files in the same
  commit, and CI rejects a PR that moves one without the other. Where they disagree, DESIGN.md
  is right. Keep the section order and numbering identical so the two can be diffed against
  each other.
- AGENTS.md is the generic guide kubebuilder generated. Ignore it wherever it conflicts with
  DESIGN.md or this file. Its CRD and `api/` directory material does not apply here — the
  absence of `api/` and `config/crd` is intended.

## Prohibited

- **No CRDs, and do not propose one.** Node labels are the only API.
- **Do not re-verify documented Kubernetes behaviour against a cluster.** ReplicaSet adoption,
  pod-deletion-cost ordering and the like are settled.
- **Do not touch a real cluster.** Tests use the kind cluster (`soft-drain-test-e2e`) only.
  Never point the user's kubeconfig context at a real cluster.
- Review agents are **read-only**. Report findings, do not fix code. When unsure, put it in the
  report instead of deciding alone.

## The three test layers

| Layer | Command | What it verifies |
|---|---|---|
| unit | `go test ./internal/...` | pure decision functions, no cluster |
| envtest | `make test` | what our controller **writes** to the API server |
| e2e | `make test-e2e` | the whole loop converging on a multi-node kind cluster |

envtest has no kube-controller-manager and no scheduler. ReplicaSet adoption, deletion and
scheduling do not happen there, so do not try to verify them there. They are visible in e2e
only, and even there what is checked is our controller's outcome — the Pod moved, Complete was
set — not ReplicaSet behaviour itself.

Timing races are tested too, but at a different layer. e2e cannot control the interleaving from
outside, so trying to *produce* a race there either passes because it never happened (false
comfort) or fails now and then (flaky). Races are arranged by hand and verified
deterministically in envtest and unit tests — a hand-over between the decision and the
deletion, a target surviving a hand-over. e2e carries the scenarios that can be induced
deterministically: the happy path, uninterrupted watching of several Deployments, both
cancellations (label removal, uncordon), Pending on insufficient resources, three
rollout-overlap cases (including early reclamation of a stale replacement), Deployment
deletion, scale-up and scale-to-zero, the all-workers-drained deadlock and its release,
preemptive cordon ownership, non-target Pods left untouched, cascading re-drain, early
reclamation when the landing node is drained, the tolerate land-and-delete loop, the controller
draining itself, the kubectl plugin path, and the return to uncordon after Complete.

## Review procedure

- Implementation is done by the main session alone. Review agents are convened fresh at each
  checkpoint: one controller finished, or just before a commit.
- Reviewer prompt: "Review this code against DESIGN.md under the rules in CLAUDE.md."
- Findings accumulate in REVIEW.md and are gone through with the user **one at a time** before
  being applied. At most two review rounds per checkpoint.

## Documentation and code style

- Documents are not verbose, do not read like translations, and do not narrate history ("this
  used to be…"). That applies to DESIGN.ko.md as much as to the English files: it is written as
  Korean, not as a word-for-word rendering.
- Everything that surfaces in CI — commit messages, test spec names — is in English.
- Code comments record only the constraints the code cannot show. Logs follow the Kubernetes
  convention: leading capital, no trailing period, past tense.
