# soft-drain

> Korean translation: [DESIGN.ko.md](DESIGN.ko.md). This file is normative.

## Why

Taking a node out means `kubectl drain`, and a workload with one replica goes down while it
runs: the Pod is killed first, and the new one only starts after. Running a second copy for
HA doubles the bill, and a PDB only makes the drain bounce with 429s — the Pod still does not
move.

soft-drain **brings the new Pod up, waits for it to be Ready, and only then lets the old one
go.** There is no moment where capacity is missing.

## How it works

A ReplicaSet has two properties.

1. It adopts an ownerless Pod that matches its selector.
2. When it has more children than `replicas`, it deletes one — the lowest `pod-deletion-cost`
   first.

soft-drain chains the two. It creates a second Pod identical to the one to be moved, but
without the `pod-template-hash` label. Without the hash the Pod does not match the selector,
so the ReplicaSet leaves it alone. Once that Pod is Ready, the hash is added. The ReplicaSet
adopts it, is now one child over `replicas`, and deletes one: the old Pod, whose
`pod-deletion-cost` is negative.

**The ReplicaSet is what moves the Pod. soft-drain only lays out the materials.**

The hash must not be there from the start. The ReplicaSet would adopt the new Pod the moment
it appears, while it is still Pending — and Pending and NotReady rank ahead of
`pod-deletion-cost` in the deletion order, so the Pod just created would be the first to die.

`pod-template-hash` belongs to the ReplicaSet selector, not to the Service selector. A
replacement therefore enters Endpoints as soon as it is Ready and never drops out across
adoption. One endpoint update per Pod, no more.

**What is guaranteed is exposure, not which Pod gets deleted.** `pod-deletion-cost` is the
fourth-ranked hint in the deletion ordering. If an unrelated Pod is NotReady at the moment of
hand-over, the ReplicaSet deletes that one and our target survives. Exposure still never falls
below `N`, because a Ready Pod is added before the surplus is removed, and the surviving target
is tried again in the next round.

## Usage

```
kubectl label node node-01 soft-drain.com/drain=true      # start
kubectl get nodes -l soft-drain.com/state=Complete        # check for completion
kubectl label node node-01 soft-drain.com/drain-          # cancel
kubectl uncordon node-01                                 # also a cancel (leaves state=Cancelled)
```

A finished node stays cordoned. What happens next — draining it, rebooting it — is not
soft-drain's business. When maintenance is over and the drain label comes off, the cordon we
placed comes off with it: removing the label is how the node is handed back. A cordon a human
placed beforehand is left alone.

### The kubectl plugin

`kubectl soft-drain` wraps the four lines above. **The only thing it writes is the drain label;
everything else is a read** — the server-side surface does not grow. The grammar follows
`git stash`: bare invocation plus nodes is the main verb, the rest are subcommands. `status`,
`release` and `version` are reserved words.

```
kubectl soft-drain node-01 [node-02 ...]   # labels, then shows progress until all are Complete (--wait=false, --timeout)
kubectl soft-drain status                  # dashboard: managed nodes, remaining targets, replacements
kubectl soft-drain status node-01 -o json  # one node, for machines (json|yaml)
kubectl soft-drain release node-01 [...]   # removes the label and waits for the restore
```

`release` cancels a drain in progress and ends management of a Complete one — the same label
removal underneath, with the same ending: everything we left behind is taken back, including
the cordon we placed. That makes `release` the verb for after maintenance; run it before the
reboot and the node you just emptied opens up again. `kubectl uncordon` also cancels, but
leaves the label and the `Cancelled` latch behind — `release` takes back all of it. On
`--timeout` it prints the Pending replacements and the scheduler's message and exits non-zero:
the automation of "reading a stuck node" below. Interrupting it changes nothing, since the
label stays and the drain continues. Draining a node that is `state=Cancelled` removes the
label to restore it first, then puts it back.

The dashboard has no "since when" — the controller is memoryless and records the start time
nowhere. What `-o` is for is the aggregation only the plugin can compute: targets, replacement
status, scheduler messages. A machine that needs a list of node names should query labels, as
usual: `kubectl get nodes -l soft-drain.com/state=Complete -o name`.

There is no `--ignore-daemonsets`, `--delete-emptydir-data` or `--force`. Those are concepts
that presuppose eviction, and none applies here.

## What the controller does

```
1. cordon the node carrying the drain label
2. write a negative pod-deletion-cost on that node's Deployment Pods (the targets)
3. keep one replacement Pod per target
4. once a replacement is Ready, attach pod-template-hash so the ReplicaSet takes it
5. repeat until no targets are left
6. with no targets left, mark it complete
```

**No state is remembered.** Every round re-reads the cluster and re-derives everything, so a
controller that dies mid-way is simply continued by the next round.

**Reads go straight to the API server.** Watches are used only to know when to look again. Once
the design starts absorbing stale-cache states the conditions get complicated fast. If load
ever becomes the problem, a cache can be added then.

### Step 1. Marking the node

A node with the `drain` label gets cordoned. The `cordoned-by-controller` annotation is written
only when we actually changed the value. That is what stops us from later lifting a cordon a
human placed beforehand.

**The cordon is not preparation; it is the reason this loop terminates.** A cordoned node takes
no new Pods. The set of Pods to remove cannot grow, so removing them one at a time eventually
empties it. The one exception is a workload that tolerates `unschedulable`: removing such a Pod
just seats another one in the same place and the count never falls. That is where it stops
(step 3).

When the `drain` label disappears, everything is reverted — the `pod-deletion-cost` values we
wrote are removed, the node is uncordoned if `cordoned-by-controller` is present, and the
`state` label is deleted.

**If someone uncordons the node, we let go — mid-drain or after.** A node whose `state` is
`InProgress` or `Complete` but which is no longer `unschedulable` got there that way: both
states are only ever set after confirming the cordon, so the combination is the evidence.
Mid-drain, the cordon was the premise that guaranteed termination, and we cannot continue
without it; after completion, a human has lifted our cordon and decided to use the node again.
Either way we do not re-cordon and fight them. The costs are removed, `cordoned-by-controller`
is deleted, `state=Cancelled` is set, and we stop. The annotation goes because the cordon it
recorded is already gone by a human's hand: a cordon they place later must not be mistaken for
ours and lifted by the restore that runs on label removal. Replacement Pods are collected by
the reclamation path. `Cancelled` is a latch — removing the label clears it, and starting over
means removing the label and adding it again.

Leaving `Complete` unfinished turns the node into a deletion magnet. The node with the most
room, just emptied, is now open, so the scheduler seats another drain's replacements exactly
there, and the landing check deletes them as fast as they arrive. The smaller the cluster, the
more every drain is pulled into that one node.

### Step 2. Marking targets

A target is **a Pod on that node whose owner is a ReplicaSet whose owner is a Deployment**.
Pods in phase `Failed` or `Succeeded` are not counted — the ReplicaSet does not count them as
active either, so their replacements already exist elsewhere, and the corpses left on the node
would only block the completion check.

The annotation used is `controller.kubernetes.io/pod-deletion-cost = -2147483648`. It is
written on targets only, and it is written before the replacement is created: there is no
reason to wait until hand-over, and if an unrelated scale-down happens in between, the drained
node's Pods are the better ones to lose.

It is not written on replacements. Before adoption the ReplicaSet does not look at that Pod, so
the value means nothing; if it survives adoption, that Pod becomes the first to die in every
later scale-down. Targets carry the value away with them when they are deleted, so there is
nothing to clean up.

An existing value is overwritten and not restored. On revert, only values that are exactly
`-2147483648` are removed. Nothing else writes that value, so a value of exactly that is one of
ours.

### Step 3. Matching replacements

This step does not only create. **It reconciles the set that should exist against the set that
does.**

```
should exist = one per target without a deletionTimestamp
does exist   = Pods with soft-drain.com/replaces = <target UID>
               that have no controller ownerRef,
               are not in phase Failed / Succeeded,
               and have no deletionTimestamp

too few, create; too many, delete
```

**When a target goes away, so does its replacement.** Cancellation, a ReplicaSet pruned by a
rollout, a deleted Deployment, a lowered `replicas`, an evicted target — all of it is covered
by that single line. Nothing needs handling on its own.

**Terminating targets are excluded from creation.** A ReplicaSet drops Pods with a
`deletionTimestamp` from its active count, so it is already making its own replacement, and
since the node is cordoned that Pod lands elsewhere. The slot is freed without us doing
anything.

**Dead replacements are not counted, and are deleted.** A Pod that went `Failed` through node
pressure eviction or a kubelet admission rejection can neither become Ready nor be adopted.
Counting it as alive stalls that target forever. There is no restart for a Pod — phase `Failed`
is terminal and `restartPolicy` is about containers — so recovery means a new Pod; and if the
dead one is neither counted nor deleted, corpses pile up with every attempt. While the cause
persists this cycles through create-die-delete, and it converges the moment the cause clears.

**Replacements seated on a draining node are not counted either, and are deleted.**
Scheduling can precede the cordon, so a Pod can land on a node that only afterwards gets the
drain label. There is no reason to wait for Ready: even Ready, handing it over would just add
one more target to the node we are emptying, so deletion is the only ending, and meanwhile it
occupies a slot. It has no hash, so it is no ReplicaSet's child, and deleting it does not lower
exposure; the same round creates a new one and the scheduler seats it away from the cordon. A
Warning Event is emitted on deletion.

A workload that tolerates `node.kubernetes.io/unschedulable` may have its new Pod land on a
draining node again, and then create-and-delete repeats. It is a workload built to ignore
cordons, so we do not invert that intent by injecting `nodeAffinity`. The repeating Warning
Events are what show that it cannot be moved.

**If the target's ReplicaSet is not the Deployment's current template, nothing is created, and
anything present is deleted.** A rollout is already replacing that target — bringing the new
version up on another node and deleting the target once it is Ready, which is exactly what we
were about to do. Our replacement would build the losing version for nothing, and Healthy(D)
blocks hand-over for the whole rollout, so it could never reach adoption anyway.
`pod-deletion-cost` stays on the target, so the old ReplicaSet's scale-down deletes the drained
node's targets first. A Normal Event is emitted on deletion. The check compares the
Deployment's `spec.template` against the target ReplicaSet's template, ignoring only the
`pod-template-hash` label (the same EqualIgnoreHash the Deployment controller uses) — an image
change, a `rollout restart`, every case where the template changes is caught the same way. The
exception is `spec.paused`: with a paused Deployment the rule does not apply even if the
templates differ, because the rollout is not actually moving and the premise of "already being
replaced" breaks. Replacements are maintained as usual and only hand-over is held back by
Healthy(D), until the rollout resumes and this rule catches it.

**If either deletion is rejected by its preconditions, the round ends.** A rejection means the
Pod changed between the decision and the deletion. Rather than carry a stale list into
hand-over, the round ends with a short requeue and the next round decides everything again from
the current state. That is why hand-over does not need to re-check for replacements seated on a
draining node.

**Pods already handed over are not counted.** The `soft-drain.com/replaces` label is removed as
part of the hand-over, so they are not candidates to begin with. This is what makes the
"handed over but the target survived" case recover on its own: if `replicas` goes up at the
moment of hand-over, the surplus is absorbed by the increase and nothing is deleted — and the
next round sees "the target is still there and nothing stands in for it" and creates one more.

**Both the creating and the deleting side must be able to wake us.** With only the traversal
that starts from the node, a pruned ReplicaSet takes the target Pods with it, leaving nothing
to traverse and no reason to ever look at the replacements again. So there is a separate path
keyed on the replacement Pod itself. The decision is the same single rule above.

A replacement is built like this.

```yaml
metadata:
  generateName: aaa-5449d4d8c8-        # the target's ReplicaSet name + "-"
  labels:
    app: aaa                            # from rs.spec.template.metadata.labels
    soft-drain.com/replaces: 3f2a...     # the target Pod's UID
    # pod-template-hash is removed
spec: <rs.spec.template.spec verbatim>
```

The spec comes **from `rs.spec.template`, not from the living Pod.** Copying the living Pod
brings `nodeName` along, and stacks another sidecar on top of the one a webhook already
injected.

`rs.spec.template.metadata.labels` **already contains** `pod-template-hash`. It is removed
explicitly after the copy. This is the single most important line in this document.

A rejected creation produces a Warning Event. A ResourceQuota overrun or an admission webhook
rejection lands here, and this is the one case where no Pod object exists, so there is no trace
to see from outside. A rejection does not stop anything; the next round tries again.

### Step 4. The hand-over

Once a replacement is Ready, a single patch adds `pod-template-hash` and removes
`soft-drain.com/replaces`.

The hash is read **from the ReplicaSet the target Pod's ownerRef points at**. The route through
the Deployment to the current ReplicaSet is not used — the replacement was built from the
target's ReplicaSet template, and during a rollout that may not be the current one.

One thing is checked before handing over. Replacements seated on a draining node were already
deleted in step 3, so they never get here.

**Is the user's Deployment healthy.**

```
Healthy(D) ≡ D.status.observedGeneration >= D.metadata.generation
           ∧ D.status.replicas == D.status.updatedReplicas
           ∧ D.status.availableReplicas >= D.spec.replicas
```

If it is not healthy, hold. Below `N` the hand-over creates no surplus and nothing gets
deleted; mid-rollout there is no single ReplicaSet to hand over to, and exposure could fall
further than the rollout settings allow.

The `replicas == updatedReplicas` term is what decides "only one ReplicaSet has Pods". The
other two terms cannot catch a rollout on their own: with `maxUnavailable: 0`,
`availableReplicas >= N` holds for the entire rollout — and that is exactly the setting a user
who wants zero downtime uses. `spec.paused` is caught by this term as well.

The check is per Deployment, and whichever is ready hands over first. Batching them lets the
slowest one hold the rest hostage.

### Step 5. Completion

With no targets left on the node, `state=Complete` is set and an Event is emitted. The
`cordoned-by-controller` annotation stays — the cordon is still ours, and it comes off when the
drain label does. `Complete` is a latch: while the cordon holds, we do not act again until the
drain label is removed. The only Pods that can newly land on a cordoned node are the ones that
tolerate `unschedulable`, and those are the kind we cannot move anyway. Without the latch a
node waiting for its reboot would reopen and be rebooted with Pods crowded back onto it — the
only moments a node opens should be a human removing the label (hand-back) and a human
uncordoning it (cancel).

A human uncordoning folds the latch into `Cancelled` (step 1). The landing ban lifts with it —
it existed because the node was headed for a reboot, and an uncordon declares that it is not.
In the short window between the uncordon and our noticing it, a landed replacement can still be
deleted, but the next round creates a new one.

**Terminating targets do count toward completion.** A `deletionTimestamp` does not stop the
work; it keeps running for the grace period. Excluding them would put `Complete` on a node
whose work is still running, and the human who reboots the node on that signal kills it. That
is the reason they are excluded from creation but counted for completion.

## Metadata

| Object | Key | Value | Written by |
|---|---|---|---|
| Node | `soft-drain.com/drain` (label) | `"true"` | human |
| Node | `soft-drain.com/state` (label) | `InProgress` / `Complete` / `Cancelled` | controller |
| Node | `soft-drain.com/cordoned-by-controller` (annotation) | `"true"` | controller |
| Target Pod | `controller.kubernetes.io/pod-deletion-cost` (annotation) | `-2147483648` | controller |
| Replacement Pod | `soft-drain.com/replaces` (label) | the target Pod's UID | controller |

Nothing but soft-drain writes `soft-drain.com/replaces`. A Pod without that label is neither
created nor deleted by us.

## Invariants

1. A replacement is created without `pod-template-hash`.
2. A replacement's spec comes from `rs.spec.template`, not from the living Pod.
3. `pod-deletion-cost` is written first, `pod-template-hash` attached later.
4. Only Pods carrying the `soft-drain.com/replaces` label are deleted.
5. Pods with a controller ownerRef are never deleted.
6. Deleting a replacement uses the UID and resourceVersion we read as preconditions. If the
   hash was attached in between and a ReplicaSet took the Pod, the deletion is rejected and the
   next round decides again.

## Non-goals

- Only Pods belonging to a Deployment are moved. StatefulSets, DaemonSets, Jobs and
  hand-made Pods are left as they are. **`Complete` means "my part is done", not "the node is
  empty".**
- Every eligible Pod on the node is moved at once. If resources run short, they wait as
  Pending.
- Workloads that cannot be moved are not filtered out ahead of time. They stay Pending and a
  human can look.
- PDBs are not consulted. The deletion is performed by the user's ReplicaSet, so it never goes
  through the eviction API.
- The user's Deployment `spec` is never modified. On user Pods we write exactly one annotation.
- Nodes are neither drained nor shut down.

## Reading a stuck node

When a node sits at `InProgress`, look at the replacements.

```bash
kubectl get pods -A -l soft-drain.com/replaces
kubectl describe pod <the Pending one>
```

The scheduler writes the reason verbatim into the message of `PodScheduled=False` — something
like `0/12 nodes are available: 5 Insufficient cpu, 7 node(s) didn't match pod anti-affinity
rules`. That is why the controller does not produce a diagnosis of its own.

If no replacement is there at all, either the creation was rejected (ResourceQuota, admission
webhook) or a rollout is performing the migration instead, so none is created. Either way it is
in the Events of `kubectl describe node <node>`.

## Known limitations

- **With no spare capacity there is no progress.** A slot opens only when an old Pod dies, and
  an old Pod dies only once a new one is Ready, so at zero headroom nothing resolves itself.
  RWO volumes and local PVs have the same shape.
- **If placement rules cannot yield a single slot, there is no progress even with resources to
  spare.** The typical case is a required `podAntiAffinity` limiting one per node with every
  candidate node already filled. This is not a soft-drain constraint but the arithmetic of
  "bring it up first, delete it later" in general — the same workload also blocks a
  `maxSurge: 1` rollout. Which is why such users already run with `maxUnavailable: 1` and accept
  dropping below `N` on every rollout. They can use `kubectl drain` to take a node out too.
- **A workload tolerating `node.kubernetes.io/unschedulable` may never move.** Every time a
  replacement lands on the draining node it is deleted and recreated, and it only ends if one
  happens to land elsewhere.
- **Before adoption a replacement has no controller, which skews PDB accounting.** It carries
  the same labels and is Ready, so it counts toward `currentHealthy` but not toward
  `expectedCount`, which raises `disruptionsAllowed` by one for that period. It never goes below
  the floor the PDB protects. For the same reason `UnmanagedPods` Warnings accumulate on the
  user's PDB.
- **The Cluster Autoscaler cannot scale down a node holding an ownerless replacement.** If
  hand-over stays stalled for long, that node keeps being excluded from consolidation.
- **The original `pod-deletion-cost` value on a user Pod is not restored.**
- **`pod-deletion-cost` is required, so Kubernetes 1.22 or newer is needed.**
