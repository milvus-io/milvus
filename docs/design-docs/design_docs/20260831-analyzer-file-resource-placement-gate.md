# Analyzer file resource placement gate

- Status: Proposed (PR #53032)
- Date: 2026-08-31
- Scope: `internal/coordinator/file_resource_observer.go`, `internal/querycoordv2/file_resource_gate.go`, `internal/querycoordv2/server.go`, `internal/querycoordv2/task/{executor.go,scheduler.go}`
- Related: issue #53031; split out of #52500

## 1. Problem

Analyzer file resources are the dictionaries, stop-word lists and synonym files an analyzer needs on local disk. `FileResourceObserver` (in `internal/coordinator`) pushes the registered set to every query node through `SyncFileResource` and records, per node, the resource version that node has acknowledged. A node that runs an analyzer over data — a BM25 field, raw text match — needs the files on its own disk, not merely somewhere in the cluster.

A query node becomes visible to QueryCoord as soon as its session registers. That is earlier than its file resource download finishes. Anything QueryCoord places on the node in that window races the download: a segment carrying a BM25 field, or a channel whose delegator runs an analyzer over growing data.

`CreateCollection` already waits for this: `validateAnalyzerInfos` retries `CheckAllQnReady` before validating the schema. But that only covers the nodes present at DDL time. A node that joins afterwards — scale-out, restart, rolling replacement — receives segments and channels through the checkers, and there is no load request anywhere on that path to gate on.

Two further details make a naive check wrong:

- A node with no sync record has confirmed nothing. Session registration is what puts a node into a resource group and gives it a task executor, and it happens before the first sync is asked for, so "no record" must read as *not ready*, not as *nothing to check*.
- Presence of a record is not enough either. A node that acknowledged version 1 keeps its record while version 2 is still downloading, so the check must compare the recorded version against the current one.

## 2. Design

The gate sits on the **action**, not on the load request. A load-time check only sees the resource group membership at the moment the request arrives; it does not cover a node that crashes and rejoins, is rolling-replaced, or joins the group afterwards. Gating the action is what makes "a node serving a shard holds the files its analyzers need" hold on every path.

```
 FileResourceObserver (coordinator)
   Sync()  ──► SyncFileResource per node ──► distribution[node] = version
   CheckNodesSynced(nodes) ──► compares distribution[node] against the snapshot
            │
            ▼  (func(nodeID) error)
 QueryCoord Server.nodeFileResourceGate()
            │  SetFileResourceGate at init, handed to every executor
            ▼
 task.Executor.Execute(task, step)
   checkFileResourceReady(action)  ──► err? drop the executing mark, return false
   (grow segment / grow channel only)       task stays queued, scheduler retries
```

### 2.1 `CheckNodesSynced` on the observer

`FileResourceObserver.CheckNodesSynced(nodeIDs)` is the query-side counterpart of `CheckAllQnReady`. Callers pass the nodes they are about to put data on, so a lagging node they are not using cannot block them. It returns `ErrServiceUnavailable` for a node with no record or with a recorded version below the current one, and nil otherwise.

It answers nil without any further work when the query-node sync mode (`common.fileResource.mode.queryNode`) is not `sync`, or when no file resource is registered. A deployment that does not use analyzer file resources never reaches the per-node comparison.

**The check must not take RootCoord's DDL lock.** `ListFileResource` on the meta table takes `ddLock`, and the executors consult the gate for every pending grow action on every dispatch. Taking a DDL lock there would couple QueryCoord's scheduling tick to RootCoord's DDL throughput. The observer therefore keeps an atomic `gateSnapshot` — whether resources are registered, and at which version — refreshed by every `Sync` pass, including the pass that returns early on a version-0 meta table (version 0 is a definite answer: not gated). Only the very first call before any `Sync` pass reads the meta table directly, and it publishes with compare-and-swap so a concurrent `Sync` result is never overwritten by the older lazy read. Between a resource change and the next `Sync` pass the gate may judge against the previous version; that is the same convergence window the node syncs themselves have.

### 2.2 Nodes that do not implement the sync RPC

`Sync` treats `merr.ErrServiceUnimplemented` from `SyncFileResource` as success and records the node at the target version. `grpcclient` maps gRPC `Unimplemented` to that error. Without this, the oldest node in a rolling upgrade would be gated forever: it cannot be asked to download anything, and holding grow actions off it until it is replaced would wedge the upgrade. Whatever such a node serves, it served before file resources existed, which is the compatibility floor.

### 2.3 The executor defers grow actions

`task.NodeFileResourceGate` is `func(nodeID int64) error`. The scheduler stores one gate (`SetFileResourceGate`, called once at init before any executor exists) and copies it into every executor it creates, so an executor added for a node that joins later carries the same gate.

`Executor.checkFileResourceReady` consults the gate only for `ActionTypeGrow` on a `SegmentAction` or `ChannelAction` — the two action types that put data on the node. Reductions and leader updates stay unblocked: a node that is behind could otherwise never be drained, which would turn a lagging download into a node that can neither serve nor be released.

The check runs at the top of `Execute`, before the channel/non-channel pool counters are taken and before the step deadline is armed. On refusal the executor removes the task from `executingTasks` and returns false, exactly like the existing pool-full paths: the task holds no pool slot, is not failed, and stays queued for the scheduler to retry on the next tick. Queueing therefore does not consume the RPC budget of the step.

The deferral is logged at info (rate-limited) with the node, task, collection and the held channel or segment. A deferral that never clears is otherwise invisible — the delegator is simply never built and the collection never becomes queryable — while the sync failure behind it is logged on the observer; this is the one line that ties the two together. It is silent on a deployment with no file resources.

### 2.4 Rediscovered nodes trigger a sync

The observer is natively notified only on the session-add path. Nodes already in the session directory when QueryCoord starts, or when it reconnects to etcd, go through `rewatchNodes` instead and never trigger a sync. With nothing gating on the result that was harmless; with the gate it is a deadlock, because such a node would sit behind the gate until some unrelated event notified the observer. `rewatchNodes` now calls `notifyFileResourceOnNodeRediscovery`, which is `Notify()` on the observer when one is installed.

### 2.5 Wiring

`Server.nodeFileResourceGate()` returns nil when no `FileResourceObserver` is installed, and otherwise a closure over `CheckNodesSynced([]int64{nodeID})`. A nil gate is the default path: nothing is consulted and every action runs as before. `initQueryCoord` installs it on the scheduler right after construction.

## 3. Compatibility

- No proto change, no new configuration, no new persisted state.
- Without analyzer file resources registered, or with `common.fileResource.mode.queryNode` set to `close`, the gate answers nil and no node state is consulted.
- A query node that predates `SyncFileResource` is recorded as synced (§2.2), so a rolling upgrade is not blocked on its oldest node.
- QueryCoord's scheduling tick never takes RootCoord's DDL lock (§2.1).
- The load path (`LoadCollection` / `LoadPartitions`) is unchanged. A node still downloading delays placement; it does not turn the load into an error.

## 4. Alternatives considered

- **Reject in `LoadCollection` / `LoadPartitions`.** Wrong place and wrong verdict. It read `s.meta.GetNodes`, which returns ResourceManager members and therefore excludes the query nodes embedded in streaming nodes — the ones running analyzers — and it covers only the nodes present when the request arrives, not a node that rejoins or is replaced afterwards. Failing the user's load is also harsher than deferring placement, which the executor gate already achieves.
- **Gate every action type.** Would stop a lagging node from being drained: a reduction removes data from the node and a leader update only changes the delegator's view, neither needs the files.
- **Read the meta table on every check.** Simplest, but puts RootCoord's `ddLock` on QueryCoord's dispatch loop. The atomic snapshot keeps the check lock-free at the cost of one `Sync`-pass convergence window that already exists for the nodes.
- **Fail the task instead of deferring it.** A failed task is re-planned by the checkers, which produce the same action for the same node and fail again; deferral keeps the task where it is and lets the download finish.
- **Treat an `Unimplemented` node as behind.** Correct in isolation, but makes a rolling upgrade un-upgradeable past its oldest node.

## 5. Testing

- `internal/querycoordv2/task/executor_file_resource_test.go`: a nil gate lets every action through; only grow segment and grow channel consult the gate (reductions and leader actions do not); a held task holds neither pool slot, is not marked executing, and carries no error; the scheduler hands the gate to every executor it creates, and leaves executors ungated when no gate is set.
- `internal/coordinator/file_resource_observer_test.go`: after the first read the gate judges from the snapshot and never re-takes the meta table per dispatch; an unrecorded node is behind and a node recorded at the target version passes; `Sync` on a version-0 meta table still publishes the snapshot before its early return.
- `golangci-lint` on every touched package.
