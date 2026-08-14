# Broadcast Ack Module

`broadcastAckModule` sends consuming-side acknowledgements for broadcast WAL
messages to StreamingCoord. It is a dedicated RecoveryStorage sink, not a
`moduleapi.Module` and not a data persistence consumer.

The common lifetime contract is defined in
[WAL Message Ack Design](message_ack.md).

## 1. Accept Ownership

```go
func (m *broadcastAckModule) Accept(owner message.OwnedImmutableMessage)
```

`Accept` takes exclusive top-level ownership of the Owner. The caller must not
use or clone it afterward.

- If `owner.Message().BroadcastHeader() == nil`, BroadcastAck immediately calls
  `owner.Release()` and creates no task.
- Otherwise, BroadcastAck appends an Ack task to its per-PChannel arrival-ordered
  task list and keeps the Owner until Coordinator Ack succeeds.

Tracker is not passed to BroadcastAck. The Owner finalizer established by
Tracker is sufficient to mark the message complete after BroadcastAck finally
releases the root reference.

## 2. Exclusive Callback

BroadcastAck registers exactly one callback before returning from `Accept`:

```go
owner.RegisterExclusiveCallback(func() {
    task.exclusive.Store(true)
    module.wakeDispatcher() // nonblocking send
})
```

The callback fires once when BroadcastAck's Owner is the only remaining tracked
reference. If the Owner is already exclusive, registration invokes the callback
immediately. Otherwise, the last Retained release invokes it after dropping the
reference-count lock.

The callback never performs Coordinator IO or submits a task directly. It only
marks the task ready and performs a nonblocking send to the module's buffered
wakeup channel. One module background goroutine consumes wakeups and runs the
dispatcher. This avoids one waiter goroutine per broadcast message and prevents
the final Retained release from blocking on Ack scheduling.

Once the Owner is accepted, no new Retained clones may be created. Therefore a
fired callback remains a stable readiness precondition for the Ack task. The
callback does not mean the message is complete, because BroadcastAck still owns
the root reference.

## 3. ResourceKey Partial Order

Every task snapshots `BroadcastHeader.ResourceKeys` when accepted. Two tasks
conflict when they contain the same `(Domain, Key)` and at least one matching
key is exclusive. Shared/shared access to the same resource does not conflict.

A task is schedulable exactly when:

```text
exclusive callback has fired
AND task is not already in flight
AND no earlier unfinished task has a conflicting ResourceKey
```

The dispatcher scans tasks in RecoveryStorage observation order and submits all
schedulable tasks. Therefore conflicting tasks preserve local WAL order while
independent or shared-only tasks may Ack concurrently. An unready earlier task
does not block a later non-conflicting task.

All new broadcasts carry SharedCluster unless they explicitly carry a Cluster
key. For compatibility with old WAL entries:

- an empty ResourceKey set is normalized to ExclusiveCluster, preserving the
  old global FIFO behavior conservatively;
- a non-empty set without a Cluster-domain key receives SharedCluster.

Import, CommitImport, and RollbackImport target their data VChannels plus
CChannel. This provides the common ordered copy needed by replicated broadcast
callback processing; their collection ResourceKeys are preserved in every WAL
copy.

## 4. Ack And Retry

Coordinator Ack is executed by the shared NodeScheduler. On success,
BroadcastAck releases the Owner, marks the task complete, and wakes the
dispatcher so later conflicting tasks may run.

If Coordinator Ack fails, the task retains the same Owner and remains an
unfinished ResourceKey predecessor. After the retry delay it becomes
schedulable again. Only later conflicting tasks remain blocked; non-conflicting
tasks continue independently. Ack is idempotent, so replay may repeat an Ack
accepted before a crash but not covered by a persisted Data checkpoint.

## 5. AckSyncUp

`BroadcastHeader.AckSyncUp` affects only StreamingCoord: it skips FastAck and
waits for this consuming-side Ack. BroadcastAck still waits only for local
Retained consumers. It does not wait for Meta/Data checkpoint publication,
DirtySnapshot persistence, TransformLog materialization, or QueryRuntime
readiness.

## 6. Close

Close stops the background dispatcher and cancels retry timers. It does not
release unfinished Owners or mark unfinished work successful. Restart
reconstructs the tasks by replaying from the persisted Data checkpoint.

## 7. Invariants

1. BroadcastAck is outside the recovery component dirty-snapshot lifecycle.
2. `Accept` consumes the Owner exactly once.
3. Non-broadcast Owners are released immediately.
4. Broadcast Owners remain live through every failed Ack attempt.
5. Coordinator Ack runs only after the Owner's one-shot exclusive callback.
6. A callback performs only a nonblocking dispatcher wakeup.
7. Earlier unfinished conflicts remain claimed through Ack retry.
8. Successful Ack releases the Owner before later conflicting tasks run.
9. BroadcastAck has no Tracker handle, checkpoint frontier, or materialization
   dependency.
