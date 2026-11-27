# Async segment load scheduling

Concurrent segment loads can interleave their field and index work at every
coroutine suspension. A single operation can therefore queue repeatedly behind
work that started later. The scheduler carries an operation's sequence through
both CPU execution and resource admission so they can order work within each
priority class consistently.

## Operation sequence

Each async segment load or reopen allocates a process-local `sequence_id` from
`milvus-common` before submitting its future. The future constructs an
`OpContext` with that sequence and its own cancellation token. A subsequent
operation on the same segment receives a new number.

Field readers, index loaders and JSON statistics use that context to bind the
sequence to their executors. Child index contexts reuse it. Eager column-group
warmup also receives the caller context. Background cache warmup captures the
sequence by value at submission and constructs its own context with the cache
slot's cancellation token. This allows warmup to outlive the submitting context.

The sequence is local to the process. It is used for pending-work ordering and
has no meaning as a segment identifier or a completion timestamp. The queue
uses zero for work with no bound sequence and places it after numbered work
within its priority class.

## CPU executor

The shared executor uses Folly's `CPUThreadPoolExecutor` with one ready-group
heap per priority class. Each `(priority, sequence_id)` group has a concurrent
FIFO. Workers reserve tasks in weighted priority rounds, selecting the lowest
sequence first within the chosen class and preserving FIFO order for ties. The
round skips empty classes and resets when no work is pending. Task reservations
consume quota independently of task duration. The executor does not preempt
running work or enforce completion order across workers.

Submitting to an already-ready group avoids the scheduling mutex. The mutex
protects group activation, selection and reservation. Consumers take reserved
tasks from the FIFO outside that lock. The group registry holds weak references
and removes groups when they are destroyed.

The executor adapter captures the sequence in Folly `RequestContext` before
each submission, including resumption from an I/O completion thread. Executor
switches bind the sequence from the currently awaited executor. Other executors
retain their own queue policies. The adapter also preserves an explicitly bound
`URGENT` class across these switches. Search sets a local runtime override
in its operation context. When Search misses the cache and reaches the async
load pipeline, CPU work and resource admission use `URGENT`. Two-class
readers handle the override as `HIGH`. The Search CPU pool and wire priorities
are unchanged. Detached warmup creates its own context and keeps its
configured priority. A Search that joins an existing cache load waits for
that load at the priority it was submitted with.

## Resource admission

The admission controller uses the same ordered classes and weights in a
separate round that counts admitted requests. In each priority class the queue
is ordered by sequence, with FIFO ties. Pending requests hold no byte or slot
reservation. Successful admissions consume one quota, and an admitted request
holds its reservation until the lease is released.

If the selected head lacks bytes or slots, it keeps its turn. Other classes
wait even if a smaller request could fit. A request in the same class with an
earlier sequence can still acquire available capacity. This policy lets large
requests make progress but can delay other work. Cancellation removes the
queued request without consuming quota. Capacity changes and cancellation
trigger another admission check. The controller completes promises after
releasing its lock.

The admission metrics include pending requests, oldest wait and queue wait
outcomes for `urgent`, `high` and `low`.

## Configuration

```yaml
queryNode:
  segcore:
    load:
      schedulingWeights: "32,8,1"
```

The three positive integers control the `URGENT`, `HIGH` and `LOW` classes in
that order. Invalid values fall back to `32,8,1`. The CPU scheduler counts
reserved tasks and admission counts admitted requests. The two rounds are
independent. The setting applies at startup. Restart QueryNode or standalone
after changing it. `queryNode.segcore.storageV2.asyncLoadSchedulingWeights`
remains a fallback for the earlier configuration key.

This design uses the [`OpContext` sequence API][common-sequence] in
`milvus-common/1.0.0-9c892db`.

[common-sequence]: https://github.com/zilliztech/milvus-common/pull/154
