# Async load sequence ordering

## Queue contract

ASYNC_LOAD retains Folly's CPUThreadPoolExecutor and replaces its blocking
queue with per-load concurrent FIFOs and one ready-group heap per CPU priority.
Ready work is selected by:

1. Weighted CPU priority rounds: URGENT 32, HIGH 8, LOW 1 task reservations.
   Empty classes forfeit their quota; an idle queue resets the round.
2. `load_seq`: smaller sequences first within the same priority.
3. Enqueue order: FIFO when both keys match.

`load_seq == 0` denotes an unspecified sequence. These tasks follow numbered
loads within their priority and retain FIFO order. Ordering applies to pending
work; it neither preempts running tasks nor guarantees completion order across
multiple workers. A continuously ready class receives its turn even under sustained URGENT traffic.
These are task-count shares, not CPU-time shares or completion-time guarantees.
Admission remains strict HIGH/LOW, so CPU progress alone does not ensure
end-to-end loading progress.

## Sequence ownership and propagation

`AsyncSegmentLoad` and `AsyncReopenSegment` allocate a sequence before submitting
their load future, using one process-wide relaxed atomic counter. The callback
copies it into its `OpContext`. A new load/reopen invocation gets a new sequence;
the value is not a segment ID, timestamp, persistent field, or cross-process key.

Field-reader, index-loader and JSON-stats scheduling entries copy the sequence
from `OpContext` into an executor adapter. Child index contexts and asynchronous
cache warmup retain the value. Background warmup copies the integer before
queueing and owns its new context, without retaining the caller's context.

The adapter owns a Folly keep-alive, immutable priority/sequence values, and a
shared queue-specific group for ASYNC_LOAD. Independent adapters with the same
key share that group. Other executors retain their own queue discipline. Its
`add()` installs the sequence in a shallow copy of RequestContext **before**
calling the underlying executor. The queue reads the CPUTask's captured context,
so a foreign I/O completion thread cannot change the coroutine's queue key.
The context contains a weak group reference, avoiding a reference cycle through
queued tasks. A context forwarded to a different queue falls back to its numeric
sequence and that queue's own group registry. Queue destruction drains pending callables before releasing ready groups, since
a callable can itself retain an adapter/group. The registry contains weak entries
and removes them on group destruction, so completed loads do not accumulate.
Folly still owns coroutine resumption and execution-context restoration.

## Group scheduling

Each `(priority, load_seq)` group owns a Folly UMPMC FIFO with 16-entry allocation
segments. An atomic word combines the number of unreserved tasks and a bit
indicating membership in the ready heap. Enqueue into an already-ready group
bypasses the scheduling mutex. An inactive group must be published under that
mutex before its producer posts a semaphore permit.

Consumers select a CPU priority by quota, reserve one task from its earliest
ready group under the mutex, then
dequeue the group's FIFO outside the mutex. This reservation is the group-order
linearization point; execution/completion order across workers is not promised.
Reserving the last task atomically clears readiness and removes the group. A
producer delayed while publishing must recheck both readiness and task count:
other producers and consumers may already have published and drained its group.
It must never insert an empty group.

Registering a group reserves sufficient ready-heap capacity before submitting
any tasks, so publishing a committed FIFO task does not need a vector allocation.
The lock order is registry then scheduler; retired groups are destroyed outside
the scheduler lock. FIFO allocation, dequeue waiting, and retired FIFO cleanup
also happen outside that lock. ThrottledLifoSem handles worker wakeup and
timeouts. Folly poison tasks are counted separately and can only be taken
when all ordinary heaps are empty; weighted LOW turns must not terminate
workers while accepted work is still pending.

Executor switches explicitly read the sequence from `co_current_executor` and
bind it to the next adapter. They do not rely on the current thread's request
context. Dedicated local I/O executors keep their existing queue discipline;
the adapter preserves metadata for subsequent admission and ASYNC_LOAD work.

## I/O admission

The admission controller keeps separate HIGH/LOW queues, each represented by a
multimap keyed by sequence. Equal keys retain insertion order. Queued requests
reserve no resources. An older request may use available capacity ahead of a
newer pending request, but cannot bypass an earlier/equal sequence or a higher
priority. Cancellation removes the exact queued iterator; terminal promises are
fulfilled outside the controller lock as before.

Enqueue is O(log N); removing the head or a known cancellation iterator is
amortized O(1). For the executor, enqueue into a ready group and reservation from
a group that stays ready do not adjust the heap. Group activation/deactivation is
O(log S), where S is the number of ready groups. FIFO operations are amortized
O(1); concurrent producers may delay publication of a reserved FIFO slot. Pending
tasks use O(N) memory plus per-live-group FIFO segments and registry entries. The
ready-heap vector retains capacity up to the historical peak group count. The admission oldest-wait metric scans pending timestamps at scrape
time because the sequence-sorted head need not be the oldest queued request.

## Dependency and verification boundary

This change requires the companion `milvus-common` changes to
`include/common/OpContext.h` and `include/cachinglayer/CacheSlot.h`. Rebuild and
publish that dependency, then update the Milvus Conan reference before merging.
Changing only an installed header is not a supported integration procedure.

Focused tests cover queue ordering, FIFO ties, coroutine resumption from an
unrelated thread, executor switching, resizing, sorted admission cancellation,
and admission ahead of a newer blocked request. Full segment-load/reopen and
background-warmup integration still require a consistent dependency build.

## Grouped implementation verification

The focused suite additionally covers concurrent submissions through shared
bindings, repeated empty-group reactivation, and FIFO across recreated adapters.
The standalone benchmark checks pending-queue destruction, expired/foreign
context bindings, group-registry cleanup, and timeouts before measurement.
See [the grouped benchmark](async-load-grouped-benchmark.md) for measured gains,
remaining gaps, sanitizer coverage, and reproduction commands.

## Three-class CPU experiment

`AsyncLoadPriority` exposes local Urgent/High/Low classes without changing the
wire enum. Existing Proto HIGH/LOW callers retain those classes; no Search caller
is automatically promoted. On the default pool these map to Folly HI/MID/LO;
`getNumPriorities()` returns 3. External executors retain the old HIGH-to-HI
mapping, so three distinct classes are only guaranteed on ASYNC_LOAD.

The existing admission controller and its two-class mapping are unchanged.
Search's context propagation and resource priority require separate integration
before claiming production URGENT behavior.

CPU selection adds no clock read, allocation, or new lock. It reuses the existing
reservation lock and scans at most three classes. Each heap reserves using the
registry's total group count, retaining an upper bound of roughly three times
the old ready-heap pointer capacity when all classes have many bindings.

Measurements and their limitations: [weighted scheduling experiment](async-load-weighted-performance.md).

## POC configuration for end-to-end comparisons

```yaml
queryNode:
  segcore:
    storageV2:
      asyncLoadSchedulingWeights: "32,8,1"
```

The order is URGENT,HIGH,LOW. Set `"1,1,1"` for equal task-count shares.
All three values must be positive int32 integers; malformed values fall back
to `32,8,1` with a warning. Restart QueryNode (or standalone) after changing
this startup-only setting. The startup log prints the applied weights.
The native setter also applies to an already initialized pool without replacing
it, but no configuration watcher is registered for this POC.

Use identical async-load settings, input, worker count and admission limits
for both runs. CPU weights affect only competing ready classes; admission still
uses HIGH/LOW and load_seq. Search is not assigned to URGENT by this setting.
If all real load tasks are HIGH, changing URGENT's weight alone cannot provide
an URGENT-versus-load latency comparison.
