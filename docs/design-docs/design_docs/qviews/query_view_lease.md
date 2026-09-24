# StreamingNode QueryView Serving Lease

## Purpose and scope

Keep an accessed SN QueryView in Up long enough for Phase 1 plans to reach
Phase 2 during normal view replacement. This is an SN-local, renewable serving
lease, separate from the existing call-scoped QueryView reference and physical
segment handles. Only Up accepts new tasks; Down does not become queryable.

## Configuration and renewal

`queryView.leaseDuration` defaults to `60s` and is not refreshable.
The duration is captured when the PChannel handler is created and inherited by
its shard handlers. Non-positive durations disable timed retention while
preserving active call references. A duration is not a maximum request execution
time.

Successful acquisition of an Up view through GetQueryPlan, SearchOnView, or
QueryOnView renews its local deadline to `max(deadline, now + duration)`.
GetQueryPlan renews again immediately before a successful return so time spent
planning does not consume the entire inter-phase window. Failed acquisition,
Coord state pushes, and repeated Down requests do not renew the deadline.
The Search ignore-growing fast path does not access an Up view and grants no
lease. A view never accessed by a query has no timed retention.

GetQueryPlan continues selecting the highest Up version. Older Up versions
remain accessible to explicit-version Phase 2 requests, and those accesses can
continue renewing them without a hard lifetime cap. Continuous reuse of an old
plan can therefore delay resource reclamation indefinitely.

## Down protocol

1. Coord marks the old view Down and sends Down to SN after a replacement is Up.
2. SN records a pending Down intent. While the serving deadline is in the future
   or a call-scoped QueryView reference exists, SN stays Up and retains its
   persisted Up record. It does not acknowledge Down early.
3. A per-view one-shot timer checks the deadline. Renewal moves the deadline;
   an early/stale timer rechecks it under the shard mutex. Duplicate Down pushes
   replace the report callback but do not reset the lease or create a report
   loop. Once the original window has elapsed, active references alone need no
   timer: their final release rechecks Down eligibility.
4. Once the deadline expires and call references reach zero, SN transitions to
   Down, deletes persisted Up metadata, then reports Down through the current
   ViewSync callback. Expiry alone never changes a view without a Down intent.
5. Coord's existing Down confirmation gate then permits Dropped to be sent to
   SN and QNs. QNs remain Ready while SN is waiting, protecting both halves of
   an outstanding plan. No per-request Coord/QN lease RPC is required.

All renewal, acquisition, expiry, and Down decisions are serialized by the
shard mutex. An expired pending-Down view must be advanced before accepting a
new acquisition even if the timer callback has been delayed. A request already
holding a call reference can finish deriving handles. The timed lease and
reference count are both required to be exhausted before normal Down.

## Reference ownership and completion

Phase 1 holds a call reference until plan generation returns. Phase 2 holds one
through MVCC waiting and handle acquisition; segment execution then owns pinned
handles independently. Dropping still waits for call references before
releasing view resources, and physical segment destruction waits for handles.
Cancellation exits MVCC waiting and releases the call reference; it does not
revoke a timed lease already granted by successful acquisition.

## Failure, recovery, and compatibility

The serving deadline and pending Down intent are process-local and are not
persisted. The clock uses Go's monotonic time component. A crash or WAL ownership
change invalidates the continuity guarantee; callers retry through existing
view/WAL errors. Recovery rebuilds persisted views as UpRecovering without a
new lease; Coord re-delivers its desired state.

Explicit Dropped (including Unrecoverable cleanup), shutdown, and WAL handoff
cancel pending timers and bypass timed retention. They preserve existing
resource/handle cleanup semantics. The lease does not make a failed node or an
unavailable QN queryable.

There is no proto change and no deadline sent to callers. Older SNs can still
reject plans immediately after Down, so mixed-version deployments retain the
existing retry requirement. Coord may be Down while SN remains Up; this is
already a supported follower state. Multiple SN-local Up versions are expected.

## Verification

Verify default/configured duration, access and successful-plan renewal,
duplicate Down with stream callback replacement, latest-version selection,
old-version Phase 2 execution, timer expiry without another RPC, cancellation,
active references across expiry, no resurrection after expiry, forced cleanup,
and recovery without inherited deadlines. Unit tests must cover the Coord Down
gate as well as SN lifetime behavior. A real SN RPC experiment must continue
SearchOnView/QueryOnView while Down is pending, then stop renewal and observe
Down followed by rejection. The current extraction uses explicit ViewSync;
this does not claim end-to-end automatic Coord scheduling or Proxy integration.
