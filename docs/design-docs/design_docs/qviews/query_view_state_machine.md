# QueryView State Machine Per-Node Analysis

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

> This document provides a detailed per-node, per-state analysis of the QueryView state machine.
> For each state, it covers: entry conditions, automatic behavior, valid transitions, and possible peer states with how the current node reacts to each.
> Reference: [Distributed Query View Design](README.md), [view.proto](../../../../pkg/proto/view.proto)

## 1. Coord State Machine

Coord is the leader of the global state machine. It generates QueryViews, drives state transitions, and persists state to ETCD for crash recovery.

Persisted states: **Preparing**, **Up**, **Down**, **Unrecoverable** (write-ahead), **Dropped** (deletion).

Each state-machine instance is derived from one Collection-scoped DataVersion.
The QueryView manager must own the corresponding `DataViewRef` from Preparing
through Ready, Up, Down, Unrecoverable, and Dropping, and release it only after
Dropped destroys the instance. On recovery, persisted QueryViews must reacquire
their DataVersions before DataView snapshot GC is enabled.

L0 compaction advances only DataView TransformVersion. It does not invalidate
the current QueryView: the existing state-machine instance keeps its exact old
DataViewRef and continues applying equivalent deletes from TransformLog. The
manager coalesces such soft updates and eventually creates a new QueryView at
the latest TransformVersion so the old Manifest revision can be released. The
TransformLog start frontier remains a TODO pending the StreamingNode shard
barrier described in
[Transform Start-After TimeTick](transform_start_after_timetick.md). The
scheduling policy is not implemented by the current DataView-only PR.

### 1.1 Preparing

**Entry Conditions:**
- Balancer generates a new view (hard triggers: StreamingVersion or
  CompactVersion change, balance request, QN online/offline, previous view
  becomes Unrecoverable, or load config changes such as
  LoadPartition/ReleasePartition). A TransformVersion-only change is a soft
  trigger: the current QueryView remains valid, updates are coalesced, and a
  later hard trigger or retention/maintenance policy eventually creates the
  replacement QueryView.
- Recovery: loaded from ETCD in Preparing state.

**Automatic Behavior:**
1. Persist Preparing to ETCD (write-ahead to prevent state loss on crash).
2. Push Preparing to target SN and all QNs via SyncQueryView.
3. Wait for all nodes to report Ready.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Ready | All SN and QNs report Ready | None |
| Unrecoverable | Any node reports Unrecoverable | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Preparing / Ready / Unrecoverable / Up
  - Preparing (async preparation in progress) → Coord waits.
  - Ready → Coord marks SN as ready; checks if all nodes are ready.
  - Unrecoverable → Coord transitions to Unrecoverable.
  - Up (recovery scenario: SN restored an old Up view from persistence) → Coord fast-forwards to Up.
- **QN**: Preparing / Ready / Unrecoverable
  - Preparing → Coord waits.
  - Ready → Coord marks this QN as ready; checks if all nodes are ready.
  - Unrecoverable → Coord transitions to Unrecoverable.

### 1.2 Ready

**Entry Conditions:**
- All SN and QNs have reported Ready (automatic transition from Preparing).

**Automatic Behavior:**
1. Push Up to SN.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Up | SN confirms Up | Persist Up to ETCD |
| Unrecoverable | Any node reports Unrecoverable | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Ready / Up
  - Ready (Up push not yet delivered) → Coord re-pushes Up.
  - Up → Coord transitions to Up.
- **QN**: Ready / Unrecoverable
  - Ready → Coord does nothing; normal.
  - Unrecoverable → Coord transitions to Unrecoverable.

> Note: Ready is NOT persisted. On Coord crash recovery, ETCD still shows Preparing; Coord re-pushes and catches up from node responses.

### 1.3 Up

**Entry Conditions:**
- SN confirmed Up (automatic transition from Ready).
- Recovery: loaded from ETCD in Up state.

**Automatic Behavior:**
1. Persist Up to ETCD (if transitioning from Ready).
2. View is now actively serving queries.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Down | Higher-version view confirms Up / ReleaseCollection | Persist Down to ETCD; push Down to SN |
| Unrecoverable | Any node reports Unrecoverable | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Up / Unrecoverable
  - Up → Coord does nothing; normal.
  - Unrecoverable → Coord transitions to Unrecoverable when the state is
    explicitly reported. An SN-local failure during UpRecovering does not
    report directly; the query path detects the unavailable view and triggers
    replacement.
- **QN**: Ready / Unrecoverable
  - Ready → Coord does nothing; normal.
  - Unrecoverable → Coord transitions to Unrecoverable.

### 1.4 Down

**Entry Conditions:**
- Higher-version view confirms Up; Coord immediately transitions the old Up
  view to Down.
- ReleaseCollection.
- Recovery: loaded from ETCD in Down state.

**Automatic Behavior:**
1. Persist Down to ETCD (if transitioning from Up).
2. Push Down to SN.

> Query lease ownership: Coord does not wait for a lease period before entering
> Down and always keeps at most one Up view. After receiving Down, StreamingNode
> stops generating new query plans from the old view, but query leases/query
> references keep its resources alive for already-generated queries. Resource
> release completes only after those references are released.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropping | SN confirms Down or reports Dropped | None (no additional persistence) |
| Unrecoverable | Any node reports Unrecoverable | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Up / Down / Dropped / Unrecoverable
  - Up (Down push not yet delivered) → Coord re-pushes Down.
  - Down → Coord transitions to Dropping.
  - Dropped → Coord transitions to Dropping. This fast-forwards recovery when
    Coord regresses from the unpersisted Dropping state to persisted Down while
    SN has already completed Dropped.
  - Unrecoverable → Coord transitions to Unrecoverable.
- **QN**: Ready / Unrecoverable
  - Ready → Coord does nothing.
  - Unrecoverable → Coord transitions to Unrecoverable.

### 1.5 Unrecoverable

**Entry Conditions:**
- Any node reports Unrecoverable while Coord is in Preparing, Ready, Up, or
  Down (automatic transition).
- Manager calls `EnterUnrecoverable` from Preparing, Ready, or Up, for example
  when preempting an in-flight view or handling RequestRelease.

> Note: QueryNode loss is delivered to Coord only for active QN-targeted syncs via `OnQueryNodeLost`. In Preparing it makes the view Unrecoverable; in Dropping it counts that QN cleanup as complete. SN is bound to the vchannel and never experiences "node lost" from Coord's per-view perspective; SN unavailability is handled at the channel assignment level.

**Automatic Behavior:**
1. Persist Unrecoverable to ETCD (if not already persisted).
2. Wait for Manager to advance to Dropping (typically after generating a replacement view).

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropping | Manager calls EnterDropping (may be pushed atomically with a new view's Preparing) | Push Dropped to all nodes |

**Possible Peer States (and Coord's reaction):**
- **SN**: Preparing / Ready / Up / Unrecoverable
  - Coord ignores node reports while in Unrecoverable; waits for Manager to call EnterDropping.
- **QN**: Preparing / Ready / Unrecoverable
  - Same as SN above.

> Note: Unrecoverable is a stable state. The Manager decides when to advance to Dropping, typically after generating a replacement view so both the old view's Dropping and the new view's Preparing can be pushed atomically.

### 1.6 Dropping

**Entry Conditions:**
- SN confirmed Down in the Down phase (automatic transition from Down).
- Manager calls EnterDropping from Unrecoverable (which itself can be reached
  from Preparing, Ready, Up, or Down).

**Automatic Behavior:**
1. Push Dropped to all nodes (SN + all QNs).

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | All nodes confirm Dropped | Delete the view from ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Preparing / Ready / Up / Down / Unrecoverable / Dropped
  - Preparing / Ready / Up / Down / Unrecoverable (Dropped push not yet delivered) → Coord re-pushes Dropped.
  - Dropped → Coord marks SN as cleaned up.
- **QN**: Preparing / Ready / Unrecoverable / Dropped
  - Preparing / Ready / Unrecoverable (Dropped push not yet delivered) → Coord re-pushes Dropped.
  - Dropped → Coord marks this QN as cleaned up; checks if all nodes are cleaned up.
  - QueryNode lost while Dropped is pending → Coord treats that QN cleanup as complete; checks if all nodes are cleaned up.

> Note: Dropping is NOT independently persisted. On Coord crash recovery, it recovers from Down or Unrecoverable and re-executes the Dropping flow.

### 1.7 Dropped

**Entry Conditions:**
- All nodes confirmed Dropped (automatic transition from Dropping).

**Automatic Behavior:**
1. Delete the view from ETCD.
2. Destroy the state machine instance.

**Transitions:** None (terminal state).

**Possible Peer States (and Coord's reaction):** None (view has been removed from all nodes).

---

## 2. StreamingNode State Machine

StreamingNode manages growing data and generates query plans. It persists Up
recovery records for crash recovery. Each persisted recovery record is the
complete `QueryViewOfShard` received from Coord, including both
`QueryViewOfStreamingNode` and `QueryViewOfQueryNode`; the latter is required to
rebuild Phase 1 query plans after StreamingNode restart.

Persisted states: **Up** recovery info only. Every version that has reached Up
is persisted independently until that view receives Down or Dropped, so
multiple Up recovery records may coexist.

> **TODO:** Wire StreamingNode resource-preparation and WAL-recovery failures
> to `snQueryViewStateMachine.OnUnrecoverable`. The current production resource
> manager reports successful readiness but does not provide an unrecoverable
> failure callback. Until that callback is introduced, the StreamingNode
> transitions to Unrecoverable described below are design intent rather than a
> production-reachable chain.

### 2.1 Preparing

**Entry Conditions:**
- Received Preparing sync signal from Coord via SyncQueryView.

**Automatic Behavior:**
1. Check replica information.
2. Transition growing segments to queryable state.
3. Check whether retained growing data can satisfy the QueryView's composite
   DataVersion, using the per-Segment Flush `streaming_version` handoff metadata.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Ready | Resource preparation succeeded | Report Ready to Coord |
| Unrecoverable | **TODO:** required growing data expired because a flushed Segment was released before the view's streaming-version watermark allowed it | Report Unrecoverable to Coord after the failure callback is wired |
| Dropped | Received Dropped push from Coord (Coord aborted this view) | Release any prepared resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing → SN continues preparing; normal scenario.
- Coord pushes Dropped → SN transitions to Dropped (Coord has abandoned this view).
- Other signals → SN ignores (invalid transition).

### 2.2 Ready

**Entry Conditions:**
- Preparing resource preparation succeeded (internal automatic transition).

**Automatic Behavior:** None; waiting for Coord to push Up.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Up | Received Up push from Coord | Persist recovery info; activate view |
| Dropped | Received Dropped push from Coord | Release resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing / Ready → SN waits for Up push.
- Coord pushes Up → SN transitions to Up.
- Coord pushes Dropped → SN transitions to Dropped.

### 2.3 Up

**Entry Conditions:**
- Received Up push from Coord.

**Automatic Behavior:**
1. Persist this view's recovery info under its full QueryView version. Recovery
   records for other Up versions are retained independently.
2. Activate the view for query plan generation.
3. Report Up to Coord.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Down | Received Down push from Coord | Delete persisted recovery info; stop generating query plans from this view (but can still serve query execution requests) |

**Possible Coord States (and this node's reaction):**
- Coord in Up / Down → SN does nothing; normal.
- Coord pushes Down → SN transitions to Down.
- Other signals → SN ignores.

### 2.4 UpRecovering (StreamingNode-Only Proto State)

UpRecovering is defined in the proto enum (`QueryViewStateUpRecovering = 8`) but is only used by StreamingNode.
Coord and QueryNode never enter this state. For Coord-visible reporting, UpRecovering maps to Up
(Coord is unaware of UpRecovering and considers the view to be in Up state).

**Entry Conditions:**
- SN crash recovery: every persisted Up view is rebuilt into its own
  UpRecovering state-machine instance.
- WAL consumption has not yet caught up; growing segment data ([A2] portion) is incomplete.

**Automatic Behavior:**
1. Replay WAL from the checkpoint position to recover growing segments.
2. Do NOT serve queries (data is incomplete).
3. Multiple UpRecovering versions may coexist. After recovery, query planning
   selects the highest available Up version.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Up | WAL consumption catches up to current position | Begin serving queries |
| Down | Received Down push from Coord | Delete recovery info; abandon WAL catch-up |
| Unrecoverable | **TODO:** local resource failure during WAL recovery (e.g., OOM) | Mark the view locally unavailable without reporting to Coord after the failure callback is wired; retain persisted Up recovery info, and let the query path trigger replacement |

**Possible Coord States (and this node's reaction):**
- Coord considers this view to be in Up state (Coord is unaware of UpRecovering).
- Coord pushes Down → SN transitions to Down directly.
- Coord pushes Preparing (recovery scenario) → SN waits until WAL catches up and transitions to Up, then reports Up to allow Coord to fast-forward.
- Local recovery failure → SN remains locally Unrecoverable without reporting;
  Coord continues to consider the view Up until the query path triggers a
  replacement.

### 2.5 Down

**Entry Conditions:**
- Received Down push from Coord.

**Automatic Behavior:**
1. Delete persisted recovery info.
2. Stop generating query plans from this view (but can still serve query execution requests under plans already generated).
3. Report Down to Coord.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release all view-related resources |

**Possible Coord States (and this node's reaction):**
- Coord in Down / Dropping → SN does nothing; normal.
- Coord pushes Dropped → SN transitions to Dropped.

### 2.6 Unrecoverable (TODO: Production Failure Wiring)

**Entry Conditions:**
- **TODO:** required growing data check failed during Preparing because a
  flushed Segment was released before the view's streaming-version watermark
  allowed it.
- **TODO:** local resource failure during UpRecovering (e.g., OOM while
  replaying WAL to recover growing segments).

**Automatic Behavior:**
1. When entered from Preparing, report Unrecoverable to Coord.
2. When entered from UpRecovering, do not report directly. Retain the persisted
   Up recovery information for a later restart retry and let the query path
   trigger replacement.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release any prepared resources |

**Possible Coord States (and this node's reaction):**
- Entered from Preparing: Coord may be in Preparing / Unrecoverable / Dropping;
  SN waits for Dropped.
- Entered from UpRecovering: Coord still considers the view Up until the query
  path triggers replacement.
- Coord pushes Dropped → SN transitions to Dropped.

### 2.7 Dropped

**Entry Conditions:**
- Received Dropped push from Coord (from Down / Unrecoverable / Preparing).

**Automatic Behavior:**
1. Release all view-related resources.
2. Report Dropped to Coord.

**Transitions:** None (terminal state; state machine instance destroyed).

---

## 3. QueryNode State Machine

QueryNode is fully stateless with no persistence and no recovery process. It does NOT observe Up, Down, or Dropping states — it can serve queries as soon as it reaches Ready.

### 3.1 Preparing

**Entry Conditions:**
- Received Preparing sync signal from Coord via SyncQueryView.

**Automatic Behavior:**
1. Resolve each Segment's loading metadata from the DataView Manifest version:
   - Version `0`: Coord keeps watching Coordinator SegmentMeta, resolves the
     complete current SegmentInfo, and freezes it into this QueryView/load
     operation before dispatch.
   - Positive version: QueryNode constructs the canonical StorageV3 Manifest
     object path from Collection/Partition/Segment IDs plus the version and
     derives all loading metadata directly from that Manifest; no Coordinator
     SegmentMeta watch is required for this Segment.
2. Asynchronously load segments from object storage.
3. Subscribe to the pure delete stream from SN.
4. Mark each segment as ready progressively; report the latest accumulated
   ready subset to Coord via `ready_segment_ids` in responses.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Ready | All segments loaded successfully | Report Ready to Coord |
| Unrecoverable | Resource preparation failed (OOM, disk full, etc.) | Report Unrecoverable to Coord |
| Dropped | Received Dropped push from Coord (Coord aborted this view) | Release loaded resources; disconnect delete stream |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing → QN continues preparing; normal.
- Coord pushes Dropped → QN transitions to Dropped directly.
- Other signals → QN ignores (invalid transition).

### 3.2 Ready

**Entry Conditions:**
- All segments loaded successfully (internal automatic transition from Preparing).

**Automatic Behavior:** None; can serve query requests. QN serves queries in Ready state — query plans are generated by SN, and QN only needs data to be ready.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release segments; disconnect delete stream |

**Possible Coord States (and this node's reaction):**
- Coord may be in any state (Preparing / Ready / Up / Down / Dropping).
- QN is unaware of Up/Down; QN continues to serve as long as it is Ready.
- Coord pushes Dropped → QN transitions to Dropped.
- Other signals → QN ignores.

### 3.3 Unrecoverable

**Entry Conditions:**
- Resource preparation failed during Preparing (OOM, disk full, etc.).

**Automatic Behavior:**
1. Report Unrecoverable to Coord.
2. Retain already-loaded resources without rollback (waiting for Coord to orchestrate unified cleanup).

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release all resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing / Unrecoverable / Dropping → QN does nothing; normal.
- Coord pushes Dropped → QN transitions to Dropped.

### 3.4 Dropped

**Entry Conditions:**
- Received Dropped push from Coord.

**Automatic Behavior:**
1. Release all view-related segments.
2. Disconnect pure delete stream subscriptions.
3. Report Dropped to Coord.

**Transitions:** None (terminal state; state machine instance destroyed).
