# Channel Management

Singletons running inside StreamingCoord that manage [PChannel/VChannel/CChannel](../channel/channel.md) assignment and metadata.

## Functionality

- **PChannel assignment**: Assigns each PChannel to exactly one StreamingNode via a pluggable balance policy (default: `vchannelfair`). Rebalancing is triggered by node join/leave, VChannel count change, periodic timer, or manual `Trigger()`.
- **Node health monitoring**: Watches StreamingNode status. Unhealthy nodes have their PChannels marked UNAVAILABLE and reassigned. StreamingNode file-resource consumers prepare their own inventories through the pull path; QN file-resource push acknowledgments are not a prerequisite for SN assignment.
- **VChannel allocation**: `AllocVirtualChannels()` assigns new VChannels to the least-loaded `AvailableInReplication` PChannels.
- **CChannel management**: Persists which PChannel hosts the singleton CChannel. Assigned once at initialization, never changes.
- **Assignment publishing**: Exposes gRPC `AssignmentDiscover` endpoint for StreamingClient. Versioned by `(Global, Local)` pair.
- **Replication config**: Persists `ReplicateConfiguration` and maintains the per-PChannel `AvailableInReplication` flag. When false, the PChannel is excluded from VChannel allocation and DDL broadcasts. `UpdateReplicateConfiguration()` recomputes this flag for all PChannels and creates CDC tasks for newly added target clusters. `AvailableInReplication` works as below:
  - **No replication config** or **not joined**: All PChannels are available.
  - **Joined replication**: Only PChannels listed in the current cluster's `ReplicateConfiguration.pchannels` are available.

## PChannel State Machine

```
UNINITIALIZED → ASSIGNING → ASSIGNED → UNAVAILABLE → ASSIGNING → ...
```

- **UNINITIALIZED**: Initial state for new PChannels (from config or dynamic `AddPChannels()`).
- **ASSIGNING**: Term incremented, persisted to catalog. Waiting for StreamingNode to confirm WAL open.
- **ASSIGNED**: StreamingNode confirmed. Operational.
- **UNAVAILABLE**: Node failure or Term mismatch. Queued for reassignment.

Assignment is two-phase: `AssignPChannels` (persist + increment Term) → `AssignPChannelsDone` (StreamingNode confirms). If the node fails during ASSIGNING, the PChannel stays in ASSIGNING and is re-assigned on the next rebalance cycle.

## Manual WAL migration

`TransferChannel` RPC and Coordinator `POST /management/streaming/transfer`
move whole PChannels through `Balancer.TransferChannels`. They do not consult
QueryCoord's legacy replica, target or delegator distribution. A VChannel name
selects its owning PChannel, including all other VChannels on that PChannel.
An omitted channel selects all PChannels currently owned by the source SN.
An omitted destination selects healthy, unfrozen SNs from the normal WAL
resource-group eligibility set, excluding the source. Explicit destinations
must satisfy the same eligibility checks. Copy mode is unsupported.

Selection and migration execute in the Balancer's serialized request loop, so
automatic balancing cannot change ownership between selection and assignment.
The operation validates the selected channels before persisting any intent,
then reuses normal assignment: persist ASSIGNING with a new term, remove old
WAL assignments, ask the target SN to open the WAL, and persist ASSIGNED.
Only completion of all selected assignments returns success. A canceled or
failed operation can have partial effects; persisted ASSIGNING state remains
recoverable by ordinary balancing. Cancellation while queued does not start a
migration. Manual placement remains available when optional rebalance is off;
it does not permanently pin the channels against later automatic balancing.

Success is a WAL assignment barrier, not a barrier for associated QueryViews
to become Up or for all in-flight queries to drain. No QueryView-ready wait is
part of this API.

The legacy Proxy REST `/management/querycoord/transfer/channel` is a deprecated
successful no-op: it always responds HTTP 200 with a `msg` stating that it is
deprecated, no migration was performed, and callers should use the Coordinator
Streaming endpoint. It neither parses migration parameters nor invokes the RPC.

## Streaming management REST

Coordinator `/management/streaming/*` reports and controls WAL state only. It
does not merge QueryNode delegator distribution or change QN scheduling state.

- `balance/status` GET reads
  `streaming.walBalancer.balancePolicy.allowRebalance`. PUT persists this setting
  through AlterConfig's etcd write and synchronous local-refresh path. The
  `UpdateWALBalancePolicy` RPC uses the same write path. Neither writes a temporary
  override; later AlterConfig writes and resets remain effective. Missing config
  storage or a failed write is an error, not a successful local-only change.
  Disabling optimization leaves new WAL assignment, failure recovery, frozen-node
  evacuation and manual transfer enabled. An already running balance round may
  finish with its previous configuration.
- `nodes/status` GET reports `active` or `suspended` for a registered SN. This is
  scheduling eligibility, not a health assessment. PUT freezes or unfreezes the
  SN through the WAL Balancer. A freeze excludes it from new placement and
  schedules its existing WALs for evacuation, but the response does not wait for
  evacuation. All requested node identities are validated before the mutation.
  Freeze state remains process-local and is removed when sessions disappear;
  it is not restored across Coordinator restart.
- `nodes` lists SN sessions, annotated with freeze state. No QN lookup or
  per-QN distribution RPC is involved.
- `nodes/distribution` combines SN identity with confirmed WAL assignments.
  A registered node with no WALs returns empty arrays; an unknown node returns
  HTTP 404. The same unknown-node distinction applies to node status. Discovery
  or service-readiness failures are errors rather than fabricated empty/active
  states. These reads are observation snapshots, not shutdown or drain barriers.

The redundant QueryCoord channel-balance methods and WAL management wrapper's
Suspend/Resume/IsSuspended methods are removed. Configuration is the sole source
of the WAL automatic-balance switch.

## Key Packages

- `internal/streamingcoord/server/balancer/` — `Balancer`, `ChannelManager`, `PChannelMeta`, balance policy
- `internal/streamingcoord/server/service/` — gRPC `AssignmentDiscover` server
- `internal/streamingcoord/client/` — Client-side assignment watcher
