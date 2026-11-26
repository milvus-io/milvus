# Incremental quota calculation in MixCoord

Large catalogs can have many collections whose segment statistics and limits stay
unchanged between quota rounds. DataCoord caches absolute statistics per
collection. RootCoord uses those statistics and a small metadata snapshot to
recalculate affected collections. Versioned `SetRates` lets a Proxy keep its
current policy when it is up to date and receive a delta when its version
matches the coordinator's latest delta base.

## Configuration

All options under `quotaAndLimits` default to false.

| Option | Effect |
| --- | --- |
| `collectionStatsCacheEnabled` | Recompute statistics from segments of changed collections. |
| `localQuotaCalculationEnabled` | Read statistics in process and recalculate affected collections. Requires the statistics cache and an in-process MixCoord source. |
| `versionedRatesEnabled` | Startup setting for the coordinator and Proxies. Negotiate versioned snapshots and check their ordering. |
| `incrementalRatesEnabled` | Send the latest delta when a Proxy is at its base version. Requires versioned synchronization. |

The statistics cache can be enabled on its own. Disabling the cache restores
DataCoord's full scans. Disabling local calculation restores RootCoord's full
calculation and existing metrics-read path. Re-enabling starts from a full snapshot.

## DataCoord statistics

DataCoord invalidates a collection after a segment change is published to
MetaStore. Failed catalog writes leave the published data and statistics alone.
Moving a segment invalidates both collections. Collection metadata changes also
invalidate the cached display labels.

A refresh reads the collection's invalidation generation before copying its
segment pointers from MetaStore. A writer can publish a newer generation during
reduction. That generation remains pending for the next refresh. Compaction's
multi-segment publication and the pointer copy use the same collection shard.
The cache replaces each collection's absolute values and updates the cluster
byte total by the difference.

QuotaCenter reads a full baseline or pending absolute values. Acknowledgement
clears only generations that the consumer incorporated. A cache reset changes
the baseline generation so an older acknowledgement cannot clear the new
baseline. The full snapshot also removes data for collections no longer present.
Healthy segments of a dropped collection continue to count toward cluster disk
usage until DataCoord removes them. The cache publishes the existing collection
and database aggregate gauges, including L0 deltalog counts and per-level
quantiles in full metrics mode.

## RootCoord calculation

RootCoord projects available collection metadata and records changes from
successful publications, including partition and property changes and restore
completion. The calculation keeps separate lists for the complete partition
baseline and the available partitions used by node membership.

Each round recomputes cluster and database rules. Collections are reconsidered
when their metadata or statistics change, or when a protection changed a
collection or partition limiter in the previous round. Current protection
rules can also limit a previously unaffected collection. They record the
affected collection at the point of mutation. This lets a restriction lift
even when its collection has no new segment statistics. Configuration or
Proxy-count changes revisit the full collection baseline.

The calculation uses the existing rule order and rate-limiter tree. A candidate
with a changed configuration, metadata generation or Proxy count is retried.
Failed calculations retain dirty scopes for the next round. Versioned
reconciliation can use the last complete publication while calculations
are unavailable. With versioning disabled, RootCoord continues sending the
full tree through the existing `SetRates` RPC.

## Versioned SetRates

The coordinator uses a timestamp allocator epoch and a sequence number that
advances when policies or scope defaults change. The coordinator stores the
current policy and the latest delta. A changed default or a new epoch starts
a full baseline. A full tree is built on demand.

The coordinator queries Proxy capability and applied version through a
read-only metrics request. An older Proxy receives the legacy full tree
after successful capability discovery. For a versioned Proxy the applied
version determines what to send.

| Proxy state | Reconciliation |
| --- | --- |
| Already at the published version | Send no policy payload. |
| At the latest delta's base, with deltas enabled | Send complete policies for changed scopes and explicit deletions. |
| Restarted, missing changes, or needs a baseline | Send the current full policy. |
| Ahead of the publisher | Report the version conflict and keep the Proxy's policy. |

The receiver verifies the Proxy process identity and the exact delta base
version. It ignores duplicate and older updates and validates rates, state
arrays, and scope hierarchy before applying a candidate. A full snapshot
reuses existing token buckets where possible. A delta updates only the
scopes carried in the request. Admission remains concurrent with updates.

A lost reply is resolved by the Proxy's applied version in the next
reconciliation. A missed update is repaired with the latest applicable
delta or a full snapshot. A restart between sampling and applying returns
the new process identity and requests a full baseline.

Enable versioned full snapshots before enabling deltas. A version-enabled
Proxy returns a retriable error for quota-checked requests until it has a
baseline. After installing a versioned baseline, that Proxy ignores legacy
policy updates. Rolling back to a legacy coordinator requires restarting the
affected Proxy sessions. Measure the largest full snapshot during rollout,
since every restarted or lagging Proxy must be able to receive one.

Work still scales with the reported node membership. Rebuilding statistics
for an active collection reads that collection's segments. A complete
policy snapshot still traverses the policy tree when a Proxy needs it.
