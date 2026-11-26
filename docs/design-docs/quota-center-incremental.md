# Incremental quota calculation in MixCoord

Large catalogs can have many collections whose segment statistics and limits stay
unchanged between quota rounds. DataCoord caches absolute statistics per
collection. RootCoord uses those statistics and a small metadata snapshot to
recalculate affected collections.

## Configuration

Both options under `quotaAndLimits` default to false.

| Option | Effect |
| --- | --- |
| `collectionStatsCacheEnabled` | Recompute statistics from segments of changed collections. |
| `localQuotaCalculationEnabled` | Read statistics in process and recalculate affected collections. Requires the statistics cache and an in-process MixCoord source. |

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
Failed calculations retain dirty scopes for the next round. Proxy receives
the full tree through the existing `SetRates` RPC. After a transport failure
the next round sends the full tree again.

Work still scales with the reported node membership and the complete policy
tree sent to Proxy. On an active collection, rebuilding statistics still
reads that collection's segments.
