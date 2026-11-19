# DataCoord segment scans

Related issue: #54143. Builds on the segment writer locks and MetaStore
publication in #54675.

A full scan currently assembles a DataCoord `SegmentInfo` wrapper for every
stored segment before filtering. Most consumers only need a count, a projection,
or a small subset. `SegmentsInfo.RangeReused` uses one temporary wrapper and
retains the existing MetaStore proto snapshot. Visitors and filters must not
retain that wrapper; `ShadowClone` creates an independent wrapper when needed.
The proto itself remains shared and immutable.

Selections copy matching wrappers only. Quota, collection row-count and segment
metrics scans consume values immediately. Dropped-segment GC retains dropped
segments, compaction edges and the output protos needed by its eligibility
checks. Its checkpoint, snapshot, retention and DataView protection rules stay
unchanged. An empty metrics result remains a non-nil slice.

## Consistency and scope

The underlying MetaStore APIs are unchanged. Reads of one collection see one
publication; a global scan takes one collection at a time and is not a globally
atomic snapshot. The visitor runs after the proto snapshot is collected and
holds no segment writer lock. Transient DataCoord state is sampled for each
candidate, as before. Reset the entire wrapper between candidates, including
when a proto has no transient state.

This changes in-memory read allocation only. It does not change persisted
metadata, WAL/CDC messages, quota calculation rules, or metric labels. It does
not include the incremental quota cache or parallel startup recovery.

## Validation

Regression coverage checks retained-wrapper independence, early termination,
collection/channel selection, transient-state reset, and concurrent publication
of several segments. Existing MetaStore atomicity, quota and dropped-segment GC
regressions exercise consumers. The million-segment benchmark compares full
wrapper materialization and the reused visitor in the same checkout; local
allocation measurements are not production latency estimates.
