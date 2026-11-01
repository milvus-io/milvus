# DataView schema certification

Each immutable DataView carries the read `SchemaVersion` supported by its
selected data. A QueryView copies this identity from the selected DataView.
The separately sampled authorized schema target controls planning admission
and recovery of retained published schemas; it never overrides a DataView's
schema. Version zero is a real schema version, not a request for latest.

`Manager.AdvanceSchema` publishes a new CompactVersion when the target schema
advances, even if membership and Manifest versions do not change. Replaying the
same or an older target is a no-op. Create supplies the initial schema; ordinary
data publications inherit the existing schema. Persistence precedes publication,
and old references retain their original schema, membership and statistics.

There are two transitions:

- With a nil projector, advance the logical schema over the existing physical
  snapshot. Ordinary NULL/default additions, removals and parameter changes
  can use this path when their read projection supports the transition.
- With a projector, wait until every segment in the current complete loadable
  projection supports the target schema, then publish that projection and schema
  atomically under the collection mutation lock. New Function output fields
  require materialization. Replacements transfer the obligation to their live
  successors; the waiter does not pin a fixed list of old segment IDs.

The projector must obey the existing DataView completeness and locking contract.
Waiters release the mutation lock before waiting. Recompute notifications wake
them even when membership is unchanged or the asynchronous queue is full.
Collection drop and caller cancellation terminate pending waits. Failed
persistence leaves the published snapshot unchanged and permits retry.

The native cache publication retains the same SchemaVersion as its DataView.
While the sampled authorized target differs, ordinary target planning waits for
a later publication. Recovery may reuse the latest QueryView's exact data and
load identities while target data is pending. It cannot select an older view
after a newer QueryView has been registered, even if the newer view fails.
Once a certified newer DataView is planned, its preparation supersedes any
older-schema Preparing/Ready view; every schema shares one preparation slot.

This extraction includes DataView publication, the native/proto contract,
Balancer consumption and the `PrepareDataViewSchema` dependency interface.
Production integration must supply initial/projection schema identities, order
target load intent before certification, use the consuming-side ALTER flush
fence, and keep destructive compaction cleanup behind direct schema publication.
RootCoord publication, Broadcaster wiring and the DataCoord implementation of
that dependency remain in the integration PR. W/A frontier production is also
separate; this change does not fabricate Transform coverage.

The parent still defers Import/AlterSchema lifecycle exclusion. In particular,
an import frozen under an older schema must not later publish data missing a
new Function output. These component tests do not establish that production
concurrency guarantee.
