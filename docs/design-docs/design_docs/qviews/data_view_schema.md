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

The production schema handoff is wired through the original ALTER callback:

```text
PrepareSchema
  → LoadConfigs.ApplySchema
  → DataCoord.PrepareDataViewSchema → DataViewManager.AdvanceSchema
  → WaitAtSchema (for loaded collections)
→ MetaTable.AlterCollection
→ ShardViewRegistry.PublishCollectionSchema
```

These operations distinguish four milestones:

| Milestone | Meaning and boundary |
| --- | --- |
| Target visible | After the Broadcast task is persisted, `GetPendingSchemaTarget()` exposes its immutable target, even before append or ACK completion. Known business-channel ACK TimeTicks may be incomplete or absent. This does not prove any channel has applied the schema. |
| Write schema effective | Each VChannel switches its write schema at its own WAL ALTER boundary. Consuming-side `AckSyncUp` also waits for pre-ALTER L1 final publication before the ordered callback can run. Neither event publishes the read schema. |
| Data preparation complete | In that callback, `PrepareSchema` applies target load intent and calls DataCoord's implemented `PrepareDataViewSchema`. `AdvanceSchema` certifies and publishes the supported DataView schema, directly or after materialization. Loaded collections must additionally reach exact-schema Up coverage through `WaitAtSchema`. |
| Read schema public | Only after preparation and any serving obligation complete does `MetaTable.AlterCollection` durably publish the read schema. `PublishCollectionSchema` then notifies View managers so superseded schema coverage can retire. |

Unloaded collections still require DataView certification, but have no serving
handoff to wait for. Bound index metadata is installed before `PrepareSchema`;
destructive compaction cleanup remains gated by direct schema publication.
W/A Transform coverage follows its own frontier contract and cannot be inferred
from schema readiness. See [schema handoff](schema_evolution.md) for callback
replay, public cache invalidation and retained read generations.

Implementation: `internal/views/coord/resource/schema.go`,
`internal/datacoord/dataview_schema.go`, `internal/dataview/schema.go`,
`internal/rootcoord/ddl_callbacks_alter_collection_properties.go`, and
`internal/streamingcoord/server/broadcaster/schema.go`.

Import/AlterSchema lifecycle exclusion remains deferred. In particular,
an import frozen under an older schema must not later publish data missing a
new Function output. These component tests do not establish that production
concurrency guarantee.
