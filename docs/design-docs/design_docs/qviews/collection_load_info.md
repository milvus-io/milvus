# QueryView collection load requirements

## Protocol consumed by the QN extraction

The parent qv implementation publishes `viewpb.CollectionLoadConfig` at
`coord/load_info/<collectionID>/<version>`. This child PR extracts the data
structures and exact-version reader; publication, version allocation, index DDL
callbacks, reference tracking, GC and Release orchestration remain in the parent.
The baseline coordinator implementation is preserved except for RPC type adapters.

Each immutable record contains CollectionID, DBID, Version, PartitionIDs,
RequiredFields, PreloadFields and replica configuration. The RPC exposes
PreloadFields as LoadFields. `viewpb.LoadFieldConfig`
contains `field_id` and repeated `index_ids`, allowing multiple JSON path indexes
on one field. WAL `messagespb.LoadFieldConfig` remains an independent intent type.

An empty index list means no selected index. `GetQueryViewLoadInfo` returns the
exact record and index definitions resolved for its selected IDs. It does not
substitute newer indexes on the same field. Missing records or selected metadata
return ServiceNotReady. The read-only adapter does not cache records beyond GC.
The producer must retain records through the referencing Views' lifetime.

Version zero is not a preparation identity. The child retains its baseline
current-load-state probe through CollectionManager; integration with the parent's
LoadConfigStore supplies current version selection. No legacy Load API publishes
new-format records in this child PR.

## Node-side ownership

QN binds collection metadata by `(CollectionID, SchemaVersion, LoadInfoVersion)`.
Bindings share the parsed native Schema by collection/schema identity, but keep
independent immutable index metadata. Segment preparation filters physical index
artifacts from GetQueryViewSegmentLoadInfo to the binding's selected IndexIDs.
Manifest and DataView continue to identify physical content independently.

SealedFieldLayout owns references to the immutable native Schema and IndexMeta,
plus a LoadScope value and derived lookup data. It does not deep-copy FieldMeta.
PublishedState owns its layout; a retained ReadState/base can therefore outlive
its original View and CCollection handle safely. Query operations still borrow
their own plan Schema and retain their View resources through result conversion.

## Deferred parent concerns

- R1: index-definition GC does not yet protect definitions referenced by retained
  LoadInfo/Views. Exact lookup can fail after physical definition deletion.
- R2: AlterIndex's unloaded check can race with Load before the collection lock.
  The references-only model assumes no live mutation of selected definitions.

Both are explicitly recorded TODOs, not fixes included in this extraction.
