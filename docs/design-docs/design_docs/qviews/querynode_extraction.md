# QueryNode QueryView integration

The [resource preparation contract](qnview/querynode_queryview_resource_preparation.md)
is implemented by `querynodev2/{qnview,qvresource,transformlogbuffer,viewquery}`,
including shared-layout ownership and the
[versioned CollectionLoadConfig contract](collection_load_info.md).
`distributed/querynode` registers ViewSyncService and ViewQueryService against the
same handler. Query handles retain their concrete physical generation and wait
for its applied Transform MVCC before execution.

## Metadata supply and production boundary

The Coordinator persists immutable LoadInfo versions containing partition
membership, required fields, preload hints, replica intent and exact IndexID
references. CreateIndex and DropIndex publish new versions for loaded
collections; AlterIndex has no hot-update promise. QueryViews retain their
versions through node cleanup and final catalog deletion. Recovery reconstructs
all View references before LoadInfo history GC is enabled. Release withdraws
intent, drains reconciliation and View removal, then deletes all versions.
Legacy metadata migration and complete physical-object retention remain separate
work; they do not make the implemented LoadInfo reference protocol pending.

Remote TransformLog transport is supplied by the base branch through
`streaming.WAL().TransformLogStreamManager()`. The QueryNode adapter returns
ServiceNotReady before WAL initialization. The node-local resource manager remains
injectable for component validation and never bypasses replay or the concrete
resource instance's MVCC wait. Physical subscription failures resume inside the
transport. Buffer observes the logical stream's Done/Error to propagate terminal
failure to its retained VChannels; Handler receives only Entry/SyncUp.

The accompanying metadata supply adds unary GetQueryViewSegmentLoadInfo and
exact retained collection LoadInfo lookup. StorageV3 loading uses committed
Manifests with published indexes. The StorageV2 fallback obtains a fresh full
loading snapshot per zero-Manifest binding before read reuse, refreshing it on
preparation failure. New View requirements trigger
metadata acquisition; ready Views do not poll or subscribe to metadata changes.
Manifest publication retains its DataView recompute notification.
`coord/load_info/<collection>/<version>` contains immutable CollectionLoadConfig
records. The metadata service supplies an exact-version adapter:
records without required fields fail with DataIntegrity, and each read resolves
only the recorded IndexIDs, including multiple indexes on one field and fields
outside preload hints. Missing selected index metadata returns ServiceNotReady.
MixCoord forwards this response without appending collection-wide indexes. A
missing retained version returns ServiceNotReady, never current collection
metadata. The node also rejects a response with a different collection/version
identity.

The producer normalizes the public skip-dynamic-field option into explicit
`load_fields` preload hints. The response and QN runtime carry no separate flag:
empty hints follow the bound schema, and explicit hints select preloaded fields.
Dynamic fields remain in `required_fields` and retain query access independently
of preloading.

Version zero is a current-load-state probe: the Coordinator resolves the current
LoadConfigStore identity, while a released collection returns
CollectionNotLoaded even if historical records have not yet drained. The probe
does not create a historical version or supply a resource-preparation identity.
Nonzero historical reads remain independent of current load state.

The production producer persists exact requirements before publishing a View,
uses the selected DataView's certified schema and W/A frontier, and supplies
Segment replay cursors with their concrete loading snapshot. QN fetches the
cursor atomically with the physical base/Manifest metadata, rather than
reconstructing it from StartPosition or DeltaPosition. A missing/zero cursor
returns ServiceNotReady instead of certifying unsafe replay. QueryCoord
placement, node preparation and Proxy routing are connected through the
QueryView lifecycle. Component tests still do not substitute for cluster-level
handoff and recovery validation.

## Native requirements

Collection bindings distinguish SchemaVersion and LoadInfoVersion. Native Schema
objects are shared by collection/schema identity; index metadata remains binding
specific. LoadScope is a separate value. SealedFieldLayout owns references to the
immutable native Schema and IndexMeta, with derived lookup data instead of copied
FieldMeta. PublishedStates/ReadStates retain the layout independently of the
creating View or CCollection shell. See [load requirements](collection_load_info.md).
ReadStates retain immutable columns/readers/index generations. Go View
operation references retain CCollection until synchronous CSegment operations
and their native children and result conversion complete. There is no additional
Collection Pin layer. Shared expression and scoring code use the plan schema.

SealedFieldLayout owns its immutable scope and shared Schema/index metadata.
Views, PublishedStates and retained reads can share the layout. Releasing the
CCollection handle drops its ownership; it does not destroy metadata still used
by a layout. Native borrowing is protected by the exact state/layout owner,
including asynchronous reopen and base adoption. Schema objects are not copied
per Segment. Physical index artifacts are filtered by the View selection before
preparation; Segment metadata cannot overwrite the logical binding.

Building the node requires matching native create/reopen/pin/adopt APIs and the
shared-Schema binding constructor. Old installed headers/libraries cannot
validate this path. See resource preparation sections 6.1–6.2 for lifecycle rules.

## Query execution protocol alignment

The QN consumer follows the View-based schema contract of the query-execution
extraction (#54550): requests match the selected View's exact schema and retain
its resources through execution. Phase 2 does not require a SchemaBinding or a
Phase 1 field allowlist. Retained requery uses QueryOnView and may select additional
fields supported by the same View and LoadScope at the original MVCC. A newer WAL
write schema does not independently revoke the retained View's fields. This follows the read-Schema publication boundary defined in
[schema evolution section 6.2](schema_evolution.md#62-dropped-field-validity),
without a separate per-field WAL removal fence. Exact-instance Transform MVCC and poison checks
remain mandatory.

The QN handler client uses fail-fast server-ID picking and disables configured
same-node gRPC retries. A missing-session error carries the shared resolver
revision notification captured before dispatch, allowing the shard client to
reconsider unavailable nodes after discovery changes without losing a racing
update. Closing the client joins its single session observer. Proxy's
QueryViewClient owns routing and replica reselection around this node client.

QueryViewOfPartition carries segment_manifest_versions at tag 4, aligned with the
Coordinator producer (#54547). The packed array follows segment_ids in the same
order. The requirements describe immutable physical bases; native preparation
and execution retain the exact selected generation.

## Segment selection contract

The shared worker plan and Phase 2 requests carry the parent's SegmentSelection
contract. QN applies selection after partition filtering and before pinning
physical handles; exact-registration Transform waits and failure checks remain
unchanged. Hybrid selections retain subrequest positions, including empty All
and None slots. SN providers accept the shared parameter without applying sealed
statistics to growing resources. SN prepares the View's versioned partition
statistics and applies global pruning during planning; Proxy dispatches the
resulting selections and retains the plan for requery. See
[segment pruning](query/segment_pruning.md).

## Node-local observability

The segment metrics endpoint includes physical bases owned by the QueryView
resource manager, independently of the legacy SegmentManager. Sampling pins
the existing resource/base operation references while reading native metadata;
it releases those references afterward so normal retirement can proceed.
Memory is counted once per physical base, while logical rows and repeated index
metadata are deduplicated per segment. View acquisition, readiness, coordinator
state application, failure, reporting and release emit the shared QueryView
observation events.
