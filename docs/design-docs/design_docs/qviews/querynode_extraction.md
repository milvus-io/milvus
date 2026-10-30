# QueryNode QueryView extraction

The [resource preparation contract](qnview/querynode_queryview_resource_preparation.md)
is implemented by `querynodev2/{qnview,qvresource,transformlogbuffer,viewquery}`.
`distributed/querynode` registers ViewSyncService and ViewQueryService against the
same handler. Query handles retain their concrete physical generation and wait
for its applied Transform MVCC before execution.

## Metadata supply and production boundary

Remote TransformLog transport is supplied by the base branch through
`streaming.WAL().TransformLogStreamManager()`. The QueryNode adapter returns
ServiceNotReady before WAL initialization. The node-local resource manager remains
injectable for component validation and never bypasses replay or the concrete
resource instance's MVCC wait. Physical subscription failures resume inside the
transport. Buffer observes the logical stream's Done/Error to propagate terminal
failure to its retained VChannels; Handler receives only Entry/SyncUp.

The accompanying metadata supply adds unary GetQueryViewSegmentLoadInfo and
exact retained collection LoadInfo lookup. Segment loading assumes StorageV3
with index artifacts published into manifests. New View requirements trigger
metadata acquisition; ready Views do not poll or subscribe to metadata changes.
Manifest publication retains its DataView recompute notification. `coord/qv-load-info/<collection>/<version>` holds immutable scope
snapshots. Nonzero versions use the shared `LoadInfoStore`: snapshots without
`required_fields` fail with DataIntegrity, and each read resolves current indexes
only for the retained required fields, including fields outside preload hints.
MixCoord forwards this response without appending collection-wide indexes. A
missing retained version returns ServiceNotReady, never current collection
metadata. The node also rejects a response with a different collection/version
identity.

The producer normalizes the public skip-dynamic-field option into explicit
`load_fields` preload hints. The response and QN runtime carry no separate flag:
empty hints follow the bound schema, and explicit hints select preloaded fields.
Dynamic fields remain in `required_fields` and retain query access independently
of preloading.

Version zero is a current-load-state probe. Until the Coordinator QueryView
runtime owns live load configuration, it reads the existing CollectionManager:
a loading collection counts as loaded configuration, while a released collection
returns CollectionNotLoaded even if retained snapshots remain. This probe does
not publish a historical scope or supply a resource-preparation identity. Once
integrated, the parent runtime resolves its current LoadConfigStore version
through the same LoadInfoStore. Nonzero historical reads remain independent of
current load state.

This extraction does not publish LoadInfo snapshots from legacy Load APIs,
generate QueryViews, generate DataView W/A or Segment replay cursors, switch Proxy
routing, or implement schema publication. A production producer must persist the
exact scope before publishing its View and provide the certified base cursor and
retained Transform range. The cursor field is transported without reconstructing
it from StartPosition/DeltaPosition. A missing/zero base cursor returns
ServiceNotReady rather than claiming safe replay. Those producer workflows are separate
changes. Explicit ViewSync/component tests do not prove automatic cluster-wide
query routing or complete production recovery.

## Native requirements

CCollection supplies an immutable Schema per collection/version. LoadScope is a
separate value. SealedFieldLayout retains only physical descriptors and mappings,
while ReadStates retain immutable columns/readers/index generations. Go View
operation references retain CCollection until synchronous CSegment operations
and their native children and result conversion complete. There is no additional
Collection Pin layer. Shared expression and scoring code use the plan schema.

Building the node requires the matching native implementation of the new create,
reopen, pin and adopt C APIs. Old installed headers/libraries cannot validate this
path. Native build artifacts must be supplied through the workspace's approved
trunk artifact binding.

## Query execution protocol alignment

The QN consumer follows the View-based schema contract of the query-execution
extraction (#54550): requests match the selected View's exact schema and retain
its resources through execution. Phase 2 does not require a SchemaBinding or a
Phase 1 field allowlist. Retained requery uses QueryOnView and may select additional
fields supported by the same View and LoadScope at the original MVCC. A newer WAL
write schema does not independently revoke the retained View's fields. This
supersedes the earlier field-removal/allowlist contract in the source snapshot's
schema_evolution.md section 6.2. Exact-instance Transform MVCC and poison checks
remain mandatory.

QueryViewOfPartition carries segment_manifest_versions at tag 4, aligned with the
Coordinator producer (#54547). The packed array follows segment_ids in the same
order. Neither the retired tag 5 nor the removed schema-binding protocol is a
released compatibility contract.

## Segment selection contract

The shared worker plan and Phase 2 requests carry the parent's SegmentSelection
contract. QN applies selection after partition filtering and before pinning
physical handles; exact-registration Transform waits and failure checks remain
unchanged. Hybrid selections retain subrequest positions, including empty All
and None slots. SN providers accept the shared parameter without applying sealed
statistics to growing resources. SN partition-statistics preparation, global
pruning and Proxy planning/requery orchestration remain separate integration work.
