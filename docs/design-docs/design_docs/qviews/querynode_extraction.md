# QueryNode QueryView extraction

The [resource preparation contract](qnview/querynode_queryview_resource_preparation.md)
is implemented by `querynodev2/{qnview,qvresource,transformlogbuffer,viewquery}`.
`distributed/querynode` registers ViewSyncService and ViewQueryService against the
same handler. Query handles retain their concrete physical generation and wait
for its applied Transform MVCC before execution.

## Metadata supply and production boundary

Remote TransformLog transport is a separate dependency. The QueryNode adapter
uses a WAL entry that exposes `TransformLogStreamManager`; without that
capability, it returns ServiceNotReady when acquiring a stream. This extraction
does not add the remote subscription RPC or its streaming client. The node-local
resource manager remains injectable for component validation and never bypasses
replay or the concrete resource instance's MVCC wait.

The accompanying metadata supply adds SegmentLoadInfo Watch and exact retained
LoadInfo lookup. `coord/qv-load-info/<collection>/<version>` holds immutable scope
snapshots; current indexes are resolved separately. A missing retained version
returns ServiceNotReady, never current collection metadata. The node also rejects
a response with a different collection/version identity.

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
