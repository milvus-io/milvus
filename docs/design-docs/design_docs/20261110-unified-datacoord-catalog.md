# Unified DataCoord Catalog: Model Types, Coordinates and Generic Primitives

Current state: Draft (POC on branch `feat/unified-catalog-poc`)

ISSUE: TBD

Keywords: metastore, catalog, GenericCatalog, DataCoordCatalog, UpdateAction, ModelType, coordinate, prefix, namespace, watch, catalog service

Related: [MixCoord Metadata Roadmap](20261020-mixcoord-meta-roadmap.md) (M2, load peripheral metadata on demand)

Released: TBD

## Summary

DataCoord persists about 25 metadata types through `DataCoordCatalog`, a
68-method interface in which almost every type has its own Save, List and
Drop method, its own key format and its own encoding. This design splits the
catalog into two layers:

- **metastore** (business): a registry of **model types**. Each migrated
  type registers one `*ModelType` in one file: its root
  (`datacoord-meta/segment-change-group`), its depth, the Go type of its
  values (the existing struct, such as `*model.SegmentChangeGroup` or
  `*indexpb.AnalyzeTask`), how to read a value's **coordinate** (its
  identity components, such as `{collectionID, groupID}`), and its codec.
- **catalog** (generic, `internal/metastore/kv/datacoord` today): the four
  primitives of `GenericCatalog`, `Update`, `Get`, `List` and `Watch`,
  written once against `ModelType`. The per-type Save/List/Drop methods go
  away.

Records form a tree: a record sits at `Root/c1/.../cn`, and the catalog
builds every key from the type and a coordinate. Writes pass the value
itself (`UpdateAction{Type, Entry: g}`); the catalog finds its type by its
Go type. Reads name the type and a coordinate (`Get`) or a leading prefix of
one (`List`, `Watch`). A delete names one record by a value; there is no
prefix delete. No primitive takes or returns
a key or raw bytes. Adding a type means one registration in metastore;
neither the interface nor the catalog changes.

The catalog calls a catalog server: a dedicated service, backed by KV
storage, that provides ACL, per-cluster namespaces, transactions and change
notification. This document covers only the client side and treats the
server as given. Until it exists, today's etcd/TiKV `MetaKv` stands in for
it, so keys and stored bytes stay identical and no data migration is needed.

## Motivation

### Metadata IO is scattered

- `DataCoordCatalog` (`internal/metastore/datacoord_catalog.go`) has 68
  methods. `internal/metastore/kv/datacoord` implements them in about 2,300
  lines (`kv_catalog.go` 1,519, `update.go` 358, `util.go` 448).
- Thirteen packages hold a `DataCoordCatalog`: `datacoord`, `analyze`,
  `bulkimport`, `compaction/targets`, `compaction/task`, `copysegment`,
  `externalrefresh`, `index`, `meta`, `partitionstats`, `snapshot`, `stats`
  and `metacache`.
- Each type repeats the same code: build a key with `fmt.Sprintf`,
  `proto.Marshal`, `MetaKv.Save`; `WalkWithPrefix` plus `proto.Unmarshal`;
  `MetaKv.Remove`. Import jobs, preimport tasks (v2, v3), import tasks (v2,
  v3), reshard tasks, copy-segment jobs and tasks, analyze, stats,
  external-refresh jobs and tasks, snapshots and export-snapshot jobs all
  follow this pattern.
- The same encoding is written twice for types that also appear in
  `Update` (`datacoord/update.go` has comments such as "Same encoding as
  catalog.SaveExternalCollectionRefreshTask").
- Some IO bypasses the catalog: the global ID allocator and the pk-index
  `metaKVStore` (`pkindex_service.go`) use `s.kv` directly. DataCoord builds
  a StreamingNode catalog on the same KV to read StreamingNode data.
  `CleanMeta` calls `RemoveWithPrefix(ctx, "")`.

### Behaviour differs per type for no stated reason

- Batch limits: `AlterIndexes` chunks at a hard-coded 64 without
  atomicity. `AlterSegmentIndexes` and `SaveCopySegmentTasksBatch` issue
  one unbounded `MultiSave`. `SaveChannelCheckpoints` uses `SaveByBatch`.
  `Update` uses `txn.Commit` (atomic if the batch fits the store's txn limit,
  ordered chunks otherwise).
- Corrupt values: DataView recovery skips them, almost every other `List`
  fails closed.
- Encodings: mostly proto. Segment change groups are JSON. The current
  partition-stats version is a decimal string. The channel-removal flag is a
  plain string.

### A catalog service needs one surface

A catalog service shared by several clusters needs a small, uniform
client surface. That surface must name every type it stores, so the
service can apply namespaces and ACLs per type. Today the service would
have to understand 68 methods.

M2 of the MixCoord metadata roadmap (load finished compaction tasks,
import jobs and similar on demand) needs filtered lists and point reads per
type. The read primitives below provide them.

### Change notification is hand-built on etcd

No catalog offers a watch today. Components that follow metadata changes
open an etcd watch themselves. The CDC controller
(`internal/cdc/controller/controller.go`) reads the StreamingCoord
replicate-pchannel keys directly from etcd. It lists them, takes the list
revision, watches from revision + 1 with previous values, and lists again
after any watch error. `kv.WatchKV` exposes the same capability, but it
returns etcd's `clientv3.WatchChan`, and the TiKV `MetaKv` does not
implement it. A catalog service needs a watch primitive that does not
depend on the storage underneath, so callers stop opening raw etcd
watches.

## Goals

1. Business code writes `UpdateAction`s and reads values through `Get`,
   `List` and `Watch`; it never handles keys or encoded bytes.
2. Every record is named by its type and its coordinate. A prefix is a
   leading part of a coordinate; a zero ID is an ordinary component, never
   a wildcard.
3. Each type declares its layout and codec in one registration, checked at
   init; the catalog holds no per-type code.
4. `DataCoordCatalog` embeds `GenericCatalog`: Update, Get, List, Watch.
   There is no raw-bytes primitive and no per-type method.
5. A delete removes one record; there is no subtree delete.
6. Keys and value bytes stay identical for every migrated type.
7. The catalog client is bound to one cluster namespace and can never
   address another cluster's keys. In the POC this holds by convention: the
   generic primitives build only keys relative to MetaKv's root, but the
   typed methods, `CleanMeta` and the ID allocator still reach MetaKv
   directly.
8. Migration is incremental: one type at a time; each step changes only that
   type's callers (and their mock expectations).

## Non-goals

- The catalog server: its storage, ACL enforcement, transactions,
  deployment and RPC protocol. This design states only what the client
  expects from it.
- Catalogs other than `DataCoordCatalog` (RootCoord, QueryCoord, the
  QueryView catalogs, StreamingCoord, StreamingNode).
- The hard cases listed in [Future Work](#future-work).
- Changing what DataCoord keeps in memory. That is M2.

## Design

### Layering

```text
 DataCoord packages (meta, bulkimport, compaction, index, ...)
        │  catalog.Update(metastore.SaveX(v)), catalog.Get(ctx, metastore.XType, coordinate),
        │  catalog.List(ctx, metastore.XType, coordinate, metastore.ListOptions{Filter: ...})
        ▼
 internal/metastore            (business, no generics)
   model_type.go      ModelType, the registry (RegisterModelType,
                      ModelTypeOf, Registered)
   *_model.go         one file per migrated type: its registered
                      *ModelType and its action constructors
   generic_catalog.go GenericCatalog (Update, Get, List, Watch),
                      ListOptions, ListResult, WatchOptions, EventBatch
   update_action.go   UpdateAction; typed entries not migrated yet
   datacoord_catalog.go  DataCoordCatalog = GenericCatalog + not-yet-migrated methods
        │
        ▼
 internal/metastore/kv/datacoord   (catalog: generic)
   model.go           keys from coordinates and their checks; apply of
                      registered values; Get, List, decode with the
                      coordinate check
   update.go          registered values to the generic path, then
                      the not-yet-migrated typed cases
   generic_catalog.go Watch over MetaKv; backend capabilities
        │
        ▼
 MetaKv (etcd / TiKV) today; catalog server later
```

### Model types and coordinates

```go
// internal/metastore/model_type.go
type ModelType struct {
    Root       string                      // "datacoord-meta/segment-change-group"
    Depth      int                         // components of a coordinate
    GoType     reflect.Type                // reflect.TypeOf((*model.SegmentChangeGroup)(nil))
    Coordinate func(v any) []string        // identity fields only, no encoding
    Marshal    func(v any) ([]byte, error)
    Unmarshal  func(value []byte) (any, error)
}

func RegisterModelType(t ModelType) *ModelType
```

Records form a tree. A record of a type sits at `Root/c1/.../cn`, where
`c1...cn` is its coordinate, outermost first, and `n` is `Depth`. Every
record of a type has exactly `Depth` components; the levels above are
directories and hold no value. A **prefix** is the first `k < Depth`
components. Selecting a later component without an earlier one
(`Root/a/*/c`) is not a prefix: list the common prefix (`Root/a`) and
narrow it with a filter.

| Coordinate | Names | Valid for |
|---|---|---|
| `{"10", "7"}` (Depth components) | one record, `Root/10/7` | `Get` |
| `{"10"}` (fewer) | every record under `Root/10/` | `List`, `Watch` |
| `{}` | the whole type | `List`, `Watch` |

A coordinate is strings so that string identities (a vchannel name, a
compaction type) fit as well as IDs. The catalog rejects an empty component
and one containing "/". An ID of 0 is the component `"0"`: no field value
widens a write or a read.

Each migrated type registers one `*ModelType` in one metastore file. The
returned pointer is the type's handle in every catalog call; values are the
type's existing Go structs, unchanged:

```go
// metastore/segment_change_group_model.go
var SegmentGroupType = RegisterModelType(ModelType{
    Root:   "datacoord-meta/segment-change-group", // unchanged layout
    Depth:  2,                                     // {collectionID, groupID}
    GoType: reflect.TypeOf((*model.SegmentChangeGroup)(nil)),
    Coordinate: func(v any) []string {
        g := v.(*model.SegmentChangeGroup)
        return []string{strconv.FormatInt(g.CollectionID, 10), strconv.FormatInt(g.GroupID, 10)}
    },
    Marshal:   ..., // model.MarshalSegmentChangeGroup
    Unmarshal: ..., // model.UnmarshalSegmentChangeGroup: validates, fails closed
})
```

`RegisterModelType` runs from package variable initializers and panics on:

- an incomplete type, or a depth below 1;
- a malformed root: empty, absolute, ending with "/", or with an empty
  component;
- a Go type registered twice, or one that is not a pointer;
- a root that overlaps a registered one. Roots are compared as directories,
  so `datacoord-meta/s` does not overlap `datacoord-meta/stats-task`, nor
  `import-task` `import-task-v3`;
- functions that do not handle the Go type: it runs `Coordinate` and
  `Marshal` on an empty value of `GoType` and checks the coordinate's
  depth. The functions take `any`, so the compiler cannot check them; this
  moves a mismatched type assertion from the first write to init.

The catalog accepts only a `*ModelType` that `RegisterModelType` returned
(`Registered`), so an unchecked layout never reaches the store.

Every decoded record is checked against its key: after `Unmarshal`, the
value's Go type must be `GoType` and its coordinate must equal the one in
the key, or the read fails (`merr.ErrDataIntegrity` for a coordinate
mismatch). A value written under the wrong key therefore fails closed
instead of being served under a foreign identity.

Cost: the registry is two lookups per action (`reflect.TypeOf` and a map
read). The functions are stored once per type, never per value.

### Typed writes through Update

Every write is an `UpdateAction`. `Entry` is one of:

- a value of a registered type: `ActionAdd` and `ActionUpdate` store it at
  its coordinate (both replace the record), `ActionDelete` removes that one
  record;
- a typed entry declared in `update_action.go`, for the types not migrated
  yet.

The catalog applies all actions of one `Update` call as one write: atomic
when the op count fits the store's txn limit, otherwise ordered chunks in
the caller's order (`txn.Commit`), except that the commit-marked ops of
the types not migrated yet land in one final txn after the others. A
migrated type has no commit markers: a write that must land last goes in
a later `Update`; see [Commit markers](#commit-markers). A single-record
write is an `Update` with one action; there is no separate Put or Delete
primitive.

Each type keeps small action constructors next to its registration
(`SaveSegmentChangeGroup(g)`, `DeleteSegmentChangeGroup(c, id)`,
`DropAnalyzeTask(id)`), which build the `UpdateAction`. Callers keep
inspecting and rewriting typed records before writing them (`datacoord/meta`
asserts `action.Entry.(*model.SegmentChangeGroup)` for validation, merging a
transition onto the stored record, ordering alive groups before members and
terminal groups after them, and memory apply). Encoding happens only when
the catalog applies the action.

### No prefix delete

A delete always names one record: `DeleteSegmentChangeGroup(10, 0)` removes
`Root/10/0`, never the collection. There is no prefix (subtree) delete:

- Its only use is cascading cleanup after the parent is dead (collection
  drop), where no writer races the delete, so deleting record by record
  loses nothing but round trips.
- A store has no revision for a directory, so a prefix delete cannot be
  guarded by a read revision (`UnchangedSince`): neither etcd nor TiKV can
  tell, in the same txn, whether a record under the prefix was created or
  deleted after the read.
- A txn cannot mix exact and prefix deletes on etcd; without prefix deletes
  the restriction is gone.

Collection drop lists the collection's groups and deletes them in one
`Update` (`Meta.DropSegmentChangeGroupsOfCollection`). It lists the catalog,
not memory, so it also removes a group that only the catalog holds (a write
that timed out but landed). The list is keys only (`ListOptions.KeysOnly`):
it does not decode values, so a corrupt record is deleted like the old
prefix delete did, instead of blocking the cleanup. Over the txn limit the
deletes take the ordered chunked fallback; GC retries until they are all
gone.

### Get and List

```go
// internal/metastore/generic_catalog.go
type ListOptions struct {
    Filter   func(v any) bool // returned when it matches; nil matches all
    Limit    int              // matches per page; 0 returns them all
    After    string           // continue after this cursor (the previous page's Next)
    Revision Revision         // read at this revision (the first page's); 0 the latest
}

type ListResult struct {
    Values   []any // values of the type's GoType
    Revision Revision
    Next     string // After of the next page; "" when the list is complete
}

// DataCoord recovery (meta.loadSegmentChangeGroups)
listed, err := m.Catalog.List(ctx, metastore.SegmentGroupType, nil, metastore.ListOptions{})
group := listed.Values[0].(*model.SegmentChangeGroup)

// One collection's groups, paged
opts := metastore.ListOptions{Limit: 1000}
for {
    page, err := catalog.List(ctx, metastore.SegmentGroupType, []string{"10"}, opts)
    // errors.Is(err, kv.ErrCompacted): start over
    // ... use page.Values
    if page.Next == "" {
        break
    }
    opts.After, opts.Revision = page.Next, page.Revision
}

// A point read
v, err := catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "7"})
// errors.Is(err, merr.ErrIoKeyNotFound) when missing
```

- `Get` decodes the record at a full coordinate into a new value. A missing
  record is `merr.ErrIoKeyNotFound` (what `MetaKv.Load` returns). A prefix
  is rejected.
- `List` walks the records under a prefix in key order, decodes every
  record and returns those `Filter` matches. A record that fails to decode
  stops the list with an error naming its key, even when the filter would
  have skipped it, which keeps segment change group recovery fail closed. A
  full coordinate is rejected (use `Get`).
- Every read uses the existing `metastore.paginationSize` (default 100000,
  0 for no limit), as every other catalog walk does. With `Limit` 0, `List`
  reads page after page into one result. With a positive `Limit` it returns
  one page and the cursor of the next, so a large type never has to fit in
  one result; without a filter, a page reads at most one record past its
  last match to know whether another page exists (with a filter it reads
  `paginationSize` records, since the filter can reject any number). With a filter, `Next` can be set when no
  match remains; the next page is then empty. The cursor is opaque: pass it
  back unchanged.
- Revisions: on etcd every read of one `List` is pinned to the first read's
  revision, which `ListResult.Revision` returns; a paged list passes it to
  every later page, so all pages read one snapshot, and a `Watch` with
  `AfterRevision` set to it misses no change and repeats none. If etcd
  compacts that revision before the last read, a `List` of every match
  that pinned no revision restarts on the latest snapshot, discarding the
  records of the failed walk; after three attempts it walks without a
  snapshot and returns revision 0, as recovery reloads did before, rather
  than fail. A paged or pinned `List` fails with an error matching
  `kv.ErrCompacted` (`pkg/kv`) instead, since only its caller can start
  over. Other backends walk without a snapshot, page by skipping to the
  cursor, and return revision 0; a `List` pinned to a revision fails there
  with `ErrServiceUnimplemented`.
- `KeysOnly` returns the coordinates of the records
  (`ListResult.Coordinates`) without decoding their values, for cleanup
  that must not fail on a corrupt value. A key that is not a record of the
  type's depth still fails the list; `KeysOnly` cannot be combined with a
  filter.
- The filter runs in the client today. To run it in the catalog server it
  must become serializable (an expression, not a Go closure).

### Generic primitives

```go
type GenericCatalog interface {
    Update(ctx context.Context, actions ...UpdateAction) error
    Get(ctx context.Context, t *ModelType, coordinate []string) (any, Revision, error)
    List(ctx context.Context, t *ModelType, coordinate []string, opts ListOptions) (ListResult, error)
    Watch(ctx context.Context, t *ModelType, coordinate []string, opts WatchOptions, fn func(EventBatch) error) error
}

type DataCoordCatalog interface {
    GenericCatalog
    // ... typed methods, migrated type by type
}
```

Go interface methods cannot have type parameters, so reads return `any`
values of the type's `GoType`, which callers assert. A generic
`ModelType[T]` was tried and dropped: the registry must hold every type in
one map, so it needed a second, erased descriptor, which cost more than the
assertions it saved.

### Watch

```go
type WatchOptions struct {
    AfterRevision Revision // changes with a revision > AfterRevision; 0 = from now
}

type EventBatch struct { Revision Revision; Events []Event } // one committed write
type Event struct {
    Type       EventType // EventPut or EventDelete
    Coordinate []string  // the record's coordinate, set for both types
    Value      any       // new value; nil for EventDelete
}

```

`Watch` blocks and calls `fn` once per batch. It returns nil when ctx is
done, the error `fn` returns (which stops the watch), an error matching
`kv.ErrCompacted`, or any other watch failure. A caller that wants the watch in
the background runs it in its own goroutine; a slow `fn` holds back the
watch, which is the backpressure. There is no handle to close and no state
to read after the watch ends.

Semantics:
- **Typed.** The scope is a type and a prefix, not a raw key; events carry
  decoded values. A record that fails to decode, a put whose coordinate
  disagrees with its key, or an event on a key that is not a record of the
  type's depth ends the watch with an error (fail closed): a watcher that
  skipped it would silently diverge from the store.
- **Order.** Batches arrive in revision order.
- **Gap-free.** A watch from a `List` revision, or resumed from the
  `Revision` of the last delivered batch, misses no change and repeats none.
  The etcd client re-establishes a broken watch stream from the last
  delivered revision by itself. Only compaction forces a new `List`,
  reported as an error matching `kv.ErrCompacted`.
- **Atomicity is visible.** An atomic `Update` is one batch. A chunked
  `Update` is several batches, in the caller's order.
- **Deletes** carry the coordinate and no previous value: a watcher that
  needs the deleted record keeps its own view from the `List` and earlier
  events. A delete-range issued outside the catalog (`CleanMeta`) arrives as
  one delete event per removed record.

On etcd the catalog maps `Watch` to an etcd prefix watch of the prefix key
from a revision (`WatchWithPrefixFromRevision`). TiKV's `MetaKv` has no
watch, so `Watch` returns `ErrServiceUnimplemented` there.

### Backend capabilities

Snapshot lists and watches need methods `kv.MetaKv` does not declare. The
etcd `MetaKv` provides them; the catalog finds them by type assertion:

| Capability | etcd | TiKV and others |
|---|---|---|
| `WalkPrefixPage` (one page after a cursor, at a revision) | yes | `List` falls back to `WalkWithPrefix`, skips to the cursor, revision 0 |
| `WatchWithPrefixFromRevision` | yes | `Watch` is unimplemented |

MixCoord passes the raw etcd `MetaKv` to the DataCoord catalog, so the
assertions succeed in production on etcd.

### Namespace

The catalog server serves several clusters, one namespace each. The client is
bound to its namespace at construction and uses only relative keys:

- With the `MetaKv` stand-in, the namespace is the meta root path
  (`EtcdCfg.MetaRootPath` / `TiKVCfg.MetaRootPath`). `MetaKv` prefixes every
  key with it, so stored keys do not change.
- No primitive accepts a raw key or prefix: the catalog builds every key from
  a registered type's root and a checked coordinate.
- `CleanMeta` (which deletes the whole root today) stays outside the catalog
  until the server offers a namespace-level reset.

### What the client expects from the server

The catalog server is a dedicated service in front of KV storage, not the KV
store itself: per-type ACL, namespace mapping, type-aware validation and
watch fan-out across clusters are behaviours the service must control. Its
design is out of scope; its capabilities are stated as assumptions.

| Capability | Used by |
|---|---|
| Put / delete of exact keys in one txn, atomic up to a reported op limit | `Update` |
| Ordered, non-atomic chunked application beyond the limit | `Update` fallback |
| Point read | `Get` |
| Ordered prefix pages after a cursor at one revision (later: with a server-side filter) | `List` |
| A per-namespace revision advanced by every committed write | `List`, `Watch` |
| Prefix watch after a revision, with compaction reporting | `Watch` |
| Namespace bound per client; per-type (per-prefix) ACL | all |

### Errors

- A marshal error, an unregistered type, a nil value, a coordinate of the
  wrong length or with a malformed component, and an unsupported action
  type are programming
  errors: `merr.WrapErrServiceInternalMsg`, before any IO.
- Decode errors are wrapped with the type's root and the key
  (`merr.Wrapf`), so an operator can find a corrupt record. A record whose
  decoded coordinate differs from its key is `merr.ErrDataIntegrity`.
- `Get` of a missing record is `merr.ErrIoKeyNotFound`.
- A missing backend capability is `merr.Wrap(merr.ErrServiceUnimplemented, ...)`.
- Server errors pass through unchanged (`merr.Wrap` adds context only), so
  their codes and retriability are preserved.

## Migration

Each step keeps keys and values byte-identical. For each type: register its
`ModelType` in one metastore file with its action constructors, move its
writes to `Update` actions and its reads to `Get`/`List`, move its callers
and their mock expectations (`List` instead of `ListX`, `Update` instead of
`DropX`), and delete the old methods.

1. Framework and the first types (done in the POC): the registry, the four
   primitives, and two types:
   - Segment change groups (`{coll}/{groupID}`, JSON):
     `ListSegmentChangeGroups` and `DropSegmentChangeGroups` are removed.
   - Analyze tasks (`{taskID}`, proto): `ListAnalyzeTasks` and
     `SaveAnalyzeTask` are removed, and the typed analyze task delete
     case leaves `update.go`. The partition-stats cleanup still composes the
     analyze task delete with the not yet migrated partition-stats actions
     in one `Update`, with the partition-stats info removal as the commit
     marker.
2. The remaining types with the plain pattern. Every DataCoord type was
   checked: under each root all records have one depth, and no level above
   the last holds a value.

   | Type | Root | Coordinate |
   |---|---|---|
   | import job, preimport task (v2, v3), import task (v2, v3), reshard task | `datacoord-meta/{import-job,preimport-task,preimport-v3-task,import-task,import-task-v3,reshard-task}` | `{id}` |
   | copy-segment job, task | `datacoord-meta/copy-segment-{job,task}` | `{id}` |
   | compaction task | `datacoord-meta/compaction-task` | `{type}/{triggerID}/{planID}` (type is a string) |
   | compaction target | `datacoord-meta/compaction-target` | `{targetID}` |
   | stats task | `datacoord-meta/stats-task` | `{taskID}` |
   | partition stats info | `datacoord-meta/partition-stats` | `{coll}/{part}/{vchannel}/{version}` |
   | external refresh job, task | `datacoord-meta/external-collection-refresh-{job,task}` | `{id}` |
   | snapshot | `datacoord-meta/snapshot` | `{coll}/{snapshotID}` |
   | export snapshot job | `datacoord-meta/export-snapshot-job` | `{jobID}` |
   | index | `field-index` | `{coll}/{indexID}` (value via `model.MarshalIndexModel`) |
   | segment index | `segment-index` | `{coll}/{part}/{seg}/{buildID}` |
   | DataView | `coord/dv` | `{coll}/"versions"/{streaming}/{compact}`, a fixed component (`corruptSkip`) |

   A vchannel name contains no "/".
3. Types with codec quirks, handled in the type's functions: channel
   checkpoint and channel removal flag (`{vchannel}`; the flag is a plain
   string), current partition-stats version (`{coll}/{part}/{vchannel}`, a
   decimal string).

### Behaviour changes to decide per type

- Decoding checks that a record's coordinate matches its key
  (`ErrDataIntegrity`); the typed list methods did not.
- A type migrates with the `Update` semantics: ActionAdd and ActionUpdate
  both replace the record, and it drops its commit markers. Analyze tasks
  had none. Segment change groups had them on terminal saves and deletes;
  their publication now writes those in a second `Update`; see the next
  section.

### Commit markers

`txn.Commit` keeps its commit markers for the types not migrated yet: over
the txn limit it flushes the ops without a marker in the recorded order,
then applies every commit-marked op in one final txn.

A migrated type has no markers. A write that must become visible last goes
in a later `Update` than the writes it depends on. Segment change group
publication (`applySegmentChangesLocked`) issues two:

1. Alive groups, member and superseded segments, partition stats and the
   DataView (whose own markers still land last within this `Update`).
2. Terminal groups (COMMITTED/FAILED/ABORTED) and group deletes.

Memory follows each `Update` as it lands, so a failure of the second one
(the retry ends only with its ctx) leaves memory where the catalog is. A
crash, or such a failure, between them leaves an alive group whose members
and DataView landed. Recovery already completes that state: a READY group with visible
members is an interrupted publish, and DataViews are recomputed for
collections with groups. The cost is a second round trip, and the gap
between the two now exists for every publication, not only for one over
the txn limit.

Dropping markers for every type is the next step: the chunked fallback
follows the caller's order, and a caller that needs its last writes atomic
issues them as a final `Update` that must not be chunked. Flush is the
hard case: its DataView and its publishing segment records must stay in
one txn, after the segment binlogs, so `SegmentEntry` must split into its
binlogs and its record.

### Conditional writes

An `Update` with `UnchangedSince(R)` is a read-modify-write on records read
at revision `R` (from `Get` or `List`). It applies only when no record it
writes changed after `R`; a value saved with `ActionUpdate` must also still
exist, so a record deleted after the read is not written back. Otherwise it
fails with an error matching `predicates.ErrNotMet` and writes nothing. It
is always one txn: it never takes the chunked fallback. The guard is an
action, `UnchangedSince(R)`, whose entry is a `Precondition`; an `Update`
takes at most one, with a positive revision. On a backend that reports no
revision (`Get` returns 0) it fails with `ErrServiceUnimplemented`.

The existence check makes a guarded create impossible through
`ActionUpdate`: the migrated save constructors (`SaveSegmentChangeGroup`,
`SaveAnalyzeTask`) use `ActionUpdate`, so a guarded write through them must
target an existing record. A guarded create needs an `ActionAdd`
constructor; none has a caller yet.

The check runs in the store, in the same txn as the writes, so nothing can
change between the check and the write. A revision is a point on the
store's commit order, so a key's last change (etcd `mod_revision`) compares
with `R`; there is no per-key counter. Records read but not written are not
guarded.

| Predicate | etcd | TiKV |
|---|---|---|
| `ValueEqual(k, v)` | `Value(k) = v` | read in the txn; a lock on k when the call does not write it |
| `KeyExist(k)` / `KeyNotExist(k)` | `CreateRevision(k) > 0` / `= 0` | read in the txn; a lock on k when the call does not write it |
| `UnchangedSince(k, R)` | `ModRevision(k) < R+1` | not yet: needs the TiKV KV to report a TSO as the revision of `Get` and `List`, then a txn started at `R`. Without a revision the catalog rejects the guard with `ErrServiceUnimplemented`; the TiKV KV rejects the predicate itself the same way |

A predicate target carries its condition (`PredTargetValueEqual`,
`PredTargetKeyExist`, ...); there is no separate predicate type.

- Batches that issue one unbounded `MultiSave` today
  (`SaveCopySegmentTasksBatch`, `AlterSegmentIndexes`) become atomic-if-fits,
  chunked otherwise. On etcd, a batch over the txn op limit fails today and
  succeeds after (as ordered, non-atomic chunks).
- `AlterIndexes` (non-atomic chunks of 64 today) becomes atomic when it fits.
- Prefix drops (`RemoveWithPrefix`, `MultiSaveAndRemoveWithPrefix`) become a
  `List` and one delete per record. Segment change groups are few per
  collection; a type with large subtrees (DataViews, segment binlogs) pays
  more round trips and, over the txn limit, a chunked delete, and needs a
  decision when it migrates.

### Compatibility

- No key or value format changes, so rolling upgrade and downgrade are
  unaffected.
- No new metadata and no WAL change, so CDC replication is unaffected. The
  catalog stays local to each cluster.
- The `DataCoordCatalog` mock is regenerated whenever the interface changes.
  In the POC its `Get`, `List` and `Watch` were edited by hand in mockery's
  layout: the checked-in `bin/mockery` (v2.53.3) fails to load packages with
  the current Go toolchain.

## Testing

In the POC (type-checked, not yet run):

- `kv/txn/commit_test.go`: a guarded builder emits one `UnchangedSince` per
  written key and the required predicates, and never takes the chunked
  fallback (ops or predicates over the limit).
- `metastore/model_type_test.go`: lookup by Go type, a copied `ModelType` is
  not registered, and every registration that panics (incomplete, malformed
  roots, zero depth, duplicate or non-pointer Go type, functions of another
  Go type, a coordinate of the wrong depth, overlapping roots).
- `kv/datacoord/generic_catalog_test.go`:
  - Byte identity: keys, JSON values and the exact KV call sequence of a
    segment change group `Update` on the atomic path and the chunked
    fallback (in the caller's order), and the recovery prefix, pinned as
    golden values.
  - Rejected writes before any IO: a nil value, an unknown action type, and
    a value of an unregistered Go type.
  - Deletes: a zero group ID removes one record; deletes apply in order on
    the chunked fallback. The "/" boundary between collections 1, 10 and 100
    on `List`. A keys-only `List` lists a corrupt value, fails on a key that
    is not a record, and rejects a filter.
  - `UnchangedSince`: one guard per written key plus the existence check of
    an `ActionUpdate` save, an unmet guard as `predicates.ErrNotMet`, two
    guards, a zero revision and an over-limit guarded write rejected without
    a write, and `ErrServiceUnimplemented` on a backend without revisions.
  - `Get` of a record and a missing record; `List` of the whole type, one
    collection, with a filter, of an empty prefix, a corrupt record failing
    even when filtered out; coordinate and prefix checks on `Get`, `List`
    and `Watch`.
  - `List` over pages: every read at one revision, a restart after a
    compaction without records of the failed walk, an unpinned walk
    after three attempts; paged lists to the end, a filtered page, a page after a
    cursor, the cursor when no match remains, a paged compaction reported as
    `kv.ErrCompacted`, a cursor outside the scope; paging without the
    capability.
  - Typed watch events: one batch per revision, decoded puts, deletes with
    their coordinate, and corrupt or mismatched records failing the batch;
    how a watch ends (ctx done, `fn` error, compaction as `kv.ErrCompacted`, a
    closed channel, a watch error, a corrupt record); `Watch` unsupported
    without the capability.
  - The decoded-coordinate check (`ErrDataIntegrity` for a group under
    another group's key), naming the absolute key.
- `kv/datacoord/segment_change_group_catalog_test.go`: the existing lifecycle
  and fail-closed tests, now through `List` and `Update`.
- `kv/datacoord/analyze_task_test.go`: the coordinate and legacy key, byte
  identity of a save, an add and a delete, a nil task rejected, `List` and
  `Get`, fail-closed decoding, and the composition with partition-stats
  actions on the chunked fallback (the partition-stats info removal lands
  last).
- Business tests in `datacoord/...`: mock expectations move from
  `ListSegmentChangeGroups` / `ListAnalyzeTasks` to `List`, and from
  `DropSegmentChangeGroups` / `SaveAnalyzeTask` to `Update`.
- etcd-backed (need the etcd from the test configuration):
  - `internal/kv/etcd`: pages after a cursor at a pinned revision across
    writes, limit 0, a compacted revision, watch from a revision with one
    revision per txn, and the revision predicates (`LoadWithRevision`, a
    write guarded by it, a key deleted after the read).
  - `kv/datacoord/generic_catalog_etcd_test.go`: a filtered `List` of one
    collection and its revision, a typed `Watch` from it, one batch per
    atomic `Update`, `Get`, a point delete, a collection's deletes in one
    batch, and the watch ending without an error when its ctx is
    cancelled; a read-modify-write guarded by the `Get` revision, a second
    write with the same revision rejected, and a record deleted after the
    read not written back.
- Not covered: a real etcd compaction during a watch (covered with a fake
  watch channel instead).

## Future Work

Hard cases that keep their typed paths for now:

- Segments: one record split over a segment key and per-field binlog keys,
  with `AlterEncoding` compatibility writes and the `publishesSegmentVersion`
  commit-marker rule. Needs types spanning several roots.
- Segment-index `Update` transitions, which must not take the chunked
  fallback (`CommitWithoutFallback`).
- `UpdateCompactionTargetState`: read-modify-write inside the catalog.
- `GcConfirm`: an existence check on segment key prefixes.
- Moving the existing fenced writes (the StreamingNode checkpoint, the
  pk-index manifest pointer) from value predicates to `UnchangedSince`.
- `UnchangedSince` on TiKV: snapshot reads that report the TSO as the
  revision, and a txn started at the read revision.
- Dropping commit markers for the types not migrated yet: a chunked
  fallback in the caller's order and a final atomic `Update`, with
  `SegmentEntry` split into binlogs and record
  ([Commit markers](#commit-markers)). Required before segments and
  DataViews migrate.
- A wire identity for a type once the catalog server exists (the root can
  serve), since calls now pass a `*ModelType`.
- IO that bypasses the catalog: the global ID allocator, the pk-index
  `metaKVStore`, DataCoord's reads of the StreamingNode catalog (L0 transform,
  GC), `CleanMeta`, and raw etcd watches on catalog keys such as the CDC
  controller's.
- A list-then-watch helper once a component consumes `Watch`.
- Other catalogs: RootCoord (timestamped reads), QueryCoord, the QueryView
  catalogs, StreamingCoord and StreamingNode.
