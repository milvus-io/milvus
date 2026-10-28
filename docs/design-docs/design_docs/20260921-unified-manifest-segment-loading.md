# Unified Manifest Loading for Internal and External Sealed Segments

- **Status:** Design and implementation contract; design approval pending
- **Original design date:** 2026-09-21
- **Feature DRI:** TBD
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** Not scheduled
- **Original design baseline:** `c819a2c5e11144020837a948f4384def1771af3b`
- **Integration baseline (2026-10-19):** `b64597022703405bdc8e7aa8fcdc76e419db6b8c`
- **Validation records:** [PR #53742](https://github.com/milvus-io/milvus/pull/53742)
- **Related issue:** [#53630](https://github.com/milvus-io/milvus/issues/53630)
- **Scope:** Segcore manifest-based sealed-segment Load/Reopen, with explicit
  integration work for external refresh admission, sampling, output reads,
  and function-input defaults in internal backfill and external refresh

The review fields above are placeholders, not claims of assignment or approval.
The original design baseline identifies the implementation examined when this
design was written. The integration baseline identifies the master commit onto
which the feature was rebased; neither is a claim of successful validation.
The issue reports a different image revision (`7f171ade`); this document does
not claim an exact-image reproduction. Section 12 defines acceptance checks;
the linked PR keeps candidate-specific results separate from this contract.

## 1. Decision

Use one manifest field planner and one LoadDiff executor for internal and
external sealed segments. Keep format-dependent column naming, Reader setup,
load policy, and system-field preparation as small adaptations inside that
pipeline. Reuse `LoadColumnGroup`, `ManifestGroupTranslator`,
`DefaultValueChunkTranslator`, caching slots, and staged state publication.

Remove `load_external_manifest` and the external-only scheduling overload of
`LoadColumnGroups` after all manifest formats use the common planner. Do not
introduce a second planner for `milvus-table` or a per-format loader hierarchy.

All external formats use the internal-table missing-field rule for ordinary
mapped data fields: an absent field with a default uses that default; an absent
nullable field uses NULL. This applies to `milvus-table`, Parquet, Lance, Vortex,
and Iceberg. Required fields, real PKs, system fields, and function outputs
retain their explicit preparation and validation rules.

Internal manifest loading also uses the target manifest as the physical-column
inventory, including transitions from physical data to eligible defaults.
Internal function backfill now fills missing inputs before execution, accepts
non-nullable inputs with defaults, and preserves physical NULLs during row
selection. The external executor shares this missing-input rule. These are
intentional behavior changes; internal row retention and the supported function
types remain unchanged.

## 2. Problem and original implementation

For the motivating case, a snapshot schema contains nullable field `revision`
with source FieldID 105. Historical source segments may legitimately have no
physical column `"105"`. The external target already preserves source field
IDs. A missing column is not necessarily a mapping failure.

The original design baseline shares the outer Load/Reopen entry points and the
bottom column loader, but has two manifest planning paths:

| Operation | Internal manifest | External manifest |
|---|---|---|
| Plan columns | `ComputeDiffColumnGroups` | Set `load_external_manifest` |
| Column identity | Several `std::stoll` sites | `Schema::ResolveColumnFieldId` |
| Task selection | load/replace/eager/lazy lists | Walk all column groups in an external overload |
| Missing fields | `ComputeDiffDefaultFields` | Explicitly excluded |
| Default-state inheritance | Retained while missing | Explicitly excluded |
| Reader | Explicit Milvus Arrow schema | Schemaless, followed by normalization |
| Execution | `LoadColumnGroup` | Same `LoadColumnGroup` |
| System fields | Physical/internal conventions | Virtual PK or real source PK; synthetic or source timestamps |

Additional constraints visible in source:

1. External fake binlogs advertise all schema fields in `ChildFields`. They
   are accounting/association metadata, not proof that a physical column exists.
2. Internal `CollectDataFields` unions binlogs, manifest columns, and indexes
   with usable raw data. Reusing that union unchanged hides external missing
   columns.
3. Schema-only default filling does not require a new physical Reader. Preserve
   the internal loader's rule: create a Reader only when column-group load or
   replacement tasks exist, while publishing schema/default changes separately.
4. The external scheduler supplies `is_replace=false`. Sharing the internal
   replacement plan is necessary for default-to-physical transitions.
5. Internal output take defaults to disabled; external output take defaults
   to enabled. Runtime default columns do not automatically fix take output.
6. Refresh field-size sampling runs in DataNode before QueryNode loading and
   has its own schemaless Reader. Segcore load unification alone cannot close
   the reported issue.

## 3. Scope and invariants

### In scope

- One initial-load and reopen planner for all manifest-based sealed segments.
- Correct field identity, data-source classification, and incremental updates.
- Reuse of runtime default columns for eligible missing fields in all external formats.
- Shared Reader creation driven by physical column tasks, as in the internal loader.
- Preservation of eager/lazy behavior, external credentials, real/virtual PK,
  source timestamps, and target-owned function outputs.
- Explicit output and sampling integration needed for the motivating behavior.
- External fragment admission based on mapped physical columns, grouping by
  their presence, and safe publication of an intentionally empty refresh.
- Optional `all_fragments_unmapped` fields in the task response and persisted
  task result, with false as the compatibility default.
- Shared missing-input defaults before function execution in internal backfill
  and external refresh, including preservation of physical NULLs.

### Out of scope

- Unifying the old binlog loader with manifest loading or changing Growing.
- Adding automatic target-schema evolution across external refreshes. Existing
  source/target schema identity validation remains in force.
- Writing NULL/default column files into source snapshots or target manifests.
- New manifest formats, public APIs, or configuration flags. The internal task
  protobuf additions listed above are included in scope.
- Changing supported external types or remapping existing field IDs.
- A general rewrite of all segment resource ownership or error-code mappings.

### Required invariants

1. A physical manifest column, a runtime default column, and a synthetic system
   field are different sources. Accounted fields are not automatically physical.
2. Each field has one coherent target read state. A planner must not schedule
   an ordinary drop that erases a replacement/default column prepared earlier.
3. Reader, schema, columns, default-field markers, and readiness form one
   coherent published segment state. A new generation may retain the old Reader
   when no physical column load or replacement is required.
4. A failed/cancelled prepare does not publish the next read state. Old readers
   keep usable references to old columns and Readers.
5. Only a successful metadata read establishing column absence can select
   default filling. I/O errors and malformed files never select that fallback.
6. Schema-level nullable/default permission does not prove why a manifest lost
   a column. This design uses the same trusted-metadata model as the internal
   loader; it cannot distinguish historical absence from a corrupted manifest
   that silently omits that field without additional provenance metadata.

## 4. Common loading pipeline

```text
Load / Reopen
  -> capture current state and target schema/load info
  -> read/cache target manifest on the updater path
  -> build current and target field-source views
  -> compute one LoadDiff
  -> prepare a target Reader when physical column tasks exist
  -> execute index/raw-data dependencies and column tasks
  -> prepare eligible default columns and system fields
  -> finalize staged markers/readiness
  -> publish through StagedStateCommitter
```

Physical column tasks also share the Storage V3 lazy-materialization policy.
When enabled and warmup is disabled, eligible columns keep their target Reader
and physical-name-to-FieldID mapping until first access. PK, system-field, and
other upstream eager-load requirements remain in force. A numeric external
column name must retain its planned target field ID in both immediate and lazy
translators. Missing-field defaults continue to use their own translator; a
reopen can replace an unread lazy physical column without materializing it.
Retained readers of the old generation keep its Reader and mapping.

The existing thread pool, cancellation plumbing, LoadDiff task lists, and
publication machinery remain the execution infrastructure. Initial load is a
diff from an empty state; schema-only and load-info reopen use the same planner.

## 5. Field identity and data-source view

### 5.1 Build a transient view, not another persisted schema

Add a small helper in `SegmentLoadInfo` that resolves manifest entries into a
FieldID-keyed view for planning. It records physical column name, current group
index, and the read-relevant column-group/file descriptor. Keep raw-data-index
availability and existing default markers separate from physical presence.

In manifest mode, fake/legacy binlog child-field lists must not override the
manifest's physical-column inventory. Existing index raw-data capability remains
an alternative source for ordinary fields. Binlog mode keeps its current source
discovery and behavior.

Build each view once per diff. Use the cached immutable column groups for both
planning and Reader creation. Avoid rescanning the manifest for each field.

### 5.2 Resolve names consistently

- Resolve the old manifest with the old schema and the new manifest with the
  target schema. Comparing both against only the target schema loses drop and
  mapping-change information.
- Replace manifest-related `std::stoll` in column diff, reload-field planning,
  and source discovery with the existing format-aware naming rules.
- Keep known dropped columns out of new load tasks. Do not turn an arbitrary
  unknown or malformed column name into an invented field ID.
- Batch resolution returns no fields for an unmapped nonnumeric external
  column, allowing historical manifests to retain dropped named columns.
  Single-field resolution remains strict, and the target schema's required
  physical columns are still validated independently.
- Validate that selected physical columns resolve unambiguously. Repeated
  files inside a group are distinct from the same field being ambiguously
  provided by multiple groups.
- Pass the selected physical-name-to-FieldID mapping to the translator, or
  otherwise make it consume exactly the same resolution rules. Its current
  numeric-first reverse parsing must not disagree with the planner for an
  external column whose literal name happens to be numeric.

This is a small shared mapping contract, not a new configurable resolver layer.
Keep the Go `StorageColumnResolver` and C++ schema rules aligned.

### 5.3 Compare data identity, not group position

Column-group array indices may change when a manifest is rewritten. A group
index is an execution address in the target manifest, not persistent identity.

For reuse/replacement, compare the physical name and read-relevant descriptor:
format, group columns/projection, ordered files, paths, row windows, and per-file
reader properties. Property maps must be compared canonically. The existing
path-plus-row-range comparison is insufficient for newly unified external
descriptors whose properties can change without changing their path.

Keep external source/spec identity separate from the descriptor. A changed
resolution/access context rebuilds affected Readers and chunk readers rather
than retaining closures with obsolete context. Do not log credentials or use
raw credential-bearing JSON in visible cache keys.

Manifest-only updates with identical data descriptors may reuse physical
columns. Reordering alone must not reload unchanged data.

## 6. Field state transitions

Plan from the target source state, rather than merely subtracting all fields
handled by the old state. Otherwise a physical field that disappears can be
incorrectly excluded from default preparation.

| Current | Target | Planned action |
|---|---|---|
| Absent | Physical | Load according to field policy |
| Default | Physical | Replace default column; clear default marker |
| Physical | Same physical source/policy | Reuse |
| Physical | Changed physical source/policy | Replace affected column(s) |
| Absent | Eligible missing field | Fill default/NULL |
| Default | Same eligible missing field | Reuse default column and marker |
| Default | Changed row count/type/default | Do not reuse; validate supported transition, then replace or reject |
| Physical | Eligible missing field | Replace with default; do not also schedule a final ordinary drop |
| Any | Removed from target schema | Drop from next state |
| Raw-data index available | Index removed/no longer serves raw | Ensure physical or eligible default source before dropping old index |
| Any | Required source missing | Fail prepare |

These transitions apply only to updates already allowed by collection and
segment contracts. They do not authorize type changes, arbitrary row-count
changes, PK-mode switches, or cross-snapshot target-schema evolution.

For a same-ID external segment, preserve the row/offset identity assumed by
virtual PK and deltalogs. Changes that require a new segment must continue to
use the existing refresh replacement lifecycle, not in-place Reopen.

For every external format, default filling requires an ordinary mapped source data
field with nullable/default permission. Exclude real PK, required source
timestamps, synthetic fields, and target function outputs. A missing target
function output remains an incomplete/corrupt generated artifact, not an empty
source column. Do not use an unrelated index to excuse missing required PK or
timestamp data.

The planner selects fields that need default materialization.
`FillDefaultValueFields` creates fresh columns for those fields and installs
them with `insert_or_assign` in the staged runtime, including when an old physical
column is already ready. The published columns remain unchanged until the next
state is committed. The next default-marker set retains eligible unchanged
defaults and adds successfully prepared defaults for internal and external
segments.

## 7. Load policy and Reader lifecycle

### 7.1 Preserve field load policies

Use one policy helper when emitting the existing eager/lazy task lists:

- Internal fields retain `ShouldLoadField`, index raw-data preference, and
  system-field rules.
- External fields retain current warmup-based eager/lazy selection.
- Real external PK and required source timestamp dependencies are guaranteed
  available before PK/delete visibility is enabled.
- Raw lazy entries remain one projected field per task. Do not accidentally
  decode all siblings in a packed group when one field is accessed.
- Vortex local-format groups retain the upstream whole-group loading policy.
  Coalesce their eager/lazy and load/replace tasks before dispatch, using the
  complete captured schema to resolve all logical aliases. Missing fields
  outside the physical group still follow the common default-value path.
- Physical projections are deduplicated, while the planner and translator keep
  every logical alias. Capture this one-to-many mapping for delayed reads so
  numeric external column names are never reinterpreted as target field IDs.
- Index-only reopen must not become full external data reload.

One scheduler may use different policy inputs. This avoids changing memory
usage and query latency merely as a side effect of removing a branch.

### 7.2 Preserve task-driven Reader creation

The shared executor keeps the original internal manifest loader's criterion:
create a Reader only when at least one of `column_groups_to_load`,
`column_groups_to_replace`, `column_groups_to_lazyload`, or
`column_groups_to_lazyreplace` is nonempty. Schema object identity, schema version,
and manifest path changes do not independently trigger Reader creation.

Initial load plans physical column tasks against an empty source view. External
fragment admission guarantees a mapped physical source, so the same execution
path creates the initial Reader and initializes synthetic system fields. Changed
files, row ranges, format/properties, external read context, or loading policy
produce column tasks through the shared planner and therefore create a Reader.

Prepare the Reader before tasks requesting `get_chunk_reader`, using the target
schema and load info. Reuse old column translators only when their source and
policy remain valid; their own old chunk-reader references remain alive
independently of a replacement output Reader.

When no column tasks exist, retain the published Reader. Schema-only default
additions still publish the target schema and runtime default columns. Output
requests involving these columns bypass `take()` and use the ordinary read path;
physical `take()` requests supply their requested columns explicitly. No-op,
index-only, or deltalog-only updates likewise reuse the Reader when they produce
no physical column tasks.

Centralize Reader setup in the executor:

| Input | Preserved behavior |
|---|---|
| Internal manifest | Explicit native Arrow schema, including existing TEXT representation |
| External manifest | External properties and current file-derived Arrow normalization |
| Physical projection | Derived from the resolved target physical fields |
| Generated function output | Target numeric column identity |
| Synthetic fields | Excluded from remote projection |

Keep the external Reader schemaless initially. Default columns are handled in
the runtime/default-output path, so unification does not depend on changing
every external format's Arrow schema negotiation.

Initialize manifest caches before publication. `HasManifestColumn` remains a
read-only lookup; do not introduce remote I/O or lazy mutable cache initialization
on a concurrent query path. Cache inheritance still requires the same immutable
manifest identity; a path/version must not be reused for different contents.

## 8. Execution, system fields, and cache ownership

The common executor consumes the existing load/replace/lazy/default task lists.
Delete the external-only enumeration/thread-pool loop. Retain the actual shared
`LoadColumnGroup` implementation and Arrow normalization.

Prepare virtual PK and synthetic timestamp state in an explicit external
system-field step, after required physical dependencies and before publication.
For real-PK `milvus-table`, keep source timestamps for delete/reinsert ordering;
never replace them with the generic external constant timestamp.

The original design baseline has reader-visible helpers outside
`RuntimeResourceState`, such as `pk_index_slot_`, virtual-PK entries in
`insert_record_`, and some counters.
Passing a committer does not by itself make those writes transactional. During
implementation, audit every helper reached by the newly shared replace path:

- Stage any changed PK/timestamp/row-identity state that must switch with a
  replaced column, using the existing runtime snapshot where possible.
- Initial-load-only state may be initialized before publication because the
  segment is not yet query-visible.
- Do not rerun mutating system initialization for an unchanged system state
  during an index-only or default-field-only reopen.
- Unchanged immutable synthetic state may be reused. Do not broaden the set of
  in-place identity transitions accepted by the current API.

This bounded ownership work is part of safe load unification where affected;
the design does not claim the baseline already stages every auxiliary member.

Prepare all next columns/Readers/default markers against one captured state.
Join parallel tasks and respect cancellation before publishing. Reuse
`StagedStateCommitter`; its current `FinalizeLoadDiffForReopen` runs before
publish despite the method name. Do not reorder it based on the name alone.

Ensure replacing a cached field cannot alias an old generation's payload. Audit
the actual cache-key/reuse behavior of the pinned dependency. If equal translator
keys can share old content, include an opaque local source/projection generation
in the affected key. Preserve unique mmap filenames. Old Readers and pinned
chunks remain usable until their owners release them; do not evict shared old
payloads in a way that breaks those readers.

The pinned caching layer creates a fresh slot for each `CreateCacheSlot` call;
translator keys do not deduplicate those slots. Default mmap files and local
text-index directories use distinct generation paths.

Text-index replacement uses the upstream `IndexInventory`. Remove only the
field's text-family entries from the staged inventory and retain their slots
in `StagedStateCommitter` until publication. Cancel retired warmup only after
the new state is published; already acquired `IndexPin` readers remain valid.
Local text builds retain distinct mmap directories for each generation.

## 9. Direct output reads and external refresh integration

### 9.1 Output reads follow the available data source

Use the default-filled set from the same captured published state used for the
query. If requested output includes a default-filled field or a column absent
from the loaded physical manifest, bypass take for that output operation and
use existing `bulk_subscript` output logic. The physical-column check uses that
same captured load-info generation and never reads storage metadata on demand.
Perform this decision before remote I/O or partial result mutation. Apply it
to both retrieve and search, and consistently to internal optional take.

This preserves non-NULL default values as well as NULL. Merely attaching an
Arrow schema to a Reader that synthesizes NULL is not a default-value solution.
Mixed take/default assembly is a later optimization, not necessary for this fix.

The C API accepts `FieldAccessible` (loaded field data or an index), as internal
execution does, or a manifest column eligible for direct take. It does not treat
schema nullability alone as readiness. An index without raw values may serve a
filter, but output still uses the existing operation-specific readers and their
checks; `HasIndex` is not a promise that arbitrary output can be reconstructed.

A scalar index with raw data legitimately replaces a default column. Keep the
existing load planner behavior that then drops the default marker. Query output
falls back to `bulk_subscript`, which can reverse-lookup NULL/default rows from
that index. This applies equally to internal collections with optional take and
to external collections, without a format-specific reading branch.

If a physical manifest column exists but a file read fails, preserve existing
failure behavior; do not reinterpret the error as a default-filled field.

### 9.2 Physical inventories for generic formats

Generic manifests previously listed requested schema columns even when files did
not contain them. The refresh path now reads the unprojected format schema via
`GetExternalFileColumns`, carries names in transient `FileInfo`/`Fragment`
metadata, and writes only actual columns into target manifests. Known row counts
are retained; metadata discovery also runs for table paths whose row count was
provided by the explorer. Column discovery passes the explored file properties
to the format reader, including Lance's `dataset_version`, so it inspects the
same snapshot as subsequent reads instead of reopening the latest version.
Metadata errors abort the refresh.

For each refresh batch, group fragments by the presence of requested columns,
then apply the existing row-count bin packing within each group. This ensures a
segment-level default never hides another file's real values. Existing segments
whose files now need different presence groups are replaced through the existing
refresh lifecycle. Compatible segments retain their IDs and append newly mapped
physical columns when necessary. Manifest construction rejects inconsistent
presence groups rather than silently intersecting them. Fragments without any
physically mapped business column are skipped before segment creation; defaults
or generated function outputs do not admit rows.

The physical inventory is not persisted in a new wire field. Nil inventory is
reserved for existing helpers reconstructing already-known column groups;
production generic exploration always resolves it before creating segments.
`milvus-table` keeps its existing source-manifest inventory and numeric mapping.

Creation of ordinary external collections rejects `external_field` mappings
consisting entirely of ASCII digits. Such source names could otherwise be
mistaken for internal field IDs after a mapping is dropped while an older
manifest is still loaded. This creation-time restriction exempts `milvus-table`
and does not apply to internally generated function output columns. It does not
retroactively validate existing collections or change schema-alter validation.

User-visible costs and boundaries:

- Refresh needs extra file-schema metadata reads, including files with known row
  counts. The existing worker pool bounds concurrency; no performance benchmark
  is claimed.
- Heterogeneous source schemas may produce more segments. Regrouping uses normal
  segment replacement and does not promise stable virtual PKs across refreshes.
- Generic-format balancing skips physical-column groups with zero total rows
  before allocating IDs or creating manifests. A nonempty task containing only
  zero-row fragments retains the existing explicit error; mapped-column
  admission and the all-unmapped proof remain separate from this row-count rule.
- Adding a nullable/default target mapping can read NULL/default from the old
  loaded snapshot immediately. Source values become visible after external
  refresh publishes metadata and `RefreshLoad` finishes loading that snapshot.
  `RefreshExternalCollection` completion alone is not QueryNode readiness.
- Supported formats retain their own snapshot and immutable-file assumptions;
  this change does not make in-place source-file mutation safe.

### 9.3 Sampling is a separate required integration

DataNode sampling must derive eligible missing fields from the same physical
manifest and field rules. Sample physical columns normally; construct bounded
sample-sized default/NULL arrays for eligible absent fields and account for
them using the existing normalization and buffer-size logic. Keep per-segment
and first-segment sampling behavior unless independently changed.

Do not advertise absent fields as physical by writing synthetic column-group
metadata. Keep fake-binlog accounting distinct from the physical inventory.
Correct the error's attempted-versus-total sample count if this path is edited;
do not infer that all segments were attempted from the existing error text.

Index-build and function-input readers do not consume QueryNode runtime
defaults. Trace and test their existing absent-field behavior before claiming
full external schema-evolution support. Fix only gaps required by supported
fields; generated function outputs must still be produced, never NULL-filled.

### 9.4 Admission policy for wholly unmapped fragments

External refresh now skips a source fragment when none of its business mapping
columns physically exists. Such fragments contribute no segment and no rows,
even when all target fields have nullable/default declarations. At least one
present mapping admits the fragment; remaining fields retain the existing
NULL/default rules. A physically present all-NULL column is still a mapping.
System timestamps, virtual primary keys and target-generated function outputs
never qualify as business source columns. A real mapped primary key does.

The filter runs after metadata discovery and before segment reuse/bin packing.
Consequently an existing segment containing a newly excluded fragment is
invalidated; its still-admitted fragments are repacked. A later refresh can
admit a previously excluded file/range when a mapped column appears. Generic
formats use discovered physical names; milvus-table resolves actual numeric
FieldIds from the source manifest with the external filesystem context. Unknown
inventories and storage failures abort refresh without publishing a partial
filtered result. Logs report skipped files, fragments and rows.

There is no physical row-carrier fallback. Constructing a manifest with no
mapped physical columns is an internal contract error, since ordinary refresh
must filter such fragments before manifest creation.

DataCoord's existing protection against an unexplained empty replacement is
retained. The worker's `all_fragments_unmapped` flag is true only after a
non-empty fragment range is fully inspected and every fragment is excluded.
It travels through the task manager, task response and persisted task result.
Only if every finished, result-ready task carries this flag can aggregation
remove every existing segment without replacement. Contradictory flagged
results containing segments are rejected. Older workers/persisted messages
omit the flag (false), so they cannot accidentally authorize an empty refresh.
A newer worker with an older coordinator still fails the old empty-result
safety check instead of clearing data. The persisted flag uses field 24;
fields 19 through 23 retain their upstream definitions. No existing RPC or
persisted field is renumbered.

This admission policy changes external source admission; internal-table row
retention is unchanged.

### 9.5 Function inputs before execution

Internal backfill uses the upstream absent-field reader, while external function
execution overlays physically missing inputs before conversion. Both use
`storage.GenerateEmptyArrayFromSchema`. For an admitted row batch:

1. Determine absence from the segment's physical inventory, never from NULLs in
   an Arrow array. Preserve actual physical values, including NULL.
2. For a missing input, use the schema default when configured, otherwise NULL
   if nullable. Fail if neither is available. A non-nullable field with a default
   is valid; `GenerateEmptyArrayFromSchema` accepts it and releases its Arrow
   builder after transferring the returned array's ownership.
3. Execute functions against this logical input view. Function outputs are
   produced only by their runners, never by ordinary missing-field synthesis.
4. Preserve row count and ordering, and publish only after every batch succeeds.

Internal backfill receives a complete logical input record from the reader.
Upstream additive reconciliation includes a physical RowID/Timestamp anchor and
all absent ordinary inputs in its read schema; the reader fills only fields
absent from physical storage. The materializer computes function outputs from
that record. Row selection clears the copy builder's default parameter so stored
NULLs are preserved. Records are borrowed until the reader's next Next/Close;
materializer cleanup releases only derived arrays. All-NULL binary TEXT needs
no LOB decoding and retains the NULL-as-empty-text adapter behavior.

External execution resolves missing inputs from its newly built input manifest,
projects physical input fields, and uses the same existing-column projection if
all function inputs are absent. `RecordToInsertDataWithDefaults` borrows each
record from its reader and overlays only known-missing fields. Conversion owns
the copied values; temporary Arrow arrays are released on success and failure.
Homogeneous physical-column groups are established by refresh before this stage.
Required missing inputs fail before opening the output writer. Output batches must match
the input row count, and the complete stream must match fragment row counts.
Writer resources are destroyed on every exit; function, provider, reader and
commit errors do not publish a successful segment result.

Function-specific boundaries remain in place:

- Internal backfill supports its existing BM25 and MinHash functions. This does
  not add TextEmbedding backfill to internal tables.
- External BM25/MinHash retain NULL/empty-text behavior after default resolution.
- External TextEmbedding retains its non-nullable input requirement and rejects
  empty text, including an empty-string default. It receives the original schema,
  so this layer does not bypass provider or function validation.
- Existing provider count/dimension checks remain responsible for malformed
  embedding responses. This is not a new generic function DAG or a promise to
  support future function types without their existing validation/executor work.
- This does not invalidate or recompute already-published function outputs or
  indexes when their inputs later change. The deferred value-generation/index
  invalidation contract in section 10.1 remains necessary.

Input resolution adds manifest metadata reads proportional to the number of
function input fields, plus an anchor search when all inputs are absent.

## 10. Errors and compatibility

- The all-unmapped result proof adds optional task RPC/persistence fields; old
  snapshots and manifest representations stay readable (see section 9.4).
- No change to target/source schema identity validation or supported field types.
- Generic external formats now accept absent ordinary nullable/default fields.
  A successful metadata read must establish absence; reader failures still fail.
- No new automatic full reload or remote data rewrite fallback.
- Invalid mapping syntax and required-field absence remain errors. An absent
  optional mapped name now means NULL/default, so a typo in that name is no
  longer distinguishable from intentional absence. Corrupt internal
  artifacts are distinct from a user's invalid request.
- Propagate metadata and read failures instead of synthesizing defaults. The
  added metadata C API preserves SegcoreError exceptions, but Arrow-status and
  generic exceptions use the existing UnexpectedError convention; no improved
  retry classification is claimed.
- Fault tests must follow errors from source through cgo/Go consumers. A helper
  test alone does not establish retry behavior; no retry improvement is claimed.

The existing snapshot design explicitly excludes schema changes between
refreshes. Loading historical segments under an already-matching snapshot/target
schema is the scope here and does not relax that exclusion.

### 10.1 Deferred: index invalidation across backfill and refresh

Internal-table backfill and external refresh still need a unified contract for
invalidating indexes when field values change under an existing segment ID.
Ordinary default materialization can preserve an index when logical values and
row offsets remain unchanged, but that does not establish safety for every
backfill operation. External refresh can also replace previously synthesized
NULL/default values with source values while retaining the segment ID.

The current refresh patch retains segment index records, and QueryNode's index
diff does not replace an index merely because its manifest or schema changed
while the index ID stayed the same. A follow-up must coordinate data publication,
index invalidation/rebuild, and QueryNode replacement, including builds racing
with backfill or refresh. This is an explicitly deferred shared limitation, not
a completed capability of the unified load path. Existing tests for indexes on
permanently absent fields do not validate a later transition to physical values.

## 11. Implementation map and integration boundaries

Every manifest format uses the shared planner and executor. The binlog path
remains separate. Integration preserves upstream asynchronous manifest loading,
immutable runtime PK state, vector raw-index skipping, refresh ownership, and
external result storage. Internal backfill uses the upstream reader/anchor
implementation rather than a separate branch-local reader.

| File/module | Responsibility |
|---|---|
| `segcore/SegmentLoadInfo.{h,cpp}` | Source view, physical/default task planning, marker inheritance |
| `segcore/ChunkedSegmentSealedImpl.{h,cpp}` | Shared execution, Reader setup, staged defaults/system state, take fallback |
| `common/Schema.{h,cpp}` | Physical column identity and field-role rules |
| `segcore/storagev2translator/ManifestGroupTranslator.{h,cpp}` | Captured physical-to-logical mapping and translator lifetime |
| `segcore/external_utils_c.cpp` | Physical format metadata and bounded default-aware sampling |
| `internal/datanode/external/task_update.go` | Group files by physical column presence, patch/rebuild manifests; preserve fake-binlog contract |
| `internal/storagev2/packed` | Fetch and carry physical inventories, filter and validate manifest column groups |
| `internal/datanode/compactor/record_materializer.go` | Function-input consumption and NULL-preserving row selection |
| `internal/storage/record_to_insert.go` | Missing-input overlay for external function execution |
| Existing C++/Go tests and snapshot SDK regression suite | Behavioral and compatibility coverage |

No new public strategy interfaces, wire enums, or default-file writer are needed.

## 12. Validation and acceptance

This section specifies acceptance criteria and reproducible checks, not recorded
passes. Historical measurements are archived in the validation records of
[PR #53742](https://github.com/milvus-io/milvus/pull/53742). Some early records
lack exact tested commit IDs; they must not certify a later integration or the
current PR head. Each new result must identify its candidate, base, environment,
commands, outcomes, and any checks that remain blocked or unexecuted.

### Planner and executor regression matrix

Test internal numeric columns, generic external named columns, and milvus-table
numeric columns, including real and virtual PK modes:

- Initial load, no-op reopen, schema-only reopen, index-only reopen, and
  manifest-only/deltalog-only updates with unchanged physical data.
- Reordered groups, changed files, changed row windows, changed format/properties,
  changed read context, and remapping detection without accidental field reuse.
- Default-to-physical, retained default, dropped default, physical-to-eligible-
  default, and invalid required-field absence. Reject unsupported transitions.
- Fake binlogs listing all fields while the manifest omits a nullable field.
- Raw-data-index addition/removal and old-index availability until replacement
  data/defaults are ready.
- Eager/lazy/warmup policy parity and single-field lazy projection.
- Reader reuse with zero physical column tasks, including freshly parsed
  unchanged schemas and schema-only default additions; physical replacements
  still create a new Reader.
- Real source timestamp delete/reinsert ordering and virtual PK offset identity.
- Function-output retention and fail-closed behavior when generated output is
  missing; optional source absence follows the common default policy.

### Cache/publication and failure tests

- Hold old state/column pins while replacing; old reads complete correctly and
  new reads see only the new state.
- Fail/cancel after Reader creation, midway through column preparation, and
  after default preparation but before publish. Old queries and retry remain
  usable; no duplicate registration or leaked next-generation resources.
- Inspect PK/system-field helpers, default mmap buffers, and translator keys
  under retries and replacement; do not infer correctness from shared_ptr alone.
- Storage timeout/throttling, corrupt manifest/file, type mismatch, and required
  missing field remain distinguishable from legitimate absent nullable fields.

### End-to-end issue acceptance

Create separate old/new source segments around AddCollectionField and import the
post-change snapshot into a matching target schema. Verify refresh, query and
search output, `is null`, equality predicates, projection of only the new field,
row counts, non-NULL defaults where allowed, release/load, and repeated refresh.
Ensure the first sampled fragment is an old segment. Test both take settings.

Add targeted index/function tests where those features use the affected field;
do not equate refresh progress 100 with complete read-path correctness.

### Focused local commands

Run these focused checks from the designated worktree root, using its own core
libraries and test binary. They cover the shared planner, default/index-backed
output routing, query-entry readiness, missing-input conversion, and lazy
physical/default replacement with numeric external names. They do not
replace the broader acceptance matrix or format-reader SDK E2E tests above.
These fixtures do not require a running Milvus, etcd, or object-store service.
The runtime path below prefers the just-built core in `cmake_build/src` over an
older installed copy in `internal/core/output/lib`, which `setenv.sh` may select.

```bash
source ~/.profile
source scripts/setenv.sh
cmake -S internal/core -B cmake_build -DBUILD_UNIT_TEST=ON
cmake --build cmake_build --target all_tests -j 8
if [ "$(uname -s)" = Darwin ]; then
  export DYLD_LIBRARY_PATH="$PWD/cmake_build/src:${DYLD_LIBRARY_PATH:-}"
else
  export LD_LIBRARY_PATH="$PWD/cmake_build/src:${LD_LIBRARY_PATH:-}"
fi
cmake_build/unittest/all_tests --gtest_filter='SegmentLoadInfoTest.*:ExternalTakeTest.DefaultOutputsBypassTakeBeforeIO:ExternalTakeTest.MissingPhysicalOutputsBypassTakeWithoutDefaultMarker:CApiTest.RetrieveByOffsetsChecksExternalFieldSources:StorageTest.UnifiedManifestLoadHistoricalDefaultsAndReopen:*LazyManifest*'
go test -tags dynamic,test -gcflags='all=-N -l' -ldflags="-r ${RPATH}" \
  github.com/milvus-io/milvus/internal/storage \
  -run '^TestRecordToInsertDataWithDefaults$' -count=1 -v
```

### Function E2E contracts

These descriptions identify fixture assertions, not successful execution:

- `TestExternalFunctionMissingInputs/bm25_minhash` checks missing-input defaults
  against explicit text, preserves physical NULL/empty text, and verifies exact
  query and BM25 hit ID sets. It runs independently of an embedding service.
- `TestExternalFunctionMissingInputs/embedding` uses the existing `tei_endpoint`
  and `tei_model_dim` flags. The endpoint must be reachable by both the test
  process and Milvus; an unavailable service skips only this subtest. Missing
  inputs and explicit text must produce equivalent vectors. One explicit-empty
  input failure checks the refresh reason and preserves the exact previously
  published ID/vector mapping.
- Provider HTTP/count/dimension validation and empty-default rejection belong
  to provider/input unit tests and external batch-execution tests. The E2E uses
  a shared service rather than starting a loopback HTTP provider.

Run either subtest independently from the repository root after loading the Go
environment. Supply the existing external-storage configuration as usual, and
use a test Milvus deployment:

```bash
source ~/.profile
source scripts/setenv.sh
go -C tests/go_client test -tags dynamic,test -gcflags='all=-N -l' \
  github.com/milvus-io/milvus/tests/go_client/testcases -count=1 -v \
  -run '^TestExternalFunctionMissingInputs$/^bm25_minhash$' -addr "$MILVUS_ADDR"
go -C tests/go_client test -tags dynamic,test -gcflags='all=-N -l' \
  github.com/milvus-io/milvus/tests/go_client/testcases -count=1 -v \
  -run '^TestExternalFunctionMissingInputs$/^embedding$' -addr "$MILVUS_ADDR" \
  -tei_endpoint "$TEI_ENDPOINT" -tei_model_dim "$TEI_MODEL_DIM"
```

### Verification limits

A planner fixture with mocked format I/O does not establish format-reader
coverage. Use representative fixtures for shared logic and real format E2E for
format behavior. A skipped embedding subtest is not a pass. Source analysis,
compilation, package coverage, and historical E2E are separate evidence; none
establishes current full acceptance on its own.

Remote-store throttle/timeout handling, race safety, and performance require
their own failure campaigns or measurements. The design makes no claim that
these have passed, that retry classification improved, or that the repository
coverage target has been met. Indexes on permanently absent fields do not prove
correct invalidation when physical values later appear (section 10.1).

## 13. References

- [LoadDiff architecture](segcore/20260204-loaddiff_based_segment_load.md)
- [Segment COW publication target](20260627-segment-reopen-atomic-read-update-cow.md)
- [Milvus snapshot external source](20260526-milvus-table-external-source.md)
- [SegmentLoadInfo](../../../internal/core/src/segcore/SegmentLoadInfo.cpp)
- [Sealed-segment loading](../../../internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp)
- [Manifest translator](../../../internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.cpp)
- [Default translator](../../../internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.cpp)
- [External sampling](../../../internal/core/src/segcore/external_utils_c.cpp)
- [External refresh task](../../../internal/datanode/external/task_update.go)
