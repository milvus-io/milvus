# Unified Manifest Loading for Internal and External Sealed Segments

- **Status:** Implemented locally; validation commands and historical results in section 12
- **Date:** 2026-09-21
- **Feature DRI:** TBD
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** Not scheduled
- **Source baseline:** `c819a2c5e11144020837a948f4384def1771af3b`
- **Related issue:** [#53630](https://github.com/milvus-io/milvus/issues/53630)
- **Scope:** Segcore manifest-based sealed-segment Load/Reopen, with explicit
  integration work for external refresh admission, sampling, output reads,
  and function-input defaults in internal backfill and external refresh

The review fields above are placeholders, not claims of assignment or approval.
The issue reports a different image revision (`7f171ade`); this design is based
on the specified local worktree, not an exact-image reproduction.

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

## 2. Problem and baseline implementation

For the motivating case, a snapshot schema contains nullable field `revision`
with source FieldID 105. Historical source segments may legitimately have no
physical column `"105"`. The external target already preserves source field
IDs. A missing column is not necessarily a mapping failure.

The baseline already shares the outer Load/Reopen entry points and the bottom
column loader, but has two manifest planning paths:

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
3. A schema-only reopen can require a different Reader even when no physical
   column group needs reloading. Reader lifecycle must not depend solely on
   nonempty column-group task lists.
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
- Reader reconstruction independent of field-task existence.
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
3. Reader, schema, columns, default-field markers, and readiness describe the
   same published segment generation.
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
  -> prepare a target Reader if required
  -> execute index/raw-data dependencies and column tasks
  -> prepare eligible default columns and system fields
  -> finalize staged markers/readiness
  -> publish through StagedStateCommitter
```

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
- Lazy entries remain one projected field per task. Do not accidentally load
  all siblings in a packed group when one field is accessed.
- Index-only reopen must not become full external data reload.

One scheduler may use different policy inputs. This avoids changing memory
usage and query latency merely as a side effect of removing a branch.

### 7.2 Give Reader rebuilding its own decision

Replace `load_external_manifest` with an internal `rebuild_manifest_reader`
decision in LoadDiff. It is not a wire field. Rebuild on initial load, manifest
version change, read-relevant schema/projection change, or external read-context
change. It must work when all column task lists are empty, including a
schema-only default-column addition and a deltalog-only manifest update.

Prepare Reader before tasks requesting `get_chunk_reader`. Schema-only changes
must use the target schema, not a separately captured old schema. Reuse old
column translators only when their source and policy remain valid; their own
old chunk-reader references remain alive independently of the new output Reader.

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

The baseline has reader-visible helpers outside `RuntimeResourceState`, such
as `pk_index_slot_`, virtual-PK entries in `insert_record_`, and some counters.
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
provided by the explorer. Metadata errors abort the refresh.

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

## 10. Errors and compatibility

- The all-unmapped result proof adds optional task RPC/persistence fields; old
  snapshots and manifest representations stay readable (see the admission
  follow-up below).
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

## 11. Implementation sequence

Each step is independently reviewable; intermediate steps must not claim the
issue is resolved. Prefer small commits in the designated worktree.

1. **Common physical inventory and column mapping.** Add helpers, route manifest
   comparisons/reload discovery through them, and establish format parity tests.
2. **Common manifest planning/execution.** Migrate every manifest format to
   common task lists, preserve policy/system preparation, decouple Reader rebuild,
   and remove the external-only flag/scheduler. Address affected staged ownership.
3. **Missing-field transitions and output.** Enable eligible default states for
   every external format, replacement/state inheritance, and take fallback.
4. **Refresh integration.** Discover physical columns before constructing
   generic manifests, group fragments by column presence, fix sampling, and run
   snapshot and per-format read/reload regressions.

Do not merge a partially migrated production path that selects the common
planner without its Reader, system-field, and ownership prerequisites.

### Expected files

| File/module | Responsibility |
|---|---|
| `segcore/SegmentLoadInfo.{h,cpp}` | Source view, task planning, Reader decision, marker inheritance |
| `segcore/ChunkedSegmentSealedImpl.{h,cpp}` | Shared execution, Reader setup, staged defaults/system state, take fallback |
| `common/Schema.{h,cpp}` | Reuse/extend physical column and field-role helpers only as needed |
| `segcore/storagev2translator/ManifestGroupTranslator.{h,cpp}` | Consume consistent selected-column mapping; cache identity if required |
| `segcore/external_utils_c.cpp` | Physical format metadata and bounded default-aware sampling |
| `internal/datanode/external/task_update.go` | Group files by physical column presence, patch/rebuild manifests; preserve fake-binlog contract |
| `internal/storagev2/packed` | Fetch and carry physical inventories, filter and validate manifest column groups |
| Existing C++/Go tests and snapshot SDK regression suite | Behavioral and compatibility coverage |

No new public strategy interfaces, wire enums, or default-file writer are needed.

## 12. Validation and acceptance

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
- Reader refresh with zero physical load tasks; no unnecessary physical reads
  on index-only/no-op updates.
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

### Current implementation

Implemented in the designated worktree against the baseline above:

- Every manifest format now uses `ComputeDiffColumnGroups` and the shared
  `LoadDiff` executor. The external-only flag and scheduling overload are gone.
- The planner resolves physical field names consistently, ignores fake binlogs
  as physical inventory, compares file descriptors, and plans default/physical
  replacement without duplicate tasks. Old binlog loading stays separate.
- All external formats use runtime defaults for missing ordinary nullable/default
  fields. Required source columns still fail when absent.
- Reader rebuild, default markers, columns, and the replaced PK cache slot use
  the staged runtime. Unchanged synthetic identity is initialized only once.
  Load/Reopen check cancellation before manifest planning can perform I/O.
- Default/physical transitions also rebuild locally created text indexes.
  Default mmap files and locally built text-index directories have distinct
  generation paths, so replacement does not overwrite pinned old resources.
  The pinned caching layer creates a fresh slot per `CreateCacheSlot` call;
  its translator key does not deduplicate those slots.
- Retrieve/search bypass take before I/O when output contains a default field
  or a column absent from the captured physical manifest.
  The sampling C API uses the same default eligibility and Arrow-array helper;
  an advertised physical column that fails to read never becomes a default.
- The C API's external-field readiness check accepts loaded field data or indexes
  through the existing internal `FieldAccessible` contract. A physical manifest
  column remains another valid source for direct take. The initial default-marker
  exception fixed historical columns but still rejected index-backed missing
  columns; the source-based check also covers that transition. A nullable schema
  declaration alone must not bypass readiness.

### Current validation commands

Run these focused checks from the designated worktree root, using its own core
libraries and test binary. They cover the shared planner, default/index-backed
output routing, query-entry readiness, and missing-input conversion. They do not
replace the broader acceptance matrix or format-reader SDK E2E tests above.
These fixtures do not require a running Milvus, etcd, or object-store service.
The runtime path below prefers the just-built core in `cmake_build/src` over an
older installed copy in `internal/core/output/lib`, which `setenv.sh` may select.

```bash
source ~/.profile
source scripts/setenv.sh
cmake --build cmake_build --target all_tests -j 8
if [ "$(uname -s)" = Darwin ]; then
  export DYLD_LIBRARY_PATH="$PWD/cmake_build/src:${DYLD_LIBRARY_PATH:-}"
else
  export LD_LIBRARY_PATH="$PWD/cmake_build/src:${LD_LIBRARY_PATH:-}"
fi
cmake_build/unittest/all_tests --gtest_filter='SegmentLoadInfoTest.*:ExternalTakeTest.DefaultOutputsBypassTakeBeforeIO:ExternalTakeTest.MissingPhysicalOutputsBypassTakeWithoutDefaultMarker:CApiTest.RetrieveByOffsetsChecksExternalFieldSources'
go test -tags dynamic,test -gcflags='all=-N -l' -ldflags="-r ${RPATH}" \
  github.com/milvus-io/milvus/internal/storage \
  -run '^TestRecordToInsertDataWithDefaults$' -count=1 -v
```

### Historical validation on 2026-09-21

The results and logs below describe the candidates tested at those points in
development. Their test counts, fixtures and coverage are historical evidence,
not a claim that the current checkout has passed the same runs. Use the commands
above for the current focused checks; later follow-up results retain their own
scope and limitations.

1. Built the worktree's C++ core and test executable. The initial
   `make build-cpp-with-unittest` invocation exposed an optional/FieldId
   comparison compile error; after correction, `cmake --build cmake_build -j 8`
   completed successfully. The final candidate passed
   `cmake --build cmake_build --target all_tests -j 8`.
2. Ran 383 tests from 27 suites with **383 passed**, including the existing
   load planner, cancellation, COW publication, PK/timestamp, default translator,
   external take, schema reopen, and manifest translator regressions. This was
   a broader run than the current focused checks above; its exact test count
   applies to that historical candidate.
3. The new real-manifest fixture flushes old/new local source segments and
   exercises internal and external loading, NULL and non-NULL defaults,
   schema-only Reader replacement without reloading the PK column, both
   default/physical transitions, old pinned columns/text indexes, text-index
   memory/mmap modes, and external sampling. Moving a required source file
   aside forces a real reopen failure; the published state remains unchanged,
   old reads still work, and retry succeeds after the file is restored.
4. `git diff --check` and the scoped `git-clang-format --diff` check passed.
   No clean-tree `make cppcheck` result is claimed: that target rewrites files
   and rejects an already-dirty C++ worktree.

The C++ build/test logs for that run are
`/tmp/schema-all-formats-cpp-final-build.log` and
`/tmp/schema-all-formats-cpp-final-test.log`. C++ fixtures use isolated
local files and need no shared Milvus, etcd, or object-store service reset.

The Go SDK E2E `TestExternalTableRefreshAcrossSourceSchemaEvolution` in
`tests/go_client/testcases/external_table_refresh_test.go` also passed on a local
standalone built in this worktree, using isolated etcd and MinIO instances and
`common.storage.useLoonFFI=true`. That version pinned 128 pre-evolution rows with
a snapshot, added nullable `revision`, inserted one row with value 42 into a
distinct segment, and refreshed a matching external collection from the second
snapshot. It asserted completion at 100%, exact row/value/NULL results, `is null`
and equality filters, nearest-neighbor output for both segments, release/load,
and repeated refresh. Both snapshots' metadata were checked for StorageV3 manifests.
The complete scenario passed with the default external take setting (23.69s)
and with `queryNode.externalCollection.useTakeForOutput=false` (23.21s); the
latter setting was confirmed through the running instance's configuration API.
The SDK test executable was compiled with `-tags dynamic,test` and
`-gcflags="all=-N -l"`, then run with
`-test.run '^TestExternalTableRefreshAcrossSourceSchemaEvolution$' -test.count=1`.
Logs are `/tmp/milvus-53630-e2e.lA6mKU/e2e-gate-fixed.log` and
`/tmp/milvus-53630-e2e.lA6mKU/e2e-no-take.log`.

The first valid StorageV3 run reached refresh completion but failed Query with
`FieldNotLoaded` for physical column 105. The published-default readiness fix
above made the complete SDK scenario pass. The existing C API missing-manifest
regression also covers eligible-but-not-published defaults, and the real local
manifest fixture now goes through `AsyncRetrieveByOffsets` before and after
schema-only reopen.

Additional format acceptance on the current extension:

- `TestExternalFormatsSchemaEvolutionDefaults` passed for Parquet, `lance-table`,
  Vortex, and `iceberg-table` (82.84s total with take enabled), using real files/tables in isolated
  MinIO. It verifies initial missing nullable and defaulted fields, schema-only
  NULL reads for a newly mapped physical source column, explicit `RefreshLoad`
  and subsequent real values, Query/Search, predicates, release/load, and repeat
  refresh. Parquet also adds a mapping whose column exists in only one file of a loaded
  segment, forcing refresh to regroup files; it asserts per-row defaults versus
  physical values. Log:
  `/tmp/milvus-53630-e2e.lA6mKU/e2e-all-formats-final-take.log`.
  The earlier initial-mixed-schema variant passed with take disabled (76.08s),
  logged in `/tmp/milvus-53630-e2e.lA6mKU/e2e-all-formats-v3.log`.
- Full Go tests for `internal/storagev2/packed` and `internal/datanode/external`
  passed with dynamic/test tags, disabled optimizations, and this worktree's
  installed core library. Real missing/corrupt Parquet metadata tests verify that
  reader errors return errors and no column inventory. Log:
  `/tmp/schema-all-formats-go-final.log`.
- The 383-test C++ regression set also passed after broadening the policy, with
  the real fixture exercising generic named Parquet columns as well as internal
  and milvus-table numeric columns. Log: `/tmp/schema-all-formats-cpp-final-test.log`.
  After correcting format labels in the planner test to supported public names,
  that test was rebuilt and rerun successfully.
- The original milvus-table SDK scenario passed again with the final server
  and take enabled (24.16s); its PASS is recorded in
  `/tmp/milvus-53630-e2e.lA6mKU/e2e-all-formats-final.log`. The later generic
  cases in that same run failed because the initial test draft added a
  non-nullable field, which the API correctly rejects. The final per-format
  test uses nullable fields and its separate log above is authoritative.

The final SDK executable was also rerun with take disabled, confirmed via the
running instance configuration API. Both the milvus-table scenario (23.68s)
and the four-format scenario including regrouping (78.83s) passed. Log:
`/tmp/milvus-53630-e2e.lA6mKU/e2e-all-formats-final-no-take.log`. Both take
settings therefore have full final-code read/refresh/reload coverage. Config
snapshots are `/tmp/schema-all-formats-configs.json` (true) and
`/tmp/schema-all-formats-configs-no-take.json` (false).

Data-source audit for the new physical-column state: generic explorer records
start with unknown inventory, `fetchRowCountsConcurrently` resolves every missing
inventory even when row counts are known, and splitting preserves it in each
fragment. Both initial manifest building and function-input manifest building
receive the grouped fragments. Existing manifest/deltalog reconstruction keeps
unknown metadata and its already-declared physical columns. Refresh patching
uses matched new fragments, so it cannot append a logical column using stale
inventory. No metadata error is converted into an empty inventory. The metadata
FFI's coarse error classification remains an explicit limitation.

The SDK's `NewLoadCollectionOption(...).WithRefresh(true)` does not populate
`LoadCollectionRequest.Refresh` in this checkout; the regression uses the
existing `RefreshLoad(NewRefreshLoadOption(...))` API, which does. The SDK option
bug is outside this patch.

Remaining acceptance work is explicit: no remote-store throttle/timeout
campaign, performance benchmark, or race-sanitizer run has been obtained.
Go statement coverage is 66.9% for packed and 91.8% for external; this does not
meet the repository 99% coverage target. The profile is
`/tmp/schema-all-formats-go-coverage.out`. Go refresh/packed production code
changed; protobuf was unchanged at that validation point. The subsequent
admission follow-up adds optional task result fields.
Scalar index building already has `CacheRawDataAndFillMissing`; function-input
readers are separate and their non-NULL-default behavior is not established by
these segcore tests. This implementation therefore does not claim complete
external function backfill, retry-classification improvements, automatic target
schema evolution, or an exact reproduction of the issue's image revision.

All future validation must continue to use this worktree and its own artifacts.
No design approval, commit, push, or PR publication is implied by this status.

### Index-backed historical fields: follow-up implementation

The query gate now checks actual loaded field/index accessibility. Both take
output paths additionally require every requested physical column to be present
in their captured manifest; otherwise they return to `bulk_subscript` before I/O.
No change to `CollectDataFields` or default-marker retirement is needed.

Validation for this extension:

- Rebuilt and installed this worktree's C++ core/test executable. 384 tests from
  27 suites passed (`/tmp/schema-index-unified-cpp-installed-test.log`). The new
  C API regression reads value 7 using only a loaded scalar index, with neither
  a physical column nor a default marker. Another regression checks search and
  retrieve bypass take before I/O for both internal and external schemas.
  The initial run linked the previous installed core: both new regressions
  failed, reproducing the old behavior (`/tmp/schema-index-unified-cpp-test.log`).
- The four-format SDK test now uses 2048 rows per source file/table (4096 total
  for the mixed Parquet fixture). It builds Bitmap indexes on permanently absent
  NULL/default columns, checks indexed row counts, then verifies Query filters,
  Query output and Search output after refresh-load and release/load.
  External take enabled: all four formats passed in 119.37s
  (`/tmp/milvus-53630-e2e.lA6mKU/e2e-index-unified-take.log`).
- The milvus-table fixture now retains 2048 historical rows without `revision`
  and a distinct new row with value 42. It builds a Bitmap index on `revision`
  for both the source internal table and external table and applies the same
  Query/Search assertions to both. Passed in 34.26s with external take enabled
  (`/tmp/milvus-53630-e2e.lA6mKU/e2e-index-unified-milvus-table-take.log`).
- Runtime logs confirm 12 actual `milvus_packed_bitmap_index.v3` writes across
  these runs (10 generic-format builds plus source/external historical-segment
  builds). The one-row new segment is allowed to skip indexing. Evidence is
  recorded without credentials in `/tmp/schema-index-unified-index-evidence.json`.

With external take disabled and internal take enabled, the source/internal and
milvus-table test passed in 93.74s; Parquet, Lance and Vortex also passed (50.01s,
39.00s and 35.60s). Iceberg's initial run failed before load on MinIO
`RequestTimeTooSkewed`; the host and Docker VM timestamps later matched again.
A retry then failed while connecting to the coordinator's old local-network IP,
before creating a collection. The task-owned standalone was restarted with
loopback component addresses for the remaining Iceberg validation. These are
fixture/environment failures, not successful acceptance runs. The original logs
remain in `e2e-index-unified-no-take.log` and
`e2e-index-unified-no-take-iceberg-retry.log` under the isolated environment root.

The isolated Iceberg rerun then passed in 28.30s with external take disabled
(`e2e-index-unified-no-take-iceberg-loopback.log`). All five external formats
therefore have successful real-index acceptance with take enabled and disabled;
the internal source was additionally checked with its optional take enabled.
`git diff --check` and the scoped clang-format diff check passed. No new coverage
measurement, race sanitizer, commit, or publication was performed for this
follow-up; the coverage limitations above remain explicit.

### Admission policy for wholly unmapped fragments

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

The temporary physical row-carrier fallback has been removed. Constructing a
manifest with no mapped physical columns is now an internal contract error,
since ordinary refresh must filter such fragments before manifest creation.

DataCoord's existing protection against an unexplained empty replacement is
retained. The worker's new `all_fragments_unmapped` flag is true only after a
non-empty fragment range is fully inspected and every fragment is excluded.
It travels through the task manager, task response and persisted task result.
Only if every finished, result-ready task carries this flag can aggregation
remove every existing segment without replacement. Contradictory flagged
results containing segments are rejected. Older workers/persisted messages
omit the flag (false), so they cannot accidentally authorize an empty refresh.
A newer worker with an older coordinator still fails the old empty-result
safety check instead of clearing data. No RPC or persisted field was renumbered;
Go bindings were regenerated with `make generated-proto-without-cpp`.

The focused SDK regression `TestExternalRefreshSkipsUnmappedFragments` passed
in 31.00s: initial/repeated empty refresh, mixed unrelated and all-NULL files,
partial-field default values, a loaded segment becoming wholly unmapped,
Query returning zero rows, repeated empty refresh, the same file range becoming
mapped again, and corrupt Parquet metadata failing refresh while preserving
the prior five visible rows. Log:
`/tmp/milvus-53630-e2e.lA6mKU/e2e-unmapped.log`.

The existing real-index regressions also passed with this admission filter:
milvus-table in 37.46s, and Parquet/Lance/Vortex/Iceberg in 119.02s combined
(`e2e-unmapped-formats.log` under the same environment root). These checks retain
the previous NULL/default, indexed Query/Search and refresh-load expectations.

Go validation passed for the external worker package, packed storage package,
and targeted DataCoord external-refresh tests. Statement coverage is 100% for
`Execute`, `filterMappedFragments`, `AllFragmentsUnmapped`, worker `UpdateResult`
and `SubmitTask`, and DataCoord `UpdateResultWithMeta`, `UpdateTaskResult` and
`ClearTaskResult`. Tests cover persistence failure, protobuf round-trip/reload,
mixed-task empty-result rejection, explicit all-empty publication, and rejection
of contradictory results. Profiles are `/tmp/schema-unmapped-external-final.cover`,
`/tmp/schema-unmapped-packed.cover` and `/tmp/schema-unmapped-dc-final.cover`.
The 99% gate for every changed production function is not established:
`applyFinishedJobSegments` is 92.6%, `applyExternalCollectionSegmentUpdate` 87.7%,
`createColumnGroups` 97.4%, `FetchFragmentsFromExternalSourceWithRange` 94.7%,
and `getManifestFieldIDsWithExtfs` 87.0%. Package percentages and targeted
DataCoord coverage are not evidence of whole-repository coverage. No new C++
coverage measurement or race run was performed for this admission follow-up.

After adding the early-retry flag reset, `make build-go` passed again
(`/tmp/schema-unmapped-build-go-final.log`). The rebuilt standalone passed the
focused admission E2E again (`e2e-unmapped-final.log` under the isolated root).
The external worker suite also passed again after adding an explicit regression
for cancellation clearing the previous attempt's all-unmapped flag. Formatting
and `git diff --check` passed. No commit or publication was performed.

This admission follow-up changes external source admission; internal-table row
retention is unchanged. Function-input defaults are implemented separately below.

The zero-row profile regression was reproduced before its fix on 2026-09-22.
The external worker package passed with both empty-profile orderings, explicit
all-zero rejection, allocation and metadata failures, and milvus-table 1:1
mapping beyond the virtual-PK worker limit. That run reported 99.4% statement
coverage for `balanceFragmentsToSegments` and 92.8% for the external package.
Log and profile: `/tmp/schema-zero-profile-after.log` and
`/tmp/schema-zero-profile.cover`.

The zero-row profile regression now uses Parquet as the representative of the
shared balancing path and retains both empty-profile orderings. The C++ named
column planner regression likewise uses one Parquet fixture for initial default
filling, default reuse, and both default/physical transitions. These tests do not
exercise format readers; actual format coverage remains in the SDK E2E tests.

### Deferred: index invalidation across backfill and refresh

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

### Function inputs: materialize missing values before execution

Internal backfill uses the upstream absent-field reader, while external function
execution overlays physically missing inputs before conversion. Both use
`storage.GenerateEmptyArrayFromSchema`. For an admitted row batch:

1. Determine absence from the segment's physical inventory, never from NULLs in
   an Arrow array. Preserve actual physical values, including NULL.
2. For a missing input, use the schema default when configured, otherwise NULL
   if nullable. Fail if neither is available. A non-nullable field with a default
   is valid; `GenerateEmptyArrayFromSchema` now accepts it and releases its Arrow
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
record from its reader and overlays only known-missing fields; conversion owns the copied values
and temporary Arrow arrays are released on success and failure. Homogeneous
physical-column groups are established by refresh before this stage. Required
missing inputs fail before opening the output writer. Output batches must match
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
  invalidation contract above remains necessary.

Current function E2E coverage:

- `TestExternalFunctionMissingInputs/bm25_minhash` checks missing-input defaults
  against explicit text, preserves physical NULL/empty text, and verifies exact
  query and BM25 hit ID sets. It runs independently of an embedding service.
- `TestExternalFunctionMissingInputs/embedding` uses the existing `tei_endpoint`
  and `tei_model_dim` flags. The endpoint must be reachable by both the test
  process and Milvus; an unavailable service skips only this subtest. Missing
  inputs and explicit text must produce equivalent vectors. One explicit-empty
  input failure checks the refresh reason and preserves the exact previously
  published ID/vector mapping.
- Provider HTTP/count/dimension fault injection and the separate empty-default
  E2E case were removed. Existing provider/input unit tests and the external
  batch-execution tests cover those lower-level contracts; the E2E no longer
  starts a loopback HTTP provider.
- After this simplification, the full Go client test package compiled with
  `-tags dynamic,test -gcflags='all=-N -l'`. The revised service-dependent E2E
  cases have not been rerun.

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

Rebase integration with current master retains upstream asynchronous manifest
loading, immutable runtime PK state, vector raw-index skipping, refresh ownership
and external result storage. The persisted all-unmapped flag uses field 24;
upstream owns fields 19 through 23. Internal backfill now reuses the upstream
reader/anchor implementation instead of the original branch-local materializer
helper. The historical measurements below predate this integration and do not
validate the rebased candidate.

Historical validation before the E2E simplification above, in the designated
worktree using its rebuilt standalone/core (these results do not validate the
current shared-TEI variant):

- `TestExternalFunctionMissingInputs` passed on the final binary in 27.39s with real Parquet refresh,
  index creation, load, Query and Search. BM25/MinHash default-generated results
  equal explicit text results; physical NULL/empty text stay distinct from a
  nonempty default. Wholly unmapped files contribute no rows. TextEmbedding used
  a deterministic local HTTP TEI test service. The then-current assertions
  passed for generated vectors and reported failed refreshes for provider error,
  wrong row count, wrong dimension, explicit empty text and an empty-string
  default. Those failure assertions checked terminal state without checking the
  specific reason; query assertions did not check the complete unique ID set.
  No paid or production embedding provider was used.
  Log: `/tmp/milvus-53630-e2e.lA6mKU/e2e-function-final.log`.
- The real StorageV3 partial-backfill regression creates a physical segment with
  only IDs/timestamps, adds a defaulted BM25 input, runs compaction and reads back
  three nonempty generated sparse vectors. Materializer tests cover nullable
  TEXT, physical NULL, selected rows, required-field and function failures.
- `make build-go`, the complete external worker package, the schema-bump suite
  and targeted materializer/storage tests passed. New shared helpers, external
  execution/input resolution/batch processing, and the internal read-projection
  helper, missing-value generation and `WrapWithSelection` have 100% statement coverage in the scoped profiles. The 99% gate for
  every touched existing function is still not met: partial backfill is 77.6%
  and the selected-record column accessor is 86.4% in the current profiles. This does not claim
  full-repository coverage or new function E2E coverage for every storage format.
- Profiles/logs: `/tmp/schema-function-storage.cover`,
  `/tmp/schema-function-external.cover`, `/tmp/schema-function-compactor.cover`,
  `/tmp/schema-function-build-go-final.log`. TEI provider and TextEmbedding bulk
  input tests also passed (`/tmp/schema-function-provider-test.log`). No new C++ build, race run, remote-store
  failure campaign or performance measurement was performed for this Go-only
  follow-up. Input resolution adds manifest metadata reads proportional to the
  number of function input fields, plus an anchor search when all are absent.

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
