# Compaction Data-Integrity E2E Design

## Status

This document records the design and execution contract for the Python compaction data-integrity E2E suite.

Phase 1A and Phase 1B together are the binding behavioral contract for the current PR and define seven L3 workloads: fixed V2 and V3 Insert/Compaction with INT64 PK, fixed V2 and V3 Import/Compaction with VARCHAR PK, V3 schema evolution with VARCHAR PK, V2 add/drop-field evolution with schema-bump compaction disabled and VARCHAR PK, and serialized V2-to-V3-to-V2 transition with VARCHAR PK.

Phase 1B implements the agreed Import ingress as a separate workload rather than mixing streaming and import mutations in one collection.

## 1. Objective

The E2E test must prove that a deterministic logical dataset remains safe throughout the Milvus data lifecycle:

```text
logical test data
    -> ingress
    -> persisted segment state
    -> compaction rewrite(s)
    -> serving handoff
    -> full retrieve verification
```

Data safety means:

- no expected row is lost;
- no row appears from nowhere;
- no duplicate row is exposed in the append-only logical result;
- the visible PK set is exact;
- every persisted field preserves its value and null/default semantics;
- a duplicate-PK extension must eventually prove that the later happens-before mutation wins.

Retrieve is the final data verification channel, but retrieve alone is insufficient because growing memory data or old loaded segments can hide persistence or handoff failures.

The complete proof therefore combines an independent test-side data oracle with server-side segment, compaction-task, lineage, and serving evidence.

## 2. Delivery Scope for the Current PR

Phase 1A covers:

- Python E2E only;
- fixed-storage V2 and V3 Insert/Compaction workloads with identical INT64-PK test behavior;
- one V3-only schema-evolution workload using VARCHAR PK;
- one independent V2 add/drop-field workload using VARCHAR PK with schema-bump compaction disabled;
- one `V2 -> V3 -> V2` rewrite workload using VARCHAR PK and running serially in a dedicated test job;
- L3-only execution in that dedicated job, excluded from per-PR E2E and ordinary Nightly jobs rather than appended as another stage of either pipeline;
- append-only unique-PK data;
- streaming Insert as the only ingress implementation;
- collection-level compaction triggering and observation;
- full row, column, and cell validation after every round;
- three rounds by default, with a one-round debug override;
- one V3-only DDL chain over an already persisted dataset: `init -> add_field -> add_function_field -> drop_field -> drop_function_field`.

Phase 1B additionally covers:

- Python Bulk Import through one deterministic Parquet payload and one import job per logical round;
- fixed-storage V2 and V3 Import/Compaction workloads with identical VARCHAR-PK behavior;
- import-job terminal-state and row-count evidence before the round delta enters the oracle;
- the same full data, segment-lineage, active-frontier, and serving-handoff proof used by Insert ingress.

The current delivery does not cover:

- Storage V1-to-V2 migration;
- Delete and Upsert semantics;
- duplicate-PK last-write-wins verification;
- index correctness as a subject under test;
- Target-based Compaction;
- REST E2E;
- fault injection, process restart, or long-term metadata GC;
- direct enumeration of raw physical rows inside object-storage files.

These exclusions constrain the first implementation without weakening the full-lifecycle data-safety objective.

Phase 1A can prove visible-row uniqueness and consistency between the expected row count and active-segment metadata, but it cannot prove that no hidden physical duplicate exists below Milvus MVCC and metadata projections.

## 3. Terminology and Ownership

### 3.1 CompactionIntegrityRound

`CompactionIntegrityRound` is one complete test round and owns all lifecycle steps for that round.

It is not an ingress abstraction.

One round performs:

```text
capture round-start evidence
    -> prepare RoundDataset
    -> execute one IngressStep
    -> establish ingress sync checkpoint
    -> verify the cumulative dataset
    -> request collection compaction
    -> establish final lifecycle checkpoint
    -> verify the cumulative dataset again
    -> validate round lineage effectiveness
```

### 3.2 RoundDataset

`RoundDataset` is only the deterministic logical input for one round.

It contains at least:

- `run_id`;
- `round_id`;
- rows submitted by this round;
- canonical expected cells indexed by physical PK;
- the round PK set;
- the `explicit_test_ts` range;
- expected row-count delta;
- enabled capability fields such as V3 TEXT.

### 3.3 ExpectedDatasetState

`ExpectedDatasetState` is the cumulative test-side oracle across all completed rounds.

For a multi-batch ingress, each batch delta is merged only after that batch has a definite successful outcome, while a single-job ingress advances only after its job reaches a definite successful terminal outcome.

Timeout, disconnect, cancellation, or any other unknown mutation outcome makes the round indeterminate, immediately stops the case, and permanently retires that collection from further validation.

The test must not resolve an unknown mutation by querying Milvus because that would make the server result decide the reachability of the oracle used to verify the same result.

### 3.4 IngressStep

`IngressStep` is one replaceable step inside `CompactionIntegrityRound`.

Its responsibilities are limited to:

- converting `RoundDataset` into the ingress-specific payload;
- submitting the ingress operation;
- returning its operation identity and definite acceptance result;
- waiting for the ingress-specific completion fence;
- exposing ingress-specific evidence for logs and assertions.

It does not trigger general compaction, construct the common checkpoint, validate the dataset, or decide round success.

### 3.5 InsertIngress and ImportIngress

`InsertIngress` is the Phase 1A implementation of `IngressStep`, while `ImportIngress` is the Phase 1B implementation.

They share the same `RoundDataset`, cumulative oracle, checkpoint logic, lineage reconstruction, and full-data verifier.

They differ only in how data enters Milvus and how definite ingress completion is observed.

## 4. Suggested Test-Side Contracts

The design favors a small protocol and value objects rather than a deep inheritance hierarchy.

```python
class IngressStep(Protocol):
    name: str

    def submit(self, ctx, dataset: RoundDataset) -> IngressReceipt:
        ...

    def wait_completed(self, ctx, receipt: IngressReceipt) -> IngressEvidence:
        ...
```

`IngressReceipt` should contain:

- ingress kind;
- operation or job IDs;
- definite submission outcome;
- submitted file or batch identities;
- expected row delta;
- ingress-specific metadata needed for diagnostics.

`IngressEvidence` should contain:

- the observed terminal ingress state;
- reported imported or written row count when available;
- progress and failure reason when available;
- timestamps and elapsed time;
- evidence useful for diagnosis but not reused as the data oracle.

The round runner can be represented conceptually as:

```python
run_compaction_integrity_round(
    round_dataset,
    ingress_step,
    expected_dataset_state,
    previous_checkpoint,
) -> RoundResult
```

## 5. Round Lifecycle

### Step 1: Capture round-start evidence

Before mutation, capture:

- all retained collection segments, including Dropped;
- the derived active segment set;
- the actual serving segment set;
- all retained collection compaction tasks;
- the current lineage DAG;
- storage versions seen in active segments.

This snapshot is the baseline for attributing later roots and compaction edges to the current round.

Ingress must not begin until this baseline is stable for multiple consecutive polls, has no in-flight collection compaction task, has every active DataCoord segment in `Flushed`, and has identical active and serving segment IDs.

### Step 2: Build the round dataset

Generate deterministic rows whose field values are derived from:

```text
(test_run, physical_pk, explicit_test_ts, field_id[, struct_element_index][, vector_element_index])
```

`explicit_test_ts` is monotonically increasing within a serial test case and aligns with the test-side happens-before order.

### Step 3: Execute the ingress step

Phase 1A calls `InsertIngress` with the round's `RoundDataset`, while Phase 1B reuses the same logical contract with `ImportIngress`.

Only a definite successful ingress outcome may advance the oracle.

### Step 4: Establish the ingress sync checkpoint

The ingress-specific completion fence is necessary but does not by itself prove persistent serving correctness.

The common ingress checkpoint additionally requires:

- mutation has stopped for the collection;
- no active Growing, Flushing, or Importing segment remains;
- active data-side segment IDs equal actual serving segment IDs;
- every active DataCoord segment is exactly `Flushed`;
- serving-side entries represent sealed serving segments;
- active segment metadata accounts for the cumulative expected row count;
- observations remain unchanged for multiple consecutive polls.

DataCoord `Sealed` is not accepted as persistent because it is still an unflushed state, whereas `Flushed` means the persistence metadata and binlog paths have been committed.

This explicitly excludes the possibility that successful retrieval is being served only by growing memory data or that a merely sealed segment is mistaken for durable storage.

### Step 5: Verify the cumulative dataset

Execute the full validation inside a stable-frontier fence:

```text
checkpoint-before
    -> retrieve every row and every output field
    -> checkpoint-after
```

Both checkpoints must independently satisfy the common readiness rules, and their active and serving frontier signatures must be identical.

If the frontier changes during Retrieve, discard that validation attempt and repeat the complete checkpoint-before, Retrieve, checkpoint-after sequence until it succeeds or the round timeout expires.

Only a validation attempt enclosed by identical frontiers may be attributed to that checkpoint and compared with `ExpectedDatasetState`.

This validation is performed at the ingress checkpoint even when mandatory or automatic compaction has already occurred during ingress settlement.

### Step 6: Request collection-level compaction

Call `compact()` and retain the returned job ID.

A non-negative compaction ID is a useful handle for the accepted manual trigger, but it does not prove successful execution or serving adoption.

`compaction_id == -1` means the request returned successfully but produced zero plans, so it is not actual job evidence.

Regardless of the returned ID, a compaction task proves only that scheduling or execution was attempted and cannot prove that a data transformation completed.

Completed transformation requires an attributable lineage path with at least one new edge, every consumed source `Dropped`, and at least one terminal output descendant `Flushed`.

Successful adoption additionally requires the consumed sources to be absent from both active and serving sets and a terminal `Flushed` output descendant to be present in both active and serving sets.

The test neither specifies a source subset nor requires every active segment to participate.

### Step 7: Establish the final lifecycle checkpoint

Wait until:

- the collection-wide retained task set has no in-flight task for multiple stable polls;
- active segments are stable;
- active segment IDs equal serving segment IDs;
- every active DataCoord segment is `Flushed` and every serving-side entry is `Sealed`;
- the cumulative active row count is correct;
- the active storage-version set matches the fixed deployment expectation;
- the round has an attributable lineage transformation.

### Step 8: Verify the cumulative dataset again

Run the same checkpoint-before, full Retrieve, checkpoint-after fenced verification after the final lifecycle checkpoint.

### Step 9: Validate round effectiveness

The round-start baseline must itself be a stable checkpoint with no in-flight collection compaction task, every active DataCoord segment `Flushed`, and identical active and serving segment IDs.

The baseline records all retained segment IDs before the round's ingress starts, and the final graph computes `new_segment_ids = final_all_segment_ids - baseline_all_segment_ids`.

A round root is attributable to the round only when it belongs to `new_segment_ids`, its `compaction_from` list is empty, it has non-zero rows, and its collection, partition, and insert channel match the test's expected ownership scope.

The test uses a dedicated collection, serial mutation, and no external writers so that a new root created inside this window cannot belong to another producer.

The round is compaction-effective only if at least one attributable round root is a source or ancestor of a new lineage edge before the final checkpoint.

Every consumed source on an accepted lineage path must be `Dropped`, while an intermediate target may also be `Dropped` only when the graph connects it to a later descendant.

At least one terminal descendant on the accepted path must be `Flushed` at the final checkpoint.

For the transformation result to be considered used, consumed sources must be absent from active and serving sets while an attributable terminal `Flushed` descendant is present in both sets.

If the explicit `compact()` produces no plan because automatic or mandatory compaction already consumed the new roots, the round remains valid only when that earlier transformation is present in the round-wide lineage delta.

If the round-wide lineage delta contains no compaction of the round's data, the round fails as an ineffective compaction test even if all data is readable.

## 6. InsertIngress

`InsertIngress` uses the streaming write path.

The default load shape is ten logical batches of 1,000 rows per round, with Insert followed by Flush for every batch.

These are test-side flush batches and must not be described or asserted as ten physical segments because server-side splitting, channels, sorting, and automatic compaction determine physical segment shape.

Its definite ingress evidence consists of:

- every Insert call completing successfully;
- every Flush call completing successfully;
- all submitted PKs belonging to the round oracle;
- the common ingress checkpoint proving that no hot growing data is masking persistence.

`InsertIngress` builds each batch's rows and expected canonical cells in temporary structures, submits the batch, verifies the returned insert count, and only then merges that batch into `ExpectedDatasetState`.

The successful Insert response and exact returned insert count are the `MutationCommitted` boundary, so the batch delta enters the oracle before Flush begins because a later Flush failure cannot undo an already accepted mutation.

If an Insert or Flush result is unknown, the round stops and the collection is discarded even when earlier batches completed successfully.

A no-error Flush response is only an operation acknowledgment and is not treated as proof that old growing data is no longer serving.

For storage modes where an original flushed segment is invisible until SortCompaction produces a sorted visible target, the retained `allSegments` history and `compaction_from` edges provide the origin-to-visible lineage evidence.

## 7. Phase 1B: ImportIngress

`ImportIngress` uses the Bulk Import path and submits one import job for one logical round by default.

Each fixed-storage Import workload runs three complete rounds by default, with an environment override for one-round local diagnosis, and each round contains 10,000 deterministic rows by default.

The import file may be internally chunked for manageable generation and upload, but the test does not require a fixed number of import tasks or physical segments.

The implementation deliberately configures one sufficiently large writer chunk and asserts one emitted file group so that the default workload has one auditable import-job identity, while remaining agnostic to server-side task and segment fan-out.

Its definite ingress evidence consists of:

- Bulk Import submission returning a job ID;
- polling that job to `ImportCompleted`;
- progress reaching 100;
- reported imported rows matching total rows and the round delta;
- the common ingress checkpoint proving serving convergence.

The existing Python SDK path supports `utility.do_bulk_insert()` and `utility.get_bulk_insert_state(job_id)`, while the REST V2 path supports create and describe import-job APIs.

The Python `MilvusClient` object itself does not currently expose an equivalent first-class import method, so Phase 1B may reuse the existing Python `utility` interface without adding a new server API.

Internally, DataCoord exposes a more detailed import state machine, while the legacy Python interface collapses Sorting, IndexBuilding, and auto-commit progress into a broad running state.

`ImportCompleted` is nevertheless a strong terminal fence because DataCoord sets it only after all vchannels acknowledge the commit fence.

`ImportCompleted` does not expose segment IDs, lineage, or QueryCoord serving adoption, so it is necessary but insufficient and must be combined with the common segment and serving checkpoint.

When SortCompaction is enabled for imported data, the round lineage should show the imported source and its sorted target through `compaction_from`.

The workload requires the startup-only `dataCoord.sortCompaction.enable=true` behavior and proves that prerequisite operationally by requiring every accepted active frontier to be `Flushed` and sorted and every non-empty round to expose an attributable origin-to-sorted lineage path.

Phase 1B preserves every server-visible value emitted by the Parquet writer, but its current writer contract cannot distinguish nullable Struct Array `None` from an empty array and nullable sparse-vector `None` from an empty sparse vector; therefore ImportIngress uses empty/non-empty Struct Arrays and deterministic non-empty sparse-vector fingerprints, while InsertIngress remains the authoritative null-semantics coverage for those two fields.

## 8. Segment and Serving Evidence

The test uses three related but distinct views.

### allSegments

`allSegments` contains all retained segments in states:

- Growing;
- Sealed;
- Flushing;
- Flushed;
- Importing;
- Dropped.

Dropped segments are required to reconstruct lineage after the sources leave the active set.

### activeSegments

`activeSegments` is a test-side semantic projection of non-Dropped collection segments rather than a required public interface name.

At a stable persistence checkpoint, every active data-side segment must be `Flushed`, while Growing, Sealed, Flushing, and Importing are all non-ready states.

### servingSegments

`servingSegments` is the actual QueryCoord/QueryNode distribution view used to determine which segments can answer reads.

The general serving interface must include growing segments when they are serving, even though a valid stable checkpoint requires that none remain.

The checkpoint compares IDs rather than assuming that target metadata alone proves successful distribution.

If CurrentTarget contains a segment that is missing from actual distribution, `activeSegments == servingSegments` does not hold and the checkpoint continues waiting.

## 9. Compaction Evidence and Lineage

The test observes the complete retained collection task set, regardless of whether tasks were triggered manually, automatically, or as mandatory sorting.

Each task record should expose:

- task ID;
- compaction type;
- state;
- sources;
- targets;
- failure reason.

Task quietness means only that no in-flight task exists during the stable observation window and does not promise that the automatic scheduler will never create another task.

The lineage graph is reconstructed from segment ID and repeated `compaction_from` relationships.

The graph is a DAG and must support N-to-M transformations, so the test must not assign a single father or a scalar generation number.

The useful proof is transition from one active set to another active set with attributable graph edges and preserved data, not a claim that every lineage branch has the same depth.

Task records are supporting scheduling and failure diagnostics only, while lineage plus segment states prove completed transformation and active/serving membership proves adoption.

### Public Proto and SDK Contract

Milvus and the local editable PyMilvus use the merged public proto revision
`ae7fea6ab2f4e958f2feef0f0edb9a0d23fa7e0c` from milvus-proto PR #668.

`CompactionMergeInfo.type` and `.state` are public protobuf enums, not strings;
Milvus translates the internal task enums at the output boundary without changing
persisted task states, cleanup behavior, failure reasons, or task selection.

PyMilvus regenerates its bindings against this revision and normalizes the enums
to the existing `Plan.compaction_type` and `Plan.state` string contract, including
`BumpSchemaVersionCompaction`, `cleaned`, and `meta_saved`.

Unknown numeric enum values remain explicitly unknown with their numeric value
retained and must never be interpreted as successful completion.

The response-level legacy `CompactionState` remains distinct from each plan's
`CompactionTaskState`, and multi-target lineage plus retained failure reasons
must survive protobuf serialization and SDK decoding unchanged.

Local verification uses this editable SDK; the dedicated test job must likewise
use a compatible SDK build, while changing the shared per-PR or Nightly SDK pin
is not required by these L3 workloads.

### 9.1 Persistent Audit Evidence

Every round must emit machine-readable audit evidence even when the test passes.

The retained pass-path evidence must include:

- run and round identity;
- ingress type and successful batch identities;
- complete all-segment, active-segment, serving-segment, and compaction-task snapshots at round start, ingress checkpoint, every Retrieve boundary, and final checkpoint;
- attributable round roots;
- source-to-target lineage edges;
- source and target states, task states, and task failure reasons;
- pre-Retrieve and post-Retrieve frontier signatures;
- active and serving segment IDs;
- storage versions and segment row counts;
- expected and retrieved row counts;
- expected and retrieved PK-set digests;
- validated field count and cell count.

Failure evidence must additionally retain the last complete observation, all observed task failure reasons, missing or unexpected PK summaries, duplicate visible PK summaries, and bounded canonical-byte mismatch samples.

If a Retrieve attempt is discarded because its frontier changed, both boundary signatures and the retry count must be logged.

## 10. Task Failure and Recovery Semantics

An old compaction task may terminate as failed or timeout and later be replaced by a new successful task.

This eventual recovery is acceptable when the final lineage, active set, serving set, and dataset are correct.

The test therefore logs terminal failures and their reasons but does not fail immediately on the first failed compaction task.

The round fails when no recovery reaches a valid checkpoint before timeout or when the final evidence is inconsistent.

Import job failure is different because it is the ingress operation's terminal outcome and prevents the round delta from entering the oracle.

## 11. Checkpoint Consistency Model

Phase 1A does not require a server snapshot revision shared by DataCoord and QueryCoord.

It instead establishes a quiescent lower-bound checkpoint by:

- stopping test mutations;
- polling all relevant views;
- requiring task quietness;
- requiring active/serving equality;
- requiring identical evidence for multiple consecutive polls.

The checkpoint is a lower bound because the server may have advanced beyond the test-observed logical point, but with mutation stopped the cumulative expected dataset must still match exactly.

### 11.1 Retrieve Stability Fence

The pre-Retrieve checkpoint proves the intended active and serving frontier is ready before validation starts.

The post-Retrieve checkpoint proves that the same frontier remained adopted throughout the potentially long full-row scan.

The compared frontier signature must include at least active segment IDs and states, serving segment IDs and states, storage versions, row-count metadata, and the absence of in-flight collection compaction tasks.

Lineage and terminal task history may grow without changing the serving frontier, so diagnostic history need not be byte-identical when the active and serving frontier signature remains unchanged.

## 12. Full Data-Integrity Oracle

The verifier must scan all rows through Query Iterator or equivalent paginated Retrieve rather than sampling or stopping at the first corrupt batch.

It validates:

- exact cumulative row count;
- exact PK set;
- no duplicate visible result row for a PK;
- no missing or unexpected PK;
- all requested fields present;
- exact null/default behavior;
- schema-aware canonical bytes equality for every cell.

The visible-row check plus active-segment row-count equality is the strongest phase-one client-visible duplicate protection and must not be described as direct proof of raw physical-row uniqueness.

Field coverage includes:

- INT8, INT16, INT32, and INT64;
- BOOL, FLOAT, and DOUBLE;
- INT64 and VARCHAR primary keys through parameterization;
- VARCHAR and variable-length payloads;
- JSON and dynamic fields;
- nullable/default fields;
- arrays;
- nullable Struct Array parents covering `None`, empty arrays, and ordered arrays of one through four elements;
- every Struct Array scalar subfield type plus FLOAT_VECTOR, BINARY_VECTOR, FLOAT16_VECTOR, BFLOAT16_VECTOR, and INT8_VECTOR subfields;
- FLOAT_VECTOR, BINARY_VECTOR, FLOAT16_VECTOR, BFLOAT16_VECTOR, SPARSE_FLOAT_VECTOR, and INT8_VECTOR;
- nullable vector variants where supported;
- V3 TEXT/LOB through a capability-controlled parameter.

Dense vector dimensions are fixed at 16 for this lifecycle suite because the oracle validates every element exactly, while high-dimensional boundary and performance coverage belongs to vector-specific tests.

The ordinary Insert case owns Struct Array coverage, while the V3 DDL case deliberately omits Struct Array and uses one top-level field for each of FLOAT_VECTOR, BINARY_VECTOR, FLOAT16_VECTOR, BFLOAT16_VECTOR, SPARSE_FLOAT_VECTOR, and INT8_VECTOR.

Across the suite every nullable field must contain both `None` and non-null values, with InsertIngress providing per-round null coverage for every nullable field and ImportIngress retaining per-round null coverage for every Parquet-representable nullable field.

Every field with a default value must cover omitted input resolving to the default, explicit `None` resolving according to the schema's nullable/default contract, and an explicit non-default value in every round.

The expected oracle stores the resolved server-visible value for these three default-value input forms rather than copying the raw submitted representation.

FLOAT and DOUBLE test values must be deterministically generated in exactly representable ranges so byte equality tests storage integrity instead of client-side rounding accidents.

Canonical equality normalizes semantically equivalent client encodings before byte comparison but compares every vector and array element without sampling.

The verifier aggregates corruption counts across the complete dataset, including affected rows and PKs, issue kinds, per-field damaged-cell counts, and bounded byte-difference samples, and fails once after the iterator is exhausted.

Struct Array canonical equality recursively preserves parent null/empty semantics, element count and order, the complete subfield set, fixed subfield identity, and every nested scalar or vector byte.

## 13. Collection, Partition, and Shard Ownership

The final data ownership key is `(collection, logical_shard, partition)`.

The Python client cannot directly retrieve one logical shard, so row-level validation is collection- or partition-scoped and naturally spans shards.

Segment metadata can still expose insert channel information to reconstruct shard placement evidence.

Partition movement and cross-collection contamination are important lifecycle invariants but remain separate follow-up cases rather than expanding the Phase 1A Insert case.

## 14. V3 Schema-Evolution Lifecycle Case

The Phase 1A DDL case is independent from the ordinary Insert-round case because its subject is schema-reconciliation compaction rather than manual Mix-compaction scheduling.

Its deployment prerequisite is `dataCoord.compaction.bumpSchemaVersion.enabled=true`, which is already set by the real E2E Helm profiles and must also be enabled for local heavy-mode verification.

It uses one collection, one partition, one shard, and one initial ingress of five Insert-plus-Flush batches containing 5,000 rows each, while treating these as logical batches rather than asserting any physical segment count.

The collection starts with the ordinary scalar and variable-length fields, all six top-level vector types, a retained V3 `TEXT`/LOB field, an analyzer-enabled `VARCHAR` field, and a `sparse_base` BM25 function output plus its bound index, while Struct Array remains isolated to the ordinary Insert case.

Every analyzer input row contains a deterministic PK-derived unique token, and the independent test oracle maps every token to its expected PK.

The lifecycle checkpoints are:

- `C0`: the initial data is fully persisted and serving, all ordinary cells match the canonical oracle, and exhaustive `sparse_base` top-1 search establishes the BM25 control baseline;
- `C1`: `added_default_payload` has been added, every historical row returns its deterministic default, an attributable successful schema-rewrite task and schema-version convergence prove reconciliation, and all retained data plus `sparse_base` remain correct;
- `C2`: `sparse_added` and its BM25 function have been added over the same input field, the bound index is ready, all ordinary cells remain correct, and exhaustive searches prove `sparse_added` has the same expected top-1 PK and score as `sparse_base` for every unique token;
- `C3`: `added_default_payload` has been dropped, the replacement rewrite has observable lineage with dropped sources and adopted targets, the removed field is absent, and all remaining ordinary fields plus both BM25 outputs remain correct;
- `C4`: the added BM25 function field has been dropped, the replacement rewrite and serving handoff are complete, the removed function/output/index are absent and unusable, and all retained ordinary fields plus `sparse_base` remain correct.

`C1` and `C2` use a lineage-optional schema-transition policy because additive V3 reconciliation may either keep the same segment IDs through a dedicated Bump task or be absorbed by a concurrent ordinary rewrite that replaces the active frontier.

A lineage-optional transition is accepted only when the collection schema version advances, collection schema-version statistics converge, the collection task delta contains a successful `BumpSchemaVersionCompaction`, `MixCompaction`, or `SortCompaction` whose target is on or reaches the final active frontier, the active DataCoord frontier is entirely `Flushed`, and the same frontier is serving.

`C3` and `C4` use a replacement schema-transition policy and require a new lineage edge, every accepted source to be `Dropped`, an active `Flushed` target descendant, and active-to-serving frontier equality.

At every `C0` through `C4`, ordinary-field validation and the stage-appropriate BM25 validation execute inside `checkpoint-before -> complete validation -> checkpoint-after`, and the entire validation attempt is retried if the active or serving frontier changes.

Every checkpoint persists the collection schema version, field/function identity, complete retained segment and task snapshots, active and serving frontiers, accepted lineage edges, expected row/field counts, and BM25 validation summary for CI postmortem analysis.

The add-field assertion knowingly combines attributable successful schema-rewrite evidence, schema convergence, and retrieved default values because the current client evidence cannot independently distinguish a physically materialized default column from read-side fallback.

### 14.1 V2 Add/Drop Field with Schema Bump Disabled

This independent L3 case keeps `common.storage.useLoonFFI=false` and
`dataCoord.compaction.bumpSchemaVersion.enabled=false` for its entire lifecycle.
It reuses the exclusive storage-config fixture and config preservation helper,
waits the existing 10-second adoption allowance after disabling Bump, and restores
and verifies both original configurations on success and failure.
An etcd read-back plus the allowance is not an authoritative per-instance config
acknowledgment; persisted V2 segments and the absence of Bump tasks supply the
observable execution evidence.

The dataset uses one partition and one shard, VARCHAR PK, five Insert/Flush batches
of 5,000 rows by default, all six top-level vector types, and the existing scalar,
array, JSON/dynamic, null/default fingerprints, without TEXT, Struct Array, or
function fields; existing DDL batch-count and batch-size overrides still apply.

Before the DDL sequence, choose existing field Y using a local seeded RNG from
the ordinary fields that are present in both the initial schema and the oracle:
`int8_value`, `int16_value`, `int32_value`, `int64_value`, `double_value`,
`varchar_payload`, `float_array`, and `string_array`.
These fields contain actual written values and have no index/function dependencies;
PK, ordering, dynamic, vector, and added-field names are excluded.
Record the seed and selected Y at C0 and every DDL checkpoint; the default seed is
the unique collection name, and `MILVUS_COMPACTION_INTEGRITY_DDL_DROP_SEED` replays
the selection without altering the process-wide RNG.

The four checkpoints are:

- `C0_initial`: the original dataset is Flushed, sorted, serving entirely as V2,
  and every row and cell matches the independent oracle;
- `C1_add_field`: add the nullable INT64 `added_default_payload` with its fixed
  default, request ordinary compaction, and verify the default on every old row
  plus exact equality of every retained field;
- `C2_drop_existing_field`: drop original field Y, request ordinary compaction,
  remove only Y from the expected schema/oracle, verify X's default on every row
  plus every other retained cell, and confirm Y has no readable values;
- `C3_drop_added_field`: drop added field X, request ordinary compaction, remove
  X from the expected schema/oracle, verify every remaining row and cell, and
  confirm both Y and X remain absent from the schema and have no readable values.

All three DDL checkpoints require schema-version convergence, a new successful
`MixCompaction` whose output reaches the final active frontier, replacement of
every pre-DDL active segment through valid lineage with Dropped sources, V2-only
Flushed/sorted active segments, matching sealed serving segments, and a full
Retrieve verification enclosed by stable frontier checkpoints.
No Bump task may appear in the dedicated collection's retained task history.
The manual job ID is retained for diagnosis, but automatic Mix rewrites are equally
acceptable and a manual zero-plan result is not an automatic failure.
The common bounded schema/checkpoint waits fail when no effective rewrite occurs;
there is no unbounded wait, skip, or query-only fallback to a passing result.

As with V3, default retrieval alone does not prove physical materialization;
the accepted evidence is the combined schema convergence, Mix rewrite, lineage,
serving adoption, and exact dataset result, not inspection of raw storage files.
Because dynamic fields remain enabled, the dropped name can legally resolve to
an absent dynamic key instead of raising an unknown-field error, so the negative
read assertion requires zero rows for `exists <dropped_field>` for every field
already dropped at that checkpoint, not only the field dropped most recently.

## 15. Storage-Version Strategy

Storage version is a deployment parameter rather than a property hard-coded into the test case.

The fixed-version test is parameterized into explicit V2 and V3 workload nodes, and each node owns the target storage-version configuration before dataset creation rather than relying on the deployment's initial value.

The two nodes otherwise execute the same INT64-PK lifecycle behavior except for storage-version capabilities such as V3 TEXT.

The V3 DDL workload likewise switches to and verifies V3 before dataset creation, while the transition workload first switches to and verifies V2 before making its controlled V3 and V2 configuration changes.

All seven L3 workloads run serially in a dedicated test job because they update the same cluster-wide dynamic storage configuration.

They are not part of per-PR E2E or ordinary Nightly jobs, including optional post-parallel stages of those jobs.

The test-side controls have separate responsibilities:

- `compaction_data_integrity_serial` identifies this workload group for precise selection and for the execution guard, while its registration in `pytest.ini` only declares the marker;
- `--run-compaction-integrity-serial` explicitly opts into these configuration-mutating workloads, which are skipped without the flag;
- the xdist-worker guard rejects parallel worker execution, and the dedicated job invokes pytest with `-n 0`;
- the etcd connection options identify the deployment's endpoint, root path, and optional credentials, while fixtures pass those values to the workload's config controller.

Neither the L3 label, the marker, nor `-n 0` provides cross-job isolation: the dedicated job must own the target Milvus deployment and its etcd configuration namespace for the entire workload, including config restoration, without overlapping other jobs or clients that depend on those settings.

The job must archive the pass-path and failure-path audit evidence required by Section 9.1; successful-test audit retention requires both the workload marker and `persist_on_pass=True` records, while unmarked tests retain their existing JSON, HTML, and worker-report formats without an added empty `logs` field.

The shared `tests/scripts/ci_e2e.sh` remains unchanged from the PR base; the dedicated job instead uses `tests/scripts/ci_compaction_integrity.sh`, which starts exactly one pytest invocation scoped to the workload class, preserves caller options, and fixes execution to `-n 0`.

The dedicated job must first run the server-independent unit step below with its
compatible observability SDK, and stop on any nonzero exit status before running
the E2E step; each step remains a separate, single pytest invocation:

```bash
CI_LOG_PATH=/tmp/compaction-integrity-unit \
  bash tests/scripts/ci_compaction_integrity_unit.sh
```

This entry explicitly collects `milvus_client/compaction_integrity_helper_tests.py`
and the data-integrity module, selecting `CompactionIntegrityUnit` to run both
the SDK-dependent helper tests and the module-level oracle tests without any
E2E class or server connection; the unit-test filename intentionally stays outside
default pytest discovery so the older shared SDK can still collect ordinary
E2E tests, and no shared SDK pin, runner, or global import-error handling changes.

After preparing the Python environment and exclusively reserving the deployment, a dedicated job invokes the entry from the repository root, for example:

```bash
CI_LOG_PATH=/tmp/compaction-integrity-job \
  bash tests/scripts/ci_compaction_integrity.sh \
  --host 127.0.0.1 --port 19530 \
  --etcd_host 127.0.0.1 --etcd_port 2379 --etcd_root_path by-dev \
  --minio_host 127.0.0.1 --minio_bucket a-bucket
```

The addresses and bucket above are examples that the job must replace with its deployment values; job provisioning, dependencies, timeouts, and artifact archival remain the job's responsibility, and no per-PR or Nightly pipeline is extended by this entry.

Each invocation must use its own `CI_LOG_PATH` so reports are not overwritten by another run; `--collect-only` and `-k` remain available for selection checks and targeted diagnosis without a second hidden pytest invocation.

Before each workload, the test records the original dynamic config, updates the Storage V3 feature flag through the test deployment's etcd endpoint, and verifies the committed value and revision; when the target schema contains V3-only TEXT, creation of the actual workload collection retries only the specific pre-adoption rejection for up to 30 seconds, so successful creation proves Proxy adoption without an extra probe collection, while later Flushed-segment checkpoints prove the persisted storage version.

After each workload, including failure paths, the test restores and verifies the exact original etcd config value; this write-back alone is not claimed as proof that every server component has already adopted the restored value.

The transition workload additionally validates both directional lineage rewrites and fenced datasets while it owns the same serialized config lifecycle.

## 16. Test Matrix and Delivery Phases

Phase 1A adds four independent test methods expanded into exactly five L3 workloads in the existing data-integrity Python file without modifying Binbin Lv's existing cases:

```text
FixedStorageV2InsertCompaction(INT64 PK)
FixedStorageV3InsertCompaction(INT64 PK)
V3SchemaEvolutionLifecycle(C0 -> C1 -> C2 -> C3 -> C4, VARCHAR PK)
V2AddDropFieldMixLifecycle(C0 -> add X -> drop existing Y -> drop X, bump disabled, VARCHAR PK)
ExclusiveStorageTransitionLifecycle(V2 -> V3 -> V2, VARCHAR PK)
```

The two fixed-storage workloads each run three rounds by default and validate every round boundary plus the final cumulative dataset, the V3 DDL workload performs one fixed ingress followed by four schema transitions, the V2 DDL workload performs one fixed ingress followed by add X / drop existing Y / drop X with an ordinary Mix rewrite after each step, both DDL workloads validate every checkpoint, and the transition workload proves both storage rewrite directions under serialized ownership of the cluster-wide setting.

PK type is fixed per workload so the suite covers both PK encodings without multiplying every storage-version path, while storage version remains the explicit workload dimension for fixed Insert/Compaction.

Phase 1B adds one independent Import method expanded into two fixed-storage workloads after Phase 1A is stable under both Storage V2 and Storage V3:

```text
FixedStorageV2ImportCompaction(VARCHAR PK)
FixedStorageV3ImportCompaction(VARCHAR PK)
```

The Import method uses one 10,000-row import job per round and three rounds by default, and it does not reuse Insert-only batch naming or imply any controllable physical segment count.

## 17. Round Acceptance Criteria

A round passes only when all of the following are true:

- ingress has a definite successful outcome;
- the ingress-specific completion fence is satisfied;
- the common ingress checkpoint excludes growing-memory-only serving;
- every active DataCoord segment is `Flushed` and its serving counterpart is `Sealed`;
- the cumulative dataset exactly matches the independent oracle inside an unchanged pre/post Retrieve frontier;
- a collection-level compaction request was made;
- at least one attributable round root participated in a completed lineage transformation;
- collection compaction tasks reached a stable terminal view;
- the transformed source is absent from active and serving sets;
- an attributable transformed target descendant is `Flushed`, stable, and present in the serving set;
- the final active set is stable and equals the serving set;
- the final cumulative dataset again matches the oracle inside an unchanged pre/post Retrieve frontier.

Passing Retrieve without the server-side evidence is insufficient, and passing server-side state checks without the complete data oracle is also insufficient.
