# Continuous Streaming Data Integrity E2E Test Design

## 1. Background and Scope

Phase 1 is not a general-purpose DataIntegrity Test for common user workloads. It is a Compaction-DataIntegrity Test dedicated to validating data integrity across the compaction lifecycle. It uses a batch model that writes one batch, waits for compaction to finish, and then verifies the data. Its primary goal is to prove that data remains complete and correct after compaction rewrite and serving handoff, rather than to validate general data integrity under continuous mutations.

Real users usually do not pause ingestion between batches or trigger compaction manually. Instead, they continuously ingest and modify data through Insert, Upsert, Delete, or continuous Import operations while Milvus automatically performs sync, seal, compaction, and serving handoff in the background.

Phase 2 therefore adds continuous streaming cases on top of the test framework established in Phase 1. These cases validate data correctness while background lifecycle changes run concurrently with sustained mixed ingress. All Phase 1 cases remain unchanged and are neither replaced nor removed.

## 2. Core Question

Phase 2 answers one core question: while users continuously ingest and modify data through different ingress paths and Milvus concurrently rewrites segments and switches the serving dataset, can every operation that is known to have succeeded still be read completely, uniquely, and accurately?

The test must maintain an independent expected state and must never use query results to prove the correctness of those same query results. Every planned mutation batch and import job must complete successfully in full. A partial failure, timeout, cancellation, disconnection, count mismatch, or indeterminate result may already have produced a partial write, so any such outcome must immediately terminate the entire case, mark the collection as unsuitable for further correctness judgments, and prohibit further replay or a passing result.

## 3. Test Model

Multiple ingress lanes generate data concurrently. Each `MutationIngress` lane continuously performs Insert, duplicate-PK Insert, Upsert, Delete, and Reinsert operations, while the `ImportIngress` lane continuously submits and completes import jobs. Operations remain ordered within each lane, while different lanes progress concurrently.

A "data prefix" is neither the first N rows in PK order nor the data that happens to be visible at a wall-clock instant. It is a stable data range defined by a test-side logical sequence. The test assigns every action a globally unique, monotonically increasing logical sequence. Insert, Upsert, Reinsert, and Import persist this value in the row's `test_sequence_id`. Delete produces no new row, so its operation sequence is retained only in the oracle and audit log within the same sequence domain. Sequence values may contain gaps. A checkpoint boundary X requires only that every allocated action whose logical sequence is less than or equal to X has reached a definitive outcome. Different lanes use non-overlapping PK ranges, allowing their boundaries to be merged into one unambiguous global expected dataset.

```text
Time ---------------------------------------------------------------------->

MutationIngress A   Epoch-1✓ | Epoch-2✓ | Epoch-3✓ | Epoch-4✓ --+
MutationIngress B   Epoch-1✓ | Epoch-2✓ | Epoch-3✓ | Epoch-4✓ --+ Mixed ingress
ImportIngress C       Job-1✓ |   Job-2✓ |   Job-3✓ |   Job-4✓ --+
                              |          |          |              |
                            Prefix-1   Prefix-2   Prefix-3       Pause
                              |          |          |              |
Validator              Validate P1  Validate P2  Validate P3  Validate final dataset

Milvus          sync / seal --> automatic compaction --> serving handoff
```

`Job-1✓` means that the import job has not only completed, but its data has also been persisted and made visible. Prefix-1 corresponds to the logical sequence boundary X1 selected by the test. The checkpoint is defined by this boundary rather than wall-clock time and includes only mutation rows and Delete actions at or below X1 whose success is definitive, plus import rows whose jobs are completed, persisted, and visible.

Prefix-2 and Prefix-3 include progressively more confirmed ingress and validate increasingly large stable prefixes. Sequence ranges are preallocated by epoch and lane: in Epoch-1, Mutation A, Mutation B, and Import use 1-3,000, 3,001-6,000, and 6,001-9,000 respectively, and every subsequent epoch advances the overall range by 9,000. These values express logical order, not the actual commit order across lanes. A checkpoint with the boundary at the end of an epoch is submitted only after all three lanes complete that epoch, so a faster lane's later epoch cannot enter an earlier prefix. Checkpoints do not pause producers and do not introduce a hard epoch barrier that blocks ingress. Each lane starts writing into a new PK epoch immediately after completing its current epoch, while the validator asynchronously validates checkpoints in sequence and requires ingress progress above the boundary during each validation interval. Epoch-4 remains planned after Prefix-3, and ingress stops only after it completes, followed by final stabilization and full-dataset validation.

The independent expected model starts from the initial state and replays every confirmed action at or below checkpoint boundary X in logical-sequence order. Insert, Upsert, and Reinsert set the corresponding PK to a Live state carrying the canonical row. Delete sets the PK to Deleted. Import merges rows from completed, persisted, and visible jobs into the same state model. The replay result is the checkpoint's `expected_data_set`, which contains only PKs whose final state is Live. Deleted PKs are removed from the Live map but retained in tombstone and audit state so the test can verify that they never become visible again. A same-type mutation request or import job is committed atomically to the oracle only after complete success and an exact result-count match; any action that is not all-success fails the entire case immediately.

`test_sequence_id <= X` defines only the prefix query domain in the current MVCC view and does not provide a point-in-time or time-travel snapshot. If a suffix modifies a prefix PK again, the current query returns only the newest surviving version, and the sequence filter cannot reconstruct the old version. After a prefix is frozen, `MutationIngress` must therefore switch to a new PK epoch, and `ImportIngress` must continue with a new non-overlapping PK range. No suffix operation may modify a prefix PK again. Each lane continues producing data in its new PK epoch, allowing the validator to scan the stable prefix in full while Milvus continues to process real mixed ingress and background lifecycle changes.

To control uncertainty in the initial case, different ingress lanes use non-overlapping PK ranges. Cross-lane contention on the same PK is reserved for a later enhancement and is not part of the first design.

## 4. Two Types of Data Checkpoint

**Streaming prefix checkpoint.** Ingress must remain active. The test selects logical sequence boundary X and uses Strong consistency to query current data with `test_sequence_id <= X`. It then compares the result with the replayed expected state, validating the complete Live PK set and every field and cell, while using tombstone and audit state to confirm that Deleted PKs are invisible. Each lane continues mutation or import activity in a new PK epoch, and successful ingress progress above X must be recorded between the start and end of validation to prove that verification occurred under a real mixed-ingress workload.

**Final quiescent checkpoint.** After the streaming phase, the test stops every ingress lane, waits for every in-flight mutation request and import job to reach a definitive result, waits for compaction and serving state to stabilize, and then validates the complete expected dataset in full.

Neither checkpoint type samples data. The only difference is that the first validates correctness while the system continues to change, whereas the second validates the final stable state.

The three ingress lanes always run concurrently, but the three streaming prefix checkpoints share one validator and run serially as Prefix-1, Prefix-2, and Prefix-3. Each checkpoint merges actions from all three lanes within their respective boundaries and replays them into one global `expected_data_set`. Correctness is not evaluated per lane, and multiple full scans are not run concurrently, avoiding a verification workload that would materially change the system under test.

"Unique" means only that each PK has one logically visible result in the serving view. The test does not claim to enumerate or exclude physical versions hidden by MVCC or PK deduplication. It must, however, prove that Milvus selects the same visible winner as the oracle's last successful mutation for that PK and that every field of that row is correct cell by cell.

A streaming prefix checkpoint captures segment lineage snapshots before and after data validation, but it neither waits for lineage convergence nor requires the two snapshots to remain unchanged because automatic compaction may continue during validation. The checkpoint passes solely on the logical correctness of the frozen data prefix.

An in-progress lineage transition that has not yet closed is allowed at this checkpoint, but a self-referential edge, cycle, or other impossible topology must immediately fail the lifecycle validation.

## 5. Correctness Foundation for Compaction and Serving

The test cannot treat a compaction task marked Completed as proof that compaction has taken effect. Task state proves only that scheduling and execution ended; it does not prove that the new data has taken over query traffic.

A separate "automatic compaction checkpoint during active ingress" validates the complete lifecycle closure. This requirement must not be added to streaming prefix checkpoints, because waiting for lifecycle convergence would turn continuous-ingress validation back into a quiescent batch test.

Phase 2A must establish separate closures for MixCompaction and L0Compaction because their physical transformation semantics differ and they cannot share the same source-to-new-target assertion.

A valid MixCompaction must show all of the following: explicit lineage from sources to a new target, source retirement from the active and serving datasets, and takeover by an output segment that is Flushed, active, and serving. If a later compaction has already consumed the direct target, its terminal descendant may satisfy adoption through the lineage chain; the polling loop is not required to catch the direct target's brief serving window. For example, in A -> B -> C, adoption by C is sufficient.

L0 coverage combines end-to-end results with lifecycle evidence. The test first reserves and confirms a set of PKs in Flushed and serving L1/L2 segments, then applies planned Delete and Reinsert operations to them. During active ingress it observes completed Level0DeleteCompaction tasks and their L0 sources entering Dropped state. The subsequent frozen-prefix validation must prove that PKs whose final state is Deleted remain invisible and that every Reinsert row and all other Live PKs remain correct cell by cell. The current public snapshot cannot prove that a particular L0 source contains the reserved Delete batch, so the test does not claim per-batch Delete-to-L0-source attribution and cannot exclude the possibility that MixCompaction applies part of the delete effect.

L0 usually does not replace the target segment ID, so the test does not require source-to-new-target lineage, a target manifest change, or a serving-frontier change. In this case, L0 task completion, retirement of that task's L0 sources, Delete visibility, and integrity of retained data together constitute coverage evidence; they do not independently prove a non-trivial L0-on-sealed path.

The existing segment-state API can enumerate L0 segments in Flushing, Flushed, and Sealed states and identify them through `Level=L0`. The test must cache L0 source IDs within the GC retention window and explicitly query Dropped state to prove source retirement instead of assuming that terminal snapshots permanently retain historical L0 metadata.

At least one MixCompaction closure and one L0Compaction source-retirement closure must complete while ingress is active, and successful ingress progress must continue after each closure. Otherwise the case is still a batch test that performs continuous ingestion first and validates only after stopping. This requirement checks global progress and does not require every lane to submit during every verification window because the Import lane may finish all jobs early.

Release/load is not part of the three streaming prefix checkpoints because it interrupts continuous serving and changes the workload under test. In the final quiescent phase, the test first completes full validation and accepts automatic Mix/L0 coverage, then performs one final manual `compact()`, waits for a new Mix task relative to the pre-request snapshot, replacement lineage, and serving handoff, and validates all data through the same Retrieve before/after frontier fence used by Phase 1. It then performs one release/load cycle and repeats fenced full validation to prove that final Deleted PKs remain invisible and all Live PKs and fields remain unchanged. Physical segment row counts are not required to equal the number of Live PKs because mutations may leave deleted or older physical versions. This phase strengthens end-to-end validation of delete effects through subsequent rewrite and reload, but it cannot guarantee detection of every lost L0 Delete and does not replace Delete-to-L0 attribution.

The streaming phase never calls `compact()`. Automatic lifecycle coverage must be independently accepted and frozen before the final request. The single manual `compact()` is used only for subsequent rewrite validation, cannot compensate for missing automatic Mix/L0 coverage during active ingress, and cannot pass solely because it returns a job ID or a task reaches Completed.

## 6. Test Boundaries and Phase 1 Integration

Phase 2A supports mixed ingress from its first version. The base workload runs two `MutationIngress` lanes and one `ImportIngress` lane concurrently against the same collection, with non-overlapping PK ranges across the three lanes.

Phase 2A does not introduce a separate test system. It adds a continuous mixed-ingress case to the Phase 1 cases and extends the existing framework's ability to express sustained workloads. Data generation, independent expected state, full-data comparison, compaction lineage, serving checkpoints, and audit evidence continue to use the methods established in Phase 1.

Phase 2A and Phase 1 cases run strictly serially in the same L3 CI workflow, reuse the same deployment, Milvus instance, and execution entry point, and require neither a separate workflow nor an instance restart or redeployment between phases.

After Phase 1 completes, the workflow uses the existing etcd helper to write the hot-effective overrides required by Phase 2, reads back the corresponding revisions, waits 10 seconds, and only then creates the Phase 2 collection. `dataCoord.segment.maxSize` affects only segments allocated after the configuration update, so neither the collection nor its G0 segments may be created before this configuration step.

Runtime setup changes only the five values that the current consumption paths can adopt dynamically and that Phase 2 requires: `dataNode.segment.syncPeriod=10` seconds, `dataCoord.segment.maxSize=64` MiB, `dataCoord.segment.maxLife=60` seconds, `streaming.flush.l0.maxLifetime=30s`, and `dataCoord.compaction.levelzero.forceTrigger.deltalogMinNum=4`. Some of these parameters are hot-effective behavior in the current implementation rather than stable public configuration contracts. Therefore, writing and reading back etcd values and waiting 10 seconds proves only that the desired value has propagated; it does not independently prove that runtime consumers have adopted it.

Effective configuration must be accepted through bounded runtime behavior. Within the deadline, the test must observe natural sync/seal, Mix lineage during active ingress, and L0 source retirement, and must record the elapsed time from configuration write to the first occurrence of each type of evidence. If any required behavior is missing, the result is `coverage_not_reached`; the test must neither wait indefinitely nor treat an etcd value alone as proof of application.

The first Phase 2 case fixes Storage V3, one partition, and one shard to isolate the three core variables: mixed ingress, automatic compaction, and serving handoff. It does not run a V2 variant or perform a storage-version transition because Phase 1 already covers V2, V3, and V2 -> V3 -> V2 transitions independently.

The first case uses the V3 all-data-type schema profile already proven by the regular Phase 1 Compaction-DataIntegrity case and adds the test-control field `test_sequence_id`: VARCHAR PK, `test_sequence_id`, scalar fields, nullable/default fields, VARCHAR, JSON, Array, dynamic field, StructArray with its five nested vector types, nullable top-level vectors, sparse vector, and TEXT/LOB. The BM25 function and DDL-specific top-level-vector profile are not combined into this case, avoiding new schema variables and indexing cost in the continuous-ingress model.

`MutationIngress` and `ImportIngress` use the same deterministic row generator, canonical encoding, and independent oracle as Phase 1. The same logical row must therefore produce exactly the same expected bytes whether it enters through mutation or import. `ImportIngress` may transform the physical file representation, but it must not rewrite a nullable StructArray from `None` to `[]` or a nullable sparse vector from `None` to a non-empty fingerprint. TEXT produces one value above the inline LOB threshold per 1,000 rows, preserving LOB-path coverage while keeping average row size bounded.

Each `MutationIngress` lane runs four PK epochs, each containing 3,000 row-level operations: 2,200 first Inserts, 400 Upserts, 200 Deletes, 100 Reinserts of deleted PKs, and 100 later ordinary Inserts against still-Live PKs. Insert, Upsert, and Delete are submitted through separate request batches containing only one operation type; they cannot be mixed in one mutation RPC. An epoch is the planned interleaving of these same-type request batches. The oracle replays them by global logical sequence and treats the canonical row or Deleted state produced by the last successful mutation as the final state of each PK.

The `ImportIngress` lane completes one 3,000-row import job in each of four epochs. The base workload therefore contains 24,000 mutation operations and 12,000 imported rows, for 36,000 ingress operations in total. When every planned operation succeeds, the three streaming checkpoints validate 7,200, 14,400, and 21,600 Live PKs, and the final `expected_data_set` contains 28,800 Live PKs.

Each `MutationIngress` lane submits a 100-row same-type request batch every two seconds: exactly 30 same-type RPC batches per epoch per lane, approximately 50 row operations per second, and about one minute per epoch. One hundred rows is not a Milvus correctness requirement; it fixes request count, production rate, receipts, and failure boundaries while avoiding meaningless tail batches. When logical sequence reaches the planned boundaries for Epoch-1, Epoch-2, and Epoch-3, the test submits the three streaming prefix validation tasks in order. A single validator executes them serially, while producers enter new PK epochs without waiting for validation. Epoch-4 and progress at higher sequences during every validation interval prove that ingress remains active.

`ImportIngress` submits the next job only after the previous job has completed and its data is persisted and visible. The sustained-ingress target is approximately four to six minutes, and one Mutation epoch takes about 60 seconds plus RPC time. The design keeps four fixed epochs and independent producers and validator, without adding a scheduling barrier or extra writes. If a prefix scan observes no successful suffix progress, the case fails immediately and emits `coverage_not_reached` with progress counts, boundary, and scan duration. CI failure rates can then determine whether a coordination mechanism is needed. Eight to twelve minutes remains the normal runtime target pending calibration. Final compaction and reload share the same cooperative 20-minute deadline without an extension, and the test does not claim that it can forcibly interrupt an SDK call that ignores its timeout.

Phase 2 overrides only the five values listed above. All other dependencies retain and record the instance defaults: `dataNode.segment.insertBufSize=16777216` (16 MiB), `dataCoord.segment.sealProportion=0.12`, `dataCoord.segment.sealProportionJitter=0.1`, `dataCoord.compaction.mix.triggerInterval=60` seconds, `dataCoord.compaction.levelzero.triggerInterval=10` seconds, `dataCoord.compaction.levelzero.forceTrigger.minSize=8388608` (8 MiB), `dataCoord.enableCompaction=true`, `dataCoord.compaction.enableAutoCompaction=true`, and `dataCoord.compaction.twoTierCompaction=false`. In particular, the test no longer attempts to change the Mix trigger interval to 10 seconds at runtime because that ticker is fixed when the compaction manager starts, while the default 60-second interval still provides several natural scheduling opportunities during the four-to-six-minute ingress window.

A single deltalog file has no fixed size; its size depends on the Delete PKs, timestamps, encoding, and compression within that flush interval. This workload's low-rate Delete traffic is expected to trigger L0Compaction primarily through deltalog count rather than the 8 MiB size threshold. The initial design therefore uses `deltalogMinNum=4`, aiming to form one non-trivial L0 plan in approximately two minutes while avoiding the excessively frequent L0 execution and Mix/Sort scheduling exclusion caused by `deltalogMinNum=2`.

Schema and write settings must be reviewed as one configuration set. The logical row size of this V3 all-data-type schema determines the actual rate of size-based sync and seal. A 64 MiB maximum with a 0.12 seal proportion and jitter produces an approximate segment seal threshold of 6.9-7.7 MiB, while a 10-second sync period and 60-second maximum lifetime provide periodic persistence and a time-based fallback. If schema, vector dimension, LOB frequency, batch size, or write rate changes, the entire configuration set must be recalibrated rather than changing one value in isolation.

This configuration does not promise an exact physical segment count. Its goal is to make a four-to-six-minute mixed-ingress window span multiple sync, seal, MixCompaction, and L0Compaction scheduling cycles and produce observable Mix lineage and L0 source retirement while producers remain active. Final validity still depends on real segment and task snapshots, at least one MixCompaction closure independent of import SortCompaction, at least one L0Compaction closure, active/serving handoff, delete visibility after release/load, and full-data correctness.

These values are initial draft settings rather than permanent constants. After the draft implementation is complete, prototype runs must record actual deltalog counts and sizes, Mix/L0 task durations, scheduling-exclusion waits, and time spent in each validation stage. The values must then be calibrated without reducing full-validation strength, and the runs must confirm that the normal eight-to-twelve-minute target and 20-minute deadline are achievable.

The test never calls `flush()`. Persistence performed by a completed Import job is not considered a test-side manual flush. The streaming and automatic-coverage phases never call `compact()`; only after those checks pass does the test execute the single final manual compaction described above and wait for a real transformation and serving handoff.

Data types continue to use the all-field data-safety model proven in Phase 1. Resource use is controlled through fixed data volume, rate, schema, and lifecycle configuration, not by sampling data or reducing field coverage.

The test must enforce explicit total-duration and resource limits. If it cannot establish both a valid MixCompaction closure and a valid L0Compaction closure within the deadline, it reports coverage not reached instead of waiting indefinitely or passing.

Dynamic storage-version transitions, multiple shards, duplicate PKs across ingress lanes, fault injection, and component restart are reserved for later independent enhancements and are not introduced together with the first mixed-ingress case. Duplicate PKs across batches within one `MutationIngress` lane are already part of the Phase 2A base workload.

## 7. Key Audit Logs

The test must emit stable, machine-readable structured evidence logs so a human or AI can reconstruct the complete ingress, checkpoint, compaction, and serving-handoff timeline without rerunning the case. Logs record event-level summaries with complete IDs and do not print every cell of large datasets.

Key observation points include the following. During configuration, record each of the five overrides' original and desired values, etcd revision, read-back result, and 10-second wait interval, plus the first observed natural sync/seal, Mix lineage, and L0 source retirement times. At case start, record Storage V3, schema fingerprint, workload parameters, random seed, and each lane's PK range. When a mutation request or import job definitively succeeds, record its logical sequence range, job ID, row count, and oracle commit. When a prefix is frozen, record checkpoint boundary X, Live and Deleted counts, and expected PK digest. When full validation completes, record validation start and end sequence, ingress progress above X during validation, expected and actual PK digests, validated row and cell counts, mismatch summary, active/serving frontiers before and after validation, and duration.

Automatic-lifecycle evidence must record complete task ID, task type and state, complete source and target segment IDs, Mix lineage edges, L0 source states, segment storage version, active/serving frontier, and handoff result. The final quiescent checkpoint must also record ingress drain, task quietness, active=serving, the final manual compaction request and its before/after checkpoints and new edges, validation after rewrite and release/load, final data summary, and one terminal classification among `passed`, `ingress_failed`, `data_integrity_failed`, `lifecycle_failed`, `coverage_not_reached`, or `indeterminate`, together with the reason.

Audit logs do not replace correctness assertions, but they must answer the following questions: which actions entered the oracle, which sequence boundary was validated, whether suffix progress continued during validation, which MixCompaction closed during active ingress, which L0 sources retired, whether Deletes remained effective after release/load, which serving frontier took over queries, and how many rows and cells a failure affected.

## 8. Result Classification

- **Passed:** Every planned ingress operation succeeds, all three streaming prefix validations pass, MixCompaction and L0Compaction coverage completes during active ingress, serving handoff succeeds, and both full validations after the independent final compaction rewrite and release/load pass.
- **Ingress failed:** Any mutation batch or import job has a partial failure, count mismatch, timeout, cancellation, disconnection, or indeterminate final result; the case stops immediately and no longer uses the collection for correctness judgments.
- **Data integrity failed:** Data is missing or unexpected, a PK has multiple visible results in the serving view, the visible winner differs from the oracle's last successful mutation, deleted data reappears, or any field value differs.
- **Lifecycle failed:** Mix lineage, L0 source retirement, target adoption, segment state, or serving handoff violates the contract.
- **Coverage not reached:** Any streaming prefix validation observes no successful suffix progress, or the test fails to observe both a valid MixCompaction closure and a valid L0Compaction closure during active ingress within the deadline; the case fails rather than skipping or reporting data corruption.
- **Indeterminate:** External interference prevents a stable, trustworthy validation boundary; indeterminate mutation or import results are classified uniformly as Ingress failed.

Phase 2A acceptance requires all of the following in one bounded test run: every `MutationIngress` and `ImportIngress` operation succeeds, all three full prefix validations observe suffix progress, at least one MixCompaction closure and one L0Compaction source-retirement closure complete during active ingress, serving handoff succeeds, and full-data validation passes after both the independent final compaction and release/load.
