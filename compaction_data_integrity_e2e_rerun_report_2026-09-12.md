# Compaction Data-Integrity E2E Rerun — 2026-09-12

## Verdict

All six selected L3 workloads passed when run as six separate, non-overlapping pytest invocations against the restarted local cluster. The first V2 Import attempt failed before ingestion because pytest's default MinIO bucket, milvus-bucket, did not exist locally; the rerun with the deployment's actual a-bucket passed. These are local E2E results, not a clean-checkout CI result.

## Environment and execution

- Milvus worktree: /home/hanchun/Documents/project/milvus/.worktrees/compaction-e2e-observability; branch codex/compaction-e2e-observability; HEAD 26ddc19406.
- Live server version returned by MilvusClient: codex-compaction-e2e-observability-20260912-26ddc19406; separate mixcoord, datanode, querynode, streamingnode, and proxy processes were running.
- PyMilvus: editable /home/hanchun/Documents/project/pymilvus/.worktrees/compaction-e2e-observability at b549a545, reporting version 3.1.0rc85.
- Python: /home/hanchun/anaconda3/envs/milvus-e2e/bin/python3, version 3.10.16; the launcher set datetime.UTC = datetime.timezone.utc before importing pytest because the shared test module imports the Python 3.11 alias.
- Selection: exactly six nodes collected from TestMilvusClientCompactionDataIntegrity with L3, the compaction_data_integrity_serial marker, the explicit opt-in flag, and -n 0; each node ran only after the previous pytest process exited.
- Local object storage: localhost:9000, confirmed bucket a-bucket, matching configs/milvus.yaml; the two successful Import invocations passed --minio_bucket a-bucket.
- Pytest command pattern: CI_LOG_PATH=<case artifact directory> /home/hanchun/anaconda3/envs/milvus-e2e/bin/python3 -c 'import datetime,sys; datetime.UTC=datetime.timezone.utc; import pytest; sys.exit(pytest.main(sys.argv[1:]))' -q -n 0 --run-compaction-integrity-serial --tags L3 -m compaction_data_integrity_serial <exact node ID>.
- Each exact node ID is milvus_client/test_milvus_client_data_integrity.py::TestMilvusClientCompactionDataIntegrity:: followed by the selector suffix in the result matrix.
- Audit artifacts: /tmp/milvus-compaction-e2e.IKw69q/<case directory>/test_report.json and test_report.html; these are temporary files and are not committed with this report.

The six successful pytest durations sum to 57m 21.51s; the failed preliminary V2 Import attempt took another 32.02s, excluding preflight and report preparation.

## Result matrix

| Order | Exact workload selector suffix | Result | Pytest duration | Final logical rows | Final ordinary fields / checked cells | Lifecycle evidence |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 1 | test_compaction_active_set_transitions_preserve_all_rows_and_fields[3_rounds-storage_v2] | PASS | 731.98s | 30,000 | 22 / 660,000 | Three round edges 24/26/26; V2 active=serving. |
| 2 | test_compaction_active_set_transitions_preserve_all_rows_and_fields[3_rounds-storage_v3] | PASS | 703.84s | 30,000 | 23 / 690,000 | Three round edges 24/26/26; V3 TEXT included. |
| 3 | test_bulk_import_compaction_preserves_all_rows_and_fields[3_rounds-storage_v2] | PASS after bucket correction | 437.56s | 30,000 | 22 / 660,000 | Three completed import jobs; round edges 2/3/3. |
| 4 | test_bulk_import_compaction_preserves_all_rows_and_fields[3_rounds-storage_v3] | PASS | 438.82s | 30,000 | 23 / 690,000 | Three completed import jobs; round edges 2/3/3; V3 TEXT included. |
| 5 | test_v3_schema_evolution_compaction_preserves_all_rows_and_fields[storage_v3] | PASS | 316.33s | 25,000 at C0–C4 | 24/25/25/24/24 fields | C1 Mix, C2 in-place Bump, C3/C4 replacement Bump; BM25 at all checkpoints. |
| 6 | test_compaction_storage_version_transitions_preserve_all_rows_and_fields[3_rounds] | PASS | 812.98s | 30,000 | 22 / 660,000 at each terminal version | V2 rounds, then evidenced V2→V3 and V3→V2 rewrites. |

All four three-round ingress workloads reached 10,000, 20,000, and 30,000 expected rows in order, and their final expected and retrieved PK digests agreed after full row/field/cell comparison. For the fixed-version workloads, every final checkpoint had exactly one active segment and the same one serving segment.

## Per-workload evidence

### 1. Fixed V2 streaming Insert

- Each round committed ten successful 1,000-row Insert batches and then reached ingress and post-compaction fenced dataset checkpoints.
- The round lineage edge counts were 24, 26, and 26; ten newly attributable roots participated in each round's graph transition.
- Exact ordinary-cell checks were 220,000, 440,000, and 660,000 for the cumulative 10k/20k/30k datasets, with matching PK digests and no reported corruption.
- Final active=serving segment: 469026336088082750, storage version 2, 30,000 rows.
- Two validation attempts noticed a frontier handoff and retried instead of accepting a moving frontier; the later stable attempts passed.
- Artifact: /tmp/milvus-compaction-e2e.IKw69q/01_insert_v2/test_report.json.

### 2. Fixed V3 streaming Insert

- The same thirty Insert batches and three round checkpoints passed, with lineage edge counts 24, 26, and 26 and ten participating new roots per round.
- The V3 output included TEXT, so the 10k/20k/30k cumulative exact-cell counts were 230,000, 460,000, and 690,000, with matching PK digests.
- Final active=serving segment: 469026336104738050, storage version 3, 30,000 rows.
- One validation attempt retried after a frontier change; the stable attempt passed.
- Artifact: /tmp/milvus-compaction-e2e.IKw69q/02_insert_v3/test_report.json.

### 3. Fixed V2 Bulk Import

- Initial attempt: FAILED in 32.02s before import completion because RemoteBulkWriter attempted to upload to the nonexistent default bucket milvus-bucket; MinIO's actual and Milvus-configured bucket was confirmed read-only as a-bucket.
- Corrected rerun: three separate 10,000-row jobs 469026336105809900, 469026336108810900, and 469026336111942100 each reported Completed, 100% progress, and the expected row count before its data entered the oracle.
- Round lineage edge counts were 2, 3, and 3; one attributable import root participated per round.
- Cumulative exact-cell checks were 220,000, 440,000, and 660,000, with matching PK digests.
- Final active=serving segment: 469026336114802800, storage version 2, 30,000 rows.
- Artifacts: /tmp/milvus-compaction-e2e.IKw69q/03_import_v2/test_report.json for the environment failure and /tmp/milvus-compaction-e2e.IKw69q/03_import_v2_retry/test_report.json for the PASS.

### 4. Fixed V3 Bulk Import

- Jobs 469026336115434000, 469026336119655200, and 469026336123266370 each reported Completed, 100% progress, and 10,000 imported rows.
- Round lineage edge counts were 2, 3, and 3; one attributable import root participated per round.
- With V3 TEXT included, the cumulative exact-cell checks were 230,000, 460,000, and 690,000, with matching PK digests.
- Final active=serving segment: 469026336126367100, storage version 3, 30,000 rows.
- Artifact: /tmp/milvus-compaction-e2e.IKw69q/04_import_v3/test_report.json.

### 5. V3 schema-evolution chain

- The initial five 5,000-row Insert-plus-Flush batches produced a 25,000-row dataset, which remained at 25,000 rows across C0, C1, C2, C3, and C4.
- Schema versions advanced 0→1→2→3→4; each checkpoint had matching active and serving IDs, and ordinary-field canonical checks covered respectively 600,000, 625,000, 625,000, 600,000, and 600,000 cells.
- C0 had three active=serving segments: 469026336128788800, 469026336128798850, and 469026336129208900.
- C1 add-field was absorbed by a successful Mix rewrite of those three segments into active=serving segment 469026336129319230; the added default and every retained ordinary field passed exact comparison.
- C2 add-function-field used BumpSchemaVersionCompaction with source=target 469026336129319230, while the new BM25 output and retained ordinary data passed validation.
- C3 drop-field replaced 469026336129319230→469026336131270000, and C4 drop-function-field replaced 469026336131270000→469026336131900500, with each new target active and serving.
- The BM25 validator checked 25,000 unique tokens at each checkpoint; C2 and C3 checked both sparse_base and sparse_added, yielding 175,000 token-field top-1 comparisons across C0–C4.
- Artifact: /tmp/milvus-compaction-e2e.IKw69q/05_ddl_v3/test_report.json.

### 6. V2→V3→V2 storage-version transition

- The initial three V2 Insert rounds passed at 10k/20k/30k rows, with lineage edge counts 24, 25, and 26 and 220,000/440,000/660,000 exact-cell checks.
- V2→V3 was evidenced by lineage edge 469026336148577600→469026336148978200, with the V3 target active=serving, 30,000 rows, and all 660,000 ordinary cells verified.
- V3→V2 was evidenced by lineage edge 469026336148978200→469026336149708860, with the final V2 target active=serving, 30,000 rows, and the same 660,000 cells verified.
- One validation attempt retried on a frontier change; the successful stable attempts preserved matching PK digests.
- The test logged storage_transition_config_restore_verified with expected_storage_version=2 and actual_storage_version=2.
- Artifact: /tmp/milvus-compaction-e2e.IKw69q/06_version_transition/test_report.json.

## Interpretation and operational notes

- These passes jointly exercise independent expected-data calculation, confirmed ingress, persisted Flushed segments, observable lineage transitions, active-to-serving handoff, and exhaustive visible-row canonical-byte checks at quiescent checkpoints.
- They do not prove delete/upsert or duplicate-PK ordering, process-restart recovery, raw object-store byte identity, hidden MVCC physical-row uniqueness, partition/shard isolation, or index correctness; BM25 is a separate behavioral search check rather than direct sparse-output byte retrieval.
- This run used a locally compiled server and an editable PyMilvus worktree, so it does not establish that the clean CI dependency pins or the default Nightly runner already execute these six L3 workloads.
- The temporary global teardown disablement left four successful fixed-version collections and the empty collection from the failed V2 Import attempt available for inspection; DDL and storage-transition methods dropped their own collections, and no manual cleanup was performed.
- The final read-only cluster check returned the expected server build and collections compaction_data_integrity_QwBXaOSY, compaction_data_integrity_bnxnkEru, import_compaction_data_integrity_ClsaQsG3, import_compaction_data_integrity_fFvZoKNZ, and import_compaction_data_integrity_mEgM96la.
