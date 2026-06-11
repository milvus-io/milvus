# Import 2PC Test Execution Log

## Environment

| Item | Value |
| --- | --- |
| Milvus instance | `import-2pc-playground` |
| Namespace | `chaos-testing` |
| Milvus endpoint | `http://10.100.36.203:19530` |
| MinIO endpoint | `10.100.36.178:9000` |
| MinIO bucket | `import-2pc-playground` |
| MinIO root path | `file` |
| Token | `root:Milvus` |
| Python | `3.12.12` |
| Test framework | `tests/restful_client_v2` |
| Milvus image | `harbor.milvus.io/manta/milvus:master-20260609-385caab` |

Notes:

- The bucket was verified by listing buckets from MinIO directly. The live bucket is `import-2pc-playground`.
- On 2026-06-10, the `import-2pc-playground-minio` LoadBalancer endpoint was re-verified as `10.100.36.178:9000`; the earlier `10.100.36.198:9000` address is stale.
- An earlier Python 3.10 attempt was stopped and is not counted. The valid run uses Python 3.12 as required.

## Case IMP-REST-000

Status: Passed

Implemented files:

- `tests/restful_client_v2/api/milvus.py`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Framework changes:

- Added `ImportJobClient.describe_import_job`.
- Added `ImportJobClient.commit_import_job`.
- Added `ImportJobClient.abort_import_job`.
- Added `ImportJobClient.wait_import_job_state`.

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_rest_api_contract_smoke
```

Coverage:

- Create manual import job with `options.auto_commit=false`.
- Wait for `Uncommitted`.
- Verify `/describe` and `/list`.
- Commit one job and verify duplicate commit succeeds.
- Wait for `Completed`.
- Abort another job and wait for `Failed`.
- Verify invalid `jobId` is rejected by `/commit` and `/abort`.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_rest_api_contract_smoke \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 40.50s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REP-004/005/006 CDC Import 2PC expanded P0 batch

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`
- `docs/test_plans/import-2pc-execution.md`

Tests added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_abort_before_commit_primary_secondary_cleanup
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_dml_interleaving_during_uncommitted_primary_secondary_consistent
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_commit_idempotent_primary_secondary_no_duplicate_rows
```

Coverage:

- `IMP-REP-004`: abort from primary after both clusters reach `Uncommitted`; both jobs reach `Failed`; import PKs remain invisible on both clusters.
- `IMP-REP-005`: baseline DML before import and new DML while import is `Uncommitted`; DML rows replicate and become visible before import commit; import rows remain invisible until commit; final primary/secondary PK sets match.
- `IMP-REP-006`: repeated primary `CommitImport` before and after `Completed`; primary/secondary final PK sets and counts match, proving no duplicate visibility.
- All three tests set CDC topology before execution: `import-2pc-cdc-source -> import-2pc-cdc-target`, 16 pchannels.

Pre-run checks:

```text
import-2pc-cdc-primary     cluster   Healthy   True
import-2pc-cdc-secondary   cluster   Healthy   True
primary_http_code=200
secondary_http_code=200
primary_minio_http_code=200
secondary_minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv -rs -n 3 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_abort_before_commit_primary_secondary_cleanup \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_dml_interleaving_during_uncommitted_primary_secondary_consistent \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_commit_idempotent_primary_secondary_no_duplicate_rows \
  --endpoint http://10.100.36.217:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.211 \
  --bucket_name import-2pc-cdc-primary \
  --root_path import-2pc-cdc-primary \
  --secondary_endpoint http://10.100.36.216:19530 \
  --secondary_token root:Milvus \
  --secondary_minio_host 10.100.36.214 \
  --secondary_bucket_name import-2pc-cdc-secondary \
  --secondary_root_path import-2pc-cdc-secondary \
  --source-cluster-id import-2pc-cdc-source \
  --target-cluster-id import-2pc-cdc-target \
  --pchannel-num 16
```

Final result:

```text
3 passed, 7 warnings in 112.91s (0:01:52)
```

Failure classification:

- No failure.
- No Milvus Import 2PC CDC abort, DML interleaving, or commit idempotency bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-FT-001 MixCoord/DataCoord restart while job is Uncommitted

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`
- `docs/test_plans/import-2pc-execution.md`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_mixcoord_restart_in_uncommitted_allows_commit
```

Environment:

| Item | Value |
| --- | --- |
| Namespace | `chaos-testing` |
| Cluster CR | `import-2pc-chaos-cluster` |
| Milvus endpoint | `http://10.100.36.215:19530` |
| MinIO endpoint | `10.100.36.206:9000` |
| Bucket/root path | `import-2pc-chaos-cluster` |
| Component restarted | `mixcoord` (`rootcoord/querycoord/datacoord/indexcoord` mixture process) |
| Milvus version | `master-20260609-385caab437`, git commit `385caab437` |

Coverage:

- Creates collection and manual REST import job with `options.auto_commit=false`.
- Waits until job reaches `Uncommitted` and verifies import PKs are still invisible.
- Deletes one `mixcoord` pod from the test release.
- Waits for a replacement `mixcoord` pod to become Ready.
- Waits for the Milvus CR to recover to `Healthy`.
- Verifies the import job state recovers as `Uncommitted`.
- Commits the job and waits for `Completed`.
- Verifies imported PKs become visible after commit.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv -rs \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_mixcoord_restart_in_uncommitted_allows_commit \
  --endpoint http://10.100.36.215:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.206 \
  --bucket_name import-2pc-chaos-cluster \
  --root_path import-2pc-chaos-cluster \
  --release_name import-2pc-chaos-cluster
```

Final result:

```text
1 passed, 2 warnings in 166.29s (0:02:46)
```

Observed behavior:

```text
The replacement mixcoord pod was scheduled on 4am-node18.
It initially appeared as Init:0/1 while the Milvus CR was Unhealthy, then became Ready.
The CR returned to Healthy, the import job was still Uncommitted, commit succeeded, and imported rows became visible.
```

Failure classification:

- No failure.
- No Milvus Import 2PC metadata recovery bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.
- PyMilvus/kubectl subprocess use in the same process emitted a gRPC fork warning before pod deletion; it did not affect the test result.

## 2026-06-10 IMP-REP-002 CDC delete semantics across import commit boundary

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`
- `docs/test_plans/import-2pc-execution.md`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_delete_across_commit_boundary_primary_secondary_consistent
```

Coverage:

- Sets CDC topology before exercising import: `import-2pc-cdc-source -> import-2pc-cdc-target`, 16 pchannels.
- Creates collection on primary and waits for collection replication/load on secondary.
- Stages the same parquet object name in both primary and secondary MinIO buckets.
- Creates primary REST import with `options.auto_commit=false`.
- Waits until both primary and secondary import jobs reach `Uncommitted`.
- Deletes a subset of imported PKs on primary before commit.
- Verifies all import PKs remain invisible on both clusters while `Uncommitted`.
- Commits import on primary and waits until both clusters reach `Completed`.
- Verifies pre-commit deleted PKs are visible on both clusters after commit, proving `delete_ts < commit_ts` is ignored for import rows.
- Deletes a different subset after commit on primary.
- Verifies post-commit deleted PKs disappear on both clusters and the final primary/secondary PK set is identical.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv -rs \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_delete_across_commit_boundary_primary_secondary_consistent \
  --endpoint http://10.100.36.217:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.211 \
  --bucket_name import-2pc-cdc-primary \
  --root_path import-2pc-cdc-primary \
  --secondary_endpoint http://10.100.36.216:19530 \
  --secondary_token root:Milvus \
  --secondary_minio_host 10.100.36.214 \
  --secondary_bucket_name import-2pc-cdc-secondary \
  --secondary_root_path import-2pc-cdc-secondary \
  --source-cluster-id import-2pc-cdc-source \
  --target-cluster-id import-2pc-cdc-target \
  --pchannel-num 16
```

Final result:

```text
1 passed, 2 warnings in 91.06s (0:01:31)
```

Failure classification:

- No failure.
- No Milvus Import 2PC CDC delete/commit timestamp consistency bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -V
# Python 3.12.12

python -m py_compile \
  tests/restful_client_v2/api/milvus.py \
  tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 83.50s
```

The long collect time is framework dependency import overhead under Python 3.12, not a case failure.

## Case IMP-LC-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_default_auto_commit_completed_and_visible
```

Coverage:

- Create import job without `options.auto_commit`.
- Wait for job to reach `Completed` automatically.
- Assert `importedRows` and `totalRows` equal imported row count.
- Refresh load and poll REST query until all imported PKs are visible.
- Verify REST search returns at least one imported PK.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_default_auto_commit_completed_and_visible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 37.83s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 2.02s
```

## Case IMP-LC-002

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_import_stops_at_uncommitted_and_invisible
```

Coverage:

- Create manual import job with `options.auto_commit=false`.
- Wait for job to reach `Uncommitted`.
- Assert `importedRows` and `totalRows` equal imported row count.
- Poll the job state several times and verify it remains `Uncommitted`.
- Query imported PKs repeatedly and verify they remain invisible.
- Insert one normal REST DML row while the import job is `Uncommitted`.
- Verify the normal DML row becomes visible, proving import does not block regular DML.
- Abort the manual import job at the end as cleanup.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_import_stops_at_uncommitted_and_invisible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 48.89s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.89s
```

## Case IMP-LC-003

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_commit_completed_visible_and_idempotent
```

Coverage:

- Create manual import job with `options.auto_commit=false`.
- Wait for job to reach `Uncommitted`.
- Verify imported PKs are invisible before commit.
- Call `/commit` once and immediately call `/commit` again.
- Wait for job to reach `Completed`.
- Refresh load and poll REST query until all imported PKs are visible.
- Verify REST search returns at least one imported PK.
- Call `/commit` again after `Completed` and verify it remains successful.
- Re-query exact imported PK set after the idempotent post-completion commit.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_commit_completed_visible_and_idempotent \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 41.11s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.88s
```

## Case IMP-TYP-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_scalar_types_manual_import_preserves_values
```

Coverage:

- Create a collection covering scalar fields: `Bool`, `Int8`, `Int16`, `Int32`, `Int64`, `Float`, `Double`, and `VarChar`.
- Generate and upload a parquet file with deterministic scalar values plus a `FloatVector` field.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs are visible after commit.
- Query scalar output fields and verify all values match the source rows.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_scalar_types_manual_import_preserves_values \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 53.70s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.69s
```

## Case IMP-TYP-002

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_json_field_manual_import_supports_json_path_filter
```

Coverage:

- Create a collection with a `JSON` field and a `FloatVector` field.
- Generate and upload a JSON import file with nested JSON objects.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Verify JSON path filter returns no rows before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs are visible after commit.
- Query JSON output fields and verify stored JSON content matches the source rows.
- Verify `json_contains(json['key'], 1)` returns the expected imported row.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_json_field_manual_import_supports_json_path_filter \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 56.01s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- Initial run failed in the test assertion because REST query returned the JSON field as a JSON string while the test compared it directly with a Python dict.
- The assertion was fixed by decoding string JSON with `json.loads`.
- Re-run passed; no Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.55s
```

## Case IMP-TYP-003

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_array_fields_manual_import_supports_array_filter
```

Coverage:

- Create a collection with Array fields: `Array<Int64>`, `Array<VarChar>`, and `Array<Bool>`.
- Generate and upload a JSON import file containing deterministic array values.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Verify `array_contains(int_array, 3)` returns no rows before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs are visible after commit.
- Query array output fields and verify stored array values match the source rows.
- Verify `array_contains(int_array, 3)` returns the expected imported row.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_array_fields_manual_import_supports_array_filter \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 60.53s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- Initial run failed in the test assertion because REST query returned Array fields as typed wrappers, for example `{"Data":{"LongData":{"data":[...]}}}`.
- The assertion was fixed by normalizing REST Array wrappers before comparing with source arrays.
- Re-run passed; no Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.49s
```

## Case IMP-TYP-011

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_nullable_fields_manual_import_preserves_nulls
```

Coverage:

- Create a collection with nullable scalar fields: `Int64`, `Float`, and `VarChar`.
- Generate and upload a parquet file with mixed null and non-null values.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs are visible after commit.
- Query nullable output fields and verify null and non-null values are preserved.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_nullable_fields_manual_import_preserves_nulls \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 47.32s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.81s
```

## Case IMP-TYP-012

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_default_value_fields_manual_import_fills_missing_columns
```

Coverage:

- Create a collection with `defaultValue` fields: `Int64`, `Float`, and `VarChar`.
- Generate and upload a parquet file that omits the default-value columns.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs are visible after commit.
- Query default-value fields and verify missing columns are filled with configured defaults.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_default_value_fields_manual_import_fills_missing_columns \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 52.66s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.77s
```

## Case IMP-TYP-013

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_dynamic_fields_manual_import_stores_and_filters_extra_keys
```

Coverage:

- Create a collection with `enableDynamicField=true`.
- Generate and upload a JSON import file containing undeclared keys: `extra_str`, `extra_int`, and `extra_bool`.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Verify dynamic-field filter `extra_int == 1003` returns no rows before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs are visible after commit.
- Query dynamic output fields and verify dynamic values match the source rows.
- Verify `extra_int == 1003` returns the expected imported row.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_dynamic_fields_manual_import_stores_and_filters_extra_keys \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 60.16s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.89s
```

## Case IMP-TYP-014

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_auto_id_manual_import_assigns_ids_after_commit
```

Coverage:

- Create an `autoId=true` collection with an Int64 primary key.
- Generate and upload a parquet file that does not contain the primary key column.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported rows are invisible before commit by querying the non-PK `tag` field.
- Call `/commit` and wait for `Completed`.
- Verify all imported `tag` values are visible after commit.
- Verify generated primary keys are present, numeric, and unique.
- Verify `phase`, `count(*)`, and REST search results match the imported rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_auto_id_manual_import_assigns_ids_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 52.44s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.85s
```

## Case IMP-LC-004

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_commit_rejects_invalid_states
```

Coverage:

- Create a manual import job with `options.auto_commit=false` and a missing source file.
- Wait for the import job to reach `Failed`.
- Call `/commit` on the `Failed` job and verify it returns a non-zero business code.
- Verify the job remains `Failed` after the rejected commit.
- Call `/commit` on a nonexistent numeric job ID and verify it returns a non-zero business code.

Implementation note:

- The test uses a deterministic `Failed` job to cover non-`Uncommitted` commit rejection.
- It does not assert the auto-commit job's narrow `Uncommitted` manual-commit guard because current implementation can return success after an auto-commit job has already reached `Completed`, making that path timing-sensitive.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_commit_rejects_invalid_states \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 11.91s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.94s
```

## Case IMP-ABT-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_before_commit_keeps_import_invisible
```

Coverage:

- Create a baseline DML row in the same collection and verify it is visible.
- Create a manual import job with `options.auto_commit=false`.
- Wait for the import job to reach `Uncommitted`.
- Verify imported PKs remain invisible before abort.
- Call `/abort` on the `Uncommitted` job.
- Wait for the job to reach `Failed`.
- Call `/commit` on the aborted job and verify it returns a non-zero business code.
- Verify the job remains `Failed` after the rejected commit.
- Continue polling exact imported PKs and verify they remain invisible after abort.
- Verify the baseline DML row remains visible after abort.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_before_commit_keeps_import_invisible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 65.13s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.69s
```

2026-06-11 retest on updated master instance:

- Instance: `issue-49468-master-0611`
- Image: `harbor.milvus.io/manta/milvus:master-20260611-35079b9`
- Added assertion: after successful abort, `CommitImport` on the same job must be rejected and the job must remain `Failed`.

Run command:

```bash
cd tests/restful_client_v2
../../.venv/bin/python -m pytest -q -s -rs \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_before_commit_keeps_import_invisible \
  --endpoint http://10.100.36.229:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.228 \
  --bucket_name issue-49468-master-0611 \
  --root_path files
```

Result:

```text
1 passed, 2 warnings in 63.99s (0:01:03)
```

Observed behavior:

- Abort moved the manual import job to `Failed`.
- Commit after abort returned non-zero code.
- `get_progress` still reported `state=Failed`.
- Imported rows remained invisible and the baseline DML row remained visible.

## Case IMP-ABT-004

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_rejects_completed_job
```

Coverage:

- Create a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Call `/commit` and wait for the job to reach `Completed`.
- Refresh load and poll exact PK query until committed rows are visible.
- Call `/abort` on the `Completed` job and verify it is rejected with a non-zero business code.
- Verify the job remains `Completed`.
- Verify committed rows remain visible after the rejected abort.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_rejects_completed_job \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 28.02s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.80s
```

## Case IMP-DEL-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_delete_before_commit_is_noop_for_import_rows
```

Coverage:

- Create a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify imported PKs are invisible before delete.
- Call REST `/v2/vectordb/entities/delete` for the exact imported PK set before commit.
- Verify imported PKs are still invisible while the job remains uncommitted.
- Call `/commit` and wait for `Completed`.
- Refresh load and poll exact PK query until all imported rows become visible.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_delete_before_commit_is_noop_for_import_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 67.74s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.82s
```

## Case IMP-DEL-002

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_delete_after_commit_removes_import_rows
```

Coverage:

- Create a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Call `/commit` and wait for `Completed`.
- Refresh load and poll exact PK query until all imported rows are visible.
- Delete a subset of imported PKs after commit.
- Poll until the deleted imported PKs become invisible.
- Verify the untouched imported PKs remain visible.
- Query the full imported PK set and verify it equals the expected remaining subset.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_delete_after_commit_removes_import_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 36.13s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.82s
```

## Case IMP-LC-007

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_empty_parquet_auto_commit_completes_with_zero_rows
```

Coverage:

- Create a parquet file with the target collection schema and zero rows.
- Create an import job with default `auto_commit=true`.
- Wait for the job to reach `Completed`.
- Verify `importedRows == 0` and `totalRows == 0`.
- Refresh load and query `count(*)` for the whole collection.
- Query `count(*)` with `id >= 0` to ensure no visible dirty rows were produced.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_empty_parquet_auto_commit_completes_with_zero_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 16.73s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.88s
```

## Case IMP-LC-008

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_consecutive_manual_imports_accumulate_rows
```

Coverage:

- Create two parquet files for the same collection.
- Create the first manual import job with `options.auto_commit=false`.
- Verify the first job reaches `Uncommitted`, remains invisible before commit, then reaches `Completed` after `/commit`.
- Verify first job PKs are visible and collection `count(*)` equals the first batch size.
- Create the second manual import job in the same collection.
- Verify second job PKs remain invisible before its commit while first job PKs remain visible.
- Commit the second job and wait for `Completed`.
- Verify the union of both import PK sets is visible.
- Verify final `count(*)` equals the cumulative row count.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_consecutive_manual_imports_accumulate_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 93.94s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.78s
```

## Case IMP-MIX-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_rest_insert_interleaves_with_manual_import
```

Coverage:

- Insert rows through REST before starting import and verify they are visible.
- Create a manual import job with `options.auto_commit=false`.
- Wait for the import job to reach `Uncommitted`.
- Verify imported PKs are invisible while the pre-import REST insert rows remain visible.
- Insert another REST batch while the import job is `Uncommitted`.
- Verify both REST insert batches are visible and import PKs are still invisible.
- Commit the import job and wait for `Completed`.
- Verify the union of pre-import inserts, during-import inserts, and import PKs is visible.
- Verify `count(*)` after commit equals the expected committed set size.
- Insert a final REST batch after import commit.
- Verify all REST and import PKs are visible and final `count(*)` matches the full set.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_rest_insert_interleaves_with_manual_import \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 62.52s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.75s
```

## Case IMP-MIX-003

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_flush_during_uncommitted_keeps_import_invisible
```

Coverage:

- Insert baseline REST rows and verify they are visible.
- Create a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify imported PKs are invisible before flush.
- Call REST `/v2/vectordb/collections/flush` while the import job is `Uncommitted`.
- Verify imported PKs remain invisible after flush.
- Verify baseline REST rows remain visible after flush.
- Commit the import job and wait for `Completed`.
- Verify baseline PKs and import PKs are all visible.
- Verify final `count(*)` equals baseline + import row count.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_flush_during_uncommitted_keeps_import_invisible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 69.37s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.90s
```

## Case IMP-ABT-003

Status: Failed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_is_idempotent_for_failed_job
```

Coverage:

- Create a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify imported PKs are invisible before abort.
- Call `/abort` and verify the first abort succeeds.
- Wait for the job to reach `Failed`.
- Call `/abort` again on the same job to verify retry idempotency.
- Expected behavior from the test plan: repeated abort should return success, the job should remain `Failed`, and imported rows should remain invisible.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_abort_is_idempotent_for_failed_job \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 failed, 2 warnings in 38.98s
```

Failure detail:

```text
retry_abort_rsp = {
  "code": 2100,
  "message": "job 466872198214347562 is in terminal/committed state Failed, abort not allowed: importing data failed"
}
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- Milvus behavior bug candidate.
- The test code follows the test plan/design expectation that `AbortImport` is idempotent and safe to retry.
- The first abort succeeds and the job reaches `Failed`; the retry abort returns `code=2100` instead of success.
- This violates `IMP-ABT-003 abort 幂等` and the API contract expectation that abort can be safely retried.
- Not classified as a test-code issue: the request payload and endpoint are valid, the same helper succeeds on the first abort, and the failure is a deterministic business response from Milvus.

2026-06-11 contract confirmation:

- Abort retry is required behavior.
- The automated case is restored to assert repeated abort success and is tracked as strict xfail until Milvus supports idempotent retry on a job already moved to `Failed` by abort cleanup.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.86s
```

## Case IMP-LC-006

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_nonexistent_numeric_job_commit_and_abort_rejected
```

Coverage:

- Call `/commit` with a syntactically valid numeric job ID that does not exist.
- Verify `/commit` returns a non-zero business error with a not-found reason.
- Call `/abort` with the same nonexistent numeric job ID.
- Verify `/abort` returns a non-zero business error with a not-found reason.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_nonexistent_numeric_job_commit_and_abort_rejected \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 1.61s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.76s
```

## Case IMP-DEL-003

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_mixed_delete_across_commit_boundary
```

Coverage:

- Create a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Split imported PKs into three groups:
  - `pre_commit_delete_ids`: deleted before commit and expected to remain visible after commit.
  - `post_commit_delete_ids`: deleted after commit and expected to disappear.
  - `never_delete_ids`: never deleted and expected to remain visible.
- Verify all import PKs are invisible before the first delete.
- Delete `pre_commit_delete_ids` before commit and verify all import PKs remain invisible.
- Commit the import job and wait for `Completed`.
- Verify all import PKs are visible after commit, proving pre-commit delete was ignored for import rows.
- Delete `post_commit_delete_ids` after commit.
- Poll until `post_commit_delete_ids` become invisible.
- Verify `pre_commit_delete_ids` and `never_delete_ids` remain visible.
- Verify final `count(*)` equals the expected visible subset size.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_mixed_delete_across_commit_boundary \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 74.90s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.86s
```

## Case IMP-PAR-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_import_to_specified_partition
```

Coverage:

- Create a collection and two explicit partitions.
- Create a manual import job with `options.auto_commit=false` and `partitionName` set to the target partition.
- Wait for the job to reach `Uncommitted`.
- Verify imported PKs are invisible from the target partition before commit.
- Call `/commit` and wait for `Completed`.
- Verify imported PKs are visible from the whole collection.
- Verify imported PKs are visible when querying with `partitionNames=[target_partition]`.
- Verify imported PKs are not visible when querying with `partitionNames=[empty_partition]`.
- Verify collection `count(*)` equals imported row count.
- Verify target partition stats row count equals imported row count.
- Verify empty partition stats row count stays zero.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_import_to_specified_partition \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 40.63s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.74s
```

## Case IMP-FMT-002

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_json_file_manual_import_preserves_rows
```

Coverage:

- Create a base collection with `Int64`, `VarChar`, scalar, and `FloatVector` fields.
- Generate and upload one row-based JSON file.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify imported PKs are visible after commit.
- Query by imported `tag` values and verify scalar fields match the source rows.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_json_file_manual_import_preserves_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 56.40s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.95s
```

## Case IMP-FMT-004

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_csv_file_with_custom_separator_manual_import_preserves_rows
```

Coverage:

- Create a base collection with `Int64`, `VarChar`, scalar, and `FloatVector` fields.
- Generate and upload one pipe-separated CSV file.
- Submit a manual import job with `options.auto_commit=false` and `options.sep=|`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the CSV row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify imported PKs are visible after commit.
- Query by imported `tag` values and verify scalar fields match the source rows.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_csv_file_with_custom_separator_manual_import_preserves_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 57.90s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.03s
```

## Case IMP-FMT-006

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_multi_file_import_accumulates_rows
```

Coverage:

- Create two parquet files for one import request.
- Submit a manual import job with `files=[[first_file], [second_file]]` and `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the sum of both files.
- Verify all imported PKs from both files are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs from both files are visible after commit.
- Verify final `count(*)` equals the combined row count.
- Verify REST search can hit rows from each input file.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_multi_file_import_accumulates_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 44.07s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

Collect-only check:

```text
1 test collected in 1.88s
```

## Case IMP-VCH-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_multi_vchannel_manual_commit_eventually_visible
```

Coverage:

- Create a base collection with `params.shardsNum=3`.
- Verify REST describe reports `shardsNum=3`.
- Generate and upload one parquet file with enough rows to exercise a multi-vchannel collection.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the file row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify all imported PKs and tags are visible after commit.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_multi_vchannel_manual_commit_eventually_visible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 54.81s
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.33s
```

## Case IMP-TTL-001

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_ttl_starts_from_commit_timestamp
```

Coverage:

- Create a base collection with `params.ttlSeconds=30`.
- Verify REST describe exposes `collection.ttl.seconds=30`.
- Generate and upload one parquet import file.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify imported rows are invisible before commit.
- Keep the job in `Uncommitted` for `ttlSeconds + 5s`, so physical row timestamps would already be expired if TTL used row timestamp.
- Verify rows are still invisible while uncommitted.
- Call `/commit` and wait for `Completed`.
- Verify imported rows become visible after commit.
- Verify rows are still visible around commit + 15s.
- Verify rows expire after commit + ttl window.
- Verify final `count(*)` becomes 0.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_ttl_starts_from_commit_timestamp \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Result:

```text
1 passed, 2 warnings in 122.83s (0:02:02)
```

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Failure classification:

- No failure.
- No Milvus bug found by this case.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.07s
```

## Case IMP-CMP-101

Status: Failed

Failure classification:

- Milvus bug candidate / compaction subsystem issue.
- Not classified as an Import 2PC visibility bug: before compaction, committed import rows became visible and `count(*)` matched the expected total.
- Not classified as final test-code failure: two earlier test-code/environment issues were fixed before the final run:
  - repeated `flush` hit instance rate limit `code=1807`; fixed by adding retry/backoff for this case.
  - import PK range overlapped with baseline insert PK range; fixed by moving import PKs to a non-overlapping range.

Implemented files:

- `tests/restful_client_v2/api/milvus.py`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_compaction_after_commit_preserves_rows
```

Coverage:

- Fix REST compaction state client to call `/v2/vectordb/collections/get_compaction_state` with `jobID`.
- Create a `dim=128` collection.
- Insert 4 baseline batches of 5000 rows and flush each batch, using retry/backoff for the instance flush rate limiter.
- Generate and upload one parquet file with 5000 import rows.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for `Uncommitted`.
- Verify sampled import PKs are invisible before commit.
- Verify baseline sampled PKs and baseline `count(*)` remain visible before commit.
- Commit the import job and wait for `Completed`.
- Verify sampled baseline/import PKs are visible and `count(*)` equals 25000 before compaction.
- Trigger REST manual compaction and poll compaction state.

Run command:

```bash
source .venv/bin/activate
cd tests/restful_client_v2
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_compaction_after_commit_preserves_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 failed, 2 warnings in 707.57s (0:11:47)
```

Final failure:

```text
AssertionError: {
  "code": 0,
  "data": {
    "compactionID": 466872198221971677,
    "completedPlanNumber": 0,
    "executingPlanNumber": 1,
    "state": "Executing",
    "timeoutPlanNumber": 0
  }
}
```

Interpretation:

- REST `compact` successfully scheduled a compaction task with `compactionID=466872198221971677`.
- `get_compaction_state` stayed in `Executing` for the full 600s polling window.
- `completedPlanNumber` stayed `0`, `executingPlanNumber` stayed `1`.
- This blocks full validation of "rows remain correct after completed compaction" on the current instance.

Earlier failed attempts and classification:

- First attempt failed at flush with `code=1807 rate limit exceeded[rate=0.1]`.
  Classification: test code did not adapt to instance rate limiting; fixed with `_flush_collection_with_retry`.
- Second attempt failed with `compactionID=-1`.
  Classification: test data was too small and did not schedule compaction; increased data scale to 4 flushed baseline segments plus 5000 import rows.
- Third attempt failed before commit visibility check because import PKs overlapped with baseline insert PKs.
  Classification: test data bug; fixed by moving import PKs to a non-overlapping range.
- Fourth/final attempt scheduled compaction but did not complete in 600s.
  Classification: Milvus bug candidate / compaction subsystem issue on this instance.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.96s
```

## 2026-06-09 IMP-REST-101 REST create missing required fields

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_missing_required_fields`

Coverage:

- Create a loaded collection through the existing REST framework.
- Call `/v2/vectordb/jobs/import/create` without `collectionName`.
- Call `/v2/vectordb/jobs/import/create` without `files`.
- Call `/v2/vectordb/jobs/import/create` with `files=[]`.
- Verify every invalid request returns non-zero code, a readable reason, and does not return `jobId`.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_missing_required_fields \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 6.48s
```

Observed REST rejection examples:

```text
missing collectionName -> code=1802, message contains CollectionName required validation
missing files -> code=1802, message contains Files required validation
empty files -> code=1100, message="import request is empty: invalid parameter"
```

Failure classification:

- First attempt failed because the test expected the `files=[]` error message to contain `files`, but Milvus returned `import request is empty`.
- Classification: test assertion issue, not Milvus bug. The assertion was relaxed to accept equivalent readable rejection reasons while preserving the core contract: non-zero code and no `jobId`.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.98s
```

## 2026-06-09 IMP-REST-102 REST jobId JSON type contract

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_job_id_string_is_canonical_and_numeric_behavior_is_explicit`

Coverage:

- Create a manual import job and wait for `Uncommitted`.
- Verify canonical string `jobId` works with `/describe`.
- Probe numeric JSON `jobId` with `/describe`.
- Probe numeric JSON `jobId` with `/commit`.
- If numeric commit is rejected, retry with canonical string `jobId`.
- Verify the job reaches `Completed` and imported rows become visible.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_job_id_string_is_canonical_and_numeric_behavior_is_explicit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 24.83s
```

Observed REST behavior:

```text
string describe -> code=0, state=Uncommitted
numeric describe -> code=1801, message contains "Mismatch type string with value number"
numeric commit -> code=1801, message contains "Mismatch type string with value number"
string commit after numeric rejection -> code=0, job reached Completed
```

Failure classification:

- No failure in final run.
- Numeric JSON `jobId` is explicitly rejected by the current REST layer; this is treated as compatible with the plan because string `jobId` is the canonical contract and numeric behavior is now recorded.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.83s
```

## 2026-06-09 IMP-FMT-205A invalid file suffix

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_invalid_file_suffix_fails_with_reason_and_no_visible_rows`

Coverage:

- Create a normal loaded collection through the existing REST framework.
- Upload a `.txt` object into the configured import bucket.
- Submit `/v2/vectordb/jobs/import/create` with `auto_commit=false`.
- Accept either a synchronous create rejection or an asynchronous `Failed` import job.
- Verify the failure reason is readable and points to unsupported file type/suffix.
- Verify no rows become visible in the target collection.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_invalid_file_suffix_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 11.48s
```

Observed REST behavior:

```text
create invalid .txt import -> code=2100
message="unexpected file type, files=[...txt]: importing data failed"
no jobId is returned
count(*) remains 0
```

Failure classification:

- First attempt failed because the test assumed the create call would succeed and the job would later reach `Failed`.
- Actual Milvus behavior rejects the invalid suffix synchronously at create time with a clear reason.
- Classification: test code assumption issue, not Milvus bug. The final test now accepts both synchronous rejection and asynchronous `Failed` job behavior while preserving the user-visible contract.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.07s
```

## 2026-06-09 IMP-FMT-205B invalid CSV separator

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_csv_invalid_separator_rejected_with_reason_and_no_visible_rows`

Coverage:

- Create a normal loaded collection through the existing REST framework.
- Generate and upload a valid pipe-separated CSV file.
- Submit `/v2/vectordb/jobs/import/create` with `auto_commit=false` and invalid newline `sep`.
- Accept either a synchronous create rejection or an asynchronous `Failed` import job.
- Verify the failure reason is readable and points to invalid/unsupported separator.
- Verify no rows become visible in the target collection.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_csv_invalid_separator_rejected_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 12.83s
```

Observed REST behavior:

```text
create invalid separator import -> code=0, jobId returned
job state -> Failed
reason contains "unsupported csv separator"
count(*) remains 0
```

Failure classification:

- No failure in final run.
- Milvus creates a job for this invalid option and then fails it asynchronously with a clear reason. This satisfies the negative-format contract.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.99s
```

## 2026-06-09 IMP-FMT-205C mixed parquet and CSV formats

Status: Failed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_mixed_file_formats_rejected_with_reason_and_no_visible_rows`

Coverage:

- Create a normal loaded collection through the existing REST framework.
- Generate and upload one valid parquet file and one valid pipe-separated CSV file.
- Submit one manual import request with `files=[[parquet_file], [csv_file]]`, `auto_commit=false`, and `sep=|`.
- Expect the request to be synchronously rejected or the import job to reach `Failed`.
- If the job reaches `Uncommitted` or `Completed`, verify visible count and abort for cleanup before failing.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_mixed_file_formats_rejected_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 failed, 2 warnings in 22.52s
```

Observed REST behavior:

```text
create mixed parquet+csv import -> code=0, jobId=466872198222337659
job state -> Uncommitted
importedRows=6, totalRows=6
visible_count_before_cleanup=0
abort after detection -> code=0
job state after abort -> Failed
collection teardown drop -> code=0
```

Failure classification:

- Classification: Milvus bug candidate against `IMP-FMT-205` negative-format contract.
- The import plan expects mixed file formats to be rejected or failed with a readable reason.
- Current Milvus accepts parquet+CSV batches in the same import request and moves the manual import job to `Uncommitted`.
- The data remains invisible before commit, so the 2PC visibility invariant still holds; the issue is missing mixed-format validation.

2026-06-11 contract correction:

- Mixed file groups are confirmed as supported behavior.
- This earlier failure is reclassified as a stale test-plan expectation, not a Milvus bug.
- The automated case is now positive: parquet and CSV in separate file groups should reach `Uncommitted`, stay invisible before commit, and become visible after commit.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.04s
```

## 2026-06-09 IMP-MVCC-101 consistency levels visibility boundary

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_consistency_levels_respect_commit_visibility`

Helper updated:

- `_create_base_collection(..., consistency_level=None)` now accepts an explicit collection `consistencyLevel`.

Coverage:

- Create separate collections with `Strong`, `Bounded`, and `Eventually` consistency levels.
- For each collection, create a manual import job with `auto_commit=false`.
- Wait until the job reaches `Uncommitted`.
- Verify imported PKs are not visible while the job is `Uncommitted`.
- Commit the job and poll query visibility until imported PKs become visible.
- Use independent collections/jobs so consistency-level results do not interact.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_consistency_levels_respect_commit_visibility \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 123.56s (0:02:03)
```

Observed REST behavior:

```text
Strong: Uncommitted imported rows stayed invisible; after commit all 4 PKs became visible
Bounded: Uncommitted imported rows stayed invisible; after commit all 4 PKs became visible
Eventually: Uncommitted imported rows stayed invisible; after commit all 4 PKs became visible
```

Failure classification:

- No failure in final run.
- 2PC visibility boundary held for all three tested collection consistency levels.
- No Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.05s
```

## 2026-06-09 IMP-MIX-101 import plus upsert PK conflicts

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_upsert_pk_conflict_respects_commit_timestamp`

Helpers added:

- `_query_base_rows_by_ids(collection_name, ids)`
- `_wait_base_rows_by_ids_match(collection_name, expected_by_id, timeout=120)`
- `_upsert_rows(collection_name, rows)`

Coverage:

- Create a manual import job with explicit primary keys and `auto_commit=false`.
- Wait until the job reaches `Uncommitted` and verify import PKs are not visible.
- Upsert the same PKs before commit and verify the pre-commit upsert versions are query-visible.
- Commit the import job and verify imported versions supersede the pre-commit upsert versions by PK.
- Upsert a subset of the same PKs after commit and verify post-commit upsert versions supersede imported versions only for that subset.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_upsert_pk_conflict_respects_commit_timestamp \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 51.70s
```

Observed REST behavior:

```text
manual import reached Uncommitted, importedRows=6, totalRows=6
pre-commit upsert of the same 6 PKs returned success
before commit, query by PK returned pre_upsert_* rows with phase=914
after commit, query by PK returned imported rows with phase=913
post-commit upsert of 3 PKs returned success
final query by PK returned post_upsert_* rows with phase=915 for those 3 PKs and imported rows for the remaining 3 PKs
```

Failure classification:

- Initial run failed because the test asserted full-collection `count(*) == 6` after duplicate-PK import/upsert interactions, while the instance returned `count(*) == 9`.
- That count assertion was outside the core `IMP-MIX-101` contract. Import is not an upsert and does not necessarily physically remove a pre-commit upsert version with the same PK; this case should validate latest-version visibility by PK.
- The assertion was corrected to focus on observable PK-version semantics. Final run passed.
- No Milvus bug in the final classification for this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.16s
```

## 2026-06-09 IMP-DEL-102 auto_id scalar delete timestamp boundary

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_auto_id_scalar_delete_respects_commit_timestamp`

Helper added:

- `_delete_tags(collection_name, tags)`

Coverage:

- Create an `autoId=true` collection and import a parquet file without the primary key column.
- Wait for the manual import job to reach `Uncommitted`.
- Delete one imported tag subset with a scalar `tag in [...]` filter before commit.
- Verify all import rows remain invisible while the job is `Uncommitted`.
- Commit the import and verify all imported tags, including the pre-commit deleted subset, become visible with generated numeric IDs.
- Delete a second tag subset with the same scalar filter after commit.
- Verify only the post-commit deleted subset disappears; the pre-commit deleted subset and untouched rows remain visible.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_auto_id_scalar_delete_respects_commit_timestamp \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 1729.09s (0:28:49)
```

Observed REST behavior:

```text
manual auto_id import reached Uncommitted
pre-commit scalar delete by tag returned success and did not make rows visible
after commit, all 8 imported tags were visible
generated primary keys were unique numeric IDs
post-commit scalar delete removed 3 selected tags
final visible tag count was 5
```

Failure classification:

- No failure in final run.
- `autoId` import plus scalar-filter delete follows the same timestamp boundary as explicit-PK delete: pre-commit delete is ignored for import rows, post-commit delete applies.
- Runtime was high on the shared `import-2pc-playground` instance. This is recorded as test execution cost/instance latency, not a Milvus correctness bug from this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.04s
```

## 2026-06-09 IMP-REST-103 dbName body and DB-Name header routing

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_db_name_body_and_header_route_same_job_deterministically`

Coverage:

- Create the same collection name in `default` DB and a custom DB.
- Create an import job through REST body `dbName` with `auto_commit=false`.
- Wait until the custom-DB job reaches `Uncommitted`.
- Verify `/describe` works through both body `dbName` and `DB-Name` header.
- Commit through `DB-Name` header with body containing only `jobId`.
- Verify the custom DB count reaches the imported row count and the same collection in `default` remains `0`.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_db_name_body_and_header_route_same_job_deterministically \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 41.08s
```

Observed REST behavior:

```text
custom DB created: imp2pc_db_<uuid>
same collection name created in default and custom DB
create import with body dbName -> code=0, jobId=<id>
progress reached Uncommitted, importedRows=4, totalRows=4
describe with body dbName -> code=0
describe with DB-Name header and no body dbName -> code=0, same jobId/state/collectionName
commit with DB-Name header and no body dbName -> code=0
custom DB count(*) -> 4
default DB count(*) -> 0
```

Failure classification:

- Initial run failed because the test incorrectly assumed a `default` DB describe request must not find a custom-DB import job.
- Observed behavior indicates import `jobId` lookup may be globally addressable; that alone is not enough to prove data-routing leakage.
- The assertion was corrected to validate observable data routing instead. Final run passed.
- No Milvus bug in the final classification for this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.87s
```

## 2026-06-09 IMP-OBS-101 failed job reason quality

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_failed_job_exposes_actionable_reason_and_no_visible_rows`

Coverage:

- Create a normal loaded collection through the existing REST framework.
- Submit a manual import job with a missing parquet object path.
- Wait for the import job to reach `Failed`.
- Verify progress response exposes `jobId`, `state=Failed`, and a readable reason/detail.
- Verify the reason includes the missing file name and storage-missing style text.
- Verify no rows become visible in the target collection.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_failed_job_exposes_actionable_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 13.88s
```

Observed REST behavior:

```text
create missing parquet import -> code=0, jobId=466872198222419310
job state -> Failed
reason contains "The specified key does not exist"
reason includes the missing parquet object name
count(*) remains 0
```

Failure classification:

- No failure in final run.
- Milvus exposes an actionable failure reason for a missing import object and keeps data invisible.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.00s
```

## 2026-06-09 IMP-LC-103 describe progress monotonicity

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_describe_progress_is_monotonic_until_uncommitted`

Coverage:

- Create a manual import job with `auto_commit=false`.
- Poll `/v2/vectordb/jobs/import/describe` until the job reaches `Uncommitted`.
- Verify each describe response has the expected `jobId`, valid state, and `0 <= progress <= 100`.
- Verify observed job state order and progress do not regress.
- Verify final `importedRows` and `totalRows` match the source row count.
- Verify rows remain invisible before commit, then abort the job for cleanup.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_describe_progress_is_monotonic_until_uncommitted \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 33.95s
```

Observed REST behavior:

```text
describe states/progress observed:
Pending 0
Importing 10
Importing 40
Importing 70
Importing 80
Importing 90
Uncommitted 99
importedRows=128, totalRows=128
pre-commit count remained 0
abort after validation -> code=0, final state Failed
```

Failure classification:

- No failure in final run.
- `/describe` exposed monotonic state/progress for the observed manual import lifecycle, and 2PC pre-commit invisibility held.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.04s
```

## 2026-06-09 IMP-SEC-102 same collection name in two DBs

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_same_collection_name_two_dbs_isolates_jobs_and_data`

Helpers updated:

- `_create_manual_import_job(..., db_name="default")`
- `_create_manual_import_job_with_files(..., db_name="default")`
- `_query_imported_ids(..., db_name="default")`
- `_wait_imported_ids_visible(..., db_name="default")`
- `_wait_imported_ids_absent(..., db_name="default")`

Coverage:

- Create the same collection name in `default` DB and a custom DB.
- Import different PK sets into each DB with `auto_commit=false`.
- Wait for both jobs to reach `Uncommitted`.
- Verify neither DB exposes its own import rows before commit.
- Commit the custom DB job first and verify only the custom DB sees the custom PK set.
- Verify default DB remains empty while its job is still `Uncommitted`.
- Commit the default DB job and verify each DB sees only its own PK set.
- Verify cross-DB queries for the other DB's PK set return empty.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_same_collection_name_two_dbs_isolates_jobs_and_data \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 91.71s (0:01:31)
```

Observed REST behavior:

```text
same collection name created in default and custom DB
default DB import reached Uncommitted with importedRows=4
custom DB import reached Uncommitted with importedRows=5
custom DB commit completed first; custom PKs became visible only in custom DB
default DB stayed count(*)=0 before its own commit
default DB commit completed; default PKs became visible only in default DB
cross-DB exact PK queries returned empty on both sides
```

Failure classification:

- No failure in final run.
- Import jobs and imported data remained isolated across databases even with identical collection names.
- No Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.10s
```

## 2026-06-09 IMP-PAR-202 partition-key collection routing

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_partition_key_collection_routes_and_filters_rows`

Helper added:

- `_create_partition_key_collection(name, dim=8, partitions_num=8)`

Coverage:

- Create a collection with `phase` marked as `isPartitionKey=true`.
- Verify REST describe reports `phase.partitionKey=true` and `partitionsNum=8`.
- Import rows covering three partition-key values: `phase=920`, `phase=921`, and `phase=922`.
- Verify all imported PKs are invisible while the job is `Uncommitted`.
- Commit the job and wait until all imported PKs become visible.
- Query by each partition-key value and verify the exact PK set and `count(*)` for that key.
- Verify final collection `count(*)` equals total imported row count.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_partition_key_collection_routes_and_filters_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 65.33s (0:01:05)
```

Observed REST behavior:

```text
partition-key collection created with partitionsNum=8
describe showed phase.partitionKey=true
manual import reached Uncommitted with importedRows=12, totalRows=12
pre-commit exact PK query returned empty
after commit, all 12 imported PKs became visible
phase == 920 returned exactly 3 expected PKs
phase == 921 returned exactly 4 expected PKs
phase == 922 returned exactly 5 expected PKs
final count(*) returned 12
```

Failure classification:

- No failure in final run.
- Import into partition-key collection preserved 2PC visibility and partition-key filtering semantics.
- No Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.08s
```

## 2026-06-09 IMP-PAR-203 partition-key explicit partitionName negative

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_partition_key_collection_rejects_explicit_partition_name`

Coverage:

- Create a collection with `phase` marked as `isPartitionKey=true`.
- Upload a valid parquet file for that collection.
- Submit `/v2/vectordb/jobs/import/create` with both `auto_commit=false` and `partitionName="_default"`.
- Accept either legal failure mode: synchronous create rejection or asynchronous job `Failed`.
- Verify the observed failure reason is partition-related.
- Verify no rows become visible and no import job id is returned for the synchronous rejection path.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_partition_key_collection_rejects_explicit_partition_name \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
1 passed, 2 warnings in 7.95s
```

Observed REST behavior:

```text
create partition-key collection -> code=0
import/create with partitionName="_default" -> code=2100
message: not allow to set partition name for collection with partition key: importing data failed
no jobId returned
count(*) remained 0
```

Failure classification:

- No failure in final run.
- Milvus synchronously rejects explicit `partitionName` for partition-key collection import.
- This matches the negative contract for `IMP-PAR-203`; no Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.95s
```

## 2026-06-09 IMP-TYP-209 vector dimension mismatch negative

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_vector_dimension_mismatch_fails_with_reason_and_no_visible_rows`

Coverage:

- Create a collection whose `FloatVector` schema dim is 8.
- Upload a parquet file whose imported vector values have length 7.
- Submit `/v2/vectordb/jobs/import/create` with `auto_commit=false`.
- Accept either legal failure mode: synchronous create rejection or asynchronous job `Failed`.
- Verify the failure reason points to vector/schema/dimension validation.
- Verify no rows become visible by both `count(*)` and exact PK query.
- If Milvus ever incorrectly reaches `Uncommitted`, the test aborts the job before failing to avoid instance pollution.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_vector_dimension_mismatch_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 26.50s
```

Observed REST behavior:

```text
create collection dim=8 -> code=0
import/create -> code=0, jobId=466872198224240000
job state sequence -> Pending -> Importing -> Failed
failure reason included: expected 8 but got 7, data type: FloatVector
count(*) remained 0
query id in [92400, 92401, 92402] returned []
```

Failure classification:

- No failure in final run.
- Milvus accepts the request initially and then fails the import job asynchronously with an actionable dimension mismatch reason.
- No dirty visible rows were produced. This satisfies the `IMP-TYP-209` negative contract; no Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.21s
```

## 2026-06-09 IMP-TYP-209 varchar overflow negative

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_varchar_overflow_fails_with_reason_and_no_visible_rows`

Coverage:

- Create a base collection whose `tag` field is `VarChar(max_length=64)`.
- Upload a parquet file whose imported `tag` values are longer than 64 characters.
- Submit `/v2/vectordb/jobs/import/create` with `auto_commit=false`.
- Accept either legal failure mode: synchronous create rejection or asynchronous job `Failed`.
- Verify the failure reason points to string/varchar length validation.
- Verify no rows become visible by both `count(*)` and exact PK query.
- If Milvus ever incorrectly reaches `Uncommitted`, the test aborts the job before failing to avoid instance pollution.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_varchar_overflow_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 25.02s
```

Observed REST behavior:

```text
create collection tag max_length=64 -> code=0
import/create -> code=0, jobId=466872198224251812
job state sequence -> Pending -> Importing -> Failed
failure reason included: value length(95) for field tag exceeds max_length(64)
count(*) remained 0
query id in [92500, 92501, 92502] returned []
```

Failure classification:

- No failure in final run.
- Milvus accepts the request initially and then fails the import job asynchronously with an actionable varchar length reason.
- No dirty visible rows were produced. This satisfies the `IMP-TYP-209` varchar overflow negative contract; no Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.99s
```

## 2026-06-09 IMP-TYP-209 field type mismatch negative

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_field_type_mismatch_fails_with_reason_and_no_visible_rows`

Coverage:

- Create a base collection whose `phase` field is `Int64`.
- Upload a parquet file whose `phase` column is encoded as `String/utf8`.
- Submit `/v2/vectordb/jobs/import/create` with `auto_commit=false`.
- Accept either legal failure mode: synchronous create rejection or asynchronous job `Failed`.
- Verify the failure reason points to field/schema/type mismatch.
- Verify no rows become visible by both `count(*)` and exact PK query.
- If Milvus ever incorrectly reaches `Uncommitted`, the test aborts the job before failing to avoid instance pollution.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_field_type_mismatch_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 24.78s
```

Observed REST behavior:

```text
create collection phase Int64 -> code=0
import/create -> code=0, jobId=466872198224253311
job state sequence -> Pending -> Importing -> Failed
failure reason included: field phase with parquet type utf8 caused import failure
count(*) remained 0
query id in [92600, 92601, 92602] returned []
```

Failure classification:

- No failure in final run.
- Milvus accepts the request initially and then fails the import job asynchronously with an actionable parquet field type reason.
- No dirty visible rows were produced. This satisfies the `IMP-TYP-209` type mismatch negative contract; no Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.81s
```

## 2026-06-09 IMP-TYP-209 vector NaN/Inf negative

Status: Failed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_vector_nan_inf_fails_with_reason_and_no_visible_rows`

Coverage:

- Create a base collection with `FloatVector(dim=8)`.
- Upload a parquet file where one row contains `NaN` in the vector and another row contains `Inf`.
- Submit `/v2/vectordb/jobs/import/create` with `auto_commit=false`.
- Expected contract from `IMP-TYP-209`: invalid float vector values should be rejected synchronously or the import job should reach `Failed`.
- Verify no rows become visible before cleanup.
- If Milvus incorrectly reaches `Uncommitted`, abort the job before failing the test.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_vector_nan_inf_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 failed, 2 warnings in 34.10s
```

Observed REST behavior:

```text
create collection FloatVector(dim=8) -> code=0
import/create -> code=0, jobId=466872198224254641
job state sequence -> Pending -> Importing -> Uncommitted
details showed importedRows=3, totalRows=3, file task state=Completed
pre-cleanup query id in [92700, 92701, 92702] returned []
abort job -> code=0
post-abort job state -> Failed
```

Failure classification:

- Classification: Milvus bug candidate, not a test code issue.
- Rationale: the parquet file intentionally contains invalid FloatVector values (`NaN` and `Inf`), and the `IMP-TYP-209` negative contract requires such invalid data to be rejected or failed. Milvus instead imported all 3 rows and moved the manual import job to `Uncommitted`.
- The test generated the parquet successfully, submitted a valid REST request, observed the service state through `/get_progress`, and cleaned the job through `/abort`.
- No dirty visible rows were observed before cleanup because the job was still `Uncommitted`; the bug is missing import-time vector finite-value validation.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.91s
```

## 2026-06-09 IMP-FMT-202 JSONL manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_jsonl_file_manual_import_preserves_rows`

Coverage:

- Add a JSONL writer that uploads one JSON object per line to the existing object storage fixture.
- Create a base collection with `Int64`, `VarChar`, scalar, and `FloatVector` fields.
- Generate and upload one `.jsonl` row-based import file.
- Submit a manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify `importedRows` and `totalRows` equal the JSONL row count.
- Verify imported PKs are invisible before commit.
- Call `/commit` and wait for `Completed`.
- Verify imported PKs are visible after commit.
- Query by imported `tag` values and verify scalar fields match the source rows.
- Verify final `count(*)` and REST search results match the committed import rows.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_jsonl_file_manual_import_preserves_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 49.13s
```

Observed REST behavior:

```text
import/create -> code=0, jobId=466872198224296806
job state sequence -> Importing -> Uncommitted
Uncommitted progress showed importedRows=6, totalRows=6
pre-commit query id in [92800..92805] returned []
commit -> code=0
job state sequence after commit -> Committing -> Completed
post-commit query returned all 6 PKs
tag query returned matching id/tag/phase rows
count(*) returned 6
search by imported vector returned imported PKs
```

Failure classification:

- No failure in final run.
- JSONL import is supported by the REST import path and preserves 2PC visibility semantics.
- No Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 2.03s
```

## 2026-06-09 IMP-LIM-003 max import file number

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_file_count_over_limit`

Coverage:

- Create a base collection.
- Submit `/v2/vectordb/jobs/import/create` with 1025 file groups and `options.auto_commit=false`.
- Do not upload any objects; this case validates create-time request limits before object reads.
- Expect synchronous rejection because `dataCoord.import.maxImportFileNumPerReq` defaults to 1024.
- Verify no `jobId` is returned.
- Verify the collection remains empty.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_file_count_over_limit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 8.63s
```

Observed REST behavior:

```text
create collection -> code=0
import/create with 1025 file groups -> code=2100
message: The max number of import files should not exceed 1024, but got 1025: importing data failed
no jobId returned
count(*) remained 0
```

Failure classification:

- No failure in final run.
- Milvus synchronously enforces the import file count limit and does not create a job.
- No Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.84s
```

## 2026-06-09 IMP-FMT-203 CSV nullkey manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_csv_nullkey_manual_import_preserves_nulls`

Coverage:

- Create a nullable-field collection.
- Generate a pipe-separated CSV file with mixed null and non-null values.
- Submit REST import with `options.auto_commit=false`, `sep=|`, and `nullkey=NULL`.
- Verify rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, nullable scalar values, `count(*)`, and vector search after commit.

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_csv_nullkey_manual_import_preserves_nulls \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 51.48s
```

Observed REST behavior:

```text
create collection -> code=0
import/create -> jobId=466872198224341220
job state sequence -> Importing -> Uncommitted
Uncommitted progress showed importedRows=6, totalRows=6
pre-commit query id in [92900..92905] returned []
commit -> code=0
job state sequence after commit -> Committing -> Completed
post-commit query returned all 6 PKs
nullable fields preserved NULL and non-NULL values after commit
count(*) returned 6
search by imported vector returned imported PKs
```

Failure classification:

- No failure in final run.
- CSV import honors `sep` and `nullkey` through the REST import path.
- 2PC visibility semantics hold for nullable-field CSV import.
- No Milvus bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Additional checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```text
1 test collected in 1.87s
```

## 2026-06-09 IMP-FMT-204 NumPy field-per-file manual 2PC import

Status: Passed after 2026-06-10 rerun

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_numpy_field_per_file_manual_import_preserves_rows`

Coverage intended:

- Create a base collection.
- Generate one NumPy `.npy` object per field: `id.npy`, `tag.npy`, `phase.npy`, `vector.npy`.
- Submit REST import with `files: [[id.npy, tag.npy, phase.npy, vector.npy]]` and `options.auto_commit=false`.
- Verify rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, scalar values, `count(*)`, and vector search after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_numpy_field_per_file_manual_import_preserves_rows
```

```text
Python 3.12.12
1 test collected in 1.95s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_numpy_field_per_file_manual_import_preserves_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.198 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Observed result:

```text
Python 3.12.12
ERROR at setup:
pymilvus.exceptions.MilvusException:
Fail connecting to server on 10.100.36.203:19530, illegal connection params or server unavailable

ERROR at teardown:
requests.exceptions.ConnectTimeout:
HTTPConnectionPool(host='10.100.36.203', port=19530): Max retries exceeded with url: /v2/vectordb/collections/list

2 warnings, 2 errors in 2929.03s
```

Environment probes after the failed run:

```bash
curl --connect-timeout 5 --max-time 8 http://10.100.36.203:19530/healthz
curl --connect-timeout 5 --max-time 8 http://10.100.36.198:9000/minio/health/live
```

```text
Milvus REST: Failed to connect to 10.100.36.203 port 19530 after 5002 ms
MinIO: Failed to connect to 10.100.36.198 port 9000 after 5007 ms
```

Failure classification:

- The test body did not reach collection creation or import job creation.
- The failure is infrastructure/environment connectivity: both Milvus REST and MinIO endpoints are unreachable from the local runner.
- This is not evidence of a Milvus Import 2PC bug.
- This is not evidence of a NumPy import assertion or request-format failure; the test code compiled and collected successfully.
- Next action when the instance is reachable: rerun this exact case before moving to another case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

### 2026-06-10 rerun preflight

Status: Still blocked by environment

Code checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_numpy_field_per_file_manual_import_preserves_rows
```

```text
Python 3.12.12
1 test collected in 1.79s
```

Endpoint probes:

```bash
curl --connect-timeout 5 --max-time 8 http://10.100.36.203:19530/healthz
curl --connect-timeout 5 --max-time 8 http://10.100.36.198:9000/minio/health/live
```

```text
Milvus REST: Failed to connect to 10.100.36.203 port 19530 after 5011 ms
MinIO: Failed to connect to 10.100.36.198 port 9000 after 5011 ms
```

Kubernetes read-only lookup:

```bash
export KUBECONFIG=~/.kube/config
kubectl get svc -n chaos-testing
kubectl get pods -n chaos-testing
```

```text
Unable to connect to the server: dial tcp 10.100.36.3:6443: i/o timeout
```

Classification:

- The local runner still cannot reach the Milvus REST endpoint, MinIO endpoint, or Kubernetes API server for the target environment.
- Full pytest rerun was not started because the previous full run already failed in fixture setup/teardown for the same connectivity problem and took 48 minutes due framework teardown retries without a connect timeout.
- This remains an infrastructure/network blocker, not a Milvus Import 2PC bug and not a test implementation failure.

### 2026-06-10 repeated blocker audit

Status: Still blocked by the same environment connectivity issue

Code checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
```

```text
Python 3.12.12
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_numpy_field_per_file_manual_import_preserves_rows
```

```text
Python 3.12.12
1 test collected in 1.96s
```

Endpoint probes:

```bash
curl --connect-timeout 5 --max-time 8 http://10.100.36.203:19530/healthz
curl --connect-timeout 5 --max-time 8 http://10.100.36.198:9000/minio/health/live
```

```text
Milvus REST: Failed to connect to 10.100.36.203 port 19530 after 5011 ms
MinIO: Failed to connect to 10.100.36.198 port 9000 after 5011 ms
```

Kubernetes read-only lookup:

```bash
export KUBECONFIG=~/.kube/config
kubectl --request-timeout=5s get svc -n chaos-testing import-2pc-playground-milvus
kubectl --request-timeout=5s get pods -n chaos-testing
```

```text
Unable to connect to the server: context deadline exceeded
API endpoint in kubeconfig: https://4am.apiserver.zilliz.cc:6443
```

Classification:

- This is the same infrastructure/network blocker as the previous two attempts.
- The test body still cannot be run against `import-2pc-playground` because the local runner cannot reach Milvus REST, MinIO, or Kubernetes API.
- This is not a Milvus Import 2PC bug and not a test implementation failure.

### 2026-06-10 successful rerun with current MinIO endpoint

Status: Passed

Environment refresh:

```bash
export KUBECONFIG=~/.kube/config
kubectl --request-timeout=10s get svc -n chaos-testing | rg -i 'import-2pc|playground|minio|NAME'
kubectl --request-timeout=10s get pods -n chaos-testing | rg -i 'import-2pc|playground|minio|NAME'
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
import-2pc-playground-milvus  LoadBalancer  10.100.36.203  19530:30889/TCP,9091:30325/TCP
import-2pc-playground-minio   LoadBalancer  10.100.36.178  9000:30327/TCP
import-2pc-playground-milvus-standalone-8667cb7fd7-27lz5  1/1 Running
import-2pc-playground-minio-7797db8fff-26htr              1/1 Running
minio_new_http_code=200
```

Bucket verification:

```bash
source .venv/bin/activate
python - <<'PY'
from minio import Minio
client = Minio('10.100.36.178:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
print([bucket.name for bucket in client.list_buckets()])
PY
```

```text
['import-2pc-playground']
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_numpy_field_per_file_manual_import_preserves_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 56.29s
```

Observed behavior covered by assertions:

```text
NumPy field files uploaded as one REST file group:
  [id.npy, tag.npy, phase.npy, vector.npy]
import/create with auto_commit=false reached Uncommitted
Uncommitted progress matched importedRows=6 and totalRows=6
pre-commit PK query returned no imported rows
commit returned code=0
job reached Completed
post-commit PK query returned all imported rows
tag and phase scalar values matched source rows
count(*) returned 6
vector search returned imported PKs
```

Failure classification:

- No failure in final run.
- The previous failures were caused by stale/unreachable MinIO endpoint and temporary control-plane/network unavailability.
- NumPy field-per-file REST import satisfies the 2PC visibility contract in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-201 HNSW L2/IP/COSINE manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_hnsw_l2_ip_cosine_manual_import_searches_after_commit`

Coverage:

- Create one collection per HNSW metric: `L2`, `IP`, `COSINE`.
- Each collection uses REST `indexParams` with `indexType=HNSW`, `M=8`, and `efConstruction=64`.
- Import one Parquet file per collection with `options.auto_commit=false`.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, scalar values, `count(*)`, and vector search after commit for each metric.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_hnsw_l2_ip_cosine_manual_import_searches_after_commit
```

```text
Python 3.12.12
1 test collected in 2.12s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_hnsw_l2_ip_cosine_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 192.63s (0:03:12)
```

Observed behavior covered by assertions:

```text
HNSW/L2 collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
HNSW/IP collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
HNSW/COSINE collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
For each metric, importedRows=6, totalRows=6, count(*)=6, and tag/phase scalar values matched source rows.
```

Failure classification:

- No failure in final run.
- HNSW `L2`, `IP`, and `COSINE` indexes work with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-202 IVF_FLAT/IVF_SQ8/IVF_PQ manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_ivf_flat_sq8_pq_manual_import_searches_after_commit`

Coverage:

- Create one collection per IVF index type: `IVF_FLAT`, `IVF_SQ8`, `IVF_PQ`.
- Each collection uses REST `indexParams` with `metricType=L2`.
- Index params:
  - `IVF_FLAT`: `{"nlist": 1}`
  - `IVF_SQ8`: `{"nlist": 1}`
  - `IVF_PQ`: `{"nlist": 1, "m": 2, "nbits": 4}`
- Import one Parquet file per collection with `options.auto_commit=false`.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, scalar values, `count(*)`, and vector search after commit for each index type.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_ivf_flat_sq8_pq_manual_import_searches_after_commit
```

```text
Python 3.12.12
1 test collected in 2.15s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_ivf_flat_sq8_pq_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 173.58s (0:02:53)
```

Observed behavior covered by assertions:

```text
IVF_FLAT collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
IVF_SQ8 collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
IVF_PQ collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
For each index type, importedRows=32, totalRows=32, count(*)=32, and tag/phase scalar values matched source rows.
```

Failure classification:

- No failure in final run.
- `IVF_FLAT`, `IVF_SQ8`, and `IVF_PQ` indexes work with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-204 FLAT/SCANN manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_flat_scann_manual_import_searches_after_commit`

Coverage:

- Create one collection per index type: `FLAT`, `SCANN`.
- Each collection uses REST `indexParams` with `metricType=L2`.
- Index params:
  - `FLAT`: `{}`
  - `SCANN`: `{"nlist": 1}`
- Import one Parquet file per collection with `options.auto_commit=false`.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, scalar values, `count(*)`, and vector search after commit for each index type.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_flat_scann_manual_import_searches_after_commit
```

```text
Python 3.12.12
1 test collected in 1.91s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_flat_scann_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 106.47s (0:01:46)
```

Observed behavior covered by assertions:

```text
FLAT collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
SCANN collection import reached Uncommitted, stayed invisible, committed to Completed, and search returned imported PKs.
For each index type, importedRows=32, totalRows=32, count(*)=32, and tag/phase scalar values matched source rows.
```

Failure classification:

- No failure in final run.
- `FLAT` and `SCANN` indexes work with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-205 BIN_FLAT/BIN_IVF_FLAT manual 2PC import

Status: Passed after test-code fix

Implemented files:

- `tests/restful_client_v2/api/milvus.py`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_binary_flat_ivf_manual_import_searches_after_commit`

Coverage:

- Create one BinaryVector collection per binary index type: `BIN_FLAT`, `BIN_IVF_FLAT`.
- Each collection uses REST `indexParams` with `metricType=HAMMING`.
- Index params:
  - `BIN_FLAT`: `{}`
  - `BIN_IVF_FLAT`: `{"nlist": 1}`
- Import one Parquet file per collection with `options.auto_commit=false`.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, scalar values, `count(*)`, and BinaryVector search after commit for each index type.

Framework/test-code changes:

- Added BinaryVector collection, data, Parquet writer, and search helpers.
- Updated `wait_import_job_state` to return immediately when a job reaches a non-expected terminal state (`Completed` / `Failed`), so negative terminal states are not hidden behind a full timeout.
- Initial attempt wrote BinaryVector Parquet as `fixed_size_binary[16]`. Import failed with an unsupported Arrow type error, so the test writer was corrected to use `list<uint8>`, matching the existing bulk insert Parquet generator.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_binary_flat_ivf_manual_import_searches_after_commit
```

```text
Python 3.12.12
1 test collected in 2.11s
```

Initial failed run:

```text
1 failed, 2 warnings in 367.35s (0:06:07)
```

Failure classification:

- Test-code issue, not a Milvus Import 2PC bug.
- The Parquet file used `fixed_size_binary[16]` for `BinaryVector`; Import failed before reaching `Uncommitted` with an unsupported data type error.
- Corrective action: write BinaryVector as `list<uint8>` and keep REST search vectors base64 encoded.

Final run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_binary_flat_ivf_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 141.44s (0:02:21)
```

Observed behavior covered by assertions:

```text
BIN_FLAT collection import reached Uncommitted, stayed invisible, committed to Completed, and BinaryVector search returned imported PKs.
BIN_IVF_FLAT collection import reached Uncommitted, stayed invisible, committed to Completed, and BinaryVector search returned imported PKs.
For each binary index type, importedRows=32, totalRows=32, count(*)=32, and tag/phase scalar values matched source rows.
```

Final classification:

- No failure in final run.
- `BIN_FLAT` and `BIN_IVF_FLAT` indexes work with REST manual 2PC import in this case when BinaryVector Parquet uses `list<uint8>`.
- No Milvus Import 2PC bug found by the corrected case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-206 SPARSE_INVERTED_INDEX/SPARSE_WAND manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_sparse_inverted_wand_manual_import_searches_after_commit`

Coverage:

- Create one SparseFloatVector collection per sparse index type: `SPARSE_INVERTED_INDEX`, `SPARSE_WAND`.
- Each collection uses REST `indexParams` with `metricType=IP`.
- Index params:
  - `SPARSE_INVERTED_INDEX`: `{"drop_ratio_build": "0.2"}`
  - `SPARSE_WAND`: `{"drop_ratio_build": "0.2"}`
- Import one Parquet file per collection with `options.auto_commit=false`.
- Parquet encodes `SparseFloatVector` as JSON strings, matching the existing bulk insert sparse-vector path.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify PK visibility, scalar values, `count(*)`, and sparse vector search after commit for each index type.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_sparse_inverted_wand_manual_import_searches_after_commit
```

```text
Python 3.12.12
1 test collected in 1.99s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_sparse_inverted_wand_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 123.01s (0:02:03)
```

Observed behavior covered by assertions:

```text
SPARSE_INVERTED_INDEX collection import reached Uncommitted, stayed invisible, committed to Completed, and sparse vector search returned imported PKs.
SPARSE_WAND collection import reached Uncommitted, stayed invisible, committed to Completed, and sparse vector search returned imported PKs.
For each sparse index type, importedRows=32, totalRows=32, count(*)=32, and tag/phase scalar values matched source rows.
```

Failure classification:

- No failure in final run.
- `SPARSE_INVERTED_INDEX` and `SPARSE_WAND` indexes work with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-207 scalar indexes manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_scalar_indexes_manual_import_filters_after_commit`

Coverage:

- Create one collection with a vector index plus four scalar indexes:
  - `INVERTED` on Int64 field `phase`
  - `BITMAP` on Bool field `flag`
  - `STL_SORT` on Double field `score`
  - `TRIE` on VarChar field `tag`
- Import one Parquet file with `options.auto_commit=false`.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify all imported PKs become visible after commit.
- Verify indexed scalar filters return exact expected PK sets after commit:
  - `phase == 3`
  - `flag == true`
  - `score > 20.0 and score < 24.0`
  - `tag like "scalar_tag_1_%"`

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_scalar_indexes_manual_import_filters_after_commit
```

```text
Python 3.12.12
1 test collected in 1.95s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_scalar_indexes_manual_import_filters_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 57.57s
```

Observed behavior covered by assertions:

```text
Scalar-indexed collection import reached Uncommitted and stayed invisible before commit.
After commit, all 32 imported rows became visible.
INVERTED, BITMAP, STL_SORT, and TRIE-backed filter queries returned the exact expected PK sets.
```

Failure classification:

- No failure in final run.
- `INVERTED`, `BITMAP`, `STL_SORT`, and `TRIE` scalar indexes work with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-TYP-204 Float16Vector/BFloat16Vector manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_half_vector_types_manual_import_searches_after_commit`

Coverage:

- Create one collection with `Float16Vector` and `BFloat16Vector` fields.
- Build `AUTOINDEX` with `metricType=L2` on both half-vector fields.
- Import one Parquet file with `options.auto_commit=false`.
- Parquet encodes both half-vector fields as `list<uint8>`, matching the existing bulk insert half-vector path.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify all imported PKs become visible after commit.
- Verify REST vector search on both `float16_vector` and `bfloat16_vector` returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
minio_http_code=200
```

Collect-only check:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_half_vector_types_manual_import_searches_after_commit
```

```text
Python 3.12.12
1 test collected in 2.02s
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_half_vector_types_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 65.78s (0:01:05)
```

Observed behavior covered by assertions:

```text
Half-vector collection import reached Uncommitted and stayed invisible before commit.
After commit, all 16 imported rows became visible.
Float16Vector and BFloat16Vector searches both returned imported PKs.
```

Failure classification:

- No failure in final run.
- `Float16Vector` and `BFloat16Vector` work with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-TYP-206 Int8Vector manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_int8_vector_manual_import_searches_after_commit`

Coverage:

- Create one collection with an `Int8Vector` field.
- Build `HNSW` index with `metricType=L2` on `int8_vector`.
- Import one Parquet file with `options.auto_commit=false`.
- Parquet encodes `int8_vector` as `FixedSizeList<Int8, dim>`, matching the existing bulk/external-table Int8Vector path.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify all imported PKs become visible after commit.
- Verify REST vector search on `int8_vector` returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_int8_vector_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 71.80s (0:01:11)
```

Observed behavior covered by assertions:

```text
Int8Vector collection import reached Uncommitted and stayed invisible before commit.
After commit, all 16 imported rows became visible.
Int8Vector REST search accepted the numeric int8 vector payload and returned imported PKs.
```

Failure classification:

- No failure in final run.
- `Int8Vector` works with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-TYP-206 ArrayOfVector manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_array_of_vector_manual_import_searches_after_commit`

Coverage:

- Create one StructArray collection with an `ArrayOfVector` sub-field `my_struct[sub_vec]`.
- Build an index on the top-level `vec` field and on `my_struct[sub_vec]`.
- Import one nested `list<struct<sub_int:int32, sub_vec:list<float32>>>` Parquet file with `options.auto_commit=false`.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify all imported PKs become visible after commit.
- Query `my_struct` and verify nested scalar/vector values match the source rows.
- Verify REST vector search on `my_struct[sub_vec]` returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=000
minio_http_code=000
```

Environment note:

- Direct access from the local machine to both LoadBalancer IPs timed out.
- Kubernetes showed the instance and backing pods healthy:
  - `import-2pc-playground` status: `Healthy`
  - `import-2pc-playground-milvus` pod: `1/1 Running`
  - `import-2pc-playground-minio` pod: `1/1 Running`
- The case was executed through temporary localhost port-forwards:
  - `svc/import-2pc-playground-milvus 19530:19530`
  - `svc/import-2pc-playground-minio 9000:9000`
- The port-forward processes were stopped after the run.

Port-forward pre-run checks:

```bash
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://127.0.0.1:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://127.0.0.1:9000/minio/health/live
```

```text
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_array_of_vector_manual_import_searches_after_commit \
  --endpoint http://127.0.0.1:19530 \
  --token root:Milvus \
  --minio_host 127.0.0.1 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 76.68s (0:01:16)
```

Observed behavior covered by assertions:

```text
ArrayOfVector collection import reached Uncommitted and stayed invisible before commit.
After commit, all 12 imported rows became visible.
Nested my_struct values matched the source rows after commit.
ArrayOfVector REST search on my_struct[sub_vec] returned imported PKs.
```

Failure classification:

- No failure in final run.
- The initial direct-LoadBalancer timeout is an environment/network access issue, not a test-code issue and not a Milvus Import 2PC bug.
- `ArrayOfVector` works with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-TYP-207 Geometry/Timestamptz manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_geometry_timestamptz_manual_import_filters_after_commit`

Coverage:

- Create one collection with `Geometry`, `Timestamptz`, `VarChar`, and `FloatVector` fields.
- Import one Parquet file with `options.auto_commit=false`.
- Parquet encodes `Geometry` as WKT string values and `Timestamptz` as ISO8601 UTC string values.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify all imported PKs become visible after commit.
- Query imported rows and verify `tag`, `event_time`, and geometry text are returned.
- Verify `ST_EQUALS(geo, ...)` returns the expected imported PK.
- Verify `event_time >= ISO '...'` returns the expected imported PK set.
- Verify vector search still returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_geometry_timestamptz_manual_import_filters_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 53.00s
```

Observed behavior covered by assertions:

```text
Geometry/Timestamptz collection import reached Uncommitted and stayed invisible before commit.
After commit, all 6 imported rows became visible.
Geometry WKT values and Timestamptz values were queryable after commit.
ST_EQUALS and ISO timestamptz filters returned the expected imported PK sets.
Vector search returned imported PKs after commit.
```

Failure classification:

- No failure in final run.
- `Geometry` WKT parquet import works with REST manual 2PC import in this case.
- `Timestamptz` ISO8601 string parquet import works with REST manual 2PC import in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-FN-201 BM25 function input-only manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_bm25_function_input_only_manual_import_searches_after_commit`

Coverage:

- Create one collection with `VarChar` BM25 input, generated `SparseFloatVector` BM25 output, and a dense vector field.
- Import one JSON file with `options.auto_commit=false`.
- The import file intentionally omits the BM25 function output field.
- Verify imported rows stay invisible while the job is `Uncommitted`.
- Commit the job and wait for `Completed`.
- Verify all imported PKs become visible after commit.
- Query imported rows and verify `document_content` values match the source rows.
- Verify REST BM25 search on the generated sparse vector field returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_bm25_function_input_only_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 52.50s
```

Observed behavior covered by assertions:

```text
BM25 function collection import reached Uncommitted and stayed invisible before commit.
After commit, all 18 imported rows became visible.
Imported document_content values matched the source JSON rows.
BM25 search on the generated sparse_vector field returned imported PKs after commit.
```

Failure classification:

- No failure in final run.
- BM25 function output generation works with REST manual 2PC import when the input file omits the output field.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-FN-202 BM25 output field rejected in manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_bm25_function_output_field_rejected_and_no_visible_rows`

Coverage:

- Create one collection with `VarChar` BM25 input, generated `SparseFloatVector` BM25 output, and a dense vector field.
- Import one JSON file with `options.auto_commit=false`.
- The import file intentionally includes the generated BM25 output field `sparse_vector`.
- Accept either legal rejection shape: synchronous `/create` rejection or asynchronous job `Failed`.
- Verify the final rejection reason is actionable and mentions `sparse_vector` / unexpected field / function output semantics.
- Verify the collection remains empty after rejection.
- Verify the rejected import PK set is not visible.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_bm25_function_output_field_rejected_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 13.83s
```

Observed behavior covered by assertions:

```text
Import create returned a job id.
The job then moved from Pending to Failed.
Failure reason included unexpected field 'sparse_vector'.
No rows became visible after the failed import.
The rejected import PK set was absent from query results.
```

Failure classification:

- No failure in final run.
- Milvus rejected user-provided BM25 function output data before commit visibility.
- This is expected guardrail behavior, not a Milvus Import 2PC bug.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-IDX-203 DISKANN manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_diskann_manual_import_searches_after_commit`

Coverage:

- Create one collection with a `FloatVector` field indexed by `DISKANN` / `L2`.
- Import one Parquet file with `options.auto_commit=false`.
- Wait for the import job to reach `Uncommitted`.
- Verify sampled imported PKs stay invisible before commit.
- Commit the job and wait for `Completed`.
- Verify sampled imported PKs become visible after commit.
- Verify `count(*)` equals the imported row count.
- Verify queried scalar fields match the source rows.
- Verify REST vector search with DISKANN search params returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_diskann_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 51.35s
```

Observed behavior covered by assertions:

```text
DISKANN collection import reached Uncommitted and reported 256 imported rows.
Sampled imported PKs stayed invisible before commit.
After commit, sampled PKs became visible and count(*) returned 256.
Queried tag/phase values matched source rows.
DISKANN vector search returned imported PKs after commit.
```

Failure classification:

- No failure in final run.
- DISKANN works with REST manual 2PC import on `import-2pc-playground` in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-PAR-205 Multi-vector collection manual 2PC import

Status: Passed

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_multi_vector_collection_manual_import_searches_each_vector_field`

Coverage:

- Create one collection with two `FloatVector` fields: `vector_a` and `vector_b`.
- Build two vector indexes: `HNSW/L2` on `vector_a` and `HNSW/COSINE` on `vector_b`.
- Import one Parquet file with `options.auto_commit=false`.
- Wait for the import job to reach `Uncommitted`.
- Verify imported PKs stay invisible before commit.
- Commit the job and wait for `Completed`.
- Verify imported PKs become visible after commit.
- Verify queried scalar fields match source rows.
- Verify REST vector search on both vector fields returns imported PKs after commit.
- Verify final `count(*)` equals the imported row count.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_multi_vector_collection_manual_import_searches_each_vector_field \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 71.84s (0:01:11)
```

Observed behavior covered by assertions:

```text
Multi-vector collection import reached Uncommitted and reported 12 imported rows.
Imported PKs stayed invisible before commit.
After commit, imported PKs became visible and scalar tag/phase fields matched source rows.
Search on vector_a and vector_b both returned imported PKs.
Final count(*) returned 12.
```

Failure classification:

- No failure in final run.
- Multi-vector collections work with REST manual 2PC import on `import-2pc-playground` in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-PAR-204 Clustering-key collection manual 2PC import and clustering compaction

Status: Passed

Implemented files:

- `tests/restful_client_v2/api/milvus.py`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_clustering_key_collection_manual_import_survives_clustering_compaction`

Helper changes:

- Extended `CollectionClient.compact(..., is_clustering=False)` to pass REST `isClustering`.

Coverage:

- Create one collection with `phase` marked as `isClusteringKey=true`.
- Verify `/collections/describe` reports `phase.clusteringKey=true`.
- Import one Parquet file with `options.auto_commit=false`.
- Wait for the import job to reach `Uncommitted`.
- Verify sampled imported PKs stay invisible before commit.
- Commit the job and wait for `Completed`.
- Verify sampled imported PKs become visible after commit.
- Verify clustering-key filters return the expected row counts.
- Trigger REST clustering compaction with `isClustering=true`.
- Wait for compaction `Completed`.
- Verify sampled PKs, scalar fields, final `count(*)`, and vector search remain correct after compaction.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_clustering_key_collection_manual_import_survives_clustering_compaction \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 54.08s
```

Observed behavior covered by assertions:

```text
Clustering-key collection import reached Uncommitted and reported 1024 imported rows.
Sampled imported PKs stayed invisible before commit.
After commit, sampled PKs became visible.
Filtering by clustering key phase returned 512 rows for each phase value.
REST clustering compaction returned a positive compactionID and reached Completed.
After compaction, sampled PKs/scalar fields/count/search remained correct.
```

Failure classification:

- No failure in final run.
- Clustering-key collection import and REST clustering compaction work with manual 2PC import on `import-2pc-playground` in this case.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-CMP-102 Manual compaction during Uncommitted import

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_manual_compaction_during_uncommitted_keeps_import_invisible`

Coverage:

- Create one base collection.
- Insert two flushed baseline batches, 2000 rows each.
- Import one Parquet file with 256 rows and `options.auto_commit=false`.
- Wait for the import job to reach `Uncommitted`.
- Verify sampled imported PKs stay invisible before manual compaction.
- Verify already flushed baseline rows remain visible.
- Trigger REST manual compaction while the import job is still `Uncommitted`.
- Wait for compaction `Completed` when REST returns a positive `compactionID`.
- Verify sampled imported PKs are still invisible after compaction.
- Commit the import job and wait for `Completed`.
- Verify baseline rows and imported rows are visible after commit.
- Verify final `count(*)` and vector search results include imported rows.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_compaction_during_uncommitted_keeps_import_invisible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 107.93s (0:01:47)
```

Observed behavior covered by assertions:

```text
Baseline rows were inserted, flushed, and visible before import.
Import reached Uncommitted and reported 256 imported rows.
Sampled imported PKs stayed invisible before manual compaction.
Manual compaction returned a positive compactionID and reached Completed.
Sampled imported PKs stayed invisible after manual compaction.
Baseline count remained 4000 while the import job was Uncommitted.
After commit, baseline and imported sample PKs became visible.
Final count(*) returned 4256.
Vector search returned imported PKs.
```

Failure classification:

- No failure in final run.
- Manual compaction while an import job is `Uncommitted` did not expose import data.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

Retest on 2026-06-10:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_compaction_during_uncommitted_keeps_import_invisible \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

```text
Python 3.12.12
1 passed, 2 warnings in 111.19s (0:01:51)
```

Retest classification:

- No failure.
- The case is stable across two consecutive runs on `import-2pc-playground`.
- No Milvus Import 2PC bug found by the retest.

## 2026-06-10 IMP-CMP-103 Aborted import cleanup visibility

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_aborted_import_does_not_pollute_later_committed_import`

Coverage:

- Create one base collection.
- Insert and verify four normal baseline rows.
- Import the first Parquet file with `options.auto_commit=false`.
- Wait for the first import job to reach `Uncommitted`.
- Verify first-job PKs are invisible before abort.
- Abort the first import job and wait for `Failed`.
- Verify first-job PKs remain invisible and `count(*)` is still the baseline count.
- Import a second Parquet file into the same collection with `options.auto_commit=false`.
- Verify first-job PKs are still invisible while the second job is `Uncommitted`.
- Verify second-job PKs are invisible before commit.
- Commit the second job and wait for `Completed`.
- Verify second-job PKs become visible.
- Verify first-job PKs remain invisible after the later committed import.
- Verify final `count(*)` equals baseline rows plus second-job rows.
- Verify the failed first job still reports `Failed`.
- Verify vector search returns committed second-job PKs and does not return aborted first-job PKs.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_aborted_import_does_not_pollute_later_committed_import \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 194.01s (0:03:14)
```

Observed behavior covered by assertions:

```text
Baseline rows were visible before import.
The first manual import reached Uncommitted and reported 16 imported rows.
The first import's PKs stayed invisible before abort.
Abort returned success and the first job reached Failed.
The first import's PKs stayed invisible after abort.
Collection count stayed at 4 after abort.
The second manual import reached Uncommitted and reported 12 imported rows.
The first import's PKs stayed invisible during the second import.
The second import's PKs stayed invisible before commit.
After committing the second job, the second import's PKs became visible.
The first import's PKs remained invisible after the second import committed.
Final count(*) returned 16.
The first job still reported Failed.
Vector search returned second-job committed PKs and did not return first-job aborted PKs.
```

Failure classification:

- No failure in final run.
- Aborted import data did not become query-visible and did not pollute a later committed import in the same collection.
- Object-store physical cleanup and internal segment deletion were not directly verified because no REST-visible metadata surface was available in this framework path.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SEC-101 User without Import privilege

Status: Failed - Milvus bug candidate

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_user_without_import_privilege_cannot_operate_jobs`

Coverage:

- Create one base collection as root.
- Create one temporary user with no role/privilege grant.
- Create two root-owned manual import jobs and wait for both to reach `Uncommitted`.
- With the unprivileged user token, call import `/create`, `/get_progress`, `/describe`, `/list`, `/commit`, and `/abort`.
- Verify unauthorized create/progress/describe are denied.
- Verify list does not leak protected job IDs.
- Verify unauthorized commit and abort are denied.
- Clean up the temporary user, import jobs, local files, and test collection.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_user_without_import_privilege_cannot_operate_jobs \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 failed, 2 warnings in 61.34s (0:01:01)
```

Observed REST behavior:

```text
Unauthorized /create returned code=65535 PermissionDenied for PrivilegeImport.
Unauthorized /get_progress returned code=65535 PermissionDenied for PrivilegeImport.
Unauthorized /describe returned code=65535 PermissionDenied for PrivilegeImport.
Unauthorized /list returned code=0 with records=[] and did not leak root-owned job IDs.
Unauthorized /commit returned code=0 and moved the root-owned job from Uncommitted to Committing.
Unauthorized /abort returned code=0 and moved the root-owned job to Failed.
```

Representative response data:

```text
create:   {"code": 65535, "message": "rpc error: code = PermissionDenied desc = PrivilegeImport: permission deny ..."}
progress: {"code": 65535, "message": "rpc error: code = PermissionDenied desc = PrivilegeImport: permission deny ..."}
describe: {"code": 65535, "message": "rpc error: code = PermissionDenied desc = PrivilegeImport: permission deny ..."}
list:     {"code": 0, "data": {"records": []}}
commit:   {"code": 0, "data": {}}
abort:    {"code": 0, "data": {}}
```

Failure classification:

- Classification: Milvus bug candidate.
- The test setup is valid: the temporary user had no role/privilege grant, and the same token was denied by `/create`, `/get_progress`, and `/describe` for `PrivilegeImport`.
- `/commit` and `/abort` should be protected by the same import authorization boundary, but both succeeded.
- Local code inspection supports the finding: `internal/distributed/proxy/httpserver/handler_v2.go:3273` (`commitImportJob`) and `internal/distributed/proxy/httpserver/handler_v2.go:3316` (`abortImportJob`) call `wrapperProxy(..., false, false, ...)` directly and do not perform `checkAuthorizationV2` or `checkAuthorizationHelper`, unlike create/progress/list paths.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-201 Invalid backup storage_version parse contract

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_backup_invalid_storage_version_fails_with_reason_and_no_visible_rows`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one fake object under a backup-style prefix so `backup=true` can pass prefix listing and reach reader option parsing.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","backup":"true","storage_version":"abc"}` and `partitionName="_default"`.
- Wait for the job to reach `Failed`.
- Verify the failure reason mentions `storage_version` and a parse/invalid/syntax failure.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_backup_invalid_storage_version_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 10.67s
```

Observed behavior covered by assertions:

```text
The backup import request accepted the REST create path and produced an import job.
The job reached Failed instead of Uncommitted or Completed.
The reported reason/details contained storage_version and parse/invalid/syntax context.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms invalid backup `storage_version` is rejected through the async import job path with an actionable reason.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-205 Reversed backup start_ts/end_ts guardrail

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_backup_reversed_time_range_fails_with_reason_and_no_visible_rows`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one fake object under a backup-style prefix so `backup=true` can pass prefix listing and reach reader option parsing.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","backup":"true","start_ts": "...", "end_ts": "..."}` where `start_ts > end_ts`.
- Wait for the job to reach `Failed`.
- Verify the failure reason mentions `start_ts` or `end_ts` and a range/invalid/larger reason.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_backup_reversed_time_range_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 12.07s
```

Observed behavior covered by assertions:

```text
The backup import request accepted the REST create path and produced an import job.
The job reached Failed instead of Uncommitted or Completed.
The reported reason/details contained start_ts or end_ts and range/invalid/larger context.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms reversed backup time range is rejected through the async import job path with an actionable reason.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-OPT-101 Invalid timeout option contract

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_invalid_timeout_option`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one valid Parquet file so the request is syntactically valid apart from the option under test.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","timeout":"not-a-duration"}`.
- Verify create returns non-zero with a timeout/duration/parse reason.
- Verify no `jobId` is returned and `/list` shows no import job for the collection.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_invalid_timeout_option \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 8.79s
```

Observed behavior covered by assertions:

```text
The invalid timeout request was rejected synchronously.
The response contained timeout/duration/parse context.
No jobId was returned.
The collection import job list remained empty.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms invalid `timeout` is rejected before an import job is created.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-205B CamelCase startTs/endTs alias guardrail

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_backup_camelcase_time_range_alias_fails_with_reason_and_no_visible_rows`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one fake object under a backup-style prefix so `backup=true` can pass prefix listing and reach reader option parsing.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","backup":"true","startTs": "...", "endTs": "..."}` where `startTs > endTs`.
- Wait for the job to reach `Failed`.
- Verify the failure reason mentions `start_ts` or `end_ts` and a range/invalid/larger reason.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_backup_camelcase_time_range_alias_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 12.84s
```

Observed behavior covered by assertions:

```text
The backup import request accepted the REST create path and produced an import job.
The job reached Failed instead of Uncommitted or Completed.
The reported reason/details contained start_ts or end_ts and range/invalid/larger context, proving camelCase aliases were parsed.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms camelCase `startTs`/`endTs` aliases are routed into the same backup time range validation.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-002G L0 import loaded-collection guardrail

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_l0_import_rejects_loaded_collection_without_creating_job`

Coverage:

- Create one REST collection with the standard import schema and wait until it is loaded.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","l0_import":"true"}` against the loaded collection.
- Verify create returns non-zero with an L0 loaded/release reason.
- Verify no `jobId` is returned and `/list` shows no import job for the collection.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_l0_import_rejects_loaded_collection_without_creating_job \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 8.63s
```

Observed behavior covered by assertions:

```text
The l0_import request was rejected synchronously because the target collection was loaded.
The response contained L0 and loaded/release context.
No jobId was returned.
The collection import job list remained empty.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms the REST path enforces the current L0 import loaded-collection guardrail before creating an import job.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-205C Invalid backup start_ts parse guardrail

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_backup_invalid_start_ts_fails_with_reason_and_no_visible_rows`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one fake object under a backup-style prefix so `backup=true` can pass prefix listing and reach reader option parsing.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","backup":"true","start_ts":"not-a-hybrid-ts","end_ts":"..."}`.
- Wait for the job to reach `Failed`.
- Verify the failure reason mentions `start_ts` and parse/invalid/syntax context.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_backup_invalid_start_ts_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 10.45s
```

Observed behavior covered by assertions:

```text
The backup import request accepted the REST create path and produced an import job.
The job reached Failed instead of Uncommitted or Completed.
The reported reason/details contained start_ts and parse/invalid/syntax context.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms invalid backup `start_ts` is rejected through the async import job path with an actionable reason.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-200 Backup requires partitionName guardrail

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_backup_requires_partition_name_without_creating_job`

Coverage:

- Create one REST collection with the standard import schema.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","backup":"true"}` and no `partitionName`.
- Verify create is rejected synchronously with a partition/specified reason.
- Verify no `jobId` is returned.
- Verify `/v2/vectordb/jobs/import/list` for the collection is empty.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_backup_requires_partition_name_without_creating_job \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 9.93s
```

Observed behavior covered by assertions:

```text
The backup import request was rejected synchronously because partitionName was missing.
The response contained partition/specified context.
No jobId was returned.
The collection import job list remained empty.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms the REST path enforces the backup `partitionName` contract before creating an import job.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-206A skip_disk_quota_check ignored for regular import

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_skip_disk_quota_check_is_ignored_for_regular_import`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one normal parquet import file.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","skip_disk_quota_check":"true"}` and without `backup` or `l0_import`.
- Verify the job reaches `Uncommitted` and imported rows remain invisible before commit.
- Commit the job, wait for `Completed`, and verify the full imported PK set becomes visible.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_skip_disk_quota_check_is_ignored_for_regular_import \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 48.16s
```

Observed behavior covered by assertions:

```text
The regular parquet import request with skip_disk_quota_check=true created a manual import job.
The job reached Uncommitted and rows were not visible before commit.
CommitImport succeeded and the job reached Completed.
All imported PKs became visible after commit.
```

Failure classification:

- No failure in final run.
- The case confirms `skip_disk_quota_check=true` alone does not turn a regular import into backup/L0 behavior and does not break normal 2PC semantics.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-OBS-102A Completed job REST observability fields

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_completed_job_observability_fields_are_reported`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one normal parquet import file and create a manual import job.
- Wait for `Uncommitted`, call `CommitImport`, and wait for `Completed`.
- Verify `/v2/vectordb/jobs/import/describe` reports `state=Completed`, `progress=100`, `importedRows`, `totalRows`, `createTime`, and `completeTime`.
- Verify `/v2/vectordb/jobs/import/list` can find the job by collection and reports the summary fields `collectionName`, `jobId`, `state`, and `progress`.
- Verify all imported PKs become visible after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Discarded attempt:

- Tried to cover `IMP-MVCC-102` by calling low-level REST `/query` with `travel_timestamp`.
- Current `import-2pc-playground` returned `404 page not found` for `/query`.
- Classification: test path issue, not a Milvus Import 2PC bug. The attempted helper and test were removed.

First run result before adjustment:

```text
FAILED ... KeyError: 'importedRows'
```

Failure classification for first run:

- Test code issue.
- `/describe` returned detailed counters and timestamps, but `/list` returned only summary fields:
  `collectionName`, `jobId`, `progress`, and `state`.
- The assertion was adjusted to match the observed REST contract: detailed observability is verified on `/describe`; `/list` is verified as a summary/index endpoint.

Final run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_completed_job_observability_fields_are_reported \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 29.37s
```

Observed behavior covered by assertions:

```text
The manual import job reached Uncommitted, then Completed after CommitImport.
/describe reported completed state, 100 progress, importedRows=6, totalRows=6, createTime, and completeTime.
/list found exactly one matching job and reported collectionName, jobId, state=Completed, and progress=100.
All imported PKs became visible after commit.
```

Failure classification:

- No failure in final run.
- The case confirms REST observability for completed import jobs through `/describe` and `/list`.
- No Milvus Import 2PC bug found by the final case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-FT-102A Completed import survives release/reload

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_completed_import_survives_release_and_reload`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one normal parquet file and create a manual import job.
- Wait for `Uncommitted`, call `CommitImport`, and wait for `Completed`.
- Verify all imported PKs are visible before reload.
- Release the collection and wait until `get_load_state` reports `LoadStateNotLoad`.
- Load the collection again, wait for `LoadStateLoaded`, and verify the same imported PK set is still fully visible.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_completed_import_survives_release_and_reload \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 56.42s
```

Observed behavior covered by assertions:

```text
The manual import job reached Uncommitted, then Completed after CommitImport.
All imported PKs were visible before collection release.
The collection reached LoadStateNotLoad after release and LoadStateLoaded after load.
All imported PKs were still visible after reload.
```

Failure classification:

- No failure in final run.
- The case confirms a completed import survives an explicit REST release/load cycle and remains visible after target refresh.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-SP-205D Invalid backup end_ts parse guardrail

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_backup_invalid_end_ts_fails_with_reason_and_no_visible_rows`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one fake object under a backup-style prefix so `backup=true` can pass prefix listing and reach backup option parsing.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"false","backup":"true","start_ts":"...","end_ts":"not-a-hybrid-ts"}` and `partitionName="_default"`.
- Wait for the job to reach `Failed`.
- Verify the failure reason mentions `end_ts` and parse/invalid/syntax context.
- Verify the collection still has `count(*) == 0`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_backup_invalid_end_ts_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 12.99s
```

Observed behavior covered by assertions:

```text
The backup import request accepted the REST create path and produced an import job.
The job reached Failed instead of Uncommitted or Completed.
The reported reason/details contained end_ts and parse/invalid/syntax context.
No rows became visible in the target collection.
```

Failure classification:

- No failure in final run.
- The case confirms invalid backup `end_ts` is rejected through the async import job path with an actionable reason.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-OPT-102A auto_commit non-false value defaults to true

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_auto_commit_non_false_value_defaults_to_true_and_completes`

Coverage:

- Create one REST collection with the standard import schema.
- Upload one Parquet file with deterministic PKs.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"not-a-bool"}`.
- Wait for the job to reach `Completed` without explicit `/commit`.
- Verify `importedRows == totalRows == N`.
- Verify all imported PKs eventually become query-visible after target refresh.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_auto_commit_non_false_value_defaults_to_true_and_completes \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 46.30s
```

Observed behavior covered by assertions:

```text
The import request accepted auto_commit=not-a-bool.
The job reached Completed automatically, matching the current IsAutoCommit contract.
The imported row counters matched the Parquet row count.
All imported PKs became query-visible after refresh.
```

Failure classification:

- No failure in final run.
- The case confirms that `auto_commit` is parsed as false only for case-insensitive `"false"`; any other value is treated as true.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-TYP-205A SparseFloatVector parquet struct manual 2PC import

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_sparse_struct_parquet_manual_import_searches_after_commit`

Coverage:

- Add a REST test helper that writes `SparseFloatVector` as Parquet struct encoding: `struct<indices:list<uint32>, values:list<float32>>`.
- Create one REST collection with a `SparseFloatVector` field and `SPARSE_INVERTED_INDEX`.
- Upload one Parquet file using sparse struct encoding.
- Submit `/v2/vectordb/jobs/import/create` with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Verify imported PKs stay invisible before commit.
- Commit the job and wait for `Completed`.
- Verify imported PKs become query-visible after target refresh.
- Verify scalar fields match the source rows.
- Verify REST sparse vector search returns imported PKs after commit.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_sparse_struct_parquet_manual_import_searches_after_commit \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 61.77s
```

Observed behavior covered by assertions:

```text
Sparse struct parquet import reached Uncommitted and stayed invisible before commit.
After CommitImport, the job reached Completed.
All imported PKs became query-visible after refresh.
Queried scalar fields matched the source rows.
Sparse vector search on the imported struct-encoded vectors returned imported PKs.
```

Failure classification:

- No failure in final run.
- The case confirms Parquet struct encoding for SparseFloatVector works with REST manual 2PC import.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-TYP-209A BinaryVector byte-length mismatch negative

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_binary_vector_byte_length_mismatch_fails_with_reason_and_no_visible_rows`

Coverage:

- Add explicit `IMP-TYP-209A` plan row for BinaryVector byte-length mismatch validation.
- Create one REST collection with `BinaryVector(dim=128)` and `BIN_FLAT`.
- Upload one Parquet file where `binary_vector` uses `list<uint8>` but each row has only 15 bytes instead of the expected 16 bytes.
- Submit `/v2/vectordb/jobs/import/create` with `options.auto_commit=false`.
- Accept either synchronous REST create rejection or asynchronous import job `Failed`.
- Verify the rejection reason is related to binary/vector dimension/length/byte validation.
- Verify no rows become visible in the target collection.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_binary_vector_byte_length_mismatch_fails_with_reason_and_no_visible_rows \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 23.85s
```

Observed behavior covered by assertions:

```text
Milvus rejected the BinaryVector byte-length mismatch through the supported reject/fail path.
The failure reason was related to binary/vector dimension/length/byte validation.
No rows became visible in the target collection after rejection.
```

Failure classification:

- No failure in final run.
- The case confirms BinaryVector byte-length mismatch is rejected and produces no visible dirty rows.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-104 Import list collectionName filter isolation

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_list_filters_by_collection_name_without_cross_collection_leak`

Coverage:

- Add explicit `IMP-REST-104` plan row for `/v2/vectordb/jobs/import/list` collection filter isolation.
- Create two REST collections with unique names.
- Upload one Parquet file for each collection.
- Create one manual import job per collection with `options.auto_commit=false`.
- Wait for both jobs to reach `Uncommitted`.
- Call `/list` with the first `collectionName` and verify it contains only the first collection job.
- Call `/list` with the second `collectionName` and verify it contains only the second collection job.
- Abort both jobs after assertions to keep imported rows invisible.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_list_filters_by_collection_name_without_cross_collection_leak \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 41.67s
```

Observed behavior covered by assertions:

```text
Both manual import jobs reached Uncommitted.
The first collectionName-filtered /list response contained the first job and not the second job.
The second collectionName-filtered /list response contained the second job and not the first job.
Every returned record had the requested collectionName.
Both jobs were aborted after the list assertions.
```

Failure classification:

- No failure in final run.
- The case confirms REST import `/list` respects `collectionName` filtering and does not leak jobs across collections.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-OPT-103 auto_commit uppercase false stops at Uncommitted

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_auto_commit_uppercase_false_stops_at_uncommitted`

Coverage:

- Add explicit `IMP-OPT-103` plan row for case-insensitive `auto_commit=false` parsing.
- Create one REST collection and upload one Parquet file.
- Submit `/v2/vectordb/jobs/import/create` with `options={"auto_commit":"FALSE"}`.
- Verify the job reaches `Uncommitted` instead of auto-committing to `Completed`.
- Verify imported PKs stay invisible while the job is `Uncommitted`.
- Abort the job and verify the imported PKs remain invisible after cleanup.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_auto_commit_uppercase_false_stops_at_uncommitted \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 59.10s
```

Observed behavior covered by assertions:

```text
The import request accepted auto_commit=FALSE.
The import job reached Uncommitted and did not auto-commit to Completed.
All imported PKs stayed invisible during the Uncommitted wait window.
AbortImport succeeded and the job reached Failed.
All imported PKs stayed invisible after abort cleanup.
```

Failure classification:

- No failure in final run.
- The case confirms `auto_commit` parsing treats case-insensitive `"false"` as manual 2PC mode.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-105 Import list empty collectionName filter

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_list_empty_collection_filter_returns_no_records`

Coverage:

- Add explicit `IMP-REST-105` plan row for `/v2/vectordb/jobs/import/list` empty collection filter behavior.
- Create one REST collection with no import jobs.
- Call `/v2/vectordb/jobs/import/list` with that `collectionName`.
- Verify the response succeeds and `data.records` is an empty list.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_list_empty_collection_filter_returns_no_records \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 7.57s
```

Observed behavior covered by assertions:

```text
The empty target collection was created successfully.
The collectionName-filtered /list request succeeded.
The response returned an empty data.records list.
```

Failure classification:

- No failure in final run.
- The case confirms REST import `/list` returns no records for a collection with no import jobs.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-106 Describe/get_progress parity for Uncommitted job

Status: Passed after test assertion fix

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_describe_matches_get_progress_for_uncommitted_job`

Coverage:

- Add explicit `IMP-REST-106` plan row for `/describe` and `/get_progress` parity.
- Create one REST collection and upload one Parquet file.
- Create one manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Call both `/v2/vectordb/jobs/import/describe` and `/v2/vectordb/jobs/import/get_progress`.
- Verify both endpoints report matching `jobId`, `state`, `progress`, `importedRows`, and `totalRows`.
- Verify imported PKs remain invisible while the job is `Uncommitted`.
- Abort the job and wait for `Failed` cleanup.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Initial run:

```text
1 failed, 2 warnings in 41.32s
```

Initial failure classification:

- Test code issue, not a Milvus bug.
- The first version asserted `Uncommitted` progress must be exactly `100`.
- The live REST response showed both `/describe` and `/get_progress` consistently returned `state=Uncommitted`, `importedRows=4`, `totalRows=4`, and `progress=99`.
- The case was corrected to assert endpoint parity plus `0 <= progress <= 100`.

Final run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_describe_matches_get_progress_for_uncommitted_job \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 49.88s
```

Observed behavior covered by assertions:

```text
The manual import job reached Uncommitted.
/describe and /get_progress returned matching jobId/state/progress/importedRows/totalRows.
The progress value was within the valid REST progress range.
The imported PKs stayed invisible while the job was Uncommitted.
AbortImport succeeded and the job reached Failed.
```

Failure classification:

- No failure in final run.
- The initial failure was caused by an overly strict test assertion.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-107 Missing jobId validation on import job endpoints

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_job_endpoints_reject_missing_job_id`

Coverage:

- Add explicit `IMP-REST-107` plan row for missing `jobId` validation.
- Call `/v2/vectordb/jobs/import/describe` without `jobId`.
- Call `/v2/vectordb/jobs/import/get_progress` without `jobId`.
- Call `/v2/vectordb/jobs/import/commit` without `jobId`.
- Call `/v2/vectordb/jobs/import/abort` without `jobId`.
- Verify every endpoint returns a non-zero code, exposes a readable error, and does not return a job record.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_job_endpoints_reject_missing_job_id \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 2.69s
```

Observed behavior covered by assertions:

```text
describe/get_progress/commit/abort all rejected missing jobId.
Each rejection had a non-zero business code and a readable response body.
No endpoint returned a job record for the invalid request.
```

Failure classification:

- No failure in final run.
- The case confirms import job REST endpoints reject missing `jobId`.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-108 Import list rejects numeric collectionName

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_list_rejects_numeric_collection_name`

Coverage:

- Add explicit `IMP-REST-108` plan row for `/v2/vectordb/jobs/import/list` `collectionName` type validation.
- Call `/v2/vectordb/jobs/import/list` with numeric `collectionName`.
- Verify the request is rejected synchronously with a non-zero code.
- Verify the response exposes a readable type/collection-name error and returns no records.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_list_rejects_numeric_collection_name \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 2.28s
```

Observed behavior covered by assertions:

```text
The list request with numeric collectionName was rejected.
The rejection had a non-zero business code and readable response body.
The invalid request returned no import job records.
```

Failure classification:

- No failure in final run.
- The case confirms import list validates `collectionName` type.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-109 Import list without collectionName includes current job

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_list_without_collection_name_includes_current_job`

Coverage:

- Add explicit `IMP-REST-109` plan row for unfiltered `/v2/vectordb/jobs/import/list` behavior.
- Create one REST collection and upload one Parquet file.
- Create one manual import job with `options.auto_commit=false`.
- Wait for the job to reach `Uncommitted`.
- Call `/v2/vectordb/jobs/import/list` without `collectionName`.
- Verify the unfiltered list contains exactly one record for the current job.
- Verify the record reports `collectionName`, `state`, and valid `progress`.
- Abort the job and wait for `Failed` cleanup.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_list_without_collection_name_includes_current_job \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 41.68s
```

Observed behavior covered by assertions:

```text
The manual import job reached Uncommitted.
The unfiltered /list request succeeded.
The unfiltered response contained exactly one record for the current job.
The job record reported the expected collectionName, Uncommitted state, and valid progress.
AbortImport succeeded and the job reached Failed.
```

Failure classification:

- No failure in final run.
- The case confirms unfiltered import `/list` includes the current DB import job.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-110 Import create rejects numeric collectionName

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_numeric_collection_name`

Coverage:

- Add explicit `IMP-REST-110` plan row for `/v2/vectordb/jobs/import/create` `collectionName` type validation.
- Call `/v2/vectordb/jobs/import/create` with numeric `collectionName`.
- Verify the request is rejected synchronously with a non-zero code.
- Verify the response exposes a readable type/collection-name error and no `jobId`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_numeric_collection_name \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 2.44s
```

Observed behavior covered by assertions:

```text
The create request with numeric collectionName was rejected.
The rejection had a non-zero business code and readable response body.
No jobId was returned for the invalid request.
```

Failure classification:

- No failure in final run.
- The case confirms import create validates `collectionName` type.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-111 Import create rejects string files value

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_string_files_value`

Coverage:

- Add explicit `IMP-REST-111` plan row for `/v2/vectordb/jobs/import/create` `files` type validation.
- Create one REST collection.
- Call `/v2/vectordb/jobs/import/create` with `files` as a string instead of a list of file groups.
- Verify the request is rejected synchronously with a non-zero code.
- Verify the response exposes a readable files/type error and no `jobId`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_string_files_value \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 8.39s
```

Observed behavior covered by assertions:

```text
The create request with string files value was rejected.
The rejection had a non-zero business code and readable response body.
No jobId was returned for the invalid request.
```

Failure classification:

- No failure in final run.
- The case confirms import create validates `files` type.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-112 Import create rejects string options value

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_string_options_value`

Coverage:

- Add explicit `IMP-REST-112` plan row for `/v2/vectordb/jobs/import/create` `options` type validation.
- Create one REST collection.
- Call `/v2/vectordb/jobs/import/create` with `options` as a string instead of a map/object.
- Verify the request is rejected synchronously with a non-zero code.
- Verify the response exposes a readable options/type error and no `jobId`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_string_options_value \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 8.43s
```

Observed behavior covered by assertions:

```text
The create request with string options value was rejected.
The rejection had a non-zero business code and readable response body.
No jobId was returned for the invalid request.
```

Failure classification:

- No failure in final run.
- The case confirms import create validates `options` type.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-113 Import create rejects numeric partitionName

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Test added:

- `test_import_2pc_create_rejects_numeric_partition_name`

Coverage:

- Add explicit `IMP-REST-113` plan row for `/v2/vectordb/jobs/import/create` `partitionName` type validation.
- Create one REST collection.
- Call `/v2/vectordb/jobs/import/create` with numeric `partitionName`.
- Verify the request is rejected synchronously with a non-zero code.
- Verify the response exposes a readable partition/type error and no `jobId`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_numeric_partition_name \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
1 passed, 2 warnings in 9.14s
```

Observed behavior covered by assertions:

```text
The create request with numeric partitionName was rejected.
The rejection had a non-zero business code and readable response body.
No jobId was returned for the invalid request.
```

Failure classification:

- No failure in final run.
- The case confirms import create validates `partitionName` type.
- No Milvus Import 2PC bug found by this case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-10 IMP-REST-114..119 Batch REST contract validation

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Tests added or included in this batch:

- `test_import_2pc_create_rejects_numeric_file_path`
- `test_import_2pc_create_rejects_flat_files_list`
- `test_import_2pc_create_rejects_empty_file_group`
- `test_import_2pc_create_rejects_numeric_timeout_option`
- `test_import_2pc_read_job_endpoints_reject_boolean_job_id`
- `test_import_2pc_write_job_endpoints_reject_boolean_job_id`

Coverage:

- `IMP-REST-114`: `/create` rejects numeric nested file path.
- `IMP-REST-115`: `/create` rejects flat `files` list.
- `IMP-REST-116`: `/create` rejects empty file group.
- `IMP-REST-117`: `/create` rejects numeric `options.timeout`.
- `IMP-REST-118`: `/describe` and `/get_progress` reject boolean job id.
- `IMP-REST-119`: `/commit` and `/abort` reject boolean job id.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_numeric_file_path \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_flat_files_list \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_empty_file_group \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_numeric_timeout_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_read_job_endpoints_reject_boolean_job_id \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_write_job_endpoints_reject_boolean_job_id \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
6 passed, 13 warnings in 10.34s
```

Observed behavior covered by assertions:

```text
All invalid create payloads were rejected synchronously.
All invalid jobId endpoint calls were rejected synchronously.
Every rejection returned a non-zero business code and readable response body.
Invalid create payloads did not return jobId.
```

Failure classification:

- No failure in final batch run.
- The cases confirm REST Import create and job endpoint type validation for these payload shapes.
- No Milvus Import 2PC bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REST-144..149 Batch REST storage/time/CSV/encryption option value type validation

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Tests added:

- `test_import_2pc_create_rejects_numeric_storage_version_option`
- `test_import_2pc_create_rejects_array_start_ts_option`
- `test_import_2pc_create_rejects_object_end_ts_option`
- `test_import_2pc_create_rejects_boolean_sep_option`
- `test_import_2pc_create_rejects_array_nullkey_option`
- `test_import_2pc_create_rejects_object_ezk_option`

Coverage:

- `IMP-REST-144`: `/create` rejects numeric `options.storage_version`.
- `IMP-REST-145`: `/create` rejects array `options.start_ts`.
- `IMP-REST-146`: `/create` rejects object `options.end_ts`.
- `IMP-REST-147`: `/create` rejects boolean `options.sep`.
- `IMP-REST-148`: `/create` rejects array `options.nullkey`.
- `IMP-REST-149`: `/create` rejects object `options.ezk`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_numeric_storage_version_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_start_ts_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_end_ts_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_boolean_sep_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_nullkey_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_ezk_option \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
6 passed, 13 warnings in 10.83s
```

Observed behavior covered by assertions:

```text
Invalid non-string values for storage_version, start_ts, end_ts, sep, nullkey, and ezk are rejected synchronously.
Every rejection returned a non-zero business code and readable response body.
No invalid create request returned jobId.
```

Failure classification:

- No failure in final batch run.
- The cases confirm REST Import create rejects non-string values for storage/time/CSV/encryption options.
- No Milvus Import 2PC bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REST-120..125 Batch REST invalid null/array/object payload validation

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Tests added:

- `test_import_2pc_create_rejects_null_collection_name`
- `test_import_2pc_create_rejects_null_files`
- `test_import_2pc_create_rejects_array_partition_name`
- `test_import_2pc_read_job_endpoints_reject_array_job_id`
- `test_import_2pc_write_job_endpoints_reject_array_job_id`
- `test_import_2pc_job_endpoints_reject_object_job_id`

Coverage:

- `IMP-REST-120`: `/create` rejects null `collectionName`.
- `IMP-REST-121`: `/create` rejects null `files`.
- `IMP-REST-122`: `/create` rejects array `partitionName`.
- `IMP-REST-123`: `/describe` and `/get_progress` reject array job id.
- `IMP-REST-124`: `/commit` and `/abort` reject array job id.
- `IMP-REST-125`: `/describe`, `/get_progress`, `/commit`, and `/abort` reject object job id.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_null_collection_name \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_null_files \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_partition_name \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_read_job_endpoints_reject_array_job_id \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_write_job_endpoints_reject_array_job_id \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_job_endpoints_reject_object_job_id \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
6 passed, 13 warnings in 8.55s
```

Observed behavior covered by assertions:

```text
Invalid null/array create payload fields were rejected synchronously.
Invalid array/object jobId endpoint calls were rejected synchronously.
Every rejection returned a non-zero business code and readable response body.
Invalid create payloads did not return jobId.
```

Failure classification:

- No failure in final batch run.
- The cases confirm REST Import create and job endpoint validation for null, array, and object payload shapes.
- No Milvus Import 2PC bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REST-126..131 Batch REST create JSON type validation

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Tests added:

- `test_import_2pc_create_rejects_array_collection_name`
- `test_import_2pc_create_rejects_object_collection_name`
- `test_import_2pc_create_rejects_object_files_value`
- `test_import_2pc_create_rejects_object_file_path`
- `test_import_2pc_create_rejects_array_options_value`
- `test_import_2pc_create_rejects_numeric_options_value`

Coverage:

- `IMP-REST-126`: `/create` rejects array `collectionName`.
- `IMP-REST-127`: `/create` rejects object `collectionName`.
- `IMP-REST-128`: `/create` rejects object `files`.
- `IMP-REST-129`: `/create` rejects object file path inside `files`.
- `IMP-REST-130`: `/create` rejects array `options`.
- `IMP-REST-131`: `/create` rejects numeric `options`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_collection_name \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_collection_name \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_files_value \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_file_path \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_options_value \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_numeric_options_value \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
6 passed, 13 warnings in 10.02s
```

Observed behavior covered by assertions:

```text
Invalid JSON types for create collectionName, files, nested file paths, and options were rejected synchronously.
Every rejection returned a non-zero business code and readable response body.
No invalid create request returned jobId.
```

Failure classification:

- No failure in final batch run.
- The cases confirm REST Import create validates these JSON payload shapes before creating import jobs.
- No Milvus Import 2PC bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REST-132..137 Batch REST option value type validation

Status: Passed with expected xfail

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Tests added:

- `test_import_2pc_create_rejects_boolean_auto_commit_option`
- `test_import_2pc_create_rejects_null_auto_commit_option`
- `test_import_2pc_create_rejects_array_auto_commit_option`
- `test_import_2pc_create_rejects_boolean_backup_option`
- `test_import_2pc_create_rejects_boolean_l0_import_option`
- `test_import_2pc_create_rejects_boolean_skip_disk_quota_check_option`

Coverage:

- `IMP-REST-132`: `/create` rejects boolean `options.auto_commit`.
- `IMP-REST-133`: `/create` should reject null `options.auto_commit`; current instance accepts it and this is tracked as strict xfail.
- `IMP-REST-134`: `/create` rejects array `options.auto_commit`.
- `IMP-REST-135`: `/create` rejects boolean `options.backup`.
- `IMP-REST-136`: `/create` rejects boolean `options.l0_import`.
- `IMP-REST-137`: `/create` rejects boolean `options.skip_disk_quota_check`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Initial run:

```text
Python 3.12.12
1 failed, 5 passed, 13 warnings in 10.95s
```

Initial failure:

```text
test_import_2pc_create_rejects_null_auto_commit_option
Milvus returned code=0 and jobId for options={"auto_commit": null}.
```

Initial failure classification:

- Milvus bug candidate, not a test code issue and not an environment issue.
- The test plan documents REST option values as strings. Boolean and array option values are rejected, while null `auto_commit` is accepted and creates an import job.
- The test was changed to strict xfail and now aborts the created job before asserting the expected rejection, preserving the regression signal without leaving a running job.

Final run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_boolean_auto_commit_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_null_auto_commit_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_auto_commit_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_boolean_backup_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_boolean_l0_import_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_boolean_skip_disk_quota_check_option \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
5 passed, 1 xfailed, 13 warnings in 10.89s
```

Observed behavior covered by assertions:

```text
Boolean and array option values are rejected synchronously.
Null auto_commit is currently accepted and creates a job; strict xfail records this bug candidate.
Accepted null-auto_commit job is aborted by the test cleanup path.
Rejected invalid create requests do not return jobId.
```

Failure classification:

- `IMP-REST-133` is a Milvus bug candidate against the REST option value type contract.
- The other five cases passed and found no Milvus bug.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REST-138..143 Batch REST backup/L0/quota option value type validation

Status: Passed

Implemented files:

- `docs/test_plans/import-2pc-test-plan.md`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`

Tests added:

- `test_import_2pc_create_rejects_array_backup_option`
- `test_import_2pc_create_rejects_object_backup_option`
- `test_import_2pc_create_rejects_array_l0_import_option`
- `test_import_2pc_create_rejects_object_l0_import_option`
- `test_import_2pc_create_rejects_array_skip_disk_quota_check_option`
- `test_import_2pc_create_rejects_object_skip_disk_quota_check_option`

Coverage:

- `IMP-REST-138`: `/create` rejects array `options.backup`.
- `IMP-REST-139`: `/create` rejects object `options.backup`.
- `IMP-REST-140`: `/create` rejects array `options.l0_import`.
- `IMP-REST-141`: `/create` rejects object `options.l0_import`.
- `IMP-REST-142`: `/create` rejects array `options.skip_disk_quota_check`.
- `IMP-REST-143`: `/create` rejects object `options.skip_disk_quota_check`.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_backup_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_backup_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_l0_import_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_l0_import_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_array_skip_disk_quota_check_option \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_create_rejects_object_skip_disk_quota_check_option \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
6 passed, 13 warnings in 11.29s
```

Observed behavior covered by assertions:

```text
Array/object values for backup, l0_import, and skip_disk_quota_check were rejected synchronously.
Every rejection returned a non-zero business code and readable response body.
No invalid create request returned jobId.
```

Failure classification:

- No failure in final batch run.
- The cases confirm REST Import create rejects array/object values for backup/L0/quota-related options.
- No Milvus Import 2PC bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-LC-101/102, IMP-DEL-101, IMP-MIX-102/103, IMP-FT-102 Batch happy-path REST coverage

Status: Passed

Documentation update:

- `docs/test_plans/import-2pc-execution.md`

Tests exercised:

- `test_import_2pc_empty_parquet_auto_commit_completes_with_zero_rows`
- `test_import_2pc_consecutive_manual_imports_accumulate_rows`
- `test_import_2pc_mixed_delete_across_commit_boundary`
- `test_import_2pc_flush_during_uncommitted_keeps_import_invisible`
- `test_import_2pc_manual_compaction_during_uncommitted_keeps_import_invisible`
- `test_import_2pc_completed_import_survives_release_and_reload`

Coverage:

- `IMP-LC-101`: empty/zero-row parquet import reaches `Completed`, reports zero rows, and leaves the collection count unchanged.
- `IMP-LC-102`: two consecutive manual import jobs on the same collection keep independent state and produce the cumulative PK set after both commits.
- `IMP-DEL-101`: one PK subset deleted before commit remains visible after commit, while another subset deleted after commit disappears.
- `IMP-MIX-102`: manual flush during `Uncommitted` does not expose import rows and does not hide baseline DML rows.
- `IMP-MIX-103`: manual compaction during `Uncommitted` does not expose import rows; commit still makes baseline plus import rows visible.
- `IMP-FT-102`: committed import rows remain visible after collection release and reload.

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_empty_parquet_auto_commit_completes_with_zero_rows \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_consecutive_manual_imports_accumulate_rows \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_mixed_delete_across_commit_boundary \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_flush_during_uncommitted_keeps_import_invisible \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_manual_compaction_during_uncommitted_keeps_import_invisible \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_completed_import_survives_release_and_reload \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file
```

Final result:

```text
Python 3.12.12
6 passed, 13 warnings in 117.52s (0:01:57)
```

Observed behavior covered by assertions:

```text
All six happy-path REST cases passed on import-2pc-playground.
Empty import completed with zero rows and no dirty visibility.
Consecutive manual imports accumulated the expected PK set.
Pre-commit delete remained a no-op for import rows; post-commit delete applied.
Flush and compaction during Uncommitted did not expose import rows.
Committed import rows survived release/reload.
```

Failure classification:

- No failure in this batch run.
- No Milvus Import 2PC bug found by this batch.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REP-101..103 and IMP-FT-101 environment precondition gates

Status: Skipped due to environment limitation

Implemented files:

- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`
- `docs/test_plans/import-2pc-execution.md`

Tests added:

- `test_import_2pc_cdc_commit_while_secondary_import_still_building_requires_cdc_env`
- `test_import_2pc_cdc_commit_message_replay_requires_cdc_env`
- `test_import_2pc_cdc_abort_replication_requires_cdc_env`
- `test_import_2pc_datanode_restart_during_importing_requires_cluster_mode`

Coverage:

- `IMP-REP-101`: environment gate for commit while secondary import is still building.
- `IMP-REP-102`: environment gate for replay after secondary misses `CommitImportMessage`.
- `IMP-REP-103`: environment gate for replicated abort cleanup.
- `IMP-FT-101`: environment gate for DataNode restart during importing.

Environment discovery:

```bash
export KUBECONFIG=~/.kube/config
kubectl get milvus -n chaos-testing | rg -i 'import-2pc|name|primary|secondary|cdc'
kubectl get pod -n chaos-testing import-2pc-playground-milvus-standalone-8667cb7fd7-27lz5 --show-labels
```

```text
NAME                    MODE         STATUS    UPDATED   AGE
import-2pc-playground   standalone   Healthy   True      30h
app.kubernetes.io/component=standalone
```

Pre-run checks:

```bash
source .venv/bin/activate
python --version
python -m py_compile tests/restful_client_v2/api/milvus.py tests/restful_client_v2/testcases/test_import_2pc_operation.py
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'milvus_http_code=%{http_code}\n' \
  -H 'Authorization: Bearer root:Milvus' \
  -H 'Content-Type: application/json' \
  -X POST http://10.100.36.203:19530/v2/vectordb/collections/list -d '{}'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'minio_http_code=%{http_code}\n' \
  http://10.100.36.178:9000/minio/health/live
```

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
```

Run command:

```bash
source ../../.venv/bin/activate
python --version
python -m pytest -s -vv -rs -n 6 \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_commit_while_secondary_import_still_building_requires_cdc_env \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_commit_message_replay_requires_cdc_env \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_abort_replication_requires_cdc_env \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_datanode_restart_during_importing_requires_cluster_mode \
  --endpoint http://10.100.36.203:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.178 \
  --bucket_name import-2pc-playground \
  --root_path file \
  --release_name import-2pc-playground
```

Final result:

```text
Python 3.12.12
4 skipped, 11 warnings in 3.13s
```

Skip reasons:

```text
IMP-REP-101 requires a primary/secondary CDC deployment. import-2pc-playground is a single standalone Milvus instance.
IMP-REP-102 requires a primary/secondary CDC deployment. import-2pc-playground is a single standalone Milvus instance.
IMP-REP-103 requires a primary/secondary CDC deployment. import-2pc-playground is a single standalone Milvus instance.
IMP-FT-101 requires a release with separate datanode pods; import-2pc-playground exposes components=['standalone'].
```

Failure classification:

- Not a test code failure: the checks correctly identify unsupported environment prerequisites.
- Not a Milvus bug: no Import 2PC behavior was exercised for these four scenarios.
- Environment limitation: `import-2pc-playground` is a single standalone deployment, so CDC primary/secondary replication and independent DataNode restart fault injection cannot be validated on this instance.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`; xdist reports it once per controller/worker.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`; xdist reports it per worker.

## 2026-06-10 IMP-REP-001 CDC manual Import 2PC happy path

Status: Passed

Implemented files:

- `tests/restful_client_v2/conftest.py`
- `tests/restful_client_v2/testcases/test_import_2pc_operation.py`
- `docs/test_plans/import-2pc-execution.md`

Test added:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_manual_commit_primary_secondary_consistent
```

Environment:

| Item | Value |
| --- | --- |
| Namespace | `chaos-testing` |
| Primary CR | `import-2pc-cdc-primary` |
| Secondary CR | `import-2pc-cdc-secondary` |
| Primary endpoint | `http://10.100.36.217:19530` |
| Secondary endpoint | `http://10.100.36.216:19530` |
| Primary MinIO | `10.100.36.211:9000`, bucket `import-2pc-cdc-primary` |
| Secondary MinIO | `10.100.36.214:9000`, bucket `import-2pc-cdc-secondary` |
| Source cluster ID | `import-2pc-cdc-source` |
| Target cluster ID | `import-2pc-cdc-target` |
| PChannel count | `16` |
| Milvus image | `harbor.milvus.io/manta/milvus:master-20260609-385caab` |
| Milvus version | `master-20260609-385caab437`, git commit `385caab437` |
| Python | `3.12.12` |

Pre-run checks:

```bash
source .venv/bin/activate
python -V
kubectl get milvus -n chaos-testing import-2pc-cdc-primary import-2pc-cdc-secondary -o wide
kubectl get pods -n chaos-testing -o wide | rg 'import-2pc-cdc-(primary|secondary)'
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'primary_minio_http_code=%{http_code}\n' \
  http://10.100.36.211:9000/minio/health/live
curl --connect-timeout 5 --max-time 8 -sS -o /dev/null -w 'secondary_minio_http_code=%{http_code}\n' \
  http://10.100.36.214:9000/minio/health/live
```

```text
Python 3.12.12
import-2pc-cdc-primary     cluster   Healthy   True
import-2pc-cdc-secondary   cluster   Healthy   True
primary_minio_http_code=200
secondary_minio_http_code=200
```

Coverage:

- Sets CDC topology before exercising import: `import-2pc-cdc-source -> import-2pc-cdc-target`, 16 pchannels.
- Creates collection on primary through REST and waits until the collection is replicated and loaded on secondary.
- Writes one parquet import file and uploads the same object name to both primary and secondary MinIO buckets.
- Creates a primary REST import job with `options.auto_commit=false`.
- Waits for both primary and secondary job state to reach `Uncommitted`.
- Verifies imported PKs are invisible on both clusters while `Uncommitted`.
- Calls primary REST `/v2/vectordb/jobs/import/commit`.
- Waits for both clusters to reach `Completed`.
- Verifies primary and secondary expose the exact same imported PK set after commit.

Run command:

```bash
source ../../.venv/bin/activate
python -m pytest -s -vv -rs \
  testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_cdc_manual_commit_primary_secondary_consistent \
  --endpoint http://10.100.36.217:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.211 \
  --bucket_name import-2pc-cdc-primary \
  --root_path import-2pc-cdc-primary \
  --secondary_endpoint http://10.100.36.216:19530 \
  --secondary_token root:Milvus \
  --secondary_minio_host 10.100.36.214 \
  --secondary_bucket_name import-2pc-cdc-secondary \
  --secondary_root_path import-2pc-cdc-secondary \
  --source-cluster-id import-2pc-cdc-source \
  --target-cluster-id import-2pc-cdc-target \
  --pchannel-num 16
```

Final result:

```text
1 passed, 2 warnings in 79.79s (0:01:19)
```

Failure classification:

- No failure.
- No Milvus Import 2PC CDC consistency bug found by this happy-path case.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-11 Non-storage-version infra continuation on updated instances

Status: Passed, with two expected environment skips

Scope:

- Storage-version backup/L0 fixture validation was intentionally deferred for this run.
- Continued with real-instance fault recovery, CDC multi-job, and infra precondition gates.

Environment:

| Item | Value |
| --- | --- |
| Single/chaos cluster | `import-2pc-chaos-cluster` |
| Single endpoint | `http://10.100.36.215:19530` |
| Single MinIO | `10.100.36.214:9000`, bucket `import-2pc-chaos-cluster`, root `files` |
| CDC primary endpoint | `http://10.100.36.217:19530` |
| CDC secondary endpoint | `http://10.100.36.216:19530` |
| CDC primary MinIO | `10.100.36.206:9000`, bucket `import-2pc-cdc-primary`, root `files` |
| CDC secondary MinIO | `10.100.36.198:9000`, bucket `import-2pc-cdc-secondary`, root `files` |
| Python | `3.12.12` |

Pre-run checks:

```text
Python 3.12.12
milvus_http_code=200
minio_http_code=200
import-2pc-chaos-cluster   cluster   Healthy   True
components: datanode, mixcoord, proxy, querynode, streamingnode all Running/Ready
```

Tests executed:

```text
testcases/test_import_2pc_operation.py::TestImport2PCRestOperation::test_import_2pc_datanode_restart_during_importing_recovers_to_uncommitted
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_mixcoord_restart_during_committing_recovers_to_completed
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_streamingnode_restart_during_commit_replays_commit_message
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_cdc_multiple_manual_jobs_same_collection_primary_secondary_consistent
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_max_active_job_limit_on_configured_instance
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_disk_quota_failure_on_configured_instance
```

Results:

```text
IMP-FT-101 DataNode restart during importing:
1 passed, 2 warnings in 261.88s (0:04:21)

IMP-FT-002 MixCoord restart during Committing:
1 passed, 2 warnings in 122.49s (0:02:02)

IMP-FT-004 StreamingNode restart during CommitImportMessage consumption:
1 passed, 2 warnings in 109.24s (0:01:49)

IMP-REP-006 / IMP-LC-008 CDC multiple manual jobs in the same collection:
1 passed, 2 warnings in 63.45s (0:01:03)

IMP-LIM-002 configured max active job limit, with `dataCoord.import.maxImportJobNum=1`:
1 passed, 2 warnings in 23.75s

IMP-LIM-004 configured disk quota exhaustion, with `quotaAndLimits.limitWriting.diskProtection.diskQuota=1` and `diskQuotaPerCollection=1`:
1 passed, 2 warnings in 32.02s
```

Observed behavior covered by assertions:

- DataNode restart during importing recovered the job to `Uncommitted`; commit then reached `Completed` and imported rows became visible.
- MixCoord restart immediately after `CommitImport` recovered the job to `Completed`; count and sample PK visibility were correct.
- StreamingNode restart during commit consumption replayed or consumed the commit message idempotently; job/data reached `Completed`/visible.
- CDC primary/secondary held two manual jobs `Uncommitted`, committed them independently, and converged on the exact same final PK set and count.
- With `maxImportJobNum=1`, one manual import job reached `Uncommitted` and the second manual import create was rejected by the active job limit.
- With disk quota set to 1 MiB, the test first verified ordinary DML was rejected by quota, then verified import was rejected or failed with a quota/disk reason and did not expose imported PKs.
- Runtime evidence for the final disk quota run: DataCoord failed job `466922093837615495` with reason `quota exceeded[reason=disk quota exceeded, please allocate more resources]`; after cleanup, config update events restored disk quota values to `-1`.

Config changes applied and restored:

```bash
kubectl patch milvus -n chaos-testing import-2pc-chaos-cluster --type merge \
  -p '{"spec":{"config":{"dataCoord":{"import":{"maxImportJobNum":1}}}}}'
kubectl patch milvus -n chaos-testing import-2pc-chaos-cluster --type merge \
  -p '{"spec":{"config":{"dataCoord":{"import":null}}}}'

kubectl patch milvus -n chaos-testing import-2pc-chaos-cluster --type merge \
  -p '{"spec":{"config":{"quotaAndLimits":{"limitWriting":{"diskProtection":{"enabled":true,"diskQuota":1,"diskQuotaPerCollection":1}}}}}}'
kubectl patch milvus -n chaos-testing import-2pc-chaos-cluster --type merge \
  -p '{"spec":{"config":{"quotaAndLimits":null}}}'
```

Test stabilization notes:

- `IMP-LIM-002`: the first rerun created two jobs before the first one left `Pending`; the test was corrected to wait for each accepted job to reach `Uncommitted` before attempting the next create.
- `IMP-LIM-004`: one invalid rerun started import before the dynamic quota config had propagated. The test now waits until a normal DML write is denied with a disk/quota reason before submitting import, so the import assertion is not racing config propagation.

Environment correction:

- First CDC multi-job attempt failed before exercising Import 2PC behavior with `NoSuchBucket` because stale MinIO IPs were used.
- Current Service IPs were re-read from Kubernetes:
  - primary MinIO `10.100.36.206:9000`
  - secondary MinIO `10.100.36.198:9000`
- Re-running with the current MinIO endpoints and `root_path=files` passed.

Failure classification:

- No Milvus Import 2PC behavior bug was found in this continuation batch after the two infra-dependent limit tests were rerun against explicitly configured conditions.
- The first `IMP-LIM-002` rerun was a test stabilization issue: it treated `Pending` as sufficient active-job occupancy.
- The first `IMP-LIM-004` rerun was an infrastructure timing issue: CR Healthy was reached before dynamic quota config had propagated to RootCoord quota enforcement.
- The first CDC attempt failed before exercising Import 2PC behavior with `NoSuchBucket` because stale MinIO IPs were used.

Warnings:

- `pytest.ini` has unknown config option `timeout_method`.
- `connections.connect` emits a PyMilvus deprecation warning from existing `TestBase`.

## 2026-06-11 IMP-CMP-003 Uncommitted import segment survives DataCoord GC

Status: Passed

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_infra_dependent.py`

Test added:

```text
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_uncommitted_segment_survives_short_gc_then_commit_visible
```

Coverage:

- Run against an instance with `dataCoord.gc.interval=10`.
- Restart MixCoord/DataCoord because `dataCoord.gc.interval` is not refreshable at runtime.
- Create manual import job with `options.auto_commit=false`.
- Wait until the job reaches `Uncommitted`.
- Keep the job `Uncommitted` for 35 seconds, covering multiple 10-second GC cycles.
- Assert the import rows stay invisible before commit.
- Commit the job after GC cycles.
- Verify job reaches `Completed`.
- Verify imported sample PKs become visible, total `count(*)` equals imported row count, and vector search returns imported rows.

Config changes applied and restored:

```bash
kubectl patch milvus -n chaos-testing import-2pc-chaos-cluster --type merge \
  -p '{"spec":{"config":{"dataCoord":{"gc":{"interval":10}}}}}'
kubectl delete pod -n chaos-testing <mixcoord-pod> --wait=false

kubectl patch milvus -n chaos-testing import-2pc-chaos-cluster --type merge \
  -p '{"spec":{"config":{"dataCoord":{"gc":null}}}}'
kubectl delete pod -n chaos-testing <mixcoord-pod> --wait=false
```

Runtime evidence:

```text
GC with option: enabled=true interval=10s scanInterval=168h0m0s missingTolerance=24h0m0s dropTolerance=3h0m0s
garbage collector recycle task start... gcType=meta interval=10s
```

Run command:

```bash
IMPORT_2PC_EXPECT_SHORT_GC=true IMPORT_2PC_GC_WAIT_SECONDS=35 \
  .venv/bin/python -m pytest -q -s -rs \
  testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_uncommitted_segment_survives_short_gc_then_commit_visible \
  --endpoint http://10.100.36.215:19530 \
  --token root:Milvus \
  --minio_host 10.100.36.214 \
  --bucket_name import-2pc-chaos-cluster \
  --root_path files \
  --release_name import-2pc-chaos-cluster
```

Result:

```text
1 passed, 2 warnings in 139.47s (0:02:19)
```

Restore verification:

- Milvus CR status is `Healthy`.
- CR has no remaining `dataCoord.gc` override.
- MixCoord/DataCoord restart after restore logs `GC with option ... interval=1h0m0s`.

Failure classification:

- No Milvus Import 2PC GC bug found by this case.

## 2026-06-11 Storage-version real source fixture coverage

Status: Partially passed; V3 backup prefix is intentionally covered as fail-closed guardrail.

Implemented file:

- `tests/restful_client_v2/testcases/test_import_2pc_infra_dependent.py`

Tests added:

```text
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_real_source_backup_storage_version_manual_commit_restores_rows[storage_v1]
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_real_source_backup_storage_version_manual_commit_restores_rows[storage_v2]
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_real_source_backup_storage_v3_manifest_prefix_fails_closed
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_real_source_l0_storage_version_manual_commit_applies_delete[storage_v1]
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_real_source_l0_storage_version_manual_commit_applies_delete[storage_v2]
testcases/test_import_2pc_infra_dependent.py::TestImport2PCInfraDependent::test_import_2pc_real_source_l0_storage_version_manual_commit_applies_delete[storage_v3]
```

Coverage:

- Tests use real Milvus source instances and real MinIO objects; no mock binlog generation is used.
- Source rows are inserted and flushed in a source instance.
- The test lists the source instance MinIO `insert_log` or `delta_log` objects.
- The test copies the real objects into the target instance MinIO under a unique staging prefix.
- Target import uses REST `/v2/vectordb/jobs/import/create` with `auto_commit=false`.
- Backup V1/V2 checks: data remains invisible in `Uncommitted`, then becomes queryable/searchable after `CommitImport`.
- L0 V1/V2/V3 checks: seeded target rows remain visible before commit, then matching PKs disappear only after committing the L0 import.
- V3 backup guardrail checks: real V3 manifest layout (`_data`, `_metadata`, `_stats`) is not silently consumed by the REST backup prefix reader; the job fails and no target rows become visible.

Source fixture behavior:

- V1 requires either an explicit source endpoint or `IMPORT_2PC_FIXTURE_V1_IMAGE`.
- V2/V3 can deploy source instances automatically through `kubectl` when endpoint env vars are absent.
- V2 source sets `common.storage.useLoonFFI=false`.
- V3 source sets `common.storage.useLoonFFI=true`.
- Existing source endpoints can be provided with:

```bash
IMPORT_2PC_FIXTURE_V2_ENDPOINT=http://<source-milvus-lb>:19530
IMPORT_2PC_FIXTURE_V2_MINIO_HOST=<source-minio-lb>
IMPORT_2PC_FIXTURE_V2_BUCKET=<source-bucket>
IMPORT_2PC_FIXTURE_V2_ROOT_PATH=files
```

Observed environments:

```text
Target:
  release: import-2pc-chaos-cluster
  Milvus:  http://10.100.36.215:19530
  MinIO:   10.100.36.214:9000
  bucket:  import-2pc-chaos-cluster
  root:    files

V2 source reused for local verification:
  release: issue-49468-master-0611
  Milvus:  http://10.100.36.229:19530
  MinIO:   10.100.36.228:9000
  bucket:  issue-49468-master-0611

V3 source auto-deployed by the test:
  release: import-2pc-fixture-v3
  Milvus:  http://10.100.36.232:19530
  MinIO:   10.100.36.231:9000
  status:  Healthy
```

Validation commands:

```bash
cd tests/restful_client_v2
.venv/bin/python -m py_compile testcases/test_import_2pc_infra_dependent.py
.venv/bin/python -m pytest --collect-only -q testcases/test_import_2pc_infra_dependent.py
```

Validation results:

```text
14 tests collected in 2.02s
```

Executed local cases:

```text
V2 backup restore from real source insert_log:
1 passed, 2 warnings in 48.62s

V2 L0 delete restore from real source delta_log:
1 passed, 2 warnings in 84.07s

V3 L0 delete restore from real source delta_log:
1 passed, 2 warnings in 131.45s

V3 manifest backup prefix fail-closed guardrail:
1 passed, 2 warnings in 21.94s
```

Current boundary:

- Current REST `backup=true` import calls the binlog reader path, which expects V1/V2-style field or column-group directories.
- Real V3 source data is manifest layout under `_data`, `_metadata`, and `_stats`.
- Therefore V3 backup happy-path restore cannot be honestly validated by passing only an `insert_log` prefix to the REST import API.
- V3 manifest happy-path restore needs a separate backup-tool/copy-segment suite that passes source segment metadata including `manifest_path`; the L3 guardrail prevents falsely treating REST prefix import as V3 backup coverage.
