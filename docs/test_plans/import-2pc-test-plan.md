# Import 2PC RESTful Test Plan

## 1. Doc Meta

| Item | Value |
| --- | --- |
| Feature | Import two-phase commit and `commit_timestamp` |
| Target version | Milvus 2.6.x |
| Primary test surface | REST v2 APIs under `/v2/vectordb/jobs/import/*` |
| Client dependency | `requests`/curl only; no SDK or TestPyPI dependency |
| Source alignment | Local Milvus code in this repository, current date 2026-06-09 |
| Related implementation | `CommitImport`/`AbortImport`, WAL broadcast, per-segment `commit_timestamp` |

This plan is REST-first because the REST surface currently exposes the complete import lifecycle:

- create import job
- describe/get progress
- list jobs
- commit import
- abort import

The test harness may still use Python for data generation and HTTP calls, but assertions must target REST-visible behavior and Milvus query/search/delete REST APIs.

## 2. Scope

### In Scope

- Import lifecycle with `auto_commit=true` and `auto_commit=false`.
- REST API contract for create, progress, list, commit, and abort.
- `commit_timestamp` semantics for visibility, delete, TTL, GC, and compaction.
- Normal DML interleaving: insert, upsert, delete, flush, compaction.
- Multi-vchannel commit behavior.
- CDC primary-secondary replication behavior.
- Backup/binlog/L0 import timestamp semantics.
- Failure recovery for DataCoord, StreamingNode, DataNode, QueryNode, and object storage.
- RBAC and multi-database behavior for REST endpoints.
- Observability through progress/list, metrics, logs, and segment metadata inspection.

### Out of Scope

- Non-REST client wrapper coverage.
- Platform-side file distribution orchestration, except validating the kernel contract it depends on.
- Strict read-after-commit consistency. Current design only guarantees write consistency; query visibility may lag until QueryNode target refresh/reload.

## 3. REST API Contract

Normal business responses return HTTP 200 for both success and errors. Tests must assert response JSON `code`, not only HTTP status. Authentication or gateway failures may still use non-200 HTTP status.

Common headers:

```http
Authorization: Bearer root:Milvus
Content-Type: application/json
DB-Name: default
```

`dbName` in the JSON body can also select the database. If neither JSON `dbName` nor `DB-Name` header is provided, REST falls back to `default`.

### Import Endpoints

| Endpoint | Body | Success data |
| --- | --- | --- |
| `POST /v2/vectordb/jobs/import/create` | `collectionName`, optional `partitionName`, `files`, optional `options` | `{ "jobId": "..." }` |
| `POST /v2/vectordb/jobs/import/get_progress` | `{ "jobId": "..." }` | state, progress, rows, task details |
| `POST /v2/vectordb/jobs/import/describe` | `{ "jobId": "..." }` | same as get_progress |
| `POST /v2/vectordb/jobs/import/list` | optional `collectionName` | records: jobId, collectionName, state, progress |
| `POST /v2/vectordb/jobs/import/commit` | `{ "jobId": "..." }` | `{}` |
| `POST /v2/vectordb/jobs/import/abort` | `{ "jobId": "..." }` | `{}` |

Create body example:

```json
{
  "dbName": "default",
  "collectionName": "imp_2pc_basic",
  "files": [["s3://bucket/import/part-0.parquet"]],
  "options": {
    "auto_commit": "false"
  }
}
```

Commit body example:

```json
{
  "jobId": "466872198213043514"
}
```

### Option Contract

REST `options` is `map[string]string`. Bool-like values are strings.

| Option | Values | Notes |
| --- | --- | --- |
| `auto_commit` | `"true"`/`"false"` | absent or any value other than case-insensitive `"false"` means true |
| `backup` | `"true"`/`"false"` | backup/binlog restore mode |
| `l0_import` | `"true"`/`"false"` | L0 delete import mode |
| `skip_disk_quota_check` | `"true"`/`"false"` | effective only for backup/L0 |
| `storage_version` | `"0"`, `"1"`, `"2"`, `"3"` | unset/0/currently 1 -> V1; 2 -> V2; 3 -> V3; illegal string fails |
| `start_ts`, `startTs` | hybrid ts string | backup time filtering |
| `end_ts`, `endTs` | hybrid ts string | backup time filtering |
| `ezk` | base64 string | encrypted backup input |
| `sep` | one char | CSV delimiter; NUL/LF/CR/quote/U+FFFD rejected |
| `nullkey` | string | CSV null marker |
| `timeout` | Go duration string | example: `"300s"`, `"1.5h"` |

## 4. Current Implementation Semantics To Test Against

These are release-critical because they define how tests should judge pass/fail.

| Area | Current expected behavior |
| --- | --- |
| Auto commit | All jobs pass through `Uncommitted`; `auto_commit=true` then checker broadcasts commit automatically. |
| Manual import | `auto_commit=false` must stop at `Uncommitted` until explicit commit or abort. |
| Commit idempotency | `CommitImport` on `Uncommitted`, `Committing`, or `Completed` returns success. |
| Abort behavior | `AbortImport` is allowed before commit and must be retry-safe. Repeated abort after the first success should return success while the job remains `Failed`; `Committing` and `Completed` are rejected because committed data cannot be rolled back. |
| Multi-vchannel visibility | Commit is per-vchannel atomic, not job-level read atomic. During `Committing`, one vchannel may already be visible while another is still invisible. |
| Completed visibility | `Completed` means all vchannels acknowledged the commit fence, but query/search may still need target refresh or reload before imported rows are visible. |
| Pre-commit delete | A delete with `delete_ts < commit_ts` must not delete imported insert rows, because the rows did not logically exist yet. |
| Post-commit delete | A delete with `delete_ts > commit_ts` must apply normally. |
| DML availability | Import must not block normal insert/upsert/delete. Delete during `Committing` should apply to already visible vchannels. |
| Standby writes | Secondary clusters should reject direct non-replicated write/broadcast operations; commit/abort must be accepted only through replicated primary WAL messages. |
| L0 import | L0 delete import must keep source delete timestamp semantics; it must not overwrite delete semantics with import commit timestamp. |

Implementation anchors:

- REST routes: `internal/distributed/proxy/httpserver/handler_v2.go`
- REST request shapes: `internal/distributed/proxy/httpserver/request_v2.go`
- Import state checker: `internal/datacoord/import_checker.go`
- Commit/abort RPCs: `internal/datacoord/services.go`
- Per-vchannel commit behavior: `internal/datacoord/import_meta.go`
- Query delete timestamp basis: `internal/querynodev2/delegator/delegator_data.go`
- REST/proto contract: `pkg/proto/internal.proto`, `pkg/proto/data_coord.proto`

## 5. Test Environment

### Single Cluster

Required variants:

- Standalone or cluster mode smoke with `DmlChannelNum=1`.
- Cluster mode with `DmlChannelNum>=4` for multi-vchannel behavior.
- QueryNode load/reload enabled.
- Object storage accessible by Milvus and by the test data uploader.

### Primary-Secondary CDC

Required variants:

- One primary and one secondary cluster with CDC replication enabled.
- Same collection schema and object storage paths reachable by both clusters.
- Same pchannel count and vchannel mapping.
- Ability to pause/restart StreamingNode/DataCoord/QueryNode pods for failure tests.
- Ability to observe CDC lag and cluster role.

### Existing Test Framework Integration

Do not create a standalone harness. Implement these cases inside the existing Milvus QA frameworks.

Primary target:

```text
tests/restful_client_v2/
├── api/milvus.py
├── base/testbase.py
├── conftest.py
├── pytest.ini
└── testcases/
    ├── test_jobs_operation.py
    └── test_import_2pc_operation.py    # recommended new file for 2PC-specific cases
```

Use the existing framework pieces:

- `TestBase` from `tests/restful_client_v2/base/testbase.py` for endpoint/token/minio fixtures and teardown.
- `ImportJobClient` from `tests/restful_client_v2/api/milvus.py` for import REST APIs.
- `CollectionClient`, `VectorClient`, `IndexClient`, and `StorageClient` from the same framework for collection, DML/query/search, index, and object storage operations.
- Existing data patterns in `tests/restful_client_v2/testcases/test_jobs_operation.py` for Parquet/JSON generation and MinIO upload.
- Existing pytest markers from `tests/restful_client_v2/pytest.ini`: `L0`, `L1`, `L2`, and `BulkInsert`.

Required framework extensions:

- Add `ImportJobClient.commit_import_job(job_id, db_name="default")`.
- Add `ImportJobClient.abort_import_job(job_id, db_name="default")`.
- Add `ImportJobClient.describe_import_job(job_id, db_name="default")` or reuse `get_import_job_progress` if describe is intentionally aliased.
- Add `ImportJobClient.wait_import_job_state(job_id, expected_state, timeout=IMPORT_TIMEOUT, db_name="default")`.
- Keep all import REST calls behind `ImportJobClient`; test cases should not call raw `requests.post` directly.

Recommended file split:

- Put new 2PC lifecycle, MVCC, delete, TTL, and multi-vchannel cases in `tests/restful_client_v2/testcases/test_import_2pc_operation.py`.
- Keep broad existing import format/type cases in `test_jobs_operation.py` unless they are rewritten specifically for 2PC.
- Put CDC-specific REST cases in the existing CDC/standby test area if the environment already provides two-cluster fixtures; otherwise add the minimum primary/secondary endpoint fixtures to the existing framework, not a new harness.

Run command shape:

```bash
cd tests/restful_client_v2
python -m pytest -v testcases/test_import_2pc_operation.py \
  --endpoint http://127.0.0.1:19530 \
  --token root:Milvus \
  --minio_host 127.0.0.1 \
  --bucket_name <minio_bucket> \
  --root_path <milvus_storage_root_path>
```

## 6. Shared Fixtures And Assertions

### Base Collection

Use a deterministic schema unless a case requires a special schema:

- `id`: Int64 primary key
- `tag`: VarChar scalar
- `phase`: Int64 scalar
- `vector`: FloatVector, dim 8

Create HNSW or AUTOINDEX before import for the main path so imported data must pass index readiness before `Uncommitted`.

### Query Assertion Rules

- Never assert immediate query visibility right after `commit` or `Completed`.
- Use the existing collection load/refresh APIs plus a small polling helper in the target test file before final visibility assertions.
- On non-empty collections, assert imported PK set visibility rather than `count(*) == 0`.
- For replication, compare exact PK sets on primary and secondary, not only row counts.

### State Waiters And Local Helpers

Implement waiters by extending the existing framework, not by creating a new harness:

- `ImportJobClient.wait_import_job_state(job_id, expected_state, timeout, db_name)` polls `/get_progress`.
- A local helper in `test_import_2pc_operation.py` may poll `self.vector_client.vector_query` until imported PKs are visible or absent.
- A local CDC helper may compare primary/secondary exact PK sets after each side has refreshed load state.
- If a helper is reused by multiple REST v2 test files, move it into the existing `tests/restful_client_v2/base/testbase.py` or `tests/restful_client_v2/utils/` area.

## 7. P0 Release Gate

### IMP-REST-000: API contract smoke

Priority: P0

Steps:

1. Call `/create` with a valid Parquet file and `auto_commit=false`.
2. Call `/get_progress`, `/describe`, and `/list` with valid bodies.
3. Use one manual job for `/commit` success and another manual job for `/abort` success.
4. For invalid job ID string, call commit/abort with `{"jobId":"not-a-number"}`.

Expected:

- Valid create returns `code=0` and string `data.jobId`.
- Valid progress/describe/list return `code=0`.
- Invalid commit/abort job ID returns `code=1100` or equivalent parameter error.
- Tests assert JSON `code`; HTTP status alone is not used as pass criteria.

### IMP-LC-001: default auto_commit compatibility

Priority: P0

Steps:

1. Create collection and index.
2. Upload Parquet file with N deterministic rows.
3. Call `/create` without `options.auto_commit`.
4. Poll `/get_progress` until terminal state.
5. Load/refresh collection and query/search imported PKs.

Expected:

- Job reaches `Completed` without explicit commit.
- Imported rows eventually become visible.
- `importedRows == totalRows == N`.
- `/list` contains the job.

### IMP-LC-002: manual import stops at Uncommitted

Priority: P0

Steps:

1. Create collection and index.
2. Upload N rows.
3. Call `/create` with `{"auto_commit":"false"}`.
4. Poll `/get_progress` until `Uncommitted`.
5. Query imported PKs repeatedly while job stays `Uncommitted`.

Expected:

- State reaches `Uncommitted` and does not auto-commit for the test timeout.
- Imported PKs are not query/search visible.
- Existing non-import rows remain visible and mutable.
- Progress is stable near 99; row counters match N.

### IMP-LC-003: manual commit happy path and commit idempotency

Priority: P0

Steps:

1. Run IMP-LC-002 until `Uncommitted`.
2. Call `/commit`.
3. Immediately call `/commit` again.
4. Poll progress until `Completed`.
5. Load/refresh collection and query/search all imported PKs.
6. Call `/commit` once more after `Completed`.

Expected:

- All commit calls return `code=0`.
- State eventually becomes `Completed`.
- Imported rows eventually become visible.
- No duplicate rows or count inflation occur.

### IMP-LC-004: invalid state commit is rejected

Priority: P0

Steps:

1. Create a manual job and immediately call `/commit` before it reaches `Uncommitted`.
2. Create an auto-commit job and call `/commit` while it is auto-managed.
3. Call `/commit` with a nonexistent numeric job ID.

Expected:

- Non-`Uncommitted` manual job commit returns non-zero code with reason containing expected state.
- Auto-commit job manual commit returns non-zero code: manual commit/abort not allowed.
- Nonexistent job returns non-zero code with job-not-found/import-failed reason.

### IMP-ABT-001: manual abort before commit

Priority: P0

Steps:

1. Create manual import and wait for `Uncommitted`.
2. Call `/abort`.
3. Poll until `Failed`.
4. Call `/commit` on the aborted job.
5. Query/search imported PKs after load/refresh.

Expected:

- Abort returns `code=0`.
- Job state becomes `Failed`.
- Commit after abort returns non-zero code and the job remains `Failed`.
- Imported PKs never become visible.
- Failed job cleanup eventually removes or marks task segments so they do not participate in query.

### IMP-ABT-002: abort after commit is rejected

Priority: P0

Steps:

1. Create manual import, wait for `Uncommitted`, call `/commit`.
2. After state is `Committing` or `Completed`, call `/abort`.
3. After first successful abort case from IMP-ABT-001, call `/abort` again on the same job.

Expected:

- Abort in `Committing`/`Completed` returns non-zero code.
- Repeated abort after a user-aborted job is already `Failed` should return success and keep the job terminal/invisible.
- No imported rows are rolled back after commit begins.

### IMP-MVCC-001: commit_timestamp visibility boundary

Priority: P0

Steps:

1. Insert a small baseline set through REST `/entities/insert`.
2. Create manual import for a disjoint PK set and wait for `Uncommitted`.
3. Query count and imported PKs.
4. Commit, wait for `Completed`, then poll/reload until imported PKs are visible.

Expected:

- Baseline rows stay visible throughout the import.
- Imported PKs are invisible before commit.
- Imported PKs become visible only after commit and query target refresh.

### IMP-DEL-001: delete before commit is ignored for imported insert rows

Priority: P0

Steps:

1. Create manual import and wait for `Uncommitted`.
2. Call REST `/entities/delete` with a filter matching imported PKs.
3. Verify delete response count is 0 or imported PKs remain absent because they are not visible.
4. Commit and wait for eventual visibility.

Expected:

- Delete timestamp is earlier than import commit timestamp.
- Imported rows are still present after commit.
- Primary key set equals the full imported PK set.

### IMP-DEL-002: delete after commit applies normally

Priority: P0

Steps:

1. Create manual import, commit it, and wait until imported PKs are visible.
2. Delete a subset of imported PKs.
3. Flush if needed and poll query/search.

Expected:

- Deleted subset disappears.
- Non-deleted imported rows remain visible.
- Count and exact PK set match expected.

### IMP-DEL-003: delete during Committing on already committed vchannel

Priority: P0

Environment: multi-vchannel cluster with fault injection or controlled delay.

Steps:

1. Create a collection with at least four vchannels.
2. Import data distributed across multiple vchannels.
3. Trigger commit and hold/delay one vchannel's commit processing if the test environment supports it.
4. While job is `Committing`, delete PKs that are already visible on a committed vchannel.
5. Let all vchannels finish and query final PK set.

Expected:

- Delete on already visible import rows applies.
- Import does not block the delete path.
- Final state is consistent with delete timestamp ordering.
- If partial visibility cannot be forced, record as not executed rather than weakening the assertion.

### IMP-TTL-001: TTL starts from commit_timestamp

Priority: P0

Steps:

1. Create collection with a short TTL, for example 30 seconds.
2. Create manual import and keep it in `Uncommitted` long enough that row physical timestamps would be old.
3. Commit the job.
4. Query at approximately commit+15s and commit+35s.

Expected:

- Rows are visible around commit+15s.
- Rows expire around commit+35s.
- Rows must not expire immediately because import row timestamps were old.

### IMP-VCH-001: multi-vchannel commit eventual consistency

Priority: P0

Steps:

1. Use `DmlChannelNum>=4`.
2. Import data that hashes across all vchannels.
3. Wait for `Uncommitted`, then call `/commit`.
4. Poll progress through `Committing` to `Completed`.
5. Query exact imported PK set after load/refresh.

Expected:

- All vchannels eventually commit.
- Job eventually reaches `Completed`.
- Final imported PK set is complete.
- The test must not fail solely because data was partially visible during `Committing`; that is current expected behavior.

### IMP-MIX-001: import interleaves with insert and delete

Priority: P0

Steps:

1. Insert baseline rows.
2. Start manual import and wait for `Uncommitted`.
3. Insert more rows with REST `/entities/insert`.
4. Delete a subset of baseline rows.
5. Commit import.
6. After eventual visibility, query all phase-specific PK sets.

Expected:

- Import does not block insert/delete.
- Baseline delete applies.
- Rows inserted during import are visible according to normal DML behavior.
- Imported rows become visible after commit.
- No PK set cross-contamination occurs.

### IMP-REP-001: CDC 2PC happy path

Priority: P0

Environment: primary-secondary replication.

Steps:

1. Create the same collection through the primary and wait for replication.
2. Upload import files reachable by both clusters.
3. Call `/create` with `auto_commit=false` on primary.
4. Poll primary and secondary until both report `Uncommitted`.
5. Assert imported PKs are invisible on both clusters.
6. Call `/commit` on primary.
7. Poll both clusters until `Completed`.
8. Poll/reload both clusters and compare exact PK sets.

Expected:

- Secondary receives Import and CommitImport through CDC.
- Both clusters eventually reach `Completed`.
- Final PK sets are identical.
- Imported rows do not appear on secondary before commit.

### IMP-REP-002: CDC delete timestamp boundary

Priority: P0

Steps:

1. Run manual replicated import to `Uncommitted` on both clusters.
2. Delete imported PKs on primary before commit.
3. Commit on primary and wait for both clusters.
4. Verify rows still exist on both clusters.
5. Delete a subset after commit on primary.
6. Verify subset disappears on both clusters.

Expected:

- `delete_ts < commit_ts` is ignored for imported insert rows on both clusters.
- `delete_ts > commit_ts` applies on both clusters.
- Primary and secondary exact PK sets remain equal.

### IMP-REP-003: standby direct commit/abort is rejected

Priority: P0

Steps:

1. Create manual import through primary and wait until secondary has the job.
2. Call `/commit` and `/abort` directly against the secondary REST endpoint.
3. Continue normal commit from primary.

Expected:

- Secondary direct commit/abort returns a non-zero code or is rejected by the write-role guard.
- Secondary does not create a local-only state transition.
- Primary commit still replicates and finishes correctly.

### IMP-REP-004: CDC replicated abort cleanup

Priority: P0

Steps:

1. Create a manual import through primary and wait until both clusters reach `Uncommitted`.
2. Assert imported PKs are invisible on both clusters.
3. Call `/abort` on primary.
4. Poll both clusters until the job reaches `Failed`.
5. Continue polling/reloading both collections and query imported PKs.

Expected:

- `AbortImport` is replicated through CDC.
- Both primary and secondary reach `Failed`.
- No imported PK becomes visible on either cluster after abort.

### IMP-REP-005: CDC DML interleaving while import is Uncommitted

Priority: P0

Steps:

1. Create collection through primary and wait for secondary replication.
2. Insert baseline rows through primary and wait until they are visible on both clusters.
3. Create manual import and wait until both jobs reach `Uncommitted`.
4. Insert more rows through primary while import remains `Uncommitted`.
5. Verify DML rows are visible on both clusters and imported rows remain invisible.
6. Commit import and wait for both clusters.
7. Compare final exact PK sets.

Expected:

- Import does not block normal DML in CDC topology.
- DML before/during import is replicated and visible before import commit.
- Import rows only become visible after `CommitImport`.
- Primary and secondary exact PK sets remain identical.

### IMP-REP-006: CDC CommitImport idempotency

Priority: P0

Steps:

1. Create manual import through primary and wait until both jobs reach `Uncommitted`.
2. Call `/commit` on primary twice before waiting for `Completed`.
3. Wait until both clusters reach `Completed`.
4. Call `/commit` again on the completed job.
5. Verify exact PK set and collection count on both clusters.

Expected:

- Duplicate `CommitImport` calls return success or canonical idempotent success.
- Import rows are not duplicated.
- Primary and secondary final PK sets and counts match.

### IMP-REP-007: secondary object storage file missing

Priority: P1

Steps:

1. Stage import file only in primary object storage.
2. Create manual import through primary.
3. Wait until primary reaches `Uncommitted` or a stable pre-commit state, and secondary reports a readable failure.
4. Abort from primary.
5. Verify no imported rows are visible on either cluster.

Expected:

- Secondary import failure is observable with a useful reason.
- Platform can abort the primary-side job before commit.
- No partial import data becomes query-visible.

### IMP-REP-008: CDC multiple import jobs on the same collection

Priority: P1

Steps:

1. Create two manual import jobs on the same replicated collection with disjoint PK ranges.
2. Wait until both jobs are `Uncommitted` on both clusters.
3. Commit one job and verify only its PK range becomes visible.
4. Abort or commit the second job and verify final exact PK set.

Expected:

- Job states are isolated under CDC.
- One job's commit does not expose another uncommitted job.
- Final primary/secondary PK sets match.

### IMP-REP-009: secondary direct Import create is rejected

Priority: P1

Steps:

1. Configure CDC topology and create collection through primary.
2. Stage import file in secondary object storage.
3. Call REST `/jobs/import/create` directly against secondary.

Expected:

- Secondary rejects direct import creation by write-role guard.
- No local-only secondary import job is created.

### IMP-FT-001: DataCoord restart recovery

Priority: P0

Steps:

1. Create manual import and wait for `Uncommitted`.
2. Restart DataCoord.
3. Verify progress still reports `Uncommitted`.
4. Commit and verify `Completed`.
5. Repeat with restart during `Committing`.

Expected:

- Job metadata recovers after restart.
- Commit can continue after an `Uncommitted` restart.
- A `Committing` job eventually finishes after checker recovery.

### IMP-FT-002: StreamingNode restart during commit

Priority: P0

Steps:

1. Create multi-vchannel manual import and wait for `Uncommitted`.
2. Call `/commit`.
3. Restart a StreamingNode handling one target pchannel/vchannel.
4. Poll job state and final visibility.

Expected:

- CommitImport WAL message is replayed or retried.
- Segment `commit_timestamp` is eventually set for affected vchannels.
- Job reaches `Completed`; final PK set is complete.

### IMP-SP-001: backup/binlog restore timestamp semantics

Priority: P0

Steps:

1. Prepare a backup/binlog source segment with known PKs and source timestamps.
2. Import with `{"backup":"true","auto_commit":"false"}`.
3. Verify import data invisible at `Uncommitted`.
4. Commit and validate query/search/delete semantics.
5. Repeat for `storage_version` unset/0, 2, and 3 where fixtures are available.

Expected:

- Source data is restored correctly.
- Time range and delete semantics match source binlog timestamps.
- The import commit timestamp must not corrupt restore semantics.

### IMP-SP-002: L0 delete import timestamp semantics

Priority: P0

Steps:

1. Prepare source insert rows around known timestamps.
2. Prepare L0 delete binlogs for a subset of PKs with known source delete timestamps.
3. Import L0 with `{"l0_import":"true","auto_commit":"false"}`.
4. Verify delete effect is not visible before commit.
5. Commit and query final PK set.

Expected:

- L0 delete keeps source delete timestamp semantics.
- Rows older than source delete timestamp are deleted after commit.
- Rows newer than source delete timestamp are not deleted.
- Behavior is identical on primary and secondary when run under CDC.

## 8. P1 Test Matrix

| ID | Area | Scenario | Expected |
| --- | --- | --- | --- |
| IMP-LC-101 | Lifecycle | Empty file or zero-row import | Completed, count does not change, no visible empty dirty segment |
| IMP-LC-102 | Lifecycle | Continuous imports on same collection | Each job has independent state; cumulative PK set is correct |
| IMP-LC-103 | Lifecycle | `/describe` progress monotonicity | State/progress do not regress except allowed terminal retry display |
| IMP-REST-101 | REST | Missing required create fields | non-zero code and readable reason |
| IMP-REST-102 | REST | `jobId` numeric JSON vs string JSON | string is canonical; numeric compatibility recorded if accepted |
| IMP-REST-103 | REST | `dbName` body vs `DB-Name` header | database routing is correct and deterministic |
| IMP-REST-104 | REST | `/list` collectionName filter isolation | listing one collection does not return import jobs from another collection |
| IMP-REST-105 | REST | `/list` empty collectionName filter | listing a collection with no import jobs returns an empty records list |
| IMP-REST-106 | REST | `/describe` and `/get_progress` parity | both endpoints report the same state/progress/row counters for the same import job |
| IMP-REST-107 | REST | missing `jobId` on job endpoints | describe/get_progress/commit/abort reject missing jobId with a readable error |
| IMP-REST-108 | REST | invalid `/list` collectionName type | numeric collectionName is rejected with a readable type error |
| IMP-REST-109 | REST | `/list` without collectionName | unfiltered list includes the current DB import job and summary fields |
| IMP-REST-110 | REST | invalid `/create` collectionName type | numeric collectionName is rejected synchronously without creating a job |
| IMP-REST-111 | REST | invalid `/create` files type | string files value is rejected synchronously without creating a job |
| IMP-REST-112 | REST | invalid `/create` options type | string options value is rejected synchronously without creating a job |
| IMP-REST-113 | REST | invalid `/create` partitionName type | numeric partitionName is rejected synchronously without creating a job |
| IMP-REST-114 | REST | invalid `/create` file path type | numeric file path inside files is rejected synchronously without creating a job |
| IMP-REST-115 | REST | invalid `/create` flat files list | `files=["a.parquet"]` is rejected synchronously without creating a job |
| IMP-REST-116 | REST | invalid `/create` empty file group | `files=[[]]` is rejected synchronously without creating a job |
| IMP-REST-117 | REST | invalid `/create` timeout option type | numeric `options.timeout` is rejected synchronously without creating a job |
| IMP-REST-118 | REST | invalid read jobId type | describe/get_progress reject boolean `jobId` with a readable error |
| IMP-REST-119 | REST | invalid write jobId type | commit/abort reject boolean `jobId` with a readable error |
| IMP-REST-120 | REST | invalid `/create` null collectionName | null collectionName is rejected synchronously without creating a job |
| IMP-REST-121 | REST | invalid `/create` null files | null files is rejected synchronously without creating a job |
| IMP-REST-122 | REST | invalid `/create` partitionName array | array partitionName is rejected synchronously without creating a job |
| IMP-REST-123 | REST | invalid read jobId array | describe/get_progress reject array `jobId` with a readable error |
| IMP-REST-124 | REST | invalid write jobId array | commit/abort reject array `jobId` with a readable error |
| IMP-REST-125 | REST | invalid jobId object | describe/get_progress/commit/abort reject object `jobId` with a readable error |
| IMP-REST-126 | REST | invalid `/create` collectionName array | array collectionName is rejected synchronously without creating a job |
| IMP-REST-127 | REST | invalid `/create` collectionName object | object collectionName is rejected synchronously without creating a job |
| IMP-REST-128 | REST | invalid `/create` files object | object files is rejected synchronously without creating a job |
| IMP-REST-129 | REST | invalid `/create` file path object | object file path inside files is rejected synchronously without creating a job |
| IMP-REST-130 | REST | invalid `/create` options array | array options is rejected synchronously without creating a job |
| IMP-REST-131 | REST | invalid `/create` options number | numeric options is rejected synchronously without creating a job |
| IMP-REST-132 | REST | invalid `/create` boolean auto_commit option | boolean `options.auto_commit` is rejected synchronously without creating a job |
| IMP-REST-133 | REST | invalid `/create` null auto_commit option | null `options.auto_commit` should be rejected synchronously without creating a job; current instance accepts it and this is tracked as xfail |
| IMP-REST-134 | REST | invalid `/create` array auto_commit option | array `options.auto_commit` is rejected synchronously without creating a job |
| IMP-REST-135 | REST | invalid `/create` boolean backup option | boolean `options.backup` is rejected synchronously without creating a job |
| IMP-REST-136 | REST | invalid `/create` boolean l0_import option | boolean `options.l0_import` is rejected synchronously without creating a job |
| IMP-REST-137 | REST | invalid `/create` boolean skip_disk_quota_check option | boolean `options.skip_disk_quota_check` is rejected synchronously without creating a job |
| IMP-REST-138 | REST | invalid `/create` array backup option | array `options.backup` is rejected synchronously without creating a job |
| IMP-REST-139 | REST | invalid `/create` object backup option | object `options.backup` is rejected synchronously without creating a job |
| IMP-REST-140 | REST | invalid `/create` array l0_import option | array `options.l0_import` is rejected synchronously without creating a job |
| IMP-REST-141 | REST | invalid `/create` object l0_import option | object `options.l0_import` is rejected synchronously without creating a job |
| IMP-REST-142 | REST | invalid `/create` array skip_disk_quota_check option | array `options.skip_disk_quota_check` is rejected synchronously without creating a job |
| IMP-REST-143 | REST | invalid `/create` object skip_disk_quota_check option | object `options.skip_disk_quota_check` is rejected synchronously without creating a job |
| IMP-REST-144 | REST | invalid `/create` numeric storage_version option | numeric `options.storage_version` is rejected synchronously without creating a job |
| IMP-REST-145 | REST | invalid `/create` array start_ts option | array `options.start_ts` is rejected synchronously without creating a job |
| IMP-REST-146 | REST | invalid `/create` object end_ts option | object `options.end_ts` is rejected synchronously without creating a job |
| IMP-REST-147 | REST | invalid `/create` boolean sep option | boolean `options.sep` is rejected synchronously without creating a job |
| IMP-REST-148 | REST | invalid `/create` array nullkey option | array `options.nullkey` is rejected synchronously without creating a job |
| IMP-REST-149 | REST | invalid `/create` object ezk option | object `options.ezk` is rejected synchronously without creating a job |
| IMP-OPT-102 | Options | `auto_commit` value other than case-insensitive `"false"` | treated as true; job auto-commits and data becomes visible |
| IMP-OPT-103 | Options | `auto_commit` case-insensitive `"false"` | uppercase/lowercase false disables auto commit and stops at `Uncommitted` |
| IMP-MVCC-101 | MVCC | Strong/Bounded/Eventually consistency levels | No pre-commit visibility; eventual post-commit visibility |
| IMP-MVCC-102 | MVCC | Time-travel query before commit timestamp | Import rows are absent from snapshots before commit |
| IMP-DEL-101 | Delete | Mixed pre/post commit deletes | Only post-commit delete subset disappears |
| IMP-DEL-102 | Delete | AutoID collection delete by scalar filter | Same timestamp behavior as explicit PK collections |
| IMP-CMP-101 | Compaction | Manual compaction after committed import | Rows remain correct; `commit_timestamp` handling survives compaction |
| IMP-CMP-102 | Compaction | Compaction while job is Uncommitted | Import data does not become visible early |
| IMP-CMP-103 | GC | Failed/aborted import cleanup | No query-visible rows; object/segment cleanup eventually occurs |
| IMP-MIX-101 | DML | Import plus upsert PK conflicts | Upsert semantics are correct relative to commit timestamp |
| IMP-MIX-102 | DML | Manual flush during import | Flush does not expose Uncommitted import data |
| IMP-MIX-103 | DML | Manual compact during import | Compact does not expose Uncommitted import data |
| IMP-REP-101 | CDC | Commit while secondary import still building | Secondary blocks/replays until local import is ready, then completes |
| IMP-REP-102 | CDC | Secondary misses CommitImport due to network pause | Recovery catches up and final PK set matches primary |
| IMP-REP-103 | CDC | Abort replicated to secondary | Both clusters move to Failed and data remains invisible |
| IMP-FT-101 | Recovery | DataNode restart during importing | Job retries or fails cleanly; no partial visibility |
| IMP-FT-102 | Recovery | QueryNode restart/reload after commit | Rows become visible after reload/target refresh |
| IMP-SEC-101 | RBAC | User without import privilege | create/progress/list/commit/abort are denied as designed |
| IMP-SEC-102 | Multi DB | Same collection name in two DBs | Jobs and data are isolated per DB |
| IMP-OBS-101 | Observability | Failure reason quality | Failed jobs expose actionable reason in progress/detail |
| IMP-OBS-102 | Observability | Metrics/logs | job count, rows, duration, state, commit lag are observable |

## 9. P2 Coverage Matrix

These cases are valuable but should not block the first release gate unless they expose a P0 invariant violation.

### Data Types

| ID | Coverage |
| --- | --- |
| IMP-TYP-201 | bool, int8/16/32/64, float, double, varchar |
| IMP-TYP-202 | JSON field with path filtering |
| IMP-TYP-203 | Array field with array filters |
| IMP-TYP-204 | FloatVector, BinaryVector, Float16Vector, BFloat16Vector |
| IMP-TYP-205 | SparseFloatVector JSON and parquet struct encoding |
| IMP-TYP-206 | Int8Vector and ArrayOfVector |
| IMP-TYP-207 | Geometry and Timestamptz |
| IMP-TYP-208 | nullable/default/dynamic fields |
| IMP-TYP-209 | Negative type mismatch, dimension mismatch, varchar overflow, NaN/Inf |
| IMP-TYP-209A | BinaryVector byte-length mismatch negative |

### Index Types

| ID | Coverage |
| --- | --- |
| IMP-IDX-201 | HNSW with L2/IP/COSINE |
| IMP-IDX-202 | IVF_FLAT, IVF_SQ8, IVF_PQ |
| IMP-IDX-203 | DISKANN |
| IMP-IDX-204 | FLAT, SCANN |
| IMP-IDX-205 | BIN_FLAT, BIN_IVF_FLAT |
| IMP-IDX-206 | SPARSE_INVERTED_INDEX, WAND |
| IMP-IDX-207 | scalar indexes: INVERTED, BITMAP, STL_SORT, Trie |

### File Formats

| ID | Coverage |
| --- | --- |
| IMP-FMT-201 | Parquet single and multi-file |
| IMP-FMT-202 | JSON and JSONL |
| IMP-FMT-203 | CSV with `sep` and `nullkey` |
| IMP-FMT-204 | NumPy field-per-file |
| IMP-FMT-205 | Mixed file groups plus invalid suffix / invalid CSV separator |

### Partitioning And Layout

| ID | Coverage |
| --- | --- |
| IMP-PAR-201 | Import into named partition |
| IMP-PAR-202 | Partition-key collection routing |
| IMP-PAR-203 | Partition-key collection with explicit partition name negative behavior |
| IMP-PAR-204 | Clustering key collection and clustering compaction |
| IMP-PAR-205 | Multi-vector collection |

### Functions

| ID | Coverage |
| --- | --- |
| IMP-FN-201 | BM25 function input-only import |
| IMP-FN-202 | BM25 output column rejected |
| IMP-FN-203 | Non-BM25 function output guardrail |
| IMP-FN-204 | Text embedding function import path |

### Backup Storage Versions

| ID | Coverage |
| --- | --- |
| IMP-SP-200 | `backup=true` requires `partitionName` and rejects before job creation |
| IMP-SP-201 | `storage_version` parse contract: unset, 0, 1, 2, 3, illegal string |
| IMP-SP-202 | StorageV1 happy path and missing RowID/Timestamp negative |
| IMP-SP-203 | StorageV2 column group happy path and missing group negative |
| IMP-SP-204 | StorageV3 manifest happy path and missing/invalid manifest negative |
| IMP-SP-205 | `start_ts`/`end_ts` filtering for insert and delta binlogs |
| IMP-SP-206 | `skip_disk_quota_check` effective only for backup/L0; regular import ignores the flag and still follows normal 2PC |
| IMP-SP-207 | encrypted backup with `ezk` |
| IMP-SP-208 | Continuous restore of V1, V2, and V3 segments into one collection |

## 10. Release Exit Criteria

Release can proceed only if:

- All P0 cases pass on single-cluster REST tests.
- All CDC P0 cases pass on primary-secondary environment, or a documented environment blocker is accepted.
- No imported PK is visible during `Uncommitted`.
- Commit eventually makes the full imported PK set visible after load/refresh.
- Pre-commit delete does not delete imported insert rows.
- Post-commit delete applies normally.
- Abort before commit leaves no query-visible imported data.
- Commit is idempotent; abort behavior is documented and consistent with implementation.
- Multi-vchannel final visibility is complete, with partial `Committing` visibility treated as current limitation rather than unexpected dirty data.
- L0/binlog timestamp semantics pass or are explicitly blocked as release defects.

## 11. Known Limitations And Bug Filing Rules

File a release-blocking bug if any of these occur:

- Imported rows are visible in `Uncommitted`.
- `auto_commit=false` job auto-commits without explicit `/commit`.
- `delete_ts < commit_ts` removes imported insert rows.
- `delete_ts > commit_ts` fails to remove committed import rows.
- `Completed` is reached but imported rows never become visible after reload/target refresh.
- Primary and secondary stable PK sets diverge after CDC catch-up.
- Direct standby commit/abort changes standby-local import state.
- Abort before commit leaves query-visible imported rows.
- L0 import applies delete using import commit timestamp instead of source delete timestamp.

Do not file a bug solely for these expected current behaviors:

- Rows are not immediately query-visible right after `/commit` or `Completed`.
- During `Committing`, a subset of vchannels is visible before the entire job reaches `Completed`.

## 12. Automation Notes

Framework wrapper conventions:

- Use `self.import_job_client.create_import_jobs(payload)` for `/create`.
- Use `self.import_job_client.get_import_job_progress(job_id)` for `/get_progress`.
- Add and use `self.import_job_client.commit_import_job(job_id)` for `/commit`.
- Add and use `self.import_job_client.abort_import_job(job_id)` for `/abort`.
- Use `self.import_job_client.list_import_jobs(payload)` for `/list`.
- Use `self.vector_client.vector_insert`, `vector_query`, `vector_search`, and `vector_delete` for DML/query/search assertions.
- Use `self.collection_client` and `self.index_client` for collection/index/load operations.
- Use `self.storage_client.upload_file` for MinIO/S3 staging.
- Do not introduce a parallel REST client, duplicate pytest fixture, or independent teardown path.

Assertion conventions:

- Assert REST transport separately only when useful; business pass/fail is `rsp["code"]`.
- Use `assert rsp["code"] == 0` for success.
- Use `assert rsp["code"] != 0` plus stable reason substring for expected errors.
- State waiters should log every observed state transition with timestamp and importedRows/totalRows.
- Every test collection name should use existing `gen_collection_name()` or the framework's UUID naming pattern to avoid cross-test pollution.

Data fixture conventions:

- PK ranges should encode phase: baseline, import, post-insert, deleted-before-commit, deleted-after-commit.
- Vector values should be deterministic so search assertions can check expected PKs.
- Multi-vchannel tests should verify route distribution by observing segment/vchannel metadata when possible.
- CDC tests should always compare exact PK sets on both clusters after waiting for replication and query visibility.

Failure artifacts to collect:

- REST request/response JSON for import endpoints.
- Import progress snapshots over time.
- DataCoord, StreamingNode, QueryNode logs around job ID.
- Segment metadata for imported segments, including `is_importing`, `commit_timestamp`, vchannel, and state.
- CDC lag/checkpoint information for primary-secondary tests.
