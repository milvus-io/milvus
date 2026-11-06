# Struct Array enhancement regression

The retained tests cover nullable Struct children, nested Array children, child
schema evolution, partial/full upserts, typed vector coordinates and scores,
wire validation, SDK/API serialization, import and bounded concurrent histories. Expected
rows, legal coordinates and scores are derived from seeded input models.

## CI testcase organization

Files follow the existing `test_milvus_client_struct_array_<behavior>.py` naming
pattern. Case names describe the operation and observable expectation. The exact
file selection lives in `tests/scripts/struct_array_regression.json`.

| Behavior | Python SDK file suffixes |
| --- | --- |
| NULL values and nested arrays | `nullable_scalar_children`, `nested_array_children`, `nested_array_predicates`, `nullable_predicates` |
| Add/drop fields and validation | `add_subfield`, `added_subfield_types`, `schema_validation`, `field_properties` |
| DML and partial updates | `dml`, `partial_update_paths`, `partial_update_rejection` |
| Vector coordinates, scores and indexes | `nullable_vector_coordinates`, `vector_scores`, `vector_index_metric`, `indexed_query`, `indexed_subfields`, `search_validation` |
| Concurrency and isolation | `concurrent_mutations`, `concurrent_read_write`, `concurrent_schema_updates`, `concurrent_subfield_drop`, `partition_isolation`, `schema_rbac` |
| Import and protobuf validation | `bulk_import`, `import_validation`, `protobuf_validity`, `nested_validity` |
| Load/storage/configuration | `growing_sealed_search`, `query_null_reduction`, `mmap_warmup`, `storage_v3_flush` |

Shared row models, typed vectors, protobuf helpers and base assertions belong to
`tests/python_client/common/struct_array_*.py`; testcase modules do not serve as
shared helper libraries. REST uses behavior-specific `test_struct_array_*.py`
files and `base/struct_array_helpers.py`. Default CI still selects L0/L1/L2;
special-deployment cases retain L3 and their explicit prerequisites.

## Coverage responsibilities

Python functional cases own the server behavior matrix, including index/metric
combinations, reload, compaction, schema histories and concurrent consistency.
Go SDK cases check schema/column construction, nullable/nested data encoding,
mutation options and public result decoding (including element offsets and
result slicing). REST cases check JSON types and field parameters, mutation
options, response projections and error codes. Both use real server round trips
with explicit expected values. Their schema refresh is setup for newly added
fields; full lifecycle coverage remains in the Python functional cases.

## Python SDK and REST

Use Python 3.12 and the selected suite's requirements in an isolated uv environment.
PyMilvus 3.1.0rc105 includes the merged enhancement and parent-parameter readback
changes; prereleases are available on Test PyPI. From the Milvus repository root:

```bash
uv venv -p 3.12
source .venv/bin/activate
uv pip install -r tests/python_client/requirements.txt
python tests/scripts/run_struct_array_regression.py --suite python \
  --artifacts /path/to/new-python-results -- --tags L0 L1 L2 -n 6 \
  --host "$MILVUS_HOST" --port "$MILVUS_PORT" --token "$MILVUS_TOKEN" \
  --minio_host "$MINIO_HOST" --minio_bucket "$MINIO_BUCKET"
```

For REST use a separate environment with `tests/restful_client_v2/requirements.txt`:

```bash
python tests/scripts/run_struct_array_regression.py --suite rest \
  --artifacts /path/to/new-rest-results -- -n 6 \
  --endpoint "$MILVUS_URI" --token "$MILVUS_TOKEN" \
  --minio_host "$MINIO_HOST" --bucket_name "$MINIO_BUCKET"
```

`tests/scripts/struct_array_regression.json` is the exact retained file selection.
The runner fails on missing files, preserves pytest's exit code and writes JUnit,
Allure results and source/selection metadata. Use a new artifact directory per
attempt. Add `--collect-only` to inspect selection without connecting to Milvus;
collection is not runtime verification. Existing pytest `-k`, `--tags` and worker
options remain available. No failures are removed by this runner.

The master E2E deployment values in `test-jobs/jenkins/milvus_e2e/` enable
schema-version backfill compaction (`dataCoord.compaction.bumpSchemaVersion.enabled=true`).
Use the same setting when replaying the schema-evolution tests elsewhere.
Import tests need the deployment's reachable MinIO service and bucket. Tests own
unique collections and object keys. Do not run RBAC mutation tests against a
shared production environment. Pin the actual server commit independently of
this test-code revision.

## Specialized profiles

The three schema permission cases carry both `L3` and `RBAC` tags. The regular
master deployment disables authorization; run these cases on an authenticated
target using an administrator connection and `--tags RBAC`. They check denial,
non-mutation, grant and revoke rather than treating absent enforcement as a skip.

Select `--tags L3` and the applicable options; absent prerequisites are skips,
not passes. Each option describes an independently verified deployment:

| Option | Required target |
| --- | --- |
| `--struct-storage-mode v2` | Storage V2 |
| `--struct-storage-mode v3` | Storage V3, growing-source flush disabled |
| `--struct-storage-mode v3-gsf` | Storage V3, growing-source flush enabled |
| `--struct-max-array-capacity N` | Effective `proxy.maxArrayCapacity=N` |

Parent mmap and warmup parameters are covered separately from proof of internal
mmap or warmup completion.

## Go SDK

The two retained files are `struct_array_dml_search_test.go` and
`struct_array_column_schema_test.go`. They reuse existing package helpers:

```bash
milvus-dev-cli gosdk-e2e local . -b master --deploy "$DEPLOYMENT" \
  -t tests/go_client/testcases \
  -k 'TestStructArray(MutationOptionsRoundTrip|SearchResultOffsetsRoundTrip|ColumnInsertPreservesNestedNulls|AddNullableParentOptionRoundTrip)' -f
```

Use the current repository toolchain and Go SDK module configuration. Run the
package's CI lint configuration, not only file-level formatting.

## C++ offset controls

The retained structure-level pending-gap control and growing-segment missing-child
compensation test live in the existing `StructElementOffsetsTest.cpp` and
`SegmentGrowingTest.cpp` suites. The latter checks sibling-derived offsets and
NULL validity of the synthesized representative child. The pending-gap control
does not establish a reachable product failure through normal DML.

```bash
milvus-dev-cli cpp-ut local . -b master -n 4 -t all_tests \
  -k 'StructElementOffsetsTest.*:Growing.InsertMissingRepresentativeUsesSiblingLengths'
```

## Bounded concurrent CI workload

`common/struct_array_concurrent_workload.py` supplies finite read/write/schema
histories to `test_milvus_client_struct_array_concurrent_read_write.py`. It retains
operation intervals, acknowledged writes, uncertain outcomes and exact final rows.
Each run records the actual build identity; `expected_product` is an optional
explicit revision constraint rather than a historical default commit.

System performance, soak, chaos, CDC and cross-version orchestration have separate
delivery work. This CI selection does not configure periodic jobs or certify
those campaigns. Test-code revision and actual server commit remain separate.

The pytest adapter attaches the operation journal, expected rows and summary to
Allure on both success and failure. Its default evidence directory is unique to
the test invocation; `STRUCT_MIXED_EVIDENCE` can select a retained artifact root.
These attachments supplement pytest's unchanged failure outcome.

## Known outcomes remain visible

- All-null partial upserts use normal success assertions. The historical
  conditional xfail was removed after all twelve seeded histories passed.
- Concurrent nullable-child removal is linked to
  `https://github.com/milvus-io/milvus/issues/55234` with strict, exception-scoped
  xfails. Only the exact code-1100 removed-child signature qualifies, after
  acknowledged-row and rejected-write reconciliation succeeds. Other failures
  remain failures; a fully successful history is a strict XPASS.
- Refresh search readiness remains a strict assertion (Milvus issue #54594).
- Three typed DISKANN score profiles have strict, exception-scoped xfails linked
  to `https://github.com/zilliztech/knowhere/issues/1956`. The original tolerance
  remains unchanged and all mutation, reload, row and coordinate checks run.
  Unrelated failures still fail; a passing score budget is a failing XPASS that
  requires removing the marker. The issue discusses the accuracy contract, not
  a confirmed Struct bug.

Historical execution on an integrated PR build is separate from verification of
any later master/release build. Retain the original run and each rerun; do not
infer that a merged fix was exercised merely from the GitHub state.
