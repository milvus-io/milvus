# Paimon external collection E2E

The suite implements the case matrix below from the reviewed Paimon plan against Milvus PR #51897,
head `4f95cb61d849bd4ecb03fa1f641179825a58b05d`, and storage
`5c2ea329889e30d94b4e65209c0bc782256e7a95`. Test infrastructure is based on
Milvus Python E2E commit `22341c46` (Python 3.12 and its existing requirements).

## Ordinary deployment

Run from `tests/python_client` with the usual Milvus and MinIO arguments:

```sh
pytest milvus_client/test_milvus_client_external_table_paimon.py \
  --uri http://milvus:19530 --token root:Milvus \
  --minio_host minio:9000 --minio_bucket milvus-bucket \
  --tags L0 L1 -n 2 --dist loadgroup --timeout 600
```

For a runner/server address difference, set the existing
`MILVUS_EXTERNAL_SOURCE_MINIO_HOST`. L0–L2 do not change server configuration,
start writers, install native storage tools, inspect etcd, or operate Kubernetes.
Paimon gets a **table root without a final slash**; Parquet gets a listing prefix.
Collections are unique to each case and are dropped before source cleanup.
The read-only class shares its loaded data within an xdist group.

The same source can be uploaded and run through the development CLI:

```sh
milvus-dev-cli build pr 51897
milvus-dev-cli deploy create <completed-build-id> --name paimon-51897-e2e --cu 2
milvus-dev-cli pysdk-e2e local <this-milvus-worktree> -b master \
  --deploy paimon-51897-e2e \
  -t tests/python_client/milvus_client/test_milvus_client_external_table_paimon.py \
  -- --tags L0 L1 -n 2 --dist loadgroup --timeout 600
```

Use `--tags L2` for the larger boundary suite. The TextEmbedding cases use the
existing `--tei_endpoint`; unavailable endpoints are reported as **Blocked skips**,
never as passing capability coverage. They compare raw model output from the
same `/embed` endpoint and record `/info` model provenance. No new service is
deployed by ordinary tests.

## CI selection

All executable cases live under `milvus_client/`. Levels use `CaseLabel.L0/L1/L2/L3`,
matching both pytest-tagging and the TCUS runner's source-file filter. No Paimon
subdirectory conftest is needed. `--paimon-l3-profile` is registered in the ordinary
root `tests/python_client/conftest.py`.

PR #51897's actual `ci-v2/e2e-default` job (TCUS build 1036) uses a file-sharded
runner with `--tags L0 L1 --dist loadgroup`, eight Python workers and a 1200-second
case timeout. Its file discovery looks for `CaseLabel.L0/L1` before pytest runs.
The legacy `tests/scripts/ci_e2e_4am.sh` instead forwards its arguments to pytest
from the same Python-client root; the in-repo ARM pipeline supplies six workers
and `--tags L0 L1`. The suite supports both paths without changing shared CI scripts.

- Ordinary PR regression selects 26 Paimon L0/L1 cases and no L3 cases.
- An L2 job selects the same ordinary module with `--tags L2` (131 cases).
- L3 requires a separate standalone hook deployment and the serial entry
  `tests/scripts/ci_paimon_external_l3.sh`; it is not added to a shared PR shard.
  See [L3 CI setup](L3.md). Adding that entry is not a claim that an external
  Jenkins L3 job has already been registered or executed.

From the repository root, CI contract checks exercise the actual pytest and shell entry points:

```sh
python -m pytest -q tests/scripts/test_paimon_ci.py
```

## Data and independent ground truth

`assets/paimon/fixtures.zip` contains content-addressed blobs and a manifest.
The ZIP container uses Python's standard-library LZMA compression (about 23.5 MB),
keeping the entire CI source bundle below TCUS's 32 MiB compressed limit. This
changes only the container encoding, not source files, snapshots or oracle bytes.
Every Java table was written using Paimon 1.2.0 and then read using a separate,
snapshot-pinned Paimon Java batch reader. Tests do not infer expected rows from
Milvus, native storage output, counts, or the writer's data formula.

The bundle contains append, MOR, bitmap32 DV, mixed historical/current schemas,
multi-bucket, partitioned, one-row, bitmap64, wide-type and converted Vortex tables.
MOR has S1, S2, S3 and all-deleted snapshots; S3 reinserts a deleted key and updates
an already updated key. All-deleted fixtures contain real delete commits, not
empty files pretending to have deletion semantics.

The wide fixture includes signed integer limits, Unicode and empty strings,
1%/20%/100% nullable columns, empty/null arrays, JSON, microsecond timestamps,
WKT points, packed binary/half/bfloat16/int8 vectors, and a sparse MAP.

Java Paimon 1.2.0 has no Vortex plugin. The Vortex fixture is explicitly a
**physical conversion of a Java append table**: Python Vortex writes each source
Parquet file; an independent Vortex read must equal the original Arrow values.
Typed Avro rewriting updates data-file and manifest sizes and snapshot references.
The logical GT remains the Java reader's result, with conversion provenance
attached. This does not certify a Java/Flink Vortex writer.

The adapter validates blob hashes, relative paths, snapshot ID/file/oracle binding,
and independent-reader provenance before uploading. Publication sends dependencies
before snapshot commits and commits before hints. It refuses immutable rewrites
and backward publication, including an ambiguous snapshot PUT whose response was
lost. Tests delete only explicitly owned dependencies.

## Rebuild fixtures offline

Only the fixture builder needs JDK 17, Internet access to Maven Central, and the
separate build requirements. These are not additions to the E2E runner image.
Five JAR SHA256 values are pinned in `build_fixtures.py`; the JVM classpath contains
only those five verified paths.

```sh
python -m pip install -r scripts/paimon/build-requirements.txt
python scripts/paimon/build_fixtures.py --java-home /path/to/jdk17 \
  --save-generated /tmp/paimon-source-output
```

`provenance.json` records the source/converter hashes and actual package versions
at generation time. Every Java oracle records its generator hash and Java version.
Repackaging preserves that source provenance and requires its matching oracle
hashes; it does not claim the current Java file produced older output:

```sh
python scripts/paimon/build_fixtures.py --reuse-generated /tmp/paimon-source-output
```

UUID filenames and Paimon commit timestamps intentionally vary between builds.
Per-file digests certify a particular generated package; byte-for-byte identical
archives across independent builds are not promised.

## Verification

```sh
PYTHONPATH=. pytest --confcutdir=common -q \
  common/test_paimon_fixture_unit.py common/test_paimon_l3_unit.py
pytest --collect-only milvus_client/test_milvus_client_external_table_paimon.py --tags L0 L1 L2
ruff check --select E,F,W,I,UP --target-version py312 --line-length 120 \
  common/paimon*.py common/test_paimon*.py conftest.py \
  milvus_client/test_milvus_client_external_table_paimon*.py \
  scripts/paimon/*.py ../scripts/test_paimon_ci.py
```

Unit checks include independent hand-calculated metric rankings, cutoff ties,
sparse empty/disjoint queries, actual SDK protobuf byte-vector output, corruption,
snapshot provenance, partial publication, response loss, and retry/repair.

ANN tests use HNSW/COSINE and IVF_FLAT/L2 after S1 has real indexed-row coverage,
then refresh the same loaded collection to S2 without recreating its index. The
bitmap32 source is sized so the S2 new-vector file exceeds the default 1024-row
threshold. They require final S2 index coverage, aggregate recall, per-hit distance,
and complete raw output before and after reload. Finished status alone is not
treated as proof of an ANN worker build.

## Case mapping and remaining scope

| Plan family | Implementation |
|---|---|
| L0-01/02 | append auto; MOR refresh with pinned control |
| L1-01 | explicit Parquet/Vortex modes; one mixed auto snapshot; same-collection mode switching |
| L1-02/03/04/05 | bitmap32 DV; typed pinned/latest; no-op/S1/S2/S3; multi-bucket iteration |
| L1-06 | existing actual HNSW/IVF index followed by incremental refresh |
| L1-07/08/09/10 | filters/get/iterator; add fields; BM25/Text Match; normal/Parquet/Lance/Paimon parity |
| L2-01 | typed scan/snapshot entry validation, -1 latest, expired IDs, key normalization and duplicates |
| L2-02/03/04 | bad mapping/truncated dependencies; wide scalar/raw GT; five vector encodings |
| L2-05 | additional metrics, nq/topK/offset and range search |
| L2-06/07/08 | partitions/schema/MOR unsafe direct; format switching; one-row/all-deleted exploration |
| L2-10/11/12 | bitmap64 direct rejection; keys/credentials; existing TEI and MinHash normal control |
| L2-13/14/15/16 | concurrent refresh; dependency repair/reload; DML/schema restrictions; jobs/redaction |
| L3-13 | executable dedicated-deployment orchestration and strict phase evidence contract |

The broader plan is not reduced to this representative matrix. Remaining source
fixtures include alternate DV positions with the same physical file/count, bad DV
CRC, BLOB, special merge engines/sidecars/row ranges and nullable/invalid sparse or
byte-vector encodings. Extended search modes, the rest of the rejection/API matrix,
source-evolution errors, concurrent schema changes, drop/recreate/dependency-loss
paths, cross-DB/async coverage and L3-01–12 are not claimed implemented by this patch.
Track them explicitly rather than adding empty or unconditional skip cases.

L3-13 includes a Kubernetes standalone driver (Service or Cell access) and an opt-in patch for a
separate test image. See [L3 CI, deployment and recovery](L3.md).
Its three cases fail setup without the dedicated profile. Actual runtime results
and the remaining plan coverage are recorded in `IMPLEMENTATION_REPORT.md`.
