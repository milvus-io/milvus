# PyUDF system regression

Run system scenarios through pytest from `tests/python_client`, using the
repository's Python 3.12 environment and suite requirements. Each node owns its
setup, workload, assertions and cleanup. All nodes in `test_workloads.py` carry
the combined `tags("L3", "pyudf")` marker; collection needs no live environment.

```sh
PYTHONPATH=..:. python -m pytest system_tests/pyudf --collect-only -q \
  --tags L3 pyudf --tags-operand AND
```

`pytest-tagging` defaults to OR, so retain `--tags-operand AND`. Regular CI uses
`--tags L0 L1 L2`, which excludes these scenarios. An unfiltered pytest run
includes L3; missing configuration is a setup error, not a passing skip.

## Reserved deployment configuration

Supply two distinct, directly addressed Proxies in an isolated deployment and
object storage accessible to its Python workers. Create a configuration file:

```json
{
  "proxy_uris": ["http://proxy-a:19530", "http://proxy-b:19530"],
  "minio_endpoint": "object-store:9000",
  "bucket": "milvus-bucket",
  "index_type": "HNSW",
  "expected_commit": "<deployed-git-commit-prefix>",
  "rows": 100000,
  "minio_secure": false
}
```

Supply `MILVUS_TOKEN`, `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` through the
environment. Keep credentials out of configuration and artifacts. Each scenario
generates its own collection name; an old `collection` setting is replaced.
Both Proxy build identities must match `expected_commit`, which can also be
overridden with the existing `--pyudf-expected-commit` option.

For example, run the complete package lifecycle, including preparation and cleanup:

```sh
PYTHONPATH=..:. python -m pytest \
  system_tests/pyudf/test_workloads.py::test_package_reuse_and_removal_preserve_search \
  --tags L3 pyudf --tags-operand AND -n 0 \
  --pyudf-system-config "$CONFIG" \
  --pyudf-system-evidence "$EVIDENCE" --junitxml=results.xml
```

Jenkins calls the same node and command, supplying its configuration and archiving
JUnit plus `$EVIDENCE`. Deployment and fault operations belong in test-jobs;
pytest scenarios, fixtures, data, assertions and custom Milvus CRD profiles belong
in Milvus. Add concrete CRD profiles under this feature's `environments/` only
when needed; test-jobs reads them from the selected Milvus revision. This change
does not create deployments, install CRDs or enable scheduled jobs.

## Scenario selection

Select a node under `system_tests/pyudf/test_workloads.py`:

| Node | Contract and approximate load duration, excluding setup |
|---|---|
| `test_soak_and_replay_preserve_rows_and_scores` | 2-hour soak at 20 scheduled requests/s with 8 threads, 5-minute drain, 10-minute replay; row and search audits |
| `test_abba_preserves_semantics_and_worker_counts[HNSW]` | 2-minute instrumentation control and 48-minute ABBA; scores, fields, concurrency and PID-bound counters |
| `test_abba_preserves_semantics_and_worker_counts[DISKANN]` | Same gates against explicit DISKANN |
| `test_batches_preserve_scores_and_input_columns[HNSW]` | Independent 6-minute NQ/candidate/input-layout matrix; scores and decoded columns |
| `test_batches_preserve_scores_and_input_columns[DISKANN]` | Same batch gates against explicit DISKANN |
| `test_concurrent_search_reaches_requested_rpc_concurrency` | 1/8/32 concurrent ordinary and PyUDF Search, 20 seconds per window; exact results and observed RPC concurrency |
| `test_cpu_and_wait_load_preserve_peer_search` | Bounded CPU/wait load with ordinary peer Search and recovery; about 3 minutes |
| `test_package_reuse_and_removal_preserve_search` | RAG document reranking through 32 wheel policies, ten replays, removal rejection and recovery in one test |

Performance nodes replace the old SDK ABBA/batch entrypoints with explicit HNSW
and DISKANN parameters, overriding `index_type` for ABBA and batch nodes.
Select either family independently; batches do not run instrumentation or ABBA. Other nodes
use `index_type` from the configuration. Tests use standard index types without
image-specific overrides. DISKANN requires a deployment with disk indexing enabled
and sufficient local disk; Search uses `search_list`, while HNSW uses `ef`.
Preparation requires all rows to be indexed, so a tiny smoke dataset below the
deployment's segment indexing threshold is not sufficient.

ABBA nodes require `function.pyUDF.server.workerCount: 1` on **each**
Proxy. Add `"proxy_management_endpoints": ["http://proxy-a:9091", "http://proxy-b:9091"]`
in the same order as `proxy_uris`. Search and management endpoints must directly
address the same host. The prerequisite is verified before initialization for these
nodes. Keep the deployment unchanged during measurement. Other workload nodes do
not require this option. A snapshot reads PID, started, completed, active and peak
counts atomically; restarts and missing/decreasing counters fail the gate. This
does not establish multi-worker aggregate accounting or a latency acceptance SLA.

Batch summaries separate semantic correctness from achieved load. `target_qps`
is the configured five requests per second; `submitted_requests` and
`skipped_slots` reconcile against the 150 slots in the 30-second submission
window. `offered_qps` divides actual submissions by that window, while
`completed_qps` divides validated completions by elapsed time through the final
completion, including drain. No requests are submitted after the window closes.
`semantic_outcome=PASS` does not imply `load_outcome=TARGET_MET`; skipped slots
produce `load_outcome=UNDER_TARGET` even when every submitted request is correct.

For a shorter runner smoke, set `duration`, `drain`, `replay` in the JSON file.
Bounds are `0 < duration <= 7200`, `0 <= drain <= 300`, `0 < replay <= 600`
seconds. Shortened runs do not establish full soak validation. The workload uses
128-dimensional vectors and retains independently generated inputs and baselines.

Package checks cover wheel loading/reuse and resource invalidation. They do not
test tiered storage or prove cache hits, eviction or memory reclamation;
`cache_hit_verification` remains `NOT_MEASURED` in their artifacts.

### Document reranking workload

The package lifecycle uses a business UDF rather than an arithmetic-only payload.
It reranks vector candidates using L2 relevance, exponential publication-age decay,
source trust and document quality. Missing quality defaults to 0.5; quality is
clamped to [0, 1], future publication times have zero age, and unknown sources
receive the fallback trust weight. The query time is fixed for reproducible CI.

The retained 100,000-row input includes publication timestamps, source categories
and nullable quality scores alongside 128-dimensional vectors. The 32 existing
wheels carry distinct freshness policies; the test independently computes scores
from retained vectors/document fields, checks exact top-k order and tie-breaking,
and requires policies to produce different rankings. First calls and all ten
replays use this oracle; removed resources must become uncallable. Full audits
also compare document metadata before and after the lifecycle.

This is deterministic business reranking, without an external model/service or a
claim about semantic relevance quality. Arithmetic UDFs remain deliberate controls
for serialization, instrumentation, throughput comparison and precise failures.

## Failure handling and evidence

Each test prints and records its unique evidence directory. Create `STOP` inside
that directory to abort preparation or load with a failure verdict. Guards run
before each insert batch, baseline query, audit page and between index/load
operations. In-flight requests drain within their RPC timeouts. Setup failures, assertion failures and cleanup errors
are reported by pytest; measurements do not turn a failed semantic gate into PASS.

Fixtures remove only their generated collection, registered resources and wheel
objects, including partially completed setup. Ownership manifests are recorded
before remote writes. Cleanup attempts the remaining resources even if one fails,
and reports errors in `cleanup.json` and pytest. The bucket itself is not owned by
the test. Retained vectors, wheels, request journals, summaries and build identities
stay on disk for diagnosis. Use `--pyudf-system-evidence` for a durable artifact
directory; otherwise pytest's temporary-directory retention applies.

`workloads/pyudf` contains reusable preparation, load and oracle code. The old
`python -m workloads.pyudf.runner <phase>` CLI and separate cleanup phase have
been removed. Preparation and package-cache steps are fixtures/internal operations,
not separately ordered pytest cases. The fixture owns clients, storage and the
prepared checker for the entire scenario; JSON/NPY files are evidence, not an
internal hand-off that reconnects clients or reloads vectors.

## Functional cases and dedicated profiles

SDK contracts remain in `milvus_client/test_milvus_client_pyudf*.py`, REST cases
in `tests/restful_client_v2/testcases/test_pyudf.py`. Run each suite from its own
directory. L0–L2 selectors and assertions remain unchanged, including the strict
cross-stage score regression for [#55212](https://github.com/milvus-io/milvus/issues/55212).

Existing SDK L3 profiles also carry `pyudf`. Worker-pool, admission, cancellation
and disabled-feature profiles require their specific deployment settings
and exact node selectors; do not run all L3 profiles on one unchanged deployment.
They retain the SDK suite's connection options, `--pyudf-management-endpoint`,
and `--run-pyudf-disabled` where applicable.

## Coverage and execution cost

Keep distinct failures and deployment prerequisites independently selectable.
Related read-only queries reuse their collection and indexes within one node:

| Coverage | Execution change | Retained assertions |
|---|---|---|
| Cross-stage scores | 144 independent setups become 24; build the requested metric directly | All 144 metric/mode/sort/stage-order combinations, including all six stage permutations and strict #55212 scores/ranks |
| Indexed hybrid fusion | 8 setups become 4; write 40,000 fewer rows across the complete matrix | All 8 merger/NQ/read-or-rewrite combinations with independent fusion scores |
| mmap/warmup | All 30 profiles remain; configured load/reload calls drop from 4 to 2 and Search calls from 36 to 24 per profile | First configured load and one reload, cold/hot queries, every configuration and the full retained-row audit |
| Ragged batches | Use NQ 1 and 8 rather than 1/2/4/8 | Singleton behavior and the complete nonempty/empty/nonempty sequence repeated twice |
| SDK nesting | Use depths 1/24/32/64 rather than 1/16/24/32/64/65 | Shallow/deep accepted inputs and both protobuf rejection forms; raw REST depth cases remain separate |
| Package lifecycle | Reuse the fixture-owned clients and in-memory input ledger through every phase | All five phases, 32 packages, ten replays each, removal rejection and recovery |

These are reductions in setup and request counts, not measured wall-clock speedups.
Assertions retain per-permutation/NQ evidence for grouped queries. An assertion
failure ends its node; different metric/mode/sort or merger/rewrite nodes remain
independent. System load durations and resource guards are unchanged.

## Upgrade, restore and CDC

`workloads/pyudf/migration.py` remains a reusable retained-data checker, not a
complete migration scenario. It generates 10,000 rows with 128-dimensional
vectors, scalar/nullable/JSON/array/dynamic fields and a UDF wheel. It verifies
ledger/wheel digests, acknowledged mutations, post-switch writes, full rows,
ordinary scores and PyUDF results, including L2 and IP.

A complete scenario must live here in Milvus: prepare the ledger and source,
perform prechecks, call the versioned test-jobs upgrade/restore/CDC operation,
wait for completion, then assert destination data and resource state. Use
`retained_ledger`, `MigrationChecker.seed`, `prepare_resource` and `mutate` for
explicit setup; use `resource_state`, `audit`, `ordinary` and `udf` for independent
verification. Require returned `outcome == "PASS"`, `registry_matches` and
`bytes_match`. Verification must not silently copy or register missing wheels.
`window` checks mixed-version Proxies through live Connect RPCs for at least 600
seconds; the caller owns resource guards and client lifetimes.

Upgrade/restore/CDC orchestration bindings are not implemented by this refactor.
Neither this checker nor collection of the load scenarios establishes live
migration coverage or scheduled regression execution.
