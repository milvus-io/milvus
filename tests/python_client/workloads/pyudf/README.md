# PyUDF regression workloads

The SDK cases own public Search, resource lifecycle, index, snapshot, admission
and cancellation assertions. REST parameter cases live in
`tests/restful_client_v2/testcases/test_pyudf.py`. Shared SDK fixture ownership is
in `base/client_v2_pyudf_base.py`; tests do not import another testcase module.

## SDK and REST selection

Use the repository's Python 3.12 environment and suite requirements. Run each
suite from its own directory so its `base`, `utils` and `conftest.py` resolve
correctly. Use pytest's standard `--junitxml` option when result properties
need to be retained in a report.

```sh
# From tests/python_client, with the repository environment activated:
PYTHONPATH=..:. python -m pytest milvus_client/test_milvus_client_pyudf*.py \
  --tags L0 L1 L2 --collect-only

# From tests/restful_client_v2:
PYTHONPATH=..:. python -m pytest testcases/test_pyudf.py \
  --tags L0 L1 L2 --collect-only
```

For execution, remove `--collect-only` and supply the suite's Milvus and object
store options. Preserve failures: L2 cross-stage score assertions reproduce
[#55212](https://github.com/milvus-io/milvus/issues/55212) on affected builds.
The Cardinal vector matrix uses the documented float-score tolerance for ordinary
index results; exact PyUDF arithmetic and ranking assertions remain unchanged.

L3 cases require a dedicated deployment and an exact node selector. The runtime
module checks the worker configuration before exercising admission or deadlines.
Worker-pool parameters require separate deployments for each selected pair.
The 256-worker case requires its explicit upper-bound configuration. The
`--pyudf-management-endpoint` option must address that deployment's management
service. Cardinal reloads additionally require `--pyudf-expected-autoindex`;
benchmarks require `--run-pyudf-benchmark`. Both accept
`--pyudf-expected-commit` to check the deployed build and record the actual build.
Do not select all L3 parameters against one unchanged deployment.

## Shared system workload

`workload.py` retains generated vectors, query inputs, candidate baselines and
wheel digests. Its checker validates IDs, scalar/NULL alignment and transformed
scores independently of the server. `benchmark.py` measures RPC activity and
equivalent operations; `stress.py` contains interference and resource-cache
scenarios. `runner.py` is the common command-line entry point.

The caller owns an isolated cluster with two distinct Proxy endpoints, object
storage, deployment/fault orchestration, resource monitoring and final environment
cleanup. Create an owned evidence directory containing `config.json`:

```json
{
  "proxy_uris": ["http://proxy-a:19530", "http://proxy-b:19530"],
  "minio_endpoint": "object-store:9000",
  "bucket": "milvus-bucket",
  "collection": "pyudf_unique_run_id",
  "index_type": "HNSW",
  "expected_commit": "<deployed-git-commit-prefix>",
  "rows": 100000,
  "minio_secure": false
}
```

Supply `MILVUS_TOKEN`, `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` through the
execution environment. Keep credentials out of the configuration and artifacts.
From `tests/python_client`:

```sh
python -m workloads.pyudf.runner initialize --evidence "$EVIDENCE"
python -m workloads.pyudf.runner soak --evidence "$EVIDENCE"
python -m workloads.pyudf.runner concurrency --evidence "$EVIDENCE"
```

Initialization refuses an existing input directory or collection. Later phases
reuse retained inputs and candidate baselines. Both Proxy build identities must
match the configured commit prefix. The soak defaults to 7,200 seconds at 20
scheduled requests/second with eight threads, followed by 300 seconds of drain
and a 600-second replay. Smaller bounded durations are available through
`--duration`, `--drain` and `--replay` for runner smoke checks. Those runs are not
full soak validation. Creating `$EVIDENCE/STOP` stops load with a failure verdict.
ABBA and batch measurements use the same stop file, including batch baseline
collection. Already in-flight requests drain within their request timeouts;
no later measurement window starts after the stop is observed. Standalone pytest
benchmarks use `STOP` in their evidence directory.

Additional phases are `performance`, `interference`, and `cache`. Cache steps
are explicitly selected with `--cache-phase baseline|cold|replay|remove|recovery`;
the caller supplies any intervening restart and verifies internal load/cache
evidence. Each phase writes separate machine-readable artifacts and propagates
assertion failures to the process exit status. The `cleanup` phase removes the
extra performance/interference registrations. Retained collections, input wheels
and evidence remain owned by the caller until final environment cleanup.

Performance counter measurements require an explicitly configured
`function.pyUDF.server.workerCount: 1` on **each** Proxy. Add
`"proxy_management_endpoints": ["http://proxy-a:9091", "http://proxy-b:9091"]`
to `config.json`, in the same order as `proxy_uris`. Each Search/management pair
must directly address the same Proxy host, not a load-balanced service. The runner
checks both management endpoints before submitting performance load; missing or
multi-worker configuration fails the prerequisite. Keep this deployment unchanged
throughout the measurement. Other workload phases do not require this option.

Each counter snapshot reads PID, started, completed, active and peak counts in
one UDF invocation under its lock. ABBA compares snapshots by Proxy/resource/PID,
and rejects restarts, missing workers or decreasing counters. This performance
profile deliberately covers one Python worker per Proxy; it does not claim
aggregate accounting for multi-worker pools.

Periodic Jenkins deployment and fault orchestration belong in `test-jobs` and
are a separate integration step. These workload files alone do not establish
that a periodic regression has run.
