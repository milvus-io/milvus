# PyUDF CI tests

These suites exercise public Python SDK, REST v2 and Go SDK Search APIs using
small, deterministic datasets and real wheel FileResources. Assertions cover
IDs, scores, returned fields, error classification and recovery after rejected
requests. System fault injection and performance workloads have separate runners.

## Profiles and prerequisites

- **Enabled:** `function.pyUDF.enabled=true`, a working Python runtime, resource
  synchronization, authentication enabled, and an administrative test principal.
  Use the suite's matching MinIO endpoint and bucket. Wheels use bucket-relative
  paths. The Python/REST fixtures use the existing suite's test MinIO credentials.
- **Disabled:** a separate instance with `function.pyUDF.enabled=false`. Select
  only `test_milvus_client_pyudf_disabled.py` and pass `--run-pyudf-disabled`.
  The flag does not modify server configuration. Without it, these two cases
  explicitly skip so they cannot accidentally fail an enabled CI lane.
- Python requires the FunctionChain/EmbeddingList SDK APIs; install the version
  pinned in `tests/python_client/requirements.txt`. REST uses its own requirements.
- The mixed-segment L2 case requires the management endpoint on port 9091 and
  `500 < indexCoord.segment.minSegmentNumRowsToEnableIndex <= 3000` (normally
  1024). It checks this prerequisite and observes segment state before searching.
- Use an owned, isolated test deployment. Run the SDK suites sequentially on a
  shared target: the Go suite's existing `TestMain` includes database cleanup.
  Parallel workers within the Python/REST suite use unique collection/resource
  names; RBAC cases remove only their own identities and grants.

## Levels and selection

| Suite/profile | L0 | L1 | L2 | Total |
| --- | ---: | ---: | ---: | ---: |
| Python enabled, including EmbeddingList/element search | 2 | 77 | 95 | 174 |
| Python disabled | 0 | 2 | 0 | 2 |
| REST v2 | 1 | 16 | 36 | 53 |
| Go leaf subtests | 2 | 7 | 2 | 11 |

L0 covers basic reranking and recovery. L1 covers routine contracts and principal
negative paths. L2 covers extended type/nesting matrices, query combinations,
index/reload/mmap, compaction, concurrency, scoped authorization and embedding
lists. Level denotes scheduling priority, not bug severity.

For Python/REST use the repository's `--tags L0 L1` selector for PR coverage,
`--tags L2` for the extended lane, or omit the selector for the full suite.
Go uses paths such as `TestPyUDFSearchContracts/L0/search_single_query`;
`-run '^TestPyUDFSearchContracts$/L[01]/'` selects L0 and L1. Go's
`-parallel 6` is its native concurrency setting; shared setup and these small
subtests remain sequential to avoid duplicate setup. It is not pytest `-n 6`.

## Full-suite timing runs

Run the following from the repository root. Supply the names of prepared
deployments in `PYUDF_ENABLED_DEPLOY` and `PYUDF_DISABLED_DEPLOY`. Dev CLI resolves
connection settings without storing credentials in this document.

```bash
milvus-dev-cli pysdk-e2e local . -b master \
  --deploy "$PYUDF_ENABLED_DEPLOY" \
  -t tests/python_client/milvus_client/test_milvus_client_pyudf.py -f -- \
  /workspace/milvus/tests/python_client/milvus_client/test_milvus_client_pyudf_embedding_list.py \
  -n 6 --timeout=240 --durations=25 -o junit_family=xunit1 \
  --junitxml=/tmp/pyudf-python.xml

milvus-dev-cli pysdk-e2e local . -b master \
  --deploy "$PYUDF_DISABLED_DEPLOY" \
  -t tests/python_client/milvus_client/test_milvus_client_pyudf_disabled.py -f -- \
  --run-pyudf-disabled -n 6 --timeout=180 --durations=10 \
  --junitxml=/tmp/pyudf-disabled.xml

milvus-dev-cli restful-e2e local . -b master \
  --deploy "$PYUDF_ENABLED_DEPLOY" \
  -t tests/restful_client_v2/testcases/test_pyudf.py -f -- \
  -n 6 --durations=25 --junitxml=/tmp/pyudf-rest.xml

milvus-dev-cli gosdk-e2e local . -b master \
  --deploy "$PYUDF_ENABLED_DEPLOY" \
  -t tests/go_client/testcases/pyudf_test.go \
  -k '^TestPyUDFSearchContracts$' -f -- \
  -tags=dynamic,test '-gcflags=all=-N -l' -parallel=6 -count=1 -timeout=10m -json
```

The additional Python path is inside the Dev CLI runner's checkout. For direct
pytest use, run from `tests/python_client` and use the two `milvus_client/...`
paths with the suite's normal connection arguments.

The execution budget is **5–10 minutes per complete SDK suite**, with Python/REST
at six workers. Report actual test duration separately from source upload,
image startup, dependency installation and Go compilation. Preserve failures,
skips, exact selected nodes and the slowest setup/call/teardown timings; a shorter
L0/L1 selection does not demonstrate the full-suite budget.

The disclosure characterization case records a synthetic canary observation in
JUnit properties; a passing probe is not a confidentiality verdict. The worker
port probe covers only its supplied endpoint. Testing a Service address does not
certify direct Pod isolation. The Go tests use public custom SearchOption
implementations while the standard builder gap is tracked in #54317.

These commands establish reproducible manual CI-suite execution. Actual pipeline
selection and deployment configuration must be connected and observed before
claiming recurring CI coverage.
