# Native MultiSearch prototype on Milvus v3.0.5

See [design](../../docs/design-docs/multisearch.md). This replaces the older
separate-service experiment. The RPC is `/milvus.proto.milvus.MilvusService/MultiSearch`.
The official Python class gets `MilvusClient.multi_search()` through the companion
PyMilvus patch; no client adapter or runtime monkey patch is used.

## Dependencies and API sources

Run `bash examples/multisearch/prepare_dependencies.sh` in this checkout. It clones
pinned public protocol/SDK revisions, applies the companion patches, creates a Go
workspace using the patched protocol module, and installs the patched SDK locally.
The main server source remains ordinary Milvus source. These unpublished dependencies
must be upstreamed/versioned before a release can use a normal module dependency.
Generated protobuf outputs are supplied in the patches and were generated with
protoc/protoc-gen-go and grpc_tools, never edited by hand.

```python
from pymilvus import MilvusClient
client = MilvusClient(uri="http://localhost:29561", token="root:Milvus")
outcomes = client.multi_search([
    {"id": "papers", "db_name": "literature", "collection_name": "papers",
     "data": [paper_vector], "anns_field": "embedding", "limit": 10},
    {"id": "guidelines", "db_name": "clinical", "collection_name": "guidelines",
     "data": [guideline_vector], "anns_field": "vector", "limit": 3},
], max_concurrent_searches=8, timeout=5)
# Each outcome has id, result, error, and the original Milvus status.
```

## Build and run

The prototype rebuilds Go and uses unchanged native libraries from the exact
v3.0.5 image digest recorded in `artifacts/multisearch-v2/build-manifest.json`.
Use the build script with Docker, Go toolchain auto-download, curl and tar available:

```bash
bash examples/multisearch/build.sh
bash examples/multisearch/run.sh stock
bash examples/multisearch/run.sh multi
.venv/bin/python examples/multisearch/seed.py --port 29560
.venv/bin/python examples/multisearch/seed.py --port 29561
.venv/bin/python examples/multisearch/verify.py
.venv/bin/pytest -q examples/multisearch/test_sdk.py
.venv/bin/python examples/multisearch/run_benchmarks.py
```

Fixture scripts create only owned `ms_v2_*` databases/collections on the local
ports. Data is ephemeral: a container exit loses its tmpfs. Wait for both health
endpoints (19096, 19097) before seeding. The scripts assume CPU IDs 32-39 and 64-71
exist; adapt all CPU sets consistently on smaller machines. Do not point the seed
script at a shared production endpoint. Do not pause the servers between runs.
The benchmark lasts about 15 minutes and prints each phase to stdout.

Focused tests require native libraries, an etcd endpoint and the required tags:

```bash
source scripts/setenv.sh
export LOCALSTORAGE_PATH=/tmp/multisearch-v305-tests
export ETCD_AUTH_ENABLED=false ETCD_ENDPOINTS=localhost:22379
go test -p 16 -race -tags dynamic,test -gcflags='all=-N -l' -count=1 ./internal/proxy -run TestMultiSearch
go test -p 16 -tags dynamic,test -gcflags='all=-N -l' -count=1 ./internal/distributed/proxy -run 'Test_NewServer|Test.*Stream'
go test -race -tags dynamic,test -gcflags='all=-N -l' -count=1 ./pkg/metrics -run TestMultiSearchStatsIsolation
```

Kubernetes manifest generation is in `make_k8s_manifest.py`; it creates a separate
`multisearch-v305` namespace with two Proxy replicas and distinct node components.
Import `milvus-multisearch:v305` into the development cluster before applying it.

Run the separate CPU-contention test after the main suite (never concurrently):

```bash
.venv/bin/python examples/multisearch/run_saturation.py
.venv/bin/python examples/multisearch/summarize.py
```

This lowers both instances to two CPUs, leaves GOMAXPROCS=4, and restores four CPUs
on exit. The six runs use 45 seconds of measurement each, with reversed variant
order on the second repetition. CPU percentages use the two-core quota.

The Kubernetes check uses an existing owned Minikube Docker node, imports the
same image, deploys a separate namespace, seeds 2,048 vectors, and tests both
Proxy replicas directly through local port forwarding:

```bash
export KUBECONFIG=/path/to/development/kubeconfig
export MULTISEARCH_KUBE_CONTEXT=multisearch-prototype
export MULTISEARCH_MINIKUBE_CONTAINER=multisearch-prototype
bash examples/multisearch/run_kubernetes.sh
```

The script pins that owned development node to CPUs 96-107. Adjust this on a
smaller host. It does not overwrite the standalone performance fixture. Test
credentials and ephemeral storage in these manifests are for local development.

`report.py` renders the English HTML report with embedded syntax highlighting after
all result artifacts are present. Install report dependencies with
`python -m pip install -r examples/multisearch/requirements-report.txt`.
The PDF is committed beside the HTML for direct GitHub preview.
The committed summaries record the measurements; raw latency samples, resource
samples and logs are supplied in the review bundle and can be regenerated by
the scripts. Full repository CI and a fresh-machine build-script replay remain
outside this prototype's verification scope.

## Resident Search and injected MultiSearch traffic

The mixed-load experiment holds 128 individual Search workers active throughout
warmup, baseline, injection and recovery. It adds either ordinary Search groups
or MultiSearch batches with identical logical searches; both injected variants
have a group-completion barrier. Added traffic is 160 or 480 logical searches/s,
or a continuously backlogged set of 16 groups of eight. Two no-injection control
runs check time drift. Both client classes use separate connections and the same
identity, so this is API contention rather than per-user isolation testing.

```bash
go build -o bin/multisearch-mixed-load ./examples/multisearch/mixed_load
.venv/bin/python examples/multisearch/run_mixed.py
.venv/bin/python examples/multisearch/summarize_mixed.py
python examples/multisearch/report_mixed.py
```

The runner temporarily uses two CPUs for the owned modified server and restores
both owned containers' original CPU settings on exit. It requires the existing
populated fixture and active test containers. Start with no raw `r*-*.json` files
in `artifacts/multisearch-mixed`; archive earlier raw outputs before another run.
The run takes about 16 minutes. A three-second prefix is excluded from each
phase's steady-state statistics; one-second completion counts still capture the
transitions. Tail percentiles do not characterize the excluded transition prefix.
Do not run checksums, profiling, data seeding or other extra work inside the tested
container while measurements are active.

See `artifacts/multisearch-mixed/excluded-runs.json` for the contamination and
paired rerun audit of the recorded experiment. A clean replay produces 14 runs
without needing these replacement runs. Original RPC/SDK behavior and server code
are unchanged by this benchmark addition.

## Current implementation: Proxy-owned coordination

`Server.MultiSearch` now delegates to `Proxy.MultiSearch`. The Proxy owns shared
batch/child admission and the coordination loop. The transport installs the
existing Search entry chain once, ending directly at `Proxy.Search`. The
`MultiSearchExecutor` type and its Server field were removed. The ordinary Search
implementation, DQL scheduler and QueryNode code remain unchanged.

After preparing the pinned dependencies/native libraries with the build steps
above, the incremental build and separate local instance used for this refactor
can be reproduced with:

```bash
source scripts/setenv.sh
set -e
go build -p 16 -pgo=configs/pgo/default.pgo \
  -tags dynamic,sonic,with_jemalloc,bytedance_tango \
  -ldflags="-checklinkname=0 -r $RPATH -X github.com/milvus-io/milvus/cmd/milvus.MilvusVersion=3.0.5-multisearch-proxy -X github.com/milvus-io/milvus/cmd/milvus.GitCommit=da46e613b1-proxy-refactor" \
  -o bin/milvus-proxy-refactor ./cmd/main.go
sed 's@COPY bin/milvus @COPY bin/milvus-proxy-refactor @' \
  examples/multisearch/Dockerfile > .cache/Dockerfile.proxy-refactor
docker build -f .cache/Dockerfile.proxy-refactor -t milvus-multisearch:v305-proxy .
bash examples/multisearch/run.sh refactor
mkdir -p artifacts/multisearch-proxy-refactor
.venv/bin/python examples/multisearch/seed.py --port 29564 \
  --output artifacts/multisearch-proxy-refactor/fixture.json
.venv/bin/python examples/multisearch/verify.py --port 29564 \
  --fixture artifacts/multisearch-proxy-refactor/fixture.json \
  --output artifacts/multisearch-proxy-refactor/functional.json
.venv/bin/python examples/multisearch/run_refactor_bench.py
.venv/bin/python examples/multisearch/summarize_mixed.py \
  --input-dir artifacts/multisearch-proxy-refactor --server-binary-changed
python examples/multisearch/report_refactor.py
```

The build metadata above identifies the measured source parent; `manifest.json`
records the exact production-source hashes and binary hash. The report generator
also requires the recorded manifest, verification and Kubernetes result files.
The four formal runs repeat the 480/s mixed-load pair twice with opposing order.
The runner checks a short smoke run and restores all three owned containers' CPU
settings. It requires the original stock and prototype containers to exist, and
refuses to overwrite existing formal raw results. Do not execute extra commands
inside the measured container during the test.

For Kubernetes, import `milvus-multisearch:v305-proxy` into the existing owned
Minikube node, then set deployment `proxy`'s container `proxy` to that image in
namespace `multisearch-v305`. Wait for rollout and run `verify.py` through each
replica, using `artifacts/multisearch-v2/kubernetes-fixture.json`. This refactor
upgrades only the two Proxy replicas; the other existing test components remain
on their preceding v3.0.5 prototype image. Both Proxy binary hashes are recorded.

The current [report PDF](../../artifacts/multisearch-proxy-refactor/report.pdf)
contains highlighted code, fresh validation and performance results. Older
reports are historical; their generators read their measured source revision.
