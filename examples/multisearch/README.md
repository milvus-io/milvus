# MultiSearch prototype

One public gRPC call carries independent ordinary `SearchRequest` messages. Proxy
runs every item through the ordinary Search entry checks and execution path.
Collections may have different fields, vector dimensions, metrics and topK.
Results are independent; no cross-collection reduce or atomic snapshot is added.

Design: [../../docs/design-docs/multisearch.md](../../docs/design-docs/multisearch.md).
Engineering report: [../../artifacts/multisearch-prototype-report.html](../../artifacts/multisearch-prototype-report.html).

## Build and run

From the repository root, with Go 1.26, Docker, a C/C++ compiler, pkg-config and
Python 3 available:

```bash
bash examples/multisearch/build_prototype.sh
bash examples/multisearch/run_prototype.sh
python3 -m venv .venv-multisearch
.venv-multisearch/bin/pip install -r examples/multisearch/requirements.txt
.venv-multisearch/bin/python examples/multisearch/verify.py --functional-only
.venv-multisearch/bin/python examples/multisearch/verify_limits.py
.venv-multisearch/bin/python examples/multisearch/benchmark.py
```

The Go server is rebuilt. The native libraries are reused unchanged from the
pinned Milvus runtime image; this is not a fresh C++/Rust build. The build helper
packages the component commands used for the prototype; a fresh-machine run of
the combined helper has not been validated. Actual build provenance and checksums
are in `artifacts/multisearch/build-manifest.json`.

The standalone container binds only localhost, using `localhost:29550` for gRPC
and `localhost:19095/healthz` for health. Test credentials are `root:Milvus`.
The scripts populate dedicated `ms_prototype_*` collections. **Do not point these
fixtures at an existing production database**: fixture population replaces its
own named collections and creates a test role/user. `--reuse` uses existing
deterministic fixture rows. Run the quota test separately from performance: it
temporarily changes collection 0's search quota and restores it in `finally`.

`client.py` is a working prototype adapter, not a released PyMilvus method.
`MultiSearchClient.multi_search()` makes one unary gRPC call. It does not retry.
It currently uses a local insecure channel; TLS and SDK ergonomics are follow-up.

## Kubernetes functional verification

The isolated Minikube deployment uses two Proxy replicas and separate coordinator,
query, data and streaming processes, backed by etcd and MinIO. Its `emptyDir`
storage is ephemeral. It tests distributed routing, not production HA.

```bash
mkdir -p .cache/multisearch
export KUBECONFIG="$PWD/.cache/multisearch/kubeconfig"
minikube start -p multisearch-prototype --driver=docker \
  --container-runtime=containerd --cpus=12 --memory=24576 \
  --kubernetes-version=v1.30.0 --embed-certs --keep-context
set -o pipefail
docker save milvus-multisearch:prototype quay.io/coreos/etcd:v3.5.25 \
  minio/minio:RELEASE.2024-05-28T17-19-04Z | \
  docker exec -i multisearch-prototype ctr -n k8s.io images import -
python3 examples/multisearch/make_k8s_manifest.py
kubectl --context multisearch-prototype apply -f examples/multisearch/kubernetes.json
kubectl --context multisearch-prototype -n multisearch-prototype \
  wait --for=condition=Available deployment --all --timeout=600s
.venv-multisearch/bin/python examples/multisearch/verify.py \
  --target "$(minikube -p multisearch-prototype ip):30530" \
  --functional-only --output artifacts/multisearch/k8s-results.json
.venv-multisearch/bin/python examples/multisearch/verify_proxy_replicas.py
```

If kube-proxy fails with `too many open files`, inspect the host inotify limit.
This test host needed `fs.inotify.max_user_instances` raised from 128 to 1024
using `docker exec multisearch-prototype sysctl -w fs.inotify.max_user_instances=1024`,
followed by restarting the affected kube-system pods. This setting is host-wide;
restore the previous value after stopping the cluster if no longer needed.

Use the isolated kubeconfig and explicit context; the commands do not require
changing another cluster's context. The local test NodePort uses test credentials
and is intended only for this development machine.

## Focused Go checks

With the native dependencies prepared, start a disposable etcd at `localhost:22379`
(or use `docker start multisearch-unit-etcd` if the test container already exists):

```bash
docker run -d --name multisearch-unit-etcd -p 127.0.0.1:22379:2379 \
  quay.io/coreos/etcd:v3.5.25 /usr/local/bin/etcd \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://localhost:2379
source scripts/setenv.sh
export LOCALSTORAGE_PATH=/tmp/multisearch-unit-data
export ETCD_AUTH_ENABLED=false ETCD_ENDPOINTS=localhost:22379
go test -p 16 -race -tags dynamic,test -gcflags='all=-N -l' \
  -count=1 ./internal/proxy -run TestMultiSearch
go test -p 16 -tags dynamic,test -gcflags='all=-N -l' \
  -count=1 ./internal/distributed/proxy -run Test_NewServer
cd pkg
go test -race -tags dynamic,test -gcflags='all=-N -l' \
  -count=1 ./metrics -run TestMultiSearchStatsIsolation
go test -tags dynamic,test -gcflags='all=-N -l' -count=1 \
  ./util/merr ./util/paramtable \
  -run 'Test.*(Classification|Segcore)|TestComponentParam/test_proxyConfig'
```

The broader existing config test has a hardcoded default storage path and fails
when `LOCALSTORAGE_PATH` is overridden. The focused Proxy config test passes.
This prototype has not run the full repository test suite.

## Regeneration and cleanup

Go protobuf output is generated with `make generated-proto-without-cpp` (requires
the repository's protobuf toolchain). Python output uses the upstream public
proto include directory and this local extension:

```bash
.venv-multisearch/bin/python -m grpc_tools.protoc \
  -I pkg/proto -I cmake_build/thirdparty/milvus-proto/proto \
  --python_out=examples/multisearch pkg/proto/multisearch.proto
python3 examples/multisearch/render_report.py
```

When finished reviewing, stop the owned standalone container with
`docker stop milvus-multisearch-prototype`, and stop the local Kubernetes cluster
with `minikube -p multisearch-prototype stop`. Delete its profile only when its
ephemeral test data is no longer needed. No unrelated services are required.
