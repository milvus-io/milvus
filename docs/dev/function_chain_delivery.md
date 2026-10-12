# FunctionChain transformation dependency delivery

The transformation feature spans milvus-proto, Milvus and PyMilvus. A Milvus
checkout using the old protocol cannot compile the new collection binding and
mutation result fields. A PyMilvus checkout with an old protocol submodule
cannot reproduce its checked-in generated products.

The draft implementation uses this single protocol snapshot:

| Consumer | Pin |
|---|---|
| milvus-proto commit | `7cf29167c3d71b21be220c4f2af09db51f291d76` |
| Milvus root, `pkg/` and `client/` modules | `v3.0.0-20261011152020-7cf29167c3d7` |
| PyMilvus `pymilvus/grpc_gen/milvus-proto` submodule | `7cf29167c3d71b21be220c4f2af09db51f291d76` |

The protocol dependency is published in
[milvus-proto PR #676](https://github.com/milvus-io/milvus-proto/pull/676).
The matching SDK is published in
[PyMilvus PR #3851](https://github.com/milvus-io/pymilvus/pull/3851). A fresh
module cache with the normal Go proxy and `sum.golang.org` successfully resolved
the pinned version and matched all three checked-in module checksums. The
protocol commit is reachable through the published upstream feature branch;
local source/module availability alone is not remote-resolution evidence.
If the protocol commit changes during review or merge, resolve its new Go
version, update all three module requirements/checksums, update the SDK
submodule, and regenerate the API products again.

Local file-proxy and checksum overrides used during initial development are
not deployment prerequisites and must not substitute for remote resolution.

Verify the server against module requirements, with the normal native build
environment configured:

```bash
GOWORK=off GOFLAGS=-mod=readonly go list -m github.com/milvus-io/milvus-proto/go-api/v3
GOWORK=off GOFLAGS=-mod=readonly go build -tags dynamic -o bin/milvus ./cmd
GOWORK=off GOFLAGS=-mod=readonly go test -race -tags dynamic,test \
  -gcflags='all=-N -l' -count=1 \
  ./internal/util/function/chain/... ./internal/proxy/dml/... ./internal/proxy/dql/...
```

In the PyMilvus checkout, initialize the pinned submodule and run
`make check_proto_product` and the SDK unit tests. The generated products must
remain unchanged after regeneration. The Python worker runtime has its own
protocol and wheel; run the Milvus `check-pyudf-proto-product` and
`build-pyudf-runtime-wheel` targets for it.

The following versions describe historical protocol milestones; coordinated
deployment of this draft requires worker runtime **0.6.0**. Host request scope
uses execution protocol **6**, retaining complete runtime schemas, query origins,
and TABLE/reduction semantics with exact successful-response acknowledgement.
See the [minimal call-context design](../design-docs/design_docs/20261012-function-chain-call-context.md).

The query-context update introduced worker runtime 0.2 and execution protocol 2.
POST TABLE and reduction require runtime 0.3 and execution protocol 3, including
the new public table-result protocol pinned above. The worker retains legacy
execution modes; a TABLE request requires an exact version-3 acknowledgment.
Deploy the current 0.6.0 wheel with the matching server, and update POST UDFs to implement
`transform_batch`. TABLE changes both the private worker protocol and the public
result/options protocol. New clients reject a worker
success response that does not acknowledge the new execution contract.

The [design verification matrix](../design-docs/design_docs/20261010-function-chain-transform-stages.md)
records feature coverage and the remaining failures in the full Go gate.
Focused checks do not replace that gate. Before enabling ingestion, upgrade
all write Proxies and coordinators and deploy the matching worker and SDK.

See the [usage guide](../user-guides/function-chain-transform.md) for wheel
registration, INSERT/PRE/POST examples and partial INSERT result handling.
