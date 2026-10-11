# FunctionChain transformation dependency delivery

The transformation feature spans milvus-proto, Milvus and PyMilvus. A Milvus
checkout using the old protocol cannot compile the new collection binding and
mutation result fields. A PyMilvus checkout with an old protocol submodule
cannot reproduce its checked-in generated products.

The current local implementation uses this single protocol snapshot:

| Consumer | Pin |
|---|---|
| milvus-proto commit | `7cf29167c3d71b21be220c4f2af09db51f291d76` |
| Milvus root, `pkg/` and `client/` modules | `v3.0.0-20261011152020-7cf29167c3d7` |
| PyMilvus `pymilvus/grpc_gen/milvus-proto` submodule | `7cf29167c3d71b21be220c4f2af09db51f291d76` |

This revision is a local commit. Publish the protocol dependency before
running normal remote CI for the dependent changes. Local source/module
availability does not establish that GitHub or the public Go proxy has it.
If the protocol commit changes during review or merge, resolve its new Go
version, update all three module requirements/checksums, update the SDK
submodule, and regenerate the API products again.

For local review before publication, the accompanying delivery directory
contains a Go file proxy for this protocol version. Use it as the first entry
in a command-local `GOPROXY` and set command-local `GONOSUMDB` for
`github.com/milvus-io/milvus-proto/go-api/v3`. Its README gives the exact
commands. These overrides serve the unpublished local dependency only.
After publication, verify dependency resolution with the normal proxy and
checksum database configuration too.

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

The query-context update requires worker runtime 0.2 and execution protocol 2.
POST TABLE and reduction require runtime 0.3 and execution protocol 3, including
the new public table-result protocol pinned above. The worker retains legacy
execution modes; a TABLE request requires an exact version-3 acknowledgment.
Deploy that wheel with the matching server, and update POST UDFs to implement
`transform_batch`. TABLE changes both the private worker protocol and the public
result/options protocol. New clients reject a worker
success response that does not acknowledge the new execution contract.

The [design verification matrix](../design-docs/design_docs/20261010-function-chain-transform-stages.md)
records feature coverage and the remaining failures in the full Go gate.
Focused checks do not replace that gate. Before enabling ingestion, upgrade
all write Proxies and coordinators and deploy the matching worker and SDK.

See the [usage guide](../user-guides/function-chain-transform.md) for wheel
registration, INSERT/PRE/POST examples and partial INSERT result handling.
