# MultiSearch: independent searches in one RPC

Related issue: [milvus-io/milvus#41791](https://github.com/milvus-io/milvus/issues/41791).

Status: implemented prototype; see `artifacts/multisearch-prototype-report.html`
and the machine-readable evidence under `artifacts/multisearch/` for validation.

## Problem and scope

A retrieval stage may need dozens of searches across collections with different
schemas, dimensions, metrics, filters and topK values. Client fan-out exposes the
application to many external RPCs and requires client-side concurrency management.
MultiSearch moves that orchestration into Proxy. It does not reduce logical query
work, combine rankings, or establish a cross-collection snapshot.

The prototype registers
`/milvus.proto.multisearch.MultiSearchService/MultiSearch` on the existing public
Proxy gRPC port. Its protocol lives in `pkg/proto/multisearch.proto`, importing
the existing public `SearchRequest` and `SearchResults` messages. A separate
extension service avoids an unpublished milvus-proto module replacement during
the feasibility exercise. An upstream public API and official SDK addition remain
release work. `examples/multisearch/client.py` supplies a working Python adapter
whose `multi_search()` method makes exactly one gRPC call and performs no retries.

## Contract

An envelope specifies one database and a list of `{id, search}` items. Item IDs
must be unique and between 1 and 128 bytes. Each item contains a complete Search
request. An omitted item database inherits the envelope database; an omitted
envelope database uses connection metadata, then the ordinary default. Conflicting
item databases are rejected before execution. Repeated collections are allowed.

For a successful envelope, outcomes have the same order and cardinality as the
inputs. Each outcome preserves either the ordinary `SearchResults`, including
its full Milvus Status, or a gRPC error code and message. Envelope success does
not imply item success. No cross-item field or metric compatibility is checked,
because each Search performs its own schema validation and normal shard reduce.

Malformed envelopes, excessive request bytes and admission rejection return an
envelope Status with no outcomes. Overall cancellation/deadline produces a gRPC
error; partially completed results are not recoverable through this unary API.
Exceeding the aggregate response budget cancels remaining work and rejects the
whole envelope with ParameterTooLarge. No result-resumption or exactly-once
execution guarantee is provided. Retrying a lost response can repeat compute and
observe newer data.

## Execution and entry checks

`internal/proxy/multisearch.go` contains a bounded dispatcher accepting a Search
callback. Its workers clone item requests, acquire a shared MultiSearch execution
slot, invoke the callback, and store each outcome at its input index. Search
failures do not cancel unrelated items. The parent deadline and cancellation are
inherited; a server timeout also bounds requests without a client deadline.

`internal/distributed/proxy/multisearch.go` provides the callback. It re-enters the
same interceptor chain used by ordinary Search, with the ordinary Search method
name and request type, then calls `Server.Search` → `Proxy.Search` → `dqQueue`.
This preserves per-item database normalization, RBAC, rate limiting, hooks, access
logging and existing standalone legacy forwarding. It does not send loopback RPCs
or bypass Proxy to contact QueryNode directly.

The envelope uses explicit authentication, access logging, request statistics and
connection handling. It does not rely on unknown-request fallbacks for collection
authorization or quota checks: those are performed on every item. Hooks also run
per item, without an additional unknown-envelope hook invocation.

Search mutates request-scoped metadata. Each worker receives a cloned request;
`metrics.WithIndependentRPCStats` creates independent mutable statistics; the
access-log interceptor creates an independent access-info object. Identity,
deadline, cancellation and trace ancestry still flow from the parent context.

## Bounds

Startup configuration under `proxy.multiSearch`:

| Setting | Default |
|---|---:|
| maxItems | 128 |
| parallelism | 16 |
| globalParallelism | 64 |
| maxBatches | 8 |
| maxRequestBytes | 4 MiB |
| maxResponseBytes | 16 MiB |
| timeoutSeconds | 30 |

The global execution limit is per Proxy and applies to MultiSearch items;
ordinary Search remains subject to its existing quotas and scheduler. Admission
is fail-fast when eight envelopes are already active. The response limit bounds
retained results, not all transient native-search allocations. Other ordinary
Search limits continue to apply. Invalid nonpositive concurrency settings are
clamped to one by the service constructor; byte limits have a 1 KiB floor.

## Verification and claim boundaries

Focused unit tests cover validation-before-dispatch, result/status preservation,
database inheritance, shared-request isolation, concurrent admission, cancellation,
response overflow and unexpected nil Search results. Race tests cover the
dispatcher and mutable statistics isolation. The existing distributed Proxy
service regression suite is run with a dedicated local etcd dependency.

The client verification harness populates mixed-schema collections and compares
50-item MultiSearch results with ordinary Search by IDs, topks, scalar fields and
scores. It exercises missing collections, invalid dimensions, no matches, RBAC,
authentication, deadlines, quota isolation and envelope bounds. Performance is
measured only after functional checks, against sequential and bounded parallel
ordinary Search with identical logical work and no retries. Raw samples, binary
checksums, image identity and deployment configuration accompany the report.

The prototype does not establish a correct retry classification for every existing
upstream error. It preserves observed Search statuses without reclassifying them.
It does not add generic automatic retries or prove recovery from GCP PSC faults,
object-storage throttling, corruption, OOM, or lost responses. These are separate
failure-injection and deployment-validation tasks.

## Production follow-up

- Finalize public protobuf ownership and official SDK ergonomics, compatibility
  negotiation, TLS support in the adapter, and full gRPC error detail preservation.
- Validate managed Cloud gateway method allowlists and private auth/billing hooks.
- Add envelope transport-byte accounting and dedicated batch observability; item
  search metrics and hooks are reused, but no new network-byte accounting is
  emitted for the in-process subrequests.
- Establish tenant fairness, capacity defaults, response-memory accounting and
  cancellation cleanup under sustained mixed Search/MultiSearch workloads.
- Benchmark representative vectors, indexes, filters, output sizes and remote
  network paths. A local FLAT microbenchmark is not a customer capacity estimate.
- Validate rolling upgrades and mixed-version deployments. No QueryNode, Segcore,
  index implementation or WAL behavior was changed by this prototype.
