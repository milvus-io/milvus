# Analyzer execution

## Entry and routing

Proxy calls `streaming.WAL().AnalyzerClient().RunAnalyzer`. WALAccesser exposes
its existing HandlerClient's `AnalyzerClient()` domain interface. The analyzer
client owns no discovery, connection pool or independent shutdown lifecycle.
QueryCoord, QueryNode and ManagerClient are not involved in this call path.

The request selects one of two sources:

- Inline `analyzer_params`: call the existing handler service without a target
  server ID; the existing picker round-robins its ready SN connections. There is
  no collection or WAL requirement on the selected server.
- Collection field: Proxy resolves collection ID, field ID
  and vchannels. Choose any collection vchannel and use its PChannel
  assignment to call the current WAL owner, carrying the assignment term.

Field analyzer access requires neither load configuration, replicas or loaded
Segments. Query resource-group scope does not restrict this WAL/schema operation.
CreateCollection registers a WAL function-runner key, WAL recovery rebuilds it,
AlterCollection updates it, and DropCollection/WAL teardown releases it.
ReleaseCollection releases query resources, not this WAL key.

## Execution and lifetime

Inline execution constructs a temporary analyzer and destroys it and each token
stream on exit. Field execution validates assignment ownership and holds the
WAL's Working lifetime. FunctionRunnerManager resolves collection ID +
WAL-vchannel key + field ID using that key's current schema. No Proxy schema
version is sent or compared. Managed BM25/MinHash analyzers are pinned
through BatchAnalyze; ordinary analyzer-enabled fields use a copied field schema
and temporary analyzer. GC before pin returns a transient error; after pin it
waits for execution. WAL shutdown waits for admitted field analyzer calls through
the existing WAL lifetime. Inline analyzers own their resources, and the existing
gRPC graceful shutdown waits for their requests to finish; the handler service
needs no additional shutdown lifecycle. Cancellation is checked before native
execution; it does not forcibly interrupt a native tokenization call already in
progress.

## Errors and retries

`StreamingNodeHandlerService.RunAnalyzer` separates two error domains:

- Streaming failures (discovery, assignment ownership, WAL shutdown and transport)
  use native gRPC errors and the existing streaming interceptors. AnalyzerClient
  allows at most three attempts for retryable failures; field attempts copy the
  request and report only assignment errors to the balancer. Inline attempts use
  the ready-connection picker.
- Analyzer failures (parameters, schema admission, runner availability and
  execution) use Milvus Status in the response. HandlerClient forwards these
  responses without retrying or converting them to streaming errors. Proxy
  preserves the Status, including its original code and retriability.

AnalyzerClient owns the single bounded retry loop for streaming failures. Proxy
resolves metadata once and forwards the result; it does not refresh metadata or
retry analyzer responses. The RunAnalyzer gRPC method has no configured retry
policy, so the connection's service-wide retry policy does not multiply the
AnalyzerClient budget. Other handler RPCs retain their existing policies.

Collection identity is pinned before execution: Proxy resolves field/channel
metadata by the original collection ID, and all AnalyzerClient attempts retain
that ID. A same-name drop/recreate cannot redirect a request to the new collection.
There is no historical schema lookup or schema-version admission check.

## Compatibility

Existing public SDK and legacy internal RPC signatures remain available. The new
Proxy path needs the new SN RPC; an older SN returns Unimplemented without a QN
fallback, so upgrade SNs before enabling these callers. Token ordering,
details/hash and analyzer-name normalization retain existing behavior. Field
requests additionally work before load and after release.

## Key Packages

- `internal/distributed/streaming/{streaming.go,wal.go}` — public streaming entry.
- `internal/streamingnode/client/handler/analyzer_client.go` — domain client and routing.
- `internal/streamingnode/server/service/analyzer.go` — native RPC dispatch/errors.
- `internal/streamingnode/server/wal/adaptor/analyzer.go` — WAL field execution.
- `internal/util/function/manager.go` — schema selection and runner lifetime.
- `internal/proxy/{impl.go,run_analyzer_streaming.go}` — SDK adaptation and collection identity.
- `pkg/proto/streaming.proto` — additive request/response and service method.
