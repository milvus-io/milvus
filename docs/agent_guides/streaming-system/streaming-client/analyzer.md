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
- Collection field: Proxy resolves collection ID, field ID, current schema
  version and vchannels. Choose any collection vchannel and use its PChannel
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
WAL-vchannel key + field ID, checking the requested schema version under the
same lock selecting its schema/runner. Managed BM25/MinHash analyzers are pinned
through BatchAnalyze; ordinary analyzer-enabled fields use a copied field schema
and temporary analyzer. GC before pin returns a transient error; after pin it
waits for execution. WAL shutdown waits for admitted field analyzer calls through
the existing WAL lifetime. Inline analyzers own their resources, and the existing
gRPC graceful shutdown waits for their requests to finish; the handler service
needs no additional shutdown lifecycle. Cancellation is checked before native
execution; it does not forcibly interrupt a native tokenization call already in
progress.

## Errors and retries

`StreamingNodeHandlerService.RunAnalyzer` returns results only, with no response
status or error code. Failures use native StreamingError in gRPC status details
and the existing streaming interceptors. Public SDK responses keep their existing
Status format through an adapter at Proxy.

Invalid analyzer input uses native INVAILD_ARGUMENT. Schema mismatch uses native
SCHEMA_VERSION_MISMATCH; Proxy refreshes metadata and re-resolves the field name
at most three times. A collection ID change during retry fails rather than
executing against a recreated collection. Missing runner resources use transient
native errors. There is no historical schema lookup.

AnalyzerClient allows at most three application attempts for transient failures.
Field attempts copy the request and report only assignment errors to the balancer.
Inline attempts use the ready-connection picker. Input errors, cancellation and
Unimplemented are not application-retried. These budgets also respect the caller
deadline; existing gRPC transport retry configuration is unchanged.

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
- `internal/proxy/{impl.go,run_analyzer_streaming.go}` — SDK adaptation and metadata refresh.
- `pkg/proto/streaming.proto` — additive request/response and service method.
