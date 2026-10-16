# Analyzer execution

## Entry and routing

Proxy calls `streaming.WAL().AnalyzerClient().RunAnalyzer`. WALAccesser exposes
an analyzer domain interface implemented directly by the existing HandlerClient's
`RunAnalyzer` method. No analyzer-specific client wrapper, discovery, connection
pool or shutdown lifecycle is added.
Proxy assembles and forwards requests directly, without a scheduler task or TSO
allocation. QueryCoord, QueryNode and ManagerClient are not involved in this call
path. Analyzer calls do not use the DQL task queue or its concurrency limit.

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
Shared runner initialization preserves tracing values but does not inherit the
request that first triggered it: cancelling one wait does not fail other waiters.
Resource preparation retains its own snapshot-fetch and per-file download limits.
Default analyzers on schemas with no file-resource IDs bypass preparation, so an
unrelated broken resource does not prevent their runner initialization. Explicit
analyzer or multi-analyzer configuration still prepares conservatively: older
schemas and MinHash fields without enable_analyzer may lack resource IDs.
Resource-dependent initialization continues to require the full snapshot; a
failed download is not silently replaced with stale inventory.

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
  use native gRPC errors and the existing streaming interceptors. HandlerClient
  reuses `createHandlerAfterStreamingNodeReady` for assignment discovery, error
  reporting and backoff until the caller context ends. Field attempts copy the
  request with the latest assignment. Inline attempts have no PChannel and use
  the ready-connection picker. Unsupported RPCs return Unimplemented immediately.
- Analyzer failures (file-resource preparation, parameters, schema admission,
  runner availability and execution) use Milvus Status in the response.
  HandlerClient forwards these responses without retrying or converting them to
  streaming errors. Proxy preserves the Status, including its original code and
  retriability.

Proxy resolves metadata once and forwards the result; it does not refresh
metadata or retry analyzer responses. RunAnalyzer uses the same gRPC service
configuration and streaming recovery helper as the other handler operations.
There is no analyzer-specific retry loop or method configuration.
RunAnalyzer and ValidateAnalyzer propagate the caller's context through discovery,
RPCs and readiness retries, without adding an analyzer-specific timeout. The caller
controls cancellation and the total deadline; without a caller deadline, readiness
retries have no additional time limit. Cancellation does not interrupt native
execution already in progress.
HandlerClient does not interpret business Status or retry business failures.

Collection identity is pinned before execution: Proxy resolves field/channel
metadata by the original collection ID, and all streaming recovery attempts retain
that ID. A same-name drop/recreate cannot redirect a request to the new collection.
There is no historical schema lookup or schema-version admission check.

## Compatibility

Existing public SDK and legacy internal RPC signatures remain available. The new
Proxy path needs the new SN RPC; an older SN returns Unimplemented without a QN
fallback, so upgrade SNs before enabling these callers. Token ordering,
details/hash and analyzer-name normalization retain existing behavior. Field
requests additionally work before load and after release.

## Configuration validation and file resources

RootCoord validates analyzer configuration through
`streaming.WAL().AnalyzerClient().ValidateAnalyzer`, implemented by HandlerClient
and `StreamingNodeHandlerService.ValidateAnalyzer`. The ready-connection picker
selects any StreamingNode; validation requires no collection, WAL assignment,
QueryView, replica, or loaded QueryNode. The legacy Coordinator `ValidateAnalyzer`
RPC remains as a forwarding entry for existing callers. There is no
`StreamingNodeManagerService.ValidateRuntime` RPC or ManagerClient validation API.

Before native validation, the selected StreamingNode calls `fileresource.Prepare`
to pull Coordinator's current resource snapshot and synchronize its local files
and native resource table. In sync mode, validation copies the applied name-to-ID map under the manager
lock, together with the filenames derived from the registered object paths,
and passes them as explicit native extra info. Only this validation snapshot
classifies an unknown resource name or a mismatched remote `file_name` as bad
input. The filename must equal the basename of the registered path; validation
does not infer caller blame from local file existence. Runtime lookups and non-sync
modes retain system-error semantics. A concurrent sync cannot change the names
seen by an in-flight validation; if it removes a referenced file, native file I/O
still reports a system error. Validation returns the deduplicated resource IDs.
RootCoord reserves these references under its existing metadata lock before DDL
publication; a concurrent resource removal must fail reservation rather than
publish a schema with missing resources. RootCoord does not wait for all QNs to
synchronize their inventories. Existing StreamingCoord assignment admission and
legacy file-resource push distribution remain unchanged.

The validation response carries a business Status and, on success, resource IDs.
Native validation failures return in Status, preserving their error code, input
classification and retriability. HandlerClient checks this Status after the
readiness loop and does not retry business failures, even when Status marks an
I/O error retriable. A mismatched remote filename returns a parameter error.
Rust classifies a direct I/O error with `ErrorKind::NotFound` as the generic
`IoNotFound` FFI discriminant, independently of the caller. The C++ analyzer
validation boundary maps this to the existing `PathNotExist` code.
This is a non-retriable system error, so a missing local path
or a missing downloaded resource file does not trigger outer DDL retries either.
Filesystem absence alone does not prove invalid input: concurrent resource
removal or worker-local state can cause the same failure. The caller must
resubmit validation after that state recovers. Permission errors and other I/O
failures retain `FileReadFailed`; the outer Proxy-to-Coordinator client may retry
them within its existing attempt limit and the caller context. Runtime analyzer
creation and index operations keep mapping both `Io` and `IoNotFound` to
`FileReadFailed`. Only analyzer validation, after resource preparation, applies
the terminal missing-path policy.
Deterministically invalid analyzer options remain input errors at their Rust
origin; HandlerClient maps these to the public DDL parameter-error contract.
Discovery and resource preparation still use the existing streaming readiness
retry loop, and Unimplemented returns immediately. The caller context controls
these retries, including cancellation of DDL validation. Known permanent storage
failures during preparation (such as a missing object or denied access) terminate the streaming retry loop as
system errors; unclassified network/gRPC errors and fetch timeouts remain eligible
for retry. With no available StreamingNode, discovery waits until a node becomes
available or the caller context ends. No independent retry loop or shutdown
lifecycle is introduced.

The internal Coordinator `GetFileResourceSnapshot` RPC uses a dedicated
`GetFileResourceSnapshotRequest`. Its `known_version` is the last snapshot
successfully applied locally, not the last version fetched. Zero denotes the
initial empty inventory. Coordinator reads the inventory and version together
under the metadata lock. On success:

- An equal response version means unchanged; the resource list is omitted.
- A different version carries the complete inventory, never a delta. A newer
  version with an empty list means all resources were removed and must be applied.
- Older responses are ignored. `Sync` rechecks the version under its lock, so
  concurrent pulls and legacy pushes cannot roll back a newer local inventory.

Downloads and native resource-table activation must succeed before the local
version advances. A failed preparation therefore retries with the last applied
version. Coordinator compares versions under the metadata lock before enumerating
resources, so unchanged responses avoid both list allocation and transfer. Pulls
still perform an RPC; there is no TTL that can hide a newly committed version.

New StreamingNodes require a Coordinator that supports this RPC, and the new DDL
validation path requires StreamingNodes that support `ValidateAnalyzer`. Existing RPC
signatures remain available; missing new RPCs surface as Unimplemented.

## Key Packages

- `internal/distributed/streaming/{streaming.go,wal.go}` — public streaming entry.
- `internal/streamingnode/client/handler/analyzer_client.go` — handler RPC and routing.
- `internal/streamingnode/server/service/analyzer.go` — native RPC dispatch/errors.
- `internal/streamingnode/server/wal/adaptor/analyzer.go` — WAL field execution.
- `internal/util/function/manager.go` — schema selection and runner lifetime.
- `internal/proxy/impl.go` — SDK adaptation and collection identity.
- `pkg/proto/streaming.proto` — additive request/response and service method.
