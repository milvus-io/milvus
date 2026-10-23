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
Resource preparation preserves tracing values without inheriting one RPC's
cancellation or deadline: cancelling one wait does not fail other waiters.
The last departing waiter cancels the shared task and its local retries. The
existing per-file download limit remains; Fetch adds no separate timeout.
Runtime consumers use the IDs already bound in their collection schema. Ready
ID entries are reused while referenced or within their idle lease; default and local-file analyzers skip
remote I/O. Unbound inline inputs fetch their referenced names in a batch,
then share loading under the returned IDs. Failed downloads never publish a
ready entry. Native code retains parameter and filename validation.

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

Schema persistence binds analyzer resources through
`CollectionSchema.file_resource_ids`. Runtime loading uses those IDs, never the
current meaning of a resource name. The internal `FetchFileResources` RPC returns
a batch of requested `FileResourceInfo` records by IDs or names; it replaces this PR's full-inventory snapshot RPC.
Metadata is cloned under Coord's metadata lock. Fetching a deleted ID reports a
system error and cannot silently select a replacement with the same name.

Create/Alter validation forwards `AnalyzerInfo` parameters to StreamingNode;
there is no resource-ID field in the validation request and no local name
resolution in Coord. StreamingNode extracts and deduplicates remote names and
fetches them in one RPC. Inline RunAnalyzer and runtime schemas without IDs use
the same path. Responses carry complete metadata including IDs, so downloading
requires no additional ID Fetch. Each call performs its own name query; a delayed
negative query cannot reject a later request after Add. There is no Resolve RPC.

`FetchFileResourcesRequest` supplies either `resource_ids` or `resource_names`.
Coord reads the requested batch under one metadata lock and clones the results
in request order. A failed batch has a business Status and no partial list.
This is a selected-resource query, not an inventory snapshot.

`fileresource.Acquire` delegates to the process-wide `AnalyzerResourceManager`,
which owns metadata access, per-ID download flights, resource references and idle
eviction. It pins ready ID entries and batches only the remaining ID
misses. Unbound name requests always fetch their current bindings. Metadata RPCs
and their retry loops belong to each requesting caller; they are not shared.
Thus overlapping requests [A,B] and [B,C] may query B metadata twice, but a
failure/cancellation of the first metadata request does not affect the second.

After metadata fetch, resources load concurrently through per-ID
`syncutil.FlightGroup` tasks. These flights share file download and readiness
publication, not the preceding batch RPC. Each caller waits with its own context;
only the last departing waiter cancels that ID's worker. A request that fails on
one resource releases its other waits without canceling another consumer's
shared download. A replacement caller can start work while an abandoned worker
unwinds; worker references prevent premature file cleanup. Download retries reuse
fetched metadata and stop on terminal storage errors or final-waiter cancellation.
The RPC batch/whole ID set is never a singleflight key.

There is no inventory version, generation or separate application flight.
Default/local-file configs do not fetch metadata. Established schemas use their
bound IDs and cannot substitute the current meaning of a same-name resource.
Metadata retries use the existing backoff and caller context. Permanent Status,
known terminal storage/input errors and Unimplemented end retry; native
construction and execution are not retried by this loading loop.

`Resources` holds references for the native consumer. Downloads use the isolated
`analyzer-demand/<resource_id>/<filename>` namespace, and the exact name-to-ID
mapping is passed as native extra info together with that namespace. The native
global resource table is not changed. A runner owns its references until it is
destroyed; temporary analyzers and validation release theirs after native work.
When the final consumer releases its reference, the manager records `lastActive`.
Successfully loaded files and metadata remain cached for
`common.fileResource.cacheTtl` (default `15m`, duration string,
configured at startup; `0s` disables idle retention). The manager schedules
eviction even if no further requests arrive. A worker still using the directory
delays removal, but does not restart the idle lease. Acquiring a cached resource
cancels its expiration; the next final release starts a new lease. Acquisitions
also check expiry, and stale timer callbacks cannot delete a new lease or entry.
This is an idle duration, not a periodic 15-minute scan.

Inline execution and validation can therefore reuse files across sequential
calls without retaining their temporary native analyzers. Name queries still
fetch current bindings from Coord: cached IDs do not hide name removal or
same-name recreation. Failed/incomplete downloads are removed immediately after
all references and workers leave. Failed native construction releases its
references but may leave a successfully downloaded file cached for the lease.
Server shutdown closes the manager, evicting idle resources and cleaning up
active resources on their final release. Legacy SyncMode/RefMode/CloseMode,
public List behavior and old push distribution are unchanged.

Validation passes the acquired name-to-ID map and registered filename basenames
as explicit native extra info. Only this validation binding classifies a
mismatched remote `file_name` as bad input. Runtime native lookups retain system
error semantics. Validation returns the deduplicated IDs actually used by the
analyzer. RootCoord reserves those references under its existing metadata lock
before publishing DDL; a concurrent resource removal must fail reservation
rather than publish a schema with missing resources. No QueryNode inventory
synchronization is needed for this validation.

The validation response carries business Status and, on success, resource IDs.
Resource acquisition and native failures preserve their codes and classification
in Status; actual caller cancellation remains a transport cancellation.
HandlerClient checks Status after its readiness loop. A missing local file maps
from Rust's generic `IoNotFound` to the existing non-retriable `PathNotExist`
code at the C++ validation boundary. Other local I/O errors retain
`FileReadFailed`. Runtime construction and index operations keep their existing
I/O mappings. This resource-loading refactor adds no Rust or C++ changes.

Only runners needing resources with the new loader enabled defer prewarming to
first use. Each RPC joins a per-runner initialization flight that acquires
resources and constructs the native runner. This is runner initialization, not
a second resource-loading phase. Native construction has no cancellation API:
a build already admitted owns its resources until it completes, and a later
caller rejoins that build. Failed/canceled preparation cannot admit a new build.
Runner signatures include the schema resource IDs, so an identical analyzer
configuration with different bindings cannot reuse the old runner.

WAL materialization leases only ready runners inside the inner interceptor
chain. A missing runner returns `PreparationRequiredError` before fields are
changed or segments assigned. The inner chain unwinds to redo, releasing WAL
locks and acknowledging the failed attempt's TimeTick. Redo prepares with the
original caller context, also canceled on WAL shutdown, then retries the inner
chain to recheck schema and transaction state. Idempotency stays outermost:
same-key duplicates wait for the owner's result; distinct executing requests
share initialization with independent cancellation. Segment readiness remains
checked before every inner attempt. Actual mutation/append uses the WAL-owned
context and still survives caller cancellation. Remote Produce uses its existing
stream context; no per-message cancellation protocol is introduced.

New StreamingNodes require a Coordinator supporting Fetch; the new DDL
validation path requires StreamingNodes supporting ValidateAnalyzer. Milvus
upgrades MixCoord before worker nodes. Existing published RPCs remain available;
an unsupported new RPC surfaces as Unimplemented.

## Key Packages

- `internal/distributed/streaming/{streaming.go,wal.go}` — public streaming entry.
- `internal/streamingnode/client/handler/analyzer_client.go` — handler RPC and routing.
- `internal/streamingnode/server/service/analyzer.go` — native RPC dispatch/errors.
- `internal/streamingnode/server/wal/adaptor/analyzer.go` — WAL field execution.
- `internal/util/function/manager.go` — schema selection and runner lifetime.
- `internal/proxy/impl.go` — SDK adaptation and collection identity.
- `pkg/proto/streaming.proto` — additive request/response and service method.
- `internal/util/fileresource/pull.go` — process-wide analyzer resource manager access.
- `internal/util/fileresource/analyzer_resource_manager.go` — ID-keyed loading, download flights, resource ownership and idle leases.
- `internal/rootcoord/file_resource_fetch.go` — Fetch and name resolution.

## Lexical highlighting

Proxy's post-search HighlightTask resolves VChannels by the search collection ID
and invokes this same AnalyzerClient once per field. Each request includes the
search text, explicit highlight queries and returned corpus texts, retaining
per-text analyzer names, with token details enabled and hashes disabled. It uses
current WAL schema, the same contract as RunAnalyzer; there is no historical
schema request or QueryView/replica discovery in this operation.

`internal/util/function/highlight.BuildFragments` performs token matching,
overlap merging, UTF-8 byte-to-rune conversion and fragment selection. Both Proxy
and the legacy delegator endpoint use it, preserving rendering behavior while
removing Proxy's dependency on QN.GetHighlight. The old internal RPC remains
available for older callers. Semantic/model-based highlighting is unchanged.

Analyzer business Status propagates without retry or relabeling; streaming
transport failures follow HandlerClient recovery and surface at the Proxy
boundary. Malformed token-result cardinality is an internal error and must not
publish partial highlight results. No new SN RPC or client lifecycle is needed.
