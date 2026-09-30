# Immutable Message Body Cache

Status: implemented in this workspace. Updated on 2026-09-30.

## Purpose and current implementation

RecoveryStorage, GrowingRuntime and IDF can consume the same immutable WAL
message. Query snapshots and live delivery already retain ordinary message
references without cloning payloads or retaining persistence Ack handles.
Previously, `SpecializedImmutableMessage.Body` decrypted and unmarshaled on
every call, allowing callers to modify independently decoded results.

This design makes the immutable message's Body a shared read-only decode result
and introduces a process-wide manager in the `message` package to evict cached
results independently of message lifetime. It supersedes the earlier proposal
to coordinate Insert materialization and split bootstrap phases in QueryRuntime.
Bootstrap does not need to be reordered solely to share decoded bodies.

## Body contract

- Immutable `Body(ctx)` performs only decryption and protobuf deserialization.
  `MustBody()` keeps its existing panic-on-error behavior and uses the same cache.
- The returned protobuf, including all nested messages, maps and slices, is
  read-only. Calls may return the same object while cached; pointer identity is
  not guaranteed across eviction. The decoded content remains equivalent.
- Body does not fill BM25/MinHash output, normalize fields, project a schema,
  install segment assignments, or rewrite timestamps.
- Consumers needing modifications must own the affected mutable data. A shallow
  request wrapper is sufficient only when every shared nested value remains
  read-only. Field normalization or nested `Base` edits require appropriate
  private copies or a refactored read-only conversion path.
- Keep the existing method signatures; do not add a parallel `ReadOnlyBody`
  API. Changing return-value ownership requires auditing and migrating callers
  together with the implementation. Go protobuf pointers do not enforce
  immutability by themselves.
- Mutable and broadcast message editing is outside this immutable cache's
  scope. Do not share a cache across `OverwriteBody` or mutable reconstruction.

## Write-before function materialization

The current shard interceptor materializes BM25 and MinHash output before WAL
append when `function.enableWriteBeforeMaterialization` is enabled. The default
`auto` gate switches on after the cluster confirms version at least 2.6.23 and
the one-minute stability window elapses. An enabled-path materialization error
fails the append; it does not silently defer that work to consumers.

Normal new Inserts after activation therefore carry function output in their
WAL body. Consumer-side filling remains necessary for older WAL, upgrade-period
messages and explicitly disabled write-before materialization. These paths
must use private mutable data and preserve already-present output fields.

Schema-dependent function-output caching is not part of this design. In
particular, the first consumer must not overwrite the shared raw Body with
output derived using its pack or runtime schema. Body caching alone does not
deduplicate compatibility-path function execution.

## Ownership and registration

```text
ImmutableMessage / specialized wrappers
    -> shared BodyCacheSlot
         - optional decoded Body
         - in-progress construction state
         - access time and estimated retained size
              ^
              |
       process-wide BodyCacheManager
              |
       one asynchronous recycler
```

The cache slot belongs to the underlying immutable representation, not to each
specialized wrapper. Repeated typed conversions of the same message use the
same slot. Immutable clones that only change transaction TimeTick and
LastConfirmedMessageID may share it because the encoded body and its decoding
context are unchanged. Execution uses the outer transaction's commit TimeTick;
the cache never rewrites timestamps in the original decoded Body.

This is object-local reuse, not a global MessageID lookup. Independently read or
reconstructed messages need not deduplicate their decoded objects. Mutable
conversion and any change to payload or decoding context must not inherit a
stale cache.

Register a cache entry when a successfully decoded result is admitted and
published. Messages whose Body is never accessed do not enter the registry.
The manager retains only the cache slot and reclamation metadata. Slots and
registry entries must not retain their parent message, transaction, encoded
payload, Ack handle, or a persistent loader closure that captures those objects.
The caller supplies the decode input only for the active construction.

An entry whose message has already become unreachable can remain until cache
eviction; idle expiry and capacity control must also reclaim its registry
metadata. The manager must not accumulate empty slots indefinitely.

## Concurrent construction and eviction

```text
Empty -> Loading -> Ready
           |
           +---- failure -> Empty

Ready -> eviction -> Empty
```

One construction runs per slot at a time. Other callers wait for that attempt;
decryption and deserialization run outside the cache-management locks. Publish
only fully constructed successful results. A failed attempt wakes its waiters
and leaves the slot retryable, without retaining partial bodies or permanently
caching cancellation/decryption failures.

Each waiter observes its own context cancellation without cancelling another
caller's work. If the constructing caller is cancelled, that attempt may fail;
other valid callers can retry. Check context cancellation consistently on both
cache hits and misses. Do not use an irreversible `sync.Once` for a computation
that can fail and later retry. There is no per-message construction goroutine.

Access-time updates, publication, registration and eviction need one coherent
synchronization protocol. An expiry decision must revalidate the current entry
before removal so it cannot evict a newly published replacement or ignore a
subsequent access. Do not hold a process-wide lock while decoding or waiting.

Eviction removes cache references, never mutates the protobuf or its backing
data. A caller already holding the result can continue reading it safely:

```text
A receives B1 -> cache evicts B1 -> B requests Body and constructs B2
A continues reading B1 -> A releases B1 -> B1 becomes eligible for GC
```

No `ReleaseBody`, read lease, `proto.Reset`, buffer reuse or object pooling is
required for returned bodies. Old and newly decoded results can coexist while
readers retain the old result. Cache hits deduplicate construction during cache
residency; eviction intentionally permits later reconstruction.

## Reclamation policy and memory accounting

Use one process-wide asynchronous recycler for periodic idle-expiry scans.
Capacity eviction happens synchronously when admitting a decoded body. Cache
policy combines:

- Idle expiry, refreshed by Body access.
- A global estimated-byte budget for admitted cached results.
- LRU eviction before admission when remaining capacity is insufficient.
- Admission bypass only for results larger than the entire budget. The caller
  still receives the successfully decoded Body without persistent residency.

Capacity eviction, admission and byte accounting run atomically under the
manager mutex. Every admitted entry can be evicted without waiting for readers,
so a body within the budget can always make room for itself. The initial
implementation uses a 256 MiB estimated budget, 30-second idle expiry and a
one-second recycler interval. These are
package defaults, not runtime configuration. A cache hit updates an LRU list
under the manager mutex; decode and size estimation run outside that mutex.

Admission accounts for `256 + 2 * proto.Size(body)` bytes: a fixed allowance for
small bodies and registration plus an approximate allowance for decoded data.
This is deliberately an estimate, not an exact protobuf heap measurement.
After decoding, admission removes entries from the LRU head until there is
enough space, then registers the new body at the tail before returning. It does
not discard the completed decode merely because the cache was full. Eviction
reclaims only the space needed for admission; there is no pressure watermark,
pressure notification or asynchronous admission retry. Bodies exceeding the
entire budget bypass residency without evicting other entries.

The budget bounds cache-owned estimated residency, not total process heap.
Active decodes, consumer-held bodies and retained encoded WAL payloads also use
memory. Eviction only removes references; actual reclamation depends on other
references and Go GC. Do not force GC on each eviction. Avoid separately caching
decrypted byte buffers when the decoded protobuf is sufficient.

Use separate cache accounting. Existing `Message.EstimateSize()` also contributes
to logical WAL byte offsets and must not change as cache entries appear or
disappear. `proto.Size()` measures encoded size, not exact Go heap occupancy;
any use as an estimate must state its limitations.

The manager owns recycler startup and shutdown, with deterministic close/cleanup
for tests. It creates no per-VChannel or per-message background workers.

## Persistence and query boundaries

Body caching does not participate in Ack completion. Releasing persistence
handles must neither invalidate a Body held by query work nor require cache
eviction. Query-held ordinary references do not delay persistence completion.
The manager can clear a cache slot while its message remains live and can retain
an admitted Body briefly after that message dies, subject to reclamation policy.

Keep WAL bytes, RPCs, recovery checkpoints, transaction boundaries, query MVCC,
Ready barriers and QueryView leases unchanged. The cache is process-local and
reconstructed on demand after restart.

## Implementation and validation

The RecoveryStorage pack writer and query materialization use
`storage.CopyInsertRequestMetadata`: each consumer owns the request, Base,
field wrappers and recursively nested struct-array wrappers. Column values,
row data and validity bitmap backing arrays remain borrowed read-only.
`typeutil.CopyFieldDataMetadata` preserves both old and new validity locations
so the existing boundary validation still rejects conflicting inputs.
Validity normalization therefore edits private wrapper metadata, and timestamp
replacement allocates a new slice. Legacy missing-output materialization appends
its results to the consumer's private field list, preserving cached raw input.

Delete readers and schema readers retain read-only bodies. TransformLog already
copies the Delete fields it owns. Mutable/broadcast callbacks keep independent
decoding. The V1 msgstream adaptor also retains its separate decoding path,
including its private timestamp rewriting; V2 schema adaptors read cached bodies.

Validate concurrent first access, typed-wrapper reuse, transaction clone reuse,
independent mutable reconstruction, decoding errors and retries, cancelled
constructors/waiters, expiry racing with hits/publication, and safe use of a
previously returned Body after eviction. Cover synchronous admission, LRU access
refresh, multi-entry eviction, oversized bypass without evicting useful entries,
registry cleanup and recycler shutdown. Verify Ack can complete while query
consumers retain messages or decoded bodies.

Run race tests and compare CPU, allocations, retained heap and decode counts for
normal multi-consumer Inserts, encrypted bodies, large snapshots, slow
persistence and query backlog. Include cache-disabled/evicted paths and legacy
function-output fallback, not only repeated cache hits.

## Key packages and related documents

- `pkg/streaming/util/message/{message,message_impl,specialized_message,builder,cipher,ref_counted_message}.go`
- `pkg/streaming/util/message/body_cache.go`
- `internal/storage/insert_request.go`
- `pkg/util/typeutil/field_data.go`
- `internal/streamingnode/server/wal/interceptors/shard/{shard_interceptor,function_materializer}.go`
- `internal/streamingnode/server/wal/vchannel/segment/pack_writer.go`
- `internal/streamingnode/server/wal/walview/materialize_insert.go`
- `internal/streamingnode/server/wal/vchannel/{growingruntime,idf}/`
- [Message model](../../../agent_guides/streaming-system/message/message.md)
- [WAL message Ack](message_ack.md)
- [SN query input view](streamingnode_vchannel_wal_view.md)
- [IDF Oracle runtime](../qviews/snview/idf_oracle_runtime.md)
