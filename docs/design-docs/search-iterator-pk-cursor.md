# Search iterator primary-key continuation and first-page negotiation

## Problem and scope

A distance-only continuation excludes the remainder of an equal-score group.
Sorting an already truncated page cannot recover those rows. Furthermore,
draining an ANN iterator does not enumerate the entire eligible corpus: an HNSW
iterator can exhaust its reachable graph while stored vectors remain unvisited.

This design adds an explicitly requested, negotiated cursor version 2 that
orders eligible snapshot rows by raw score and primary key. It retains the
default distance cursor and ANN search cost. SDKs reuse the configured first
page for negotiation instead of issuing and discarding a topk=1 request.

Ordinary ANN searches retain approximate candidate selection. Equal-score
ordering applies to rows retained by search/reduction; it does not promise a
globally smallest PK outside the ANN candidate pool. Complete enumeration is
provided by the negotiated exact iterator path described below.

## Exact ordering and snapshot

Distance metrics order raw float32 scores ascending; similarity metrics order
them descending. Exactly equal numerical scores use PK ascending: numeric
Int64 order or existing VarChar lexicographic order. Nearby unequal scores are
not ties. Scores, PKs, offsets, element indices, and grouped result fields move
together when rows are reordered. Cursor metadata uses raw scores, not SDK
display rounding.

The first response pins the existing positive iterator snapshot timestamp.
Every subsequent request retains that timestamp and collection ID. After the
first page, a result is eligible when it sorts strictly after the previous
raw `(score, PK)` tuple. Radius/range predicates and MVCC/deletion/null bitmaps
still restrict the eligible corpus.

## Server execution

Strict mode dispatches before ANN index selection on sealed and growing
segments. It reads raw vectors through the segment bulk-subscript interface,
which can resolve a loaded column or raw vector data retained by an index.
Growing searches freeze their active row count. Nullable compact storage is
mapped back to logical offsets; excluded or null rows do not enter scoring.

At most 256 eligible logical offsets are scored per chunk. Dense float,
float16, bfloat16, int8, and binary vectors use exact scoring. Sparse IP uses
the sparse dot kernel directly, including zero, negative scores, and empty
queries that a positive-score-only sparse search would otherwise omit.
A bounded heap keeps the best B rows under raw score plus actual PK order.
No graph connectivity assumption or ANN stream ordering can discard rows.

Each request scans the eligible snapshot corpus again. Selection memory is
O(B + 256), excluding source vectors, index/storage caches, and result
reduction. CPU and raw-vector I/O can be substantial, particularly across
many pages. This is an explicit opt-in feature, not a default SDK upgrade
cost. Earlier measurements of an ANN-draining prototype are not measurements
of this implementation.

The fresh FirstBatchCost microbenchmark uses N=4096, dimension=16, B=32,
one warm-up and five measured runs with median timing:

| Raw vector source | Default first page | Exact first page | Exact/default |
| --- | ---: | ---: | ---: |
| Growing brute force | 1955 us | 1198 us | 0.613x |
| HNSW index GetVector | 68 us | 1489 us | 21.90x |

Both strict runs examine all 4096 PK candidates. The test uses identical
in-memory vectors, an in-memory PK mapper, and no column I/O or RPC. These
figures characterize this fixture, not production throughput.

If raw vector data cannot be retrieved, strict execution fails with a system
error and does not acknowledge execution or return a legacy page. Actual
storage/I/O errors are preserved. Strict-mode rejection must not use a
compatibility error that SDK wrappers interpret as a pre-V2 fallback.

BM25 is rejected in strict mode: query IDF and average document length are
rebuilt from live statistics on each request and are not frozen by a read
timestamp. The Proxy checks both an explicit BM25 metric and a resolved BM25
function output when the metric was omitted. Native plan parsing also rejects
BM25, including zero-segment and all-filtered execution. Ordinary sparse IP
and default distance iteration remain supported.

Rounded scoring, iterative filtering, and vector-array searches are rejected
in strict mode rather than silently changing the requested ordering.

## Wire contract and negotiation

No public milvus-proto version bump is required. Existing search KVs carry:

| Key | Meaning |
| --- | --- |
| search_iter_cursor_version | Explicit request for "2" |
| search_iter_last_pk_type | "int64" or "varchar" |
| search_iter_last_pk | Exact decimal Int64 or literal VarChar |
| search_iter_id | Existing opaque iterator token |
| search_iter_last_bound | Existing raw score boundary |

The successful response uses existing Status.extra_info for the version and
typed final raw PK. Empty VarChar PKs are valid. Int64 values and UInt64
timestamps must not pass through an unsafe JavaScript Number representation.

The internal SearchIteratorV2Info protobuf adds cursor_version and last_pk
additively. QueryNode acknowledges strict execution only when every executed
segment used it. Worker/shard/proxy reduction preserves the acknowledgement
only when all original participating results acknowledged it, including empty
participants. A zero-segment strict plan can acknowledge empty completion.
Service response construction preserves existing status/cost metadata.

An initial response without the marker, including an old worker in a mixed
cluster, latches the existing distance mode. That compatibility path does not
promise complete equal-score enumeration. Once mode 2 is negotiated, a
missing/changed marker, token, raw shape, score boundary, typed PK, or snapshot
is an error; continuation never silently downgrades. Actual pre-V2 fallback
remains limited to each SDK's existing incompatibility signal.

## SDK pages, duplicate PKs, and limit

SDK construction requests the configured first B and caches that useful
response. A short nonempty raw page is not EOF. Cursor updates use the last
raw server row even when a callback rejects the entire page.

Ordinary insert can retain several visible versions of one PK. Vector search
deduplicates within a response rather than applying scalar query's
latest-timestamp reduction. Versions with different scores can therefore
appear on different pages. Counting both against limit can hide another PK:
A at distance 0, A at distance 1, B at distance 2 with batch=1 and limit=2.

In negotiated mode 2, SDKs retain identities across pages and pending cached
rows. Duplicate-only raw pages continue fetching without consuming limit or
signalling EOF. Identity memory is O(distinct accepted PKs); it is not bounded
by B. The first accepted representation in score order wins, without adding
latest-write-wins vector semantics.

Python, Java, Rust, and C++ deduplicate accepted rows after their existing
external filter, and limit counts delivered distinct rows. Node preserves
its existing limit-before-external-predicate contract: it deduplicates raw
PKs before the predicate and counts distinct raw identities. Node uses the
retained protobuf IDs rather than potentially transformed display IDs.
External filters must retain row primary-key identity; arbitrary identity
rewrites do not have a defined original-row deduplication meaning.

A callback failure retains the raw response for retry. Validation, filtering,
row selection, identities, cache, limit, and raw cursor changes commit only
after processing succeeds. Legacy/default mode retains its existing behavior.

Rust durable checkpoints include the accepted PK identities required for
continued distinct iteration. They retain the existing timestamp/JSON/token
layout, bind the collection ID and canonical query fingerprint, validate
identity/count state, and use atomic replacement. Checkpointing must not
advance beyond unreturned buffered results without preserving their state.
Old distance checkpoints cannot invent a PK continuation or migrate silently
to a stronger ordering.

## Compatibility and validation boundaries

Compatibility means preserving the default API behavior and negotiating
capabilities, not guaranteeing strict enumeration on servers that do not
implement it. Explicit strict requests can now reject unsupported scoring
options, malformed state, raw-data unavailability, or inconsistent rolling
upgrade responses.

Native tests cover actual HNSW raw-vector access, all 257 tied rows across
recreated pages for Int64/VarChar and L2/IP/COSINE, dense types, nullable
logical offsets, sparse IP zero/negative/empty queries, raw-data failures,
BM25 rejection, and exact-score row alignment. Go tests exercise CGo getters,
QueryNode acknowledgement, original-participant reduction, Proxy metadata,
and the actual service response. SDK mock/unit tests cover negotiation,
precision, EOF, duplicate PKs across different scores, limit, callback retry,
and Rust checkpoint resume.

These checks do not replace end-to-end rolling-upgrade or storage/index
matrix tests. Full Go validation currently has an unrelated proxy fixture
timeout, and Node CI has a MinIO image authorization failure before SDK
system tests start. Publication remains draft until those boundaries have
been reviewed.

## Publication

The upstream milvus-design-docs repository is archived and rejected design PR
creation. This document is published on the author's fork and copied into the
server change so reviewers have a versioned design alongside the implementation.

Related SDK draft PRs:
[Go #53961](https://github.com/milvus-io/milvus/pull/53961),
[Python #3822](https://github.com/milvus-io/pymilvus/pull/3822),
[Java #2105](https://github.com/milvus-io/milvus-sdk-java/pull/2105),
[Node #616](https://github.com/milvus-io/milvus-sdk-node/pull/616),
[Rust #178](https://github.com/milvus-io/milvus-sdk-rust/pull/178),
[C++ #612](https://github.com/milvus-io/milvus-sdk-cpp/pull/612).
