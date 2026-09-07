# MEP: Exact Integer Membership with roaring_match

> **Superseded.** `roaring_match` never shipped in a release. Its syntax is
> superseded by the unified `membership_match` surface (identical MRB1 blob,
> magic-header dispatch); see
> [20260822-membership-match-expression.md](./20260822-membership-match-expression.md).
> No deprecation period is needed because the name was never part of a release.

- **Created:** 2026-07-14
- **Author(s):** @xiaofan-luan
- **Status:** Accepted (implementation under review in
  [#51968](https://github.com/milvus-io/milvus/pull/51968))
- **Component:** SDK | Proxy | QueryNode | Index
- **Related Issues:** [#52094](https://github.com/milvus-io/milvus/issues/52094)
- **Released:** TBD

## Summary

This MEP introduces `roaring_match(field, {bitmap})`, an exact membership
predicate for `INT8`, `INT16`, `INT32`, and `INT64` fields. A client maps signed
integers into the unsigned Roaring64 key space, builds a portable Roaring64
bitmap, wraps it in the versioned MRB1 envelope, and sends the blob as a native
protobuf `bytes` expression-template value. The Proxy validates the complete
blob and materializes a dedicated `RoaringFilterExpr`. QueryNode validates the
blob independently, parses each logical expression into shared immutable
membership state, and probes raw field data or a bounded index-only
reverse-lookup fallback.

The predicate is exact: it has neither false positives nor false negatives.
Consequently, it is eligible for delete expressions. This differs from the
approximate `bloom_match` predicate, which must remain prohibited in delete
expressions because Bloom false positives could delete rows outside the
intended set.

## Motivation

Large integer membership sets occur in access-control lists, compliance lists,
tenant visibility rules, exact audience inclusion or exclusion, and
delete-by-ID-set workflows. A repeated `IN` list is exact, but it sends every
value through the expression plan and makes request and plan size grow roughly
with member count. Approximate membership can reduce transfer size, but it is
not acceptable when a false positive changes authorization, exclusion, or
deletion behavior.

Roaring is useful when the integer distribution contains dense regions,
clusters, or ranges. Its size is nevertheless distribution-sensitive. Uniformly
random values across the full signed 64-bit domain can create many high-key
containers and can serialize as large as, or larger than, a repeated int64
list. Therefore, `roaring_match` is not a universal replacement for `IN`, does
not replace `bloom_match`, and does not support strings in version 1. Users must
choose based on exactness, field type, value distribution, and measured payload
size rather than member count alone.

<!-- markdownlint-disable MD013 -->

| Property | `IN` | `bloom_match` | `roaring_match` |
| --- | --- | --- | --- |
| Semantics | Exact | Approximate; false positives possible | Exact |
| Version 1 field types | Existing scalar types supported by `TermExpr` | Integer and `VARCHAR` fields | Signed `INT8`/`INT16`/`INT32`/`INT64` only |
| Client payload | Repeated scalar values | MBF1 envelope with a Parquet split-block Bloom filter | MRB1 envelope with portable Roaring64 |
| Payload-size behavior | Roughly proportional to member count | Primarily determined by cardinality and target false-positive rate | Strongly dependent on value distribution and container selection |
| Delete expression | Allowed | Rejected | Allowed |
| Plan node | `TermExpr` | `BloomFilterExpr`, oneof field 22 | `RoaringFilterExpr`, oneof field 23 |

<!-- markdownlint-enable MD013 -->

The MRB1 design in this MEP supersedes the earlier future-work sketch in the
sibling Bloom proposal that assigned Roaring to `MBF1 algo=2`. MBF1 remains a
Bloom-specific envelope. Exact Roaring membership uses an independent MRB1
envelope and a dedicated plan node so that validation, exactness, and delete
safety remain explicit.

## Goals and Non-goals

### Goals

- Provide exact positive and negated membership for top-level signed
  `INT8`, `INT16`, `INT32`, and `INT64` fields.
- Keep those four fixed server field and wire-value types while allowing the Go
  SDK to accept `[]int` as an explicitly Go-specific construction convenience.
- Define a versioned, deterministic wire representation that can be produced
  and consumed independently across SDKs and QueryNode.
- Avoid sending or reconstructing the original member list at the Proxy.
- Preserve identical semantics for growing segments, sealed segments with raw
  data, and sealed index-only segments using the bounded fallback.
- Reject malformed input and unsupported mixed-version execution rather than
  silently dropping the predicate.
- Permit the predicate in delete expressions because membership is exact.

### Non-goals

- `VARCHAR`, `JSON`, dynamic or nested paths, arrays, unsigned integers,
  floating-point values, or arbitrary third-party serialized Roaring variants.
- Server-side bitmap construction from a repeated list.
- Persistent or registered bitmap handles and their lifecycle, authorization,
  invalidation, quota, or distributed caching.
- Automatic selection among `IN`, `bloom_match`, and `roaring_match`.
- An index-native distinct-value enumeration or posting-list union operator.
  The version 1 index-only path is a correctness fallback, not index
  acceleration.
- A public API that accepts a bare portable Roaring64 body without MRB1.

## Public Interfaces and Semantics

### Expression syntax

The public syntax is independent of `IN` and `bloom_match`:

```text
roaring_match(<integer-field>, {<bitmap-bytes-template>})
not roaring_match(<integer-field>, {<bitmap-bytes-template>})
```

Example Go usage:

```go
bitmap, err := milvusclient.NewRoaringBitmapBlob([]int64{-7, 42, 1001})
if err != nil {
    return err
}

option := milvusclient.NewQueryOption(collectionName).
    WithFilter("roaring_match(creator_id, {ids})").
    WithTemplateParam("ids", bitmap)

deleteOption := milvusclient.NewDeleteOption(collectionName).
    WithExpr("roaring_match(creator_id, {ids})").
    WithTemplateParam("ids", bitmap)
```

The expression takes exactly two arguments:

1. The first argument MUST resolve to a top-level `INT8`, `INT16`, `INT32`, or
   `INT64` field.
2. The second argument MUST be a `{template}` placeholder whose resolved value
   is protobuf `bytes` containing one complete MRB1 blob.

Literal lists, scalar template values, bare bitmap bodies, JSON or dynamic
paths, nested fields, arrays, unsigned values, strings, booleans, and floating
point values MUST be rejected. The server field types and cross-language wire
value domains are exactly signed 8-, 16-, 32-, and 64-bit integers.

The Go SDK builder intentionally accepts `[]int` in addition to `[]int8`,
`[]int16`, `[]int32`, and `[]int64`. Go `int` is architecture-width input, and
every Go `int` value converts exactly to int64 without truncation before the
normative signed mapping is applied. `[]int` is an SDK convenience, not a new
Milvus field type, MRB1 key type, or cross-language wire type. Unsigned integer,
floating-point, string, boolean, and other input slices remain rejected.

Function syntax is intentional. The payload has a construction and validation
contract that differs materially from a repeated `IN` list, and it receives a
dedicated wire-plan node. Overloading `IN` based on template type would hide
that contract and complicate validation and mixed-version auditing.

### Exactness, duplicates, empty sets, and NULL

- Membership is exact. For every non-NULL field value, the predicate is true
  if and only if the mapped value is present in the decoded bitmap.
- Duplicate client values are deduplicated by the bitmap. The MRB1 cardinality
  is the number of distinct mapped values.
- An empty bitmap makes positive `roaring_match` false for every non-NULL row.
- `not roaring_match(field, {empty})` is true for every non-NULL row.
- `roaring_match(NULL, {bitmap})` evaluates to NULL, not false.
- `not roaring_match(NULL, {bitmap})` also evaluates to NULL. SQL
  three-valued logic therefore excludes NULL rows from filtering and delete
  selection for both positive and negated forms.

## Signed Integer Mapping (normative)

All producers and consumers MUST apply the same mapping:

```text
signed source value
  -> sign-extend to int64
  -> preserve the int64 two's-complement bit pattern as uint64
```

Equivalent operations include `uint64(int64(value))` in Go and
`static_cast<uint64_t>(static_cast<int64_t>(value))` in C++. Java producers
must preserve the raw signed `long` bit pattern. They must not ZigZag-encode the
value or add `2^63`.

| Source value | Sign-extended int64 | Roaring64 unsigned key |
| --- | ---: | --- |
| `INT8(-1)` | `-1` | `0xffffffffffffffff` |
| `INT8(-128)` | `-128` | `0xffffffffffffff80` |
| `INT16(-32768)` | `-32768` | `0xffffffffffff8000` |
| `INT32(-1)` | `-1` | `0xffffffffffffffff` |
| `INT32_MIN` | `-2147483648` | `0xffffffff80000000` |
| `INT64_MIN` | `-9223372036854775808` | `0x8000000000000000` |
| `42` | `42` | `0x000000000000002a` |
| `INT64_MAX` | `9223372036854775807` | `0x7fffffffffffffff` |

The unsigned sort order inside Roaring is not observable because this
predicate performs membership tests, not range arithmetic. The following
mappings are incompatible and MUST be rejected by conformance tests:

- Zero-extending a narrow signed value, such as mapping `INT8(-1)` to
  `0x00000000000000ff`.
- ZigZag encoding.
- Biasing by `2^63` to preserve signed sort order.
- Encoding decimal strings instead of integer bits.

Checked-in golden vectors MUST lock this mapping across every supported SDK and
the C++ execution engine.

## MRB1 Wire Format (normative)

MRB1 consists of a fixed 32-byte little-endian header followed by exactly one
portable Roaring64 body.

| Offset | Size | Field | Required value |
| ---: | ---: | --- | --- |
| 0 | 4 | `magic` | ASCII `MRB1` |
| 4 | 2 | `version` | unsigned little-endian integer `1` |
| 6 | 2 | `format` | unsigned little-endian integer `1` (`portable_roaring64`) |
| 8 | 8 | `cardinality` | unsigned little-endian exact distinct-key count |
| 16 | 8 | `body_length` | unsigned little-endian body length in bytes |
| 24 | 8 | `reserved` | unsigned little-endian integer `0` |
| 32 | `body_length` | `body` | portable Roaring64 serialization |

The complete blob length is `32 + body_length`. The body length MUST NOT exceed
128 MiB (`128 * 1024 * 1024` bytes).

### Portable Roaring64 body

The body is the portable Roaring64 extension implemented by the Go Roaring v2
library and the CRoaring C++ `Roaring64Map` portable read/write path. It is not
a language-native or library-private serialization. At the top level, the body
contains a little-endian uint64 count of high-32 containers followed by
strictly increasing uint32 high keys and one portable Roaring32 child for each
key. Roaring32 children may contain array, bitmap, or run containers according
to the upstream portable format.

Current cross-language interoperability is claimed only for independently
generated Go and CRoaring C++ fixtures. Java support MUST NOT be claimed or
released until a Java implementation independently produces and consumes the
same fixtures, including signed boundaries and every container type.

### Required validation

Both the Proxy and QueryNode MUST reject a blob unless all of the following are
true:

- The blob contains the complete 32-byte header.
- `magic`, `version`, and `format` equal the values above.
- `reserved` is zero.
- `body_length` equals all remaining bytes and is at most 128 MiB. Truncation
  and trailing bytes are rejected.
- The portable body consumes exactly `body_length` bytes.
- High keys, child keys, offsets, array values, bitmap cardinalities, run
  intervals, and run cardinalities satisfy the portable format's structural
  rules.
- The cardinality computed by the structural scan equals the header
  `cardinality`.
- The body contains at most 262,144 high-32 containers.
- The estimated decoded size is at most 64 MiB, using the normative admission
  estimate `body_length + high_container_count * 128 +
  low_container_count * 64`.

The Proxy validator MUST run in time linear in the supplied bytes. It MUST
bound the high-container count from available body bytes before allocating,
compute cardinality and container counts directly from the wire, perform no
heap allocation on successful validation, and avoid library validation paths
whose behavior can be quadratic on adversarial run containers. It MUST NOT
construct a `roaring64.Bitmap` merely to validate a request. QueryNode MUST
independently complete the same allocation-free scan and decoded-memory
admission before reserving or decoding any high-container object; decoder
exceptions, exact serialized size, and decoded cardinality are then checked in
the admitted decode pass.

### Size limits

Structural validity is not sufficient: a perfectly well-formed bitmap can still
be too expensive to fan out. Two limits bound that independently.

| Limit | Default | Bounds |
| --- | --- | --- |
| `proxy.maxMembershipFilterSize` | 64 MiB | One MBF1 or MRB1 body. The fixed 32-byte header is allowed on top. For Roaring, decoded-memory admission usually becomes the tighter bound before the full wire budget is usable. |
| `proxy.maxMembershipFilterPlanSize` | 128 MiB | The aggregate serialized size of every membership-filter-bearing plan in one Search, HybridSearch, Query, or complex Delete. |
| MRB1 high-container admission | 262,144 | Separately allocated Roaring32 children per expression, enforced by SDK, Proxy, and QueryNode. |
| MRB1 estimated decoded admission | 64 MiB | `body + 128 bytes/high container + 64 bytes/low container`, enforced by SDK, Proxy, and QueryNode. |

The per-blob gate MUST be checked before the body is decoded, so an oversized
blob is rejected without walking a hostile structure.

Both configurable budgets are deliberately **shared with `bloom_match`**. The
two expressions consume the same wire resources — a client-built blob embedded
in the serialized plan and fanned out to every QueryNode — so a request carrying
one of each must not be allowed to spend twice what either alone may. The legacy
Bloom-specific keys remain fallbacks for deployments that already set them.

After template materialization, Proxy applies the exact `proto.Size` gate before
`proto.Marshal`; this catches repeated embedded copies, protobuf overhead, and
HybridSearch accumulation. Each Roaring template is structurally validated and
must independently satisfy the 64 MiB decoded estimate. Materialized scorer
filters may carry membership expressions and their blobs count against the same
per-request budget, so a blob embedded in a scorer filter is charged exactly as
one embedded in the main predicate.

Sizing by bytes rather than member count is the point. Unlike an SBBF body,
whose size follows `(n, fpr)` alone, a Roaring body's size is driven by the
value distribution. The shared 64 MiB wire budget may admit a larger compact
body, but the independent 64 MiB decoded estimate includes container overhead
and is therefore normally the effective Roaring ceiling. A 32 MiB body can hold
roughly 16M members spread uniformly over the whole `INT32` domain, and tens of
millions when the values are dense or contiguous. Full-range sparse `INT64`
input costs roughly 22 bytes per member in the worst case, where each value
lands alone in its own 2^32 high container, but reaches the fixed high-container
admission first: approximately 262K distinct high-32 buckets is the hard ceiling
even when the serialized body is well below 64 MiB. Operators should use
`bloom_match` for that distribution and
reason about both container shape and bytes, not member count alone.

## Design Details / End-to-end Flow

```text
Application / SDK
  1. Sign-extend values, preserve bits as uint64, dedupe, RunOptimize,
     serialize portable Roaring64, and prepend MRB1.
  2. Send MRB1 as TemplateValue.bytes_val.
        |
        v
Proxy parser and template materialization
  3. Parse roaring_match as a generic call with a bytes placeholder.
  4. Require a supported top-level integer field and bytes template.
  5. Validate the MRB1 blob in linear time without decoding and enforce the
     per-filter wire and decoded-estimate limits.
  6. Materialize
     planpb.RoaringFilterExpr {column_info, bitmap_blob}.
        |
        v
QueryNode / segcore
  7. Independently validate and decode each RoaringFilterExpr into immutable
     RoaringMembership.
  8. Share that membership across per-segment PhyRoaringFilterExpr objects.
  9. Probe raw values or the bounded index-only reverse-lookup fallback.
```

The shared expression-template prerequisite is the native
`schemapb.TemplateValue.bytes_val` to `planpb.GenericValue.bytes_val` transport.
It is satisfied. `schemapb.TemplateValue.bytes_val` landed in
[milvus-io/milvus-proto#632](https://github.com/milvus-io/milvus-proto/pull/632),
and the internal `planpb.GenericValue.bytes_val` (field 6) landed with
`bloom_match` in milvus-io/milvus#51140. This is a transport dependency, not a
dependency on Bloom semantics or MBF1.

The Proxy never receives or expands the original member list. After template
resolution, it materializes the following additive plan node:

```proto
message RoaringFilterExpr {
  ColumnInfo column_info = 1;
  bytes bitmap_blob = 2;
}

message Expr {
  oneof expr {
    // Existing fields omitted.
    RoaringFilterExpr roaring_filter_expr = 23;
  }
}
```

Field number 23 is reserved for `RoaringFilterExpr` and MUST NOT be reused.
The generic `CallExpr` is only an intermediate representation while template
values are unresolved; QueryNode receives the dedicated node.

QueryNode parses each logical `RoaringFilterExpr` once, not once per segment,
batch, or row. The per-segment physical expressions created from that logical
expression share a `const`/immutable membership object.
Membership expressions are supported in `ScoreFunction.filter` as well as in
the main query predicate, matching what `bloom_match` already shipped with.
`PhyRoaringFilterExpr` consumes offset input, so the boost path evaluates the
filter natively against the candidate offsets a segment produced rather than
materializing a whole-segment bitset. NULL rows are folded to FALSE and receive
no boost, the same as every other native scorer filter.
Each active valid row performs one widened-int64 membership probe.
Earlier predicates pass a `bitmap_input`; rows already removed by that bitmap
MUST skip both value retrieval and membership probing.

`roaring_match` is rejected inside `element_filter(...)` in version 1.
Element-level execution supplies global element IDs while the physical
membership expression consumes row offsets, so admitting it as-is would read a
different row or go out of bounds.

The rejection is a fail-closed stopgap, not a statement that the combination is
meaningless. Broadcast semantics are well defined — an element takes the
value of the row that owns it, so it matches if and only if its row matches —
and the element-to-row mapping already exists
(`IArrayOffsets::ElementIDToRowInfo`).
What is missing is the lowering that applies it on every read path. That gap is
not specific to `roaring_match`: any top-level field reference inside an
element expression has it today, including ordinary range and `IN` predicates,
which the parser currently admits. Closing it belongs in its own change,
covering all element-space operators and every offset dispatch site rather
than a subset.

A document-level `roaring_match(...) && element_filter(...)` conjunction remains
valid and is the cheaper form regardless: the membership test is evaluated once
per row instead of once per element.

Both `BloomFilterExpr` and `RoaringFilterExpr` belong to the post-selective
membership optimizer tier. Cheaper or more selective numeric, indexed, and
string predicates run first so their `bitmap_input` reduces membership work.
The same pruning contract applies on raw and index-only paths. A direct `NOT`
wrapper may remain in the generic later tier; that changes evaluation cost but
not exactness or NULL semantics.

The expression is non-cacheable in version 1. A future result-cache key may
include it only after the key has a collision-resistant identity for the entire
MRB1 blob in addition to the field and operator. Blob length or cardinality
alone is not an identity. Diagnostic strings and logs may include the field,
body size, and cardinality, but MUST NOT include raw bitmap bytes or member
values.

## Index-only Fallback

The index-only path preserves correctness when a sealed segment was loaded
without raw scalar field data. It is not an index-native acceleration for
Roaring membership.

The physical expression MUST follow these rules:

1. Snapshot raw-field-data availability when the physical expression is
   constructed. If raw data was present at construction, use the raw path even
   if an index is also loaded. Do not switch paths using a live
   `HasFieldData()` check after cursor state has been initialized.
2. For a segment constructed index-only, pin a compatible scalar index and use
   per-row `Reverse_Lookup` only when the index reports both
   `HasRawData() == true` and `SupportFastReverseLookup() == true`.
3. Gate on capabilities, not an index-name allowlist. Bare `BITMAP` without an
   offset cache is rejected because its reverse lookup is linear in index
   cardinality per row. Offset-cached `BITMAP` is acceptable; `HYBRID` is
   acceptable only when its inner index advertises the same capabilities; any
   other index must pass the same capability checks.
4. Set `CanExecuteAllAtOnce() == false`. Evaluation remains batched and MUST NOT
   allocate a whole-segment offset vector.
5. Use the candidate-mask-aware `ProcessIndexLookupByOffsetsWithMask()`
   behavior: test `bitmap_input` before reverse lookup, preserve candidate
   order for explicit offsets, support non-contiguous and duplicate offsets,
   and preserve NULL validity.
6. If neither raw field data nor a qualifying reverse-lookup index is
   available, fail with `FieldNotLoaded` explaining that raw field data or an
   offset cache is required. The engine MUST NOT perform an
   `O(rows * cardinality)` scan and MUST NOT return an unfiltered result.

The fallback remains `O(active candidates)`: it performs one fast reverse
lookup for every candidate that survives `bitmap_input`, including candidates
whose reverse lookup reports NULL. Only active candidates with a valid value
then perform an exact membership probe. A future index-native design could
enumerate distinct indexed values, test each once, and union postings, but that
requires a separate MEP and index API contract.

## Delete Safety

`roaring_match` is eligible for existing delete-expression handling because
its result is exact:

- Positive membership cannot select an out-of-set row because there is no
  encoding false positive.
- Proxy and QueryNode independently reject malformed, unsupported-version, or
  structurally invalid MRB1 before execution.
- Mixed-version execution fails closed instead of dropping the predicate.
- NULL results remain unknown and are excluded from deletion.
- `not roaring_match(...)` is the exact complement over non-NULL rows. It can
  intentionally select a large delete set, just as `NOT IN` can; that is the
  requested semantics, not an approximation failure.
- A positive empty bitmap deletes no rows. A negated empty bitmap selects all
  non-NULL rows.

The Bloom delete prohibition MUST remain scoped to `BloomFilterExpr`.
`RoaringFilterExpr` MUST NOT be classified as Bloom by parser, validation, or
delete-safety checks.

## Size, Performance, Security, and Observability

### Distribution-sensitive size

MRB1 adds exactly 32 bytes. The portable body size depends on high-key
distribution and Roaring's array, bitmap, and run-container choices. Dense
ranges and clustered values may be compact, while uniformly random signed
64-bit values may be as large as or larger than a repeated int64 list. No
universal member-count capacity or compression ratio is part of this contract.

Before release, benchmarks must cover consecutive ranges, several clustered
ranges, uniform random values in a 32-bit domain, uniform random values across
the full signed 64-bit domain, and duplicate-heavy input. For 10K, 100K, 1M,
and, where feasible, 10M input values, report:

- Input count and distinct cardinality.
- MRB1 bytes and bits per distinct member.
- Serialized repeated-int64/`TermExpr` size.
- MBF1 size at the documented Bloom false-positive rate, clearly labeled
  approximate.
- Client build time, Proxy validation time, QueryNode parse time, and probe
  throughput.
- End-to-end request latency and p50/p99 execution latency for `IN`,
  `bloom_match`, and `roaring_match`.

### Reference Go client build measurement (non-normative)

The following local measurement used an Apple M4 Pro and the repository's
required test flags (including disabled compiler optimization/inlining), so the
latencies are conservative development-build numbers rather than production
SLOs:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -run '^$' \
  -bench '^BenchmarkBuild' -benchmem -benchtime=3x ./roaringfilter

MILVUS_RUN_ROARING_RESOURCE_REPORT=1 \
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  -run '^TestBuildResourceReport$' -v ./roaringfilter
```

| Distribution | Input members | Caller input | Build time | MRB1 bytes | Bytes/member | Build B/op | Allocs/op | Peak build live heap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Contiguous | 1,000,000 | 8,000,000 B | 22.4 ms | 274 | 0.000274 | 533,090 | 323 | 0.4 MiB |
| Shuffled INT32 domain | 1,000,000 | 8,000,000 B | 218.1 ms | 2,524,108 | 2.524 | 28,179,749 | 283,611 | 26.6 MiB |
| Snowflake-like clustered | 1,000,000 | 8,000,000 B | 204.3 ms | 1,992,600 | 1.993 | 20,452,581 | 10,787 | 17.5 MiB |
| Shuffled full-range INT64, admitted | 200,000 | 1,600,000 B | 111.0 ms | 4,400,016 | 22.00 | 53,735,629 | 1,600,081 | 43.1 MiB |
| Shuffled full-range INT64, rejected | 300,000 | 2,400,000 B | 52.0 ms | 0 | n/a | 2,401,490 | 9 | 2.3 MiB |

`Build time`, `Build B/op`, and `Allocs/op` are the three-run benchmark results;
`peak build live heap` is the separate resource report's sampled live heap
attributable to `Build`. Neither includes the caller-owned input slice shown
separately. The rejected full-range case pays for one 2.4 MiB
sorted-key copy and then fails the high-container admission before constructing
the Roaring bitmap, avoiding the millions of allocations seen in an admitted
sparse build. The table also demonstrates why no member-count-only capacity
claim is valid: one million contiguous IDs encode in 274 bytes, while 200K
full-range IDs encode in 4.4 MiB and cost roughly 43 MiB of transient build
heap.

The 128 MiB body ceiling is an engine admission limit, not a promise that an
MRB1 blob of that size can pass every transport. Request framing, other
parameters, and configured gRPC receive limits also apply. In particular,
`proxy.grpc.serverMaxRecvSize` ships as 128 MiB in `configs/milvus.yaml`, equal
to the MRB1 body cap rather than above it, so a body at the cap can never fit
once the 32-byte header and the rest of the request framing are included: gRPC
rejects it before MRB1 validation ever runs. Practical capacity is therefore
bounded by transport limits, the 128 MiB body cap, the high-container cap, and
the 64 MiB decoded estimate; the tightest bound is distribution-sensitive.

### Execution and memory cost

- The client sorts when needed, counts high/low containers before bitmap
  construction, rejects impossible sparse shapes early, then inserts,
  deduplicates, and optimizes. It applies the final decoded-memory admission
  from those counts and the encoder's exact body length before allocating and
  serializing the MRB1 envelope, without rescanning its own output.
- Proxy validation is linear in supplied bytes, allocation-free on success, and
  never materializes a bitmap or rebuilds a raw list.
- Each materialized Roaring expression is validated independently at Proxy and
  QueryNode and must satisfy the per-filter decoded estimate.
- QueryNode parses each logical expression once and shares its immutable decoded
  bitmap across the corresponding per-segment physical expressions.
- The raw path performs one probe per active valid row.
- The index-only fallback performs one fast reverse lookup per active
  candidate, including candidates whose values are NULL, and performs the
  membership probe only for active valid candidates.
- `bitmap_input` pruning reduces both paths.
- Complex delete finalizes output fields and marshals the plan once before
  shard fan-out; every shard and retry reads the same immutable bytes.
- Decoded bitmap memory lives for the query. Sharing avoids per-segment copies;
  concurrent filters and requests can still create tenant or QueryNode-wide
  pressure, so concurrency testing must determine whether an operational
  admission controller is required.

### Security and privacy

- Validate size, version, format, reserved bits, and body length before trusting
  header-derived allocation sizes.
- Bound high-container count by bytes present in the body.
- Enforce the fixed high-container and estimated-decoded-memory limits before
  any QueryNode vector reserve or CRoaring child decode.
- Keep Proxy structural validation linear on adversarial array, bitmap, and run
  containers; do not invoke a known quadratic validator on untrusted input.
- Catch Go decoder panics and C++ decoder exceptions and return a bounded query
  error.
- Require exact byte consumption and exact cardinality at both layers. Do not
  require a valid portable body to retain the same size after library
  reserialization: compact run-cookie encodings may normalize to a different
  but equivalent representation.
- Never log, trace, or include raw bitmap bytes in error messages. This is
  absolute: the blob is the feature's payload, it is measured in megabytes, and
  no diagnostic needs it.
- The expression text contains only the template name, such as `{ids}`; the
  resolved MRB1 bytes are supplied out of band. Parser errors, Search logs, and
  access logs therefore keep the expression text unchanged. Generic expression
  truncation and redaction are not part of this feature.
- After template materialization, Proxy plan debug rendering replaces only
  Bloom and Roaring blob fields with `<blob>` and restores the original plan
  after rendering. Ordinary template values and literals retain the existing
  logging behavior; changing that policy belongs to a separate cross-expression
  logging change.
- Redact both Bloom and Roaring blob fields before rendering plan protos in
  Proxy or QueryNode debug logs. A generic plan-size threshold is insufficient:
  dense sets with millions of exact members can serialize to only a few KiB.
- Unsupported-expression errors MUST be bounded and redacted. An old QueryNode
  protobuf runtime can preserve unknown field 23 even though its generated
  `Expr` oneof accessor is unset, and its legacy TextFormat error path can print
  the preserved MRB1 payload as escaped bytes. The generic redacted default
  error lives in the `ProtoParser::ParseExprs` default case in
  `internal/core/src/query/PlanProto.cpp`, which reports only the oneof
  discriminant. It protects only binaries that contain that change; it does not
  protect unpatched old QueryNodes.
- Redacting the error is NOT sufficient on its own. `LogPlanProtoDebug` renders
  the plan before the parser ever rejects the expression, so the payload
  escapes through the debug log rather than the error. Field-level redaction
  cannot reach it either: on a node whose descriptor pool predates field 23 the
  blob lives in an `UnknownFieldSet`, which a reflection walk over
  `descriptor->field_count()` never visits. The renderer therefore drops
  unknown-field content unconditionally, keeping only the count and byte size
  (`ClearUnknownFields` and `PlanProtoDebugString` in
  `internal/core/src/query/PlanProto.cpp`), and any backport MUST carry that
  change together with the redacted error.
- Keep the expression non-cacheable until a collision-resistant full-blob
  identity is part of the cache key.

### Observability

Implementations should expose aggregate, non-sensitive metrics for:

- MRB1 blob and body size histograms.
- Declared and validated cardinality histograms.
- Proxy validation and QueryNode parse latency.
- Rejection counts by reason, including size, header, body, cardinality, and
  unsupported version or format.
- Rows probed and rows skipped by `bitmap_input`.
- Index-only fallback usage and slow-index rejection count.
- QueryNode memory attributable to decoded membership blobs where practical.

Metrics and logs MUST NOT carry blob contents or any value resolved from an
expression template. Literals present in the expression text the caller sent
are echoed as written; see the security note above.

## Compatibility, Rolling Upgrade, and Rollback

The proto change is additive, but feature use is neither availability-safe nor
automatically privacy-safe during a mixed-version rollout.

<!-- markdownlint-disable MD013 -->

| Combination | Expected behavior |
| --- | --- |
| Old client, new cluster, no `roaring_match` | Existing queries are unchanged. |
| New client to old Proxy | The Proxy cannot recognize or materialize the new function/bytes form; the request is rejected. |
| New Proxy to fully patched old QueryNode | "Fully patched" means BOTH backports are present: the redacted parser default AND the plan-log unknown-field elision. The old generated API sees the expression oneof as unset while protobuf preserves unknown field 23; the debug plan log drops the unknown-field content and the bounded, redacted parser default then rejects the plan, so neither path prints the payload. |
| New Proxy to partially patched old QueryNode (redacted error only) | The error is bounded, but `LogPlanProtoDebug` renders the plan before the parser rejects it, so the preserved MRB1 payload still leaks through the debug log. Not privacy-safe; routing this combination is prohibited. |
| New Proxy to unpatched old QueryNode | The parser still rejects the unset expression, preserving correctness, but both the debug plan log and the legacy TextFormat error path may expose the preserved MRB1 payload. Routing this combination is prohibited. |
| Mixed old and new QueryNodes | Unsupported workers must carry BOTH backports (redacted error and plan-log unknown-field elision) or be excluded by a Proxy, routing, or version gate. No worker may execute without the predicate. |
| Fully upgraded Proxy and QueryNode | `roaring_match` is supported. |

<!-- markdownlint-enable MD013 -->

The correctness fail-closed property depends on two invariants: field 23 is
dedicated to `RoaringFilterExpr`, and an unset or unsupported expression oneof
is rejected by the QueryNode parser default case. An old generated API does not
recognize the field-23 variant, but protobuf can retain the unknown field bytes.
Therefore, rejecting the unset oneof prevents unfiltered success, while the
error path and the debug plan-log path must each separately prevent disclosure
of the retained payload. Mixed execution remains correctness-safe but is not
availability-safe, and a worker missing either the redacted error or the
plan-log unknown-field elision is not privacy-safe.

Before any mixed-version worker is eligible to receive field-23 plans,
operators MUST satisfy one of these privacy preconditions:

1. Backport BOTH the bounded, redacted unsupported-expression error AND the
   plan-log unknown-field elision to every old QueryNode that can receive
   requests during the upgrade. Backporting only the error leaves the debug-log
   path exposed, because the plan is rendered before the expression is
   rejected; or
2. Enforce a Proxy, routing, or version gate that prevents field-23 plans from
   reaching every QueryNode that is not fully patched, where "fully patched"
   means carrying both changes above.

The generic redacted error in the current implementation does not retroactively
protect old binaries. Applications MUST NOT send or enable `roaring_match`
until the privacy precondition is active and every serving Proxy and QueryNode
has completed the feature rollout. Capability advertisement may be added later
to improve deterministic availability and client error reporting, but it does
not replace the privacy precondition.

Plans without `roaring_match` retain their existing wire representation and
behavior. MRB1 is query-scoped; no persisted segment, index, collection
metadata, or storage format changes. No data migration or index rebuild is
required.

Rollback begins by stopping clients from sending `roaring_match` and draining
all field-23 requests before any old QueryNode that is not fully patched
becomes eligible for routing. Proxy and QueryNode can then be rolled back in
the normal order.
Existing collections, segments, and indexes require no rewrite.

## Test Plan

### Go codec and security tests

- Round-trip empty sets, duplicate-heavy input, positive and negative values,
  signed minimum and maximum values, and all four supported widths after
  sign-extension.
- Verify every MRB1 offset, field width, little-endian value, body length,
  reserved field, and exact cardinality.
- Reject bad magic, unsupported version or format, non-zero reserved data,
  truncation, trailing data, mismatched length, mismatched cardinality, and an
  over-128-MiB body.
- Reject malformed high keys, child keys, offsets, array ordering, bitmap
  cardinality, run ordering, run overlap, run cardinality, and truncated
  containers.
- Exercise adversarial maximum run counts and assert validation cost remains
  linear in input bytes.
- Fuzz `Validate` and require no panic, bounded allocation, and bounded runtime.
- Assert `Validate` performs zero successful-path heap allocations and reports
  cardinality plus high/low container counts without constructing a bitmap.
- Reject valid bodies that exceed the per-filter decoded-resource admission limit.

### Cross-language conformance tests

- Check in fixed bytes for signed boundaries, empty membership, array, bitmap,
  and run containers, and multiple high-32 containers.
- Consume Go-generated fixtures in CRoaring C++.
- Consume independently generated CRoaring C++ fixtures in Go.
- Do not claim Java SDK interoperability until a Java-produced and
  Java-consumed fixture passes the same suite.

### SDK, parser, and proto tests

- Accept Go `[]int`, `[]int8`, `[]int16`, `[]int32`, and `[]int64`; verify every
  `[]int` element converts exactly to int64 without truncation before mapping.
- Keep `[]int` Go-specific and reject unsigned, string, boolean,
  floating-point, and other unsupported input slices.
- Verify native bytes-template marshaling without text or base64 conversion.
- Require exactly two arguments, a supported top-level field, a placeholder as
  the second argument, a present template value, and a bytes value of the
  correct format.
- Verify Search, Query, and Delete SDK options carry the bitmap through native
  `ExprTemplateValues` bytes without bypassing the high-level client.
- Reject JSON, dynamic, nested, array, non-integer, literal-list, and raw scalar
  forms.
- Reject `roaring_match` inside `element_filter`, including nested unary and
  binary forms, while allowing a document-level sibling conjunction.
- Materialize `RoaringFilterExpr` at oneof field 23 and verify proto round-trip.
- Reject malformed MRB1 at Proxy.
- Repeat one template across multiple AST leaves and verify each materialized
  expression remains valid while the serialized plan-size gate counts every
  embedded copy.
- Prove a Roaring plan is not classified as `BloomFilterExpr`.

### QueryNode logical and physical execution tests

- Reject malformed or unsupported MRB1 independently at QueryNode.
- Verify growing/sealed and raw/index parity for every integer width, signed
  boundaries, negative values, duplicates, empty sets, NULL values, and `NOT`.
- Verify iterative batches, `FilterBits`, non-contiguous/out-of-order/duplicate
  offsets, and NULL-validity preservation.
- Verify `bitmap_input` pruning and exact probe counts.
- Verify one parse per logical expression and shared membership across its
  physical segment expressions.
- Verify both Bloom and Roaring membership expressions are accepted in scorer
  filters at Proxy and at QueryNode `ParseScorer`, and that the boost path
  actually executes them: drive `ComputeScorerScores` over non-contiguous,
  out-of-order, repeating offsets and assert matched rows are boosted while
  unmatched and NULL rows are not. Assert the filter takes the native
  offset-input branch, so the assertions cannot silently degrade into testing
  the whole-segment bitset fallback.
- Verify diagnostic strings contain no bytes or members and the expression is
  non-cacheable.
- Verify QueryNode plan debug rendering redacts both small compact Roaring blobs
  and Bloom blobs, while preserving the original plan proto.

### Index-only and optimizer tests

- Compare `STL_SORT` index-only results and validity with raw-field results,
  including explicit offsets.
- Assert reverse-lookup count equals active `bitmap_input` candidates,
  including active candidates later found NULL, while membership-probe count
  equals active valid candidates only.
- Exercise small batches and verify the index cursor advances exactly once.
- Load raw data after expression construction and verify the construction-time
  data snapshot keeps path and cursors consistent.
- Reject bare `BITMAP` without an offset cache with `FieldNotLoaded`.
- Accept offset-cached `BITMAP` and capability-compatible `HYBRID` or other
  indexes.
- Verify `CanExecuteAllAtOnce() == false` prevents a whole-segment offset
  allocation.
- Verify selective numeric/indexed predicates precede both Bloom and Roaring in
  the post-selective membership tier and prune reverse lookups end to end.

### Delete and integration tests

- Delete using a positive exact set, a negated set, an empty set, and a negated
  empty set.
- Verify NULL field values are never selected by positive or negated membership.
- Reject malformed blobs before delete dispatch.
- Exercise mixed growing and sealed segments in search, query, and delete.
- Run dense, clustered, random-32-bit, and random-64-bit large-set smoke tests.
- Test old Proxy/new client, old QueryNode/new Proxy, and mixed QueryNode cases;
  every unsupported combination must fail and must never return unfiltered
  success.
- Test both privacy-safe upgrade strategies: the paired backport (redacted
  parser error AND plan-log unknown-field elision) and a Proxy/routing/version
  gate that prevents dispatch to any old worker that is not fully patched.
- Treat the partially patched worker (redacted error, no plan-log elision) as a
  prohibited combination, not a supported one: the upgrade check MUST verify
  both changes are present on a worker before it is eligible for field-23
  plans, because the error alone leaves the debug-log path exposed.
- With debug logging enabled, assert a QueryNode whose descriptor pool predates
  field 23 renders the preserved unknown field as count and byte size only.
- For every mixed-version rejection, assert returned errors and component logs
  contain no `MRB1` magic and no serialized or escaped blob bytes.
- Assert expression errors retain the template name without expanding its
  out-of-band bytes.
- Assert Proxy plan-log redaction covers pure Roaring and mixed Bloom plus
  Roaring without mutating the original plan.

### Performance tests

- Publish the distribution-sensitive size matrix described above rather than a
  universal member-count claim.
- Compare request size, client build, Proxy validation, QueryNode parse,
  decoded memory, probe throughput, and p50/p99 latency for `IN`, Bloom, and
  Roaring.
- Verify parse and validation are linear in blob size and that index-only
  execution stays batched.
- Exercise concurrent large blobs to establish a QueryNode/tenant-wide
  operational admission policy before broad enablement.
- Benchmark client construction with `-benchmem` for contiguous, shuffled
  INT32, Snowflake-like, admitted full-range INT64, and rejected full-range
  INT64 shapes; report time, bytes/op, allocations/op, output bytes, and
  bytes/member.

## Rollout Plan

1. Merge this MEP and the native bytes-template transport prerequisite.
2. Land the additive proto field 23 together with Proxy and QueryNode support,
   including the bounded, redacted unsupported-expression default error; keep
   client use opt-in.
3. Before mixed-version routing, either backport that redacted error *and* the
   plan-log unknown-field elision to every old QueryNode eligible during the
   rollout, or deploy a Proxy/routing/version gate that prevents field-23 plans
   from reaching nodes missing either change. The error alone is insufficient:
   the plan is rendered to the debug log before the expression is rejected.
4. Complete codec fuzzing, Go-to-C++ and C++-to-Go golden fixtures, raw and
   index-only parity, delete safety, and mixed-version failure tests.
5. Roll all QueryNodes and Proxies. Do not publish, send, or enable client use
   until the privacy precondition is active and both components have completed
   rollout.
6. Release the Go SDK builder and user documentation. Release additional SDK
   builders only after their independent golden-vector conformance passes;
   Java specifically requires its own fixture before support is claimed.
7. Monitor blob sizes, validation failures and latency, decoded memory,
   index-only fallback use, slow-index rejection, and query p99. Add or tighten
   QueryNode/tenant-wide operational admission if concurrency results require
   it, without changing the MRB1 version 1 format limit.

No data migration or index rebuild is part of rollout or rollback.

## Rejected Alternatives

### Use `IN` with a repeated raw list

`IN` is exact and remains the simplest choice for many sets, but its request and
plan payload grows roughly with member count. It also does not provide the
client-built distribution-aware compression contract specified here.

### Use Bloom with an application recheck

An application recheck cannot recover exact negative/exclusion semantics or
safe server-side delete after Milvus has already selected rows using an
approximate predicate. Exactness must be part of the predicate evaluated by
Milvus.

### Encode Roaring as `MBF1 algo=2`

Rejected. MBF1 fields such as target false-positive rate, Bloom block count,
and Bloom algorithm identifier do not describe an exact Roaring bitmap. An
independent MRB1 version space and dedicated plan node keep exact/delete
semantics explicit. This decision supersedes the old `algo=2` future-work
sketch in the stacked Bloom proposal.

### Overload `IN` based on template type

Rejected because it hides a materially different payload and validation
contract, complicates old-node behavior, and makes plan auditing dependent on
the runtime template type.

### Build the bitmap at Proxy from a raw list

Rejected because it preserves the client-to-Proxy transfer cliff and moves
deduplication, bitmap construction, and transient memory pressure into the
shared Proxy tier.

### Keep a generic `CallExpr` on the plan wire

Rejected because it loses the dedicated field-23 fail-closed node, obscures
exactness from delete validation, and defers function dispatch into the
execution engine.

### Use a dense bitset

A dense bitset is efficient for a known bounded dense domain but performs
poorly for sparse or wide domains. Roaring adapts container representation to
local density, although it is still not guaranteed to compress random full
64-bit IDs.

### Persist or register bitmap handles in version 1

Reusable handles may reduce repeated transfer, but they require lifecycle,
authorization, invalidation, quota, persistence, and distributed-cache design.
That is separate future work.

### Add index-native enumeration in version 1

Testing each distinct indexed value once and unioning postings may accelerate
some workloads, but it requires a larger scalar-index API and capability
contract. Version 1 uses the bounded exact fallback instead.

## References

- [Boolean expression baseline](./20220105-query_boolean_expr.md)
- [Delegator-side segment predicate pruning](./20260324-segment_filter_pk_predicate_pruning.md)
- [Regex filter execution and optimizer precedent](./20260409-regex_filter.md)
- [Scalar index version management](./20260313-scalar_index_version_management.md)
- [JSON path scalar-index types and rolling-upgrade gating](./20260410-json_path_index_multi_type.md)
- [Bloom Filter Membership Expression: `bloom_match`](./20260707-bloom-filter-expression.md)
- [RoaringBitmap portable format specification](https://github.com/RoaringBitmap/RoaringFormatSpec)
- [RoaringBitmap/roaring Go library](https://github.com/RoaringBitmap/roaring)
- [RoaringBitmap/CRoaring C++ library](https://github.com/RoaringBitmap/CRoaring)
