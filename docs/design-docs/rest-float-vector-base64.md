# Optional Base64 FLOAT_VECTOR responses

## Scope and wire contract

V2 Query, Get, Search, Advanced Search and Hybrid Search accept an optional
top-level request property, `outputVectorEncoding`:

- Omitted, `null`, `""` or `"array"`: preserve existing numeric-array output.
- `"base64"`: return each FLOAT_VECTOR as a standard padded Base64 JSON string.
- Other values or types: reject during request binding, before Proxy RPCs,
  using the existing incorrect-parameter-format response.

Each string encodes contiguous IEEE 754 binary32 coordinates in little-endian
order, with no dimension prefix or line breaks. For example, `[1, -2]` becomes
`"AACAPwAAAMA="`. Dimension is the decoded byte length divided by four.
Null vectors stay JSON `null`; finite coordinates, including negative zero,
preserve their exact bits. Nonfinite coordinates remain encoding errors.

The option applies to typed FLOAT_VECTOR fields, including those inside struct
arrays. Scalar arrays, dynamic/JSON fields, IDs, scores, and other vector types
keep their existing representations. Search aggregation has no typed
FLOAT_VECTOR hit value in its current response schema and is unchanged.
Search inputs, insert/upsert, V1, gRPC and SDK APIs are not changed.

## Conventions and implementation

Adopt [RFC 4648 standard Base64](https://www.rfc-editor.org/rfc/rfc4648#section-4)
and Go's [TextMarshaler extension](https://pkg.go.dev/encoding#TextMarshaler).
Adapt Milvus's existing little-endian float-vector byte convention
(`typeutil.Float32ArrayToBytes`); Base64 alone does not define byte order or a
numeric type. The request property is a Milvus API choice, not an industry
standard. Do not adopt a custom framing protocol, lossy rounding, or a library
replacement.

Projection retains a typed view over the existing float32 slice. Sonic invokes
its TextMarshaler only when encoding that field. The TextMarshaler reserves one
backing allocation for binary coordinates and their Base64 representation.
It does not cache all converted vectors in the result maps. This also avoids
Sonic 1.15.2's ARM64 `[]byte` encoder's repeated small buffer growth when a batch
contains many vectors. Sonic quotes/escapes the returned text and copies it into
its normal encoding buffer. Using TextMarshaler instead of JSONMarshaler avoids
an additional JSON validation/compaction pass without changing encoder options.

Existing single-batch buffering, multi-batch buffer reuse, HTML escaping,
timeout ownership, cancellation checks and partial-response failure behavior
are unchanged. This is not zero-copy and does not bound total server RSS.
Each vector still allocates temporary bytes; concurrent results still retain
the original Proxy response and projected row maps.

## Client use and compatibility

Clients must opt in and decode Base64, then interpret the bytes as little-endian
float32 values. For example, in Python:

```python
import base64
import struct

raw = base64.b64decode("AACAPwAAAMA=", validate=True)
if len(raw) % 4:
    raise ValueError("invalid float32 vector byte length")
vector = struct.unpack(f"<{len(raw) // 4}f", raw)  # (1.0, -2.0)
```

Older servers may ignore unknown request properties: clients must verify that
the field is a string before applying Base64 decoding, or verify server support.
Base64 is encoding, not encryption or compression. It can be larger than short
JSON numbers such as `0` and `1`; no universal payload or latency improvement is
claimed.

## Verification

Tests cover default output, all five routes, the 256/257-row buffered/streamed
boundary, invalid options before RPC, nullable and nested vectors, unmodified
dynamic arrays, HTML escaping, finite bit fidelity and nonfinite rejection.
Codec benchmarks include Sonic's real TextMarshaler string quoting and HTML
escaping; prior raw-byte Base64 benchmarks are not performance evidence for this
implementation. End-to-end CPU, latency, memory and client-decoding benefits
require a subsequent same-workload comparison.
