# HTTP body compression for the RESTful API

This design uses commit `efd7ef8d11` on `milvus-io/milvus` `master` as its baseline. The proxy's RESTful server can compress response bodies with zstd or gzip and can accept request bodies that a client compressed with either one. Both directions use standard HTTP content coding (RFC 9110 §8.4, §12.5.3, and RFC 7694), so a client that sends neither header is served exactly as before. A client that labels a plain body with a registered coding such as `gzip` is not: that body used to be parsed as JSON and is now rejected (see Compatibility). Response compression is on by default and can be switched off at runtime. Request decompression does not depend on that switch: a client that compresses a body gets it accepted by any server with this change.

gRPC is out of scope. On a shared port, the proxy dispatches gRPC requests before this layer, and gRPC negotiates its own message compression.

## Why

Before this change the RESTful server never compressed anything. A client that sent `Accept-Encoding: gzip` got plain JSON back. A client that sent a gzip body got HTTP 200 with `ErrIncorrectParameterFormat` (1801), because the compressed bytes were parsed as JSON. That error looks the same as a JSON syntax error, so the client cannot tell that the real problem is the encoding.

Query results with many scalar or text fields compress well, and so do large insert batches. Float vectors written as JSON numbers compress about 2.4 times with either codec, mostly by undoing the cost of spelling floats as decimal text. For vector-heavy query and search results, the binary vector encoding (`HTTPHeaderFloatVectorEncoding`) removes that cost without spending CPU; its base64 output does not compress further.

## Behavior

The code lives in `internal/distributed/proxy/httpserver/httpcompress` and has two parts.

- **`Handler`** is an `http.Handler` for response compression and the advertisement header. It wraps the RESTful chain outside DQL streaming and gin: `httpHandler(httpcompress.Handler(DQLStreamingHTTPHandler(gin)))`. `httpHandler` dispatches gRPC before it.
- **`RequestBodyMiddleware`** is a gin middleware for request decoding. It runs after global authentication. On the five v2 DQL routes (query, get, search, advanced search, and hybrid search), the proxy also checks DQL admission before decoding. With admission enabled, a full DQL queue returns HTTP 429 before the middleware reads the body. A request that matches no route is not decoded either, since nothing reads its body. When authorization is disabled, anyone who can reach the port can already write data, and the size limit below is the bound.

The proxy negotiates ordinary REST responses before dispatch so it can reject a request before a write takes effect. In the table, `identity` means an uncompressed response. Request decoding is independent of the response compression switch. Both coding headers use the HTTP server’s existing request-header size limit (`proxy.http.maxHeaderBytes`).

| Request | Result |
| --- | --- |
| No `Accept-Encoding`, no `Content-Encoding` | Same body and status as before. The response also carries `Accept-Encoding: zstd, gzip`, and ordinary REST responses carry `Vary: Accept-Encoding` with either switch setting. |
| `Accept-Encoding` allows zstd or gzip | With compression enabled, the proxy prefers the higher-weighted codec, with zstd first on a tie. An explicit higher weight for `identity` selects a plain response. A coding listed with `q=0` is excluded even when `*` allows others. Compressed responses carry `Content-Encoding` and drop the original `Content-Length`. |
| Client allows `identity` | The proxy can send a plain response below `minResponseSize` (default 1024 B) or when all encoder slots are occupied. |
| Client excludes `identity` with `identity;q=0` or with `*;q=0` and no explicit override for `identity` | If gzip or zstd is enabled and acceptable, the proxy reserves a slot before dispatch and compresses even small bodies. It returns an empty HTTP 406 when no available coding is acceptable, including when compression is disabled, or an empty HTTP 503 when all slots are occupied. Neither rejection invokes the route. |
| `HEAD` or Milvus `OPTIONS` | These bodyless requests skip response coding negotiation. |
| Status 1xx, 204, or 304, or a handler that already set `Content-Encoding` | The middleware leaves bodyless responses and handler-provided encodings alone. |
| `Content-Encoding: zstd`, `gzip`, or `x-gzip` | The middleware decodes the body after authentication and any applicable DQL admission check. Handlers see a plain body, `Content-Encoding` removed, and `Content-Length` set to the decoded size. The decoded bytes are also cached under gin's `BodyBytesKey`. `ShouldBindBodyWith` and the v2 insert/upsert binder read that cache first, so they do not read the request into a second buffer. An empty gzip member or zstd frame decodes to an empty body, which the route validates as usual. `identity` is ignored. |
| Zero bytes on the wire with a declared coding | `gzip` and `x-gzip` require a gzip header and return HTTP 400, code 1100. The zstd decoder accepts zero bytes and passes an empty body to the route. |
| Any other registered content coding (`br`, `deflate`, `compress`, ...) or stacked codings such as `gzip, zstd` | HTTP 415, `Accept-Encoding: zstd, gzip`, and a JSON body with code 1100 (`ErrParameterInvalid`). The handler does not run. |
| A value that is not a registered content coding, such as a charset sent as `Content-Encoding: UTF-8` | Ignored, as before this feature. Apache HttpClient's `setContentEncoding("UTF-8")` produces exactly this header. |
| Corrupt gzip or zstd frame, including a body that reaches EOF normally before the frame is complete | HTTP 400, code 1100. A zstd stream ending at a frame boundary is complete as far as the decoder can tell. |
| Timeout while reading the compressed request body | HTTP 408, code 10001. |
| Transient transport failure while reading the compressed request body | HTTP 503, code 2. |
| Caller cancels the request during the upload | The middleware records code 10000 for the access log and aborts without writing a JSON response. |
| A zstd frame that needs a window over 8 MiB: one that declares such a window, or a single-segment frame (no window descriptor) whose content is over 8 MiB, since its window is its whole content. libzstd writes these at levels 20 to 22 or with long-distance matching (`zstd --ultra -20`, `--long`), never at level 19 or below. | HTTP 400, code 1100, with the decoder's own message. RFC 9659 §3 forbids such frames in HTTP; the decoded-size limit plays no part. |
| Body that expands past `proxy.grpc.serverMaxRecvSize` (128 MiB in the shipped config) | HTTP 413, code 1102 (`ErrParameterTooLarge`). |

The middleware records the code and message on the Gin context for the access log. When it sends an error response, it sets the HTTP status and uses the RESTful `{"code", "message"}` envelope. Each rejection happens before the route handler runs.

A compressed response with no `Content-Type` gets one sniffed from its first 512 bytes, because net/http does not sniff a body whose `Content-Encoding` is set. RESTful handlers set `Content-Type` themselves, so this only keeps the previous behavior for a handler that does not. A header that is flushed before any body byte gets no type, as in net/http, and a zero-length write does not decide the response. The compressing writer forwards `CloseNotify`, which gin asserts without checking, so `c.Stream` works whatever the client negotiated.

With the switch off, an ordinary REST request receives an uncompressed response if it allows `identity`, or HTTP 406 if it excludes `identity`. Both outcomes carry `Vary: Accept-Encoding`. The proxy advertises request decoding with either setting.

## How a client knows the server supports it

Response compression needs no discovery. The client always sends `Accept-Encoding` and checks `Content-Encoding` on the response. Older servers ignore the header.

Request compression needs a signal, because an older server answers a compressed body with HTTP 200 and error 1801 instead of 415. The bind fails before any proxy call, so nothing is written. Every response from a server with this change carries `Accept-Encoding: zstd, gzip`, as RFC 7694 §3 allows, whatever the switch says. The header describes the instance that answered. Behind a load balancer, during a rolling upgrade or after a rollback, the next request can reach an instance without this change, so a client needs the fallback below in any case. It can choose when to start compressing:

- **Probe first.** Send one small uncompressed request, such as listing collections, and compress later request bodies only if that response advertised a coding. Keep the fallback below for a request that reaches an older instance anyway.
- **Try and fall back.** Send the body compressed. On HTTP 200 with code 1801 and no `Accept-Encoding` header in the response, resend it uncompressed, and stop compressing for that endpoint for a while. Nothing ran the first time, so the resend has no side effect. A server with this change also answers 1801 for the client's own mistakes, such as malformed JSON or a wrong field type, but always with the header, so that answer says nothing about compression and needs no resend.

## Compatibility

- **Responses.** Clients that negotiate gzip automatically start receiving it: Go `net/http`, Python `requests`, Apache HttpClient, OkHttp, Node `fetch`, browsers. All of them also decode it transparently. Clients that explicitly exclude `identity` can now receive a bodyless 406 or 503 before their request runs. Code that sets `Accept-Encoding` by hand but does not decode the response would see compressed bytes. None was found in the SDKs, Cloud components, or test frameworks.
- **Request bodies.** Before this change the server ignored a request's `Content-Encoding`. Now, on every matched v1 and v2 route, a plain JSON body labelled `gzip`, `x-gzip`, or `zstd` is rejected with 400, and one labelled with any other registered coding gets 415, even on a request without a body, such as a v1 GET with `Content-Encoding: br`. The switch does not change this, since decoding does not depend on it. The proxy continues to ignore `identity` and values such as `UTF-8`. A client that sends no body bytes with `Content-Encoding: gzip` now receives HTTP 400, including on a bodyless GET. Nothing in this repository sends a request `Content-Encoding`; a client or gateway that mislabels plain bodies would now be rejected.

## Configuration

| Key | Default | Refreshable | Meaning |
| --- | --- | --- | --- |
| `proxy.http.compression.enabled` | `true` | yes | Compresses responses for clients whose `Accept-Encoding` allows it. Request decompression does not depend on it. |
| `proxy.http.compression.minResponseSize` | `1024` | yes | When the client accepts `identity`, the proxy can send responses below this many bytes uncompressed. Up to this many bytes of each response are held in memory until that is known, outside the encoder slots, so keep it small. |

## Resource bounds

- **Decompression bombs.** A plain body is bounded by the bytes the client actually sends, but a compressed one is not: gzip reaches about 1000:1 on repetitive input and zstd tens of thousands to one, so a few hundred KiB could expand to gigabytes. The decoded body is therefore read through a limit of `proxy.grpc.serverMaxRecvSize + 1` bytes and cut off after at most one extra byte. The limit reuses the existing bound on a single gRPC request to the proxy, so one setting governs both protocols. The two are not equivalent: JSON spells the same rows in roughly 3 to 5 times the bytes of protobuf, so a compressed REST insert reaches the limit with fewer rows than a gRPC request does, and a plain REST body has no limit of its own beyond the bytes the client sends. The REST side reads the value on every request, while the gRPC server reads it once at startup. The zstd decoder also rejects frames that need a window larger than 8 MiB, the most RFC 9659 lets an HTTP zstd encoder use, because it allocates its history from the frame header before producing any output. A single-segment frame needs a window as large as its content. Without that cap a few hundred bytes could reserve up to 512 MiB, the library's default limit. The same reasoning is in `internal/util/grpcclient/compression_zstd.go`.
- **Request memory.** Decoding is eager: the body is read and decoded in full before the route handlers run, into one slice of exactly the decoded size (`io.ReadAll` needs about twice that while it reads). The slice is cached under gin's `BodyBytesKey`, so a binder that looks there does not read the request into a second buffer: `ShouldBindBodyWith` reads the cache first, and so does `bindCollectionDataMetadata`, the binder of v2 insert and upsert, which used to read the request body unconditionally. That binder parses the slice in place, so a compressed v2 insert or upsert holds one copy of its decoded body, as a plain one holds one of its body. Other routes bind through gin's JSON binding, which copies the body again while decoding it, for a plain body and a decoded one alike. Four v1 routes (collection create and drop, query, search) bind with `ShouldBindWith`, which ignores the cache and reads the decoded body from the request, so there the cached slice is one copy more than a plain request holds. Handlers keep references into the slice (row conversion borrows it through `unsafe.String`), so it is never pooled, and gin's pooled context keeps it until the context is reused. The decoder has no separate concurrency limit. Each admitted compressed request can expand to the limit, and the peak heap while it is bound measured about 3 to 4 times its decoded size with `encoding/json` and more with `sonic`. Sixteen zstd requests totalling 231 KB on one HTTP/2 connection took the heap to about 6.7 GiB at the 128 MiB limit. A plain body costs as much memory but needs every byte on the wire. The gRPC port has the same amplification today, before authentication. An exposed proxy should run with authorization on, limit connections and streams in front of it, or lower `proxy.grpc.serverMaxRecvSize`.
- **What eager decoding costs.** An admitted request uploads and decodes its compressed body before the route starts its `Request-Timeout` clock. With `proxy.http.readTimeout` at its default of 0, a stalled compressed upload can hold the goroutine until the client disconnects.
- **Response memory.** An encoder is held for the whole response, including while a slow client drains it. At most `8 × GOMAXPROCS` responses hold one at a time, with GOMAXPROCS taken as the process starts, before Milvus adjusts it to a container's CPU limit. Clients that allow an uncompressed response acquire a slot only when the writer chooses to compress. Clients that require compression reserve a slot before dispatch and hold it through request handling. The writer acquires the encoder when it starts a compressed body. zstd uses `SpeedFastest` with a 512 KiB window. On the benchmark payloads below, that window compresses within 0.1% of a 1 MiB window and retains 1.8 MB per encoder instead of 2.9 MB; smaller windows save nothing more. A gzip `BestSpeed` encoder retains 0.85 MB. On a 16-core proxy the cap bounds the encoders in use at about 240 MB. Idle encoders stay in their pools until garbage collection drops them, and zstd and gzip have one pool each, so after bursts of both about 340 MB was measured.
- **Codec state.** Encoders and decoders come from `sync.Pool`s and run at concurrency 1, so they start no goroutines and need no `Close` when the pool drops them. A pooled zstd decoder keeps the history its last frame needed, about 9 MiB after a frame with the largest allowed 8 MiB window. A pooled gzip reader keeps its own small read buffer but no reference to the request body. Decoders are not capped the way encoders are.

## Codec choice

Both codecs come from `github.com/klauspost/compress`. Milvus already depends on it for gRPC zstd/s2/snappy. It is pure Go, so no cgo build or per-call cgo overhead, and its gzip is a drop-in replacement for the standard library's. Each payload below was encoded once per iteration with a reused encoder, the same way the pools are used. The zstd rows were measured with a 1 MiB window; the 512 KiB window in use gives the same ratio to three significant digits. The machine was an Apple M-series laptop with Go 1.26, so expect different absolute numbers on Linux amd64.

| Payload | Codec | Encode time | Ratio |
| --- | --- | --- | --- |
| 449 KiB query result: 1000 rows of id, score, category, title, 40-word text | zstd `SpeedFastest` (used) | 1.37 ms | 3.38 |
| | zstd `SpeedDefault` | 1.76 ms | 3.46 |
| | klauspost gzip level 1 (used) | 1.07 ms | 3.04 |
| | klauspost gzip level 6 | 2.00 ms | 3.26 |
| | stdlib gzip level 1 | 2.04 ms | 3.02 |
| | stdlib gzip level 6 | 5.70 ms | 3.28 |
| 840 KiB search result: 100 rows with 768-d float vectors | zstd `SpeedFastest` (used) | 3.67 ms | 2.43 |
| | zstd `SpeedDefault` | 5.35 ms | 2.31 |
| | klauspost gzip level 1 (used) | 3.45 ms | 2.33 |
| | stdlib gzip level 6 | 39.5 ms | 2.45 |

Decoding the text payload ran at 1675 MB/s for zstd, 452 MB/s for klauspost gzip, and 312 MB/s for stdlib gzip.

- **Levels.** The fastest levels are on the efficient frontier. Raising zstd to its default level costs 28–46% more CPU, for a 2% better ratio on text and a slightly worse one on vectors. gzip level 6 doubles the CPU cost for 7% more.
- **Vectors.** Compression of vector-heavy JSON mostly removes the cost of writing floats as decimal text. The binary vector encoding (`HTTPHeaderFloatVectorEncoding`) achieves that without spending CPU.
- **Alternatives not used.** cgo bindings to libzstd add a cgo build dependency and per-call overhead, and they win mainly at high levels this layer does not use. Brotli (`andybalholm/brotli`, already an indirect dependency) was measured on the same text payload. At level 0 it took 1.24 ms for a ratio of 2.93, worse than gzip level 1. At level 4 it took 4.44 ms for 3.40, about zstd's ratio at three times the CPU. Browsers, its main users, also accept zstd now. A client that sends `Accept-Encoding: br` implicitly allows an uncompressed response, and a `br` request body gets 415. Parallel gzip (`pgzip`) speeds up a single large stream, but it works against the one-goroutine-per-request model. `klauspost/compress/gzhttp` from the same module handles responses only, so the request decoder would still be needed, and keeping this design's negotiation (`*`, `x-gzip`, `q=0` against `*`) and enforcing the encoder slot limit takes about as much glue as the writer it would replace.

## Metrics

Response compression sits outside gin, so gin's byte counters see the uncompressed response. `RestfulSendBytes` and the per-route outbound payload metrics keep measuring the logical payload.

On the receive side, `RequestBodyMiddleware` sets `Content-Length` to the decoded size. The per-route `restfulSizeMiddleware`, which feeds `ProxyReceiveBytes`, runs after it and sees the decoded size. That matches gRPC, whose stats handler records `InPayload.Length` and not the wire length. The global `MetricsHandlerFunc` runs first in the chain, before authentication, so `RestfulReceiveBytes` sees the wire size of a compressed request that declares its length, and nothing for one that does not, as for a plain body without a declared length. For a compressed request the two receive counters therefore measure different things. The send counters measure a response before compression. Metrics for compression ratio are not part of this change.

## Client configuration

| Client | Responses | Request bodies |
| --- | --- | --- |
| curl | `--compressed` sends `Accept-Encoding` for the codings curl was built with, and decodes the response. | Compress the body yourself, then send `-H 'Content-Encoding: zstd' --data-binary @body.json.zst`. |
| Python `requests` | gzip works by default. For zstd, install `urllib3[zstd]` (urllib3 2.8 pulls in `backports-zstd` on Python < 3.14 and uses the standard library on 3.14). urllib3 then adds `zstd` to its default `Accept-Encoding` and decodes it. With urllib3 2.8, installing only `zstandard` does not enable zstd. | Compress with `gzip`, `compression.zstd`/`backports.zstd`, or `zstandard`, then set `Content-Encoding`. |
| Go `net/http` | The default transport asks for gzip and decodes it transparently. For zstd, set `Accept-Encoding: zstd` and decode the body yourself; the transport then leaves it untouched. | Compress the body and set `Content-Encoding`. |
| Browsers | Negotiated automatically. | Not used. CORS `Access-Control-Allow-Headers` does not list `Content-Encoding` yet. |

## Testing

`httpcompress_test.go` covers the following:

- The switch off, which stops response compression only, and a plain client with the switch on.
- `Accept-Encoding` weighting, explicit `identity`, `*`, `q=0`, and invalid weights.
- The minimum response size at its boundary and required gzip or zstd on a smaller body. Tests for 406 and 503 verify that the handler has not run.
- zstd, gzip, x-gzip, and identity request bodies, the receive limit at `math.MaxInt64`, and a decoded body allocated without growth slack.
- A single-segment zstd frame of exactly 8 MiB, accepted, and one byte over, rejected.
- Long coding lists use the same response negotiation and request decoding rules as shorter lists.
- Unsupported coding (415), corrupt or truncated frames and oversized zstd windows (400), and a decoded body above the size limit (413).
- Compressed uploads that time out (408/10001), encounter a transport failure (503/2), or are canceled by the caller (access-log code 10000, no JSON response).
- Zero-byte uploads and valid encoded empty bodies under gzip and zstd, unmatched routes, and the decoded body cached for `ShouldBindBodyWith`.
- Content-Type sniffing, which copies at most 512 bytes of the write it samples and is skipped for a header flushed before the body, a handler-set Content-Length, a large write after a small one, 1xx statuses, encoder slot exhaustion and return, and an aborted response that is not a valid stream.
- Error statuses, 204, HEAD, bodies that the handler already encoded, many small writes, `Flush`, and `http.ResponseController`: its `Flush`, and `SetWriteDeadline` reaching the connection through `Unwrap` on a real server.
- A handler that writes nothing, which leaves the response to net/http, a zero-length first write, which does not decide the response, and a status sent after the header, which is ignored.
- A client that goes away: the failed write is returned to the handler, nothing panics, and the encoder slot comes back.
- The `http.ErrAbortHandler` panic that DQL streaming raises.
- gin's `c.Stream` with a client that asked for zstd.
- Runtime toggling, and concurrent requests under `-race`.
- Real Go clients over a real connection.

`compression_e2e_test.go` serves the v2 routes behind a copy of the proxy's chain: h2c, compression, DQL streaming, then gin with a stand-in authenticator and a mocked proxy. It covers the following:

- A query larger than one DQL batch, answered to a client that sends no `Accept-Encoding`, to one that asks for zstd, to one that asks for gzip, and to Go's default transport.
- An admitted gzip query returns its rows. A separate route test fills the DQL queue and checks all five DQL routes with gzip and zstd. Each returns 429 and `Retry-After: 1` without reading the compressed body.
- Plain, zstd, and gzip insert bodies, each reaching `Insert` with the decoded row. The handler binds a decoded body without reading it from the request a second time.
- A `br` body, rejected with 415 before the handler runs.
- With authentication required, a body that would expand past the limit is rejected with 401 before decoding, and only an authenticated request reaches the 413.
- With the switch off, responses are uncompressed and advertised, and a zstd insert body is still accepted.

`TestBindCollectionDataMetadataReusesCachedBody` in `handler_v2_test.go` checks the insert binder directly: with a body cached under `BodyBytesKey` it does not read the request and keeps the same buffer.

`service_test.go` starts the proxy's real HTTP server in shared-port mode. `Test_NewHTTPServer_PortShare_Compression` checks that a RESTful request over h2c gets a zstd response and the advertisement header, that a `br` body on a v2 route gets 415 and a corrupt gzip body on a v1 route gets 400, and that a gRPC health check on the same port succeeds without any of this layer's headers. `Test_NewHTTPServer_DecodesOnlyAfterAuthentication` runs the same server with authorization on: without credentials a `br` or corrupt body gets 401, and with credentials it gets 415 or 400, so the decoder runs after authentication on the real chain.

The same server was also exercised by hand with external clients. One was curl 8.7, which has no zstd: its default, `--compressed`, and explicit `Accept-Encoding: zstd` (decoded with the zstd CLI), plus zstd, gzip, and br request bodies. The other was Python `requests`, both with urllib3 1.26 and with 2.8 plus `urllib3[zstd]`. Each was run with the switch on and with it off. With it off, responses came back uncompressed but advertised, and zstd and gzip insert bodies were still accepted.

## Follow-ups

- Add metrics for wire bytes and compression ratio.
- Add `Content-Encoding` to the CORS allow list if browser clients ever need to send compressed bodies.
- Add SDK support. pymilvus uses the RESTful API only for its bulk-import helpers, through `requests`, which negotiates and decodes gzip by itself, so it is unaffected. Clients built on RESTful can adopt the discovery rule above.
