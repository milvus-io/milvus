# MEP: Bound proxy REST body reads with a per-request read/write deadline

Current state: In Progress

ISSUE: [[Enhancement]: Bound proxy REST body reads with a read budget (readTimeout ships as 0s) #53074](https://github.com/milvus-io/milvus/issues/53074)

Keywords: Proxy, REST, HTTP server, DoS, Timeout, gRPC shared port

Released: N/A

## Summary

`proxy.http.readHeaderTimeout` defaults to `5s`, but `proxy.http.readTimeout` defaults
to `0s` (disabled). `0s` means "no deadline" for the entire request body read. A
client that sends valid headers with a declared `Content-Length` and then withholds
(or trickles) the body pins one goroutine, one TCP connection, and one file
descriptor on the proxy indefinitely, for the cost of a few bytes and an idle
connection. This happens on every REST route, because every route is wrapped by the
same `wrapperPost` → `gCtx.ShouldBindBodyWith(...)` body-read call.

`proxy.http.writeTimeout` has the identical `0s` gap on the response-writing side and
is fixed alongside `readTimeout` here.

## Revision note

The first version of this change set `readTimeout`/`writeTimeout` directly on the
shared `http.Server{}` (`internal/distributed/proxy/service.go`). PR review (both a
maintainer pass and an automated multi-agent review) found that this is a real
regression, not just a style concern: in the **default** deployment
(`proxy.http.port` empty), this proxy's `http.Server` also carries external gRPC
traffic over the same shared HTTP/2 listener (`listener_manager.go`'s
`portShareMode`, `service.go`'s `httpHandler` dispatching to
`grpcExternalServer.ServeHTTP`), and Go's HTTP/2 implementation
(`golang.org/x/net/http2/server.go`) arms **per-stream** read/write deadlines
directly from `Server.ReadTimeout`/`WriteTimeout`. Setting those fields would
therefore also cut long-running gRPC RPCs in the default configuration -- confirmed
by reading both Milvus's dispatch code and the vendored `x/net/http2` source, not
just asserted. This directly contradicted this doc's own stated non-goal ("REST
path only") and reversed a deliberate `0s` decision made in an earlier PR (#49951)
for exactly this reason.

The design below is the corrected version: the deadline is applied per-request, via
`http.ResponseController`, inside gin middleware that gRPC traffic structurally
cannot reach -- never via the shared `http.Server` struct.

## Why the existing per-request timeout doesn't already cover this

`timeoutMiddleware` (`internal/distributed/proxy/httpserver/timeout_middleware.go`)
races the handler against `proxy.http.requestTimeoutMs` (default `30s`) in a
goroutine, and on timeout writes a response to the client and returns. It does
**not** stop the spawned goroutine -- Go has no API to forcibly kill a goroutine, and
a raw `net.Conn.Read()` is not `context.Context`-aware, so cancelling the request's
context does nothing to a read already blocked on the socket. The client sees a
prompt timeout response; the leaked goroutine/connection/fd underneath is unaffected.

The same missing deadline also affects early-return paths (a validation error, or
the pre-decode 429 from DQL admission): after the handler returns, `net/http`'s own
`finishRequest` drains any unread body so the connection can be reused for
keep-alive -- another unbounded socket read on the same underlying connection.

## Design

Add `httpserver.BodyDeadlineMiddleware()`
(`internal/distributed/proxy/httpserver/body_deadline_middleware.go`), a gin
middleware that, per request, calls
`http.NewResponseController(gCtx.Writer).SetReadDeadline` /
`.SetWriteDeadline` using `proxy.http.readTimeout` (`30s`) /
`proxy.http.writeTimeout` (`60s`).

It is registered as the **first** global middleware on the gin engine
(`ginHandler.Use(httpserver.BodyDeadlineMiddleware())` in
`internal/distributed/proxy/service.go`, before `MetricsHandlerFunc` and everything
else) for two reasons:

1. **Correctness**: `http.ResponseController` needs to reach the real,
   connection-backed `http.ResponseWriter` to set a deadline on the underlying
   connection. `timeoutMiddleware` (registered per-route, deeper in the chain) swaps
   `gCtx.Writer` for a `timeoutResponseRecorder` that buffers into memory and has no
   `Unwrap()` method, so `http.ResponseController` cannot reach through it. Running
   before that swap is not optional -- it is required for the deadline call to work
   at all.
2. **gRPC safety**: `internal/distributed/proxy/service.go`'s `httpHandler` diverts
   gRPC requests to `grpcExternalServer.ServeHTTP` *before* gin's `ServeHTTP` is ever
   invoked (`if r.ProtoMajor == 2 && ...application/grpc... { grpcExternalServer.ServeHTTP(w, r); return }`).
   gin middleware -- this one included -- therefore never executes for a gRPC
   request, regardless of where in the gin chain it's registered. This is what makes
   the approach immune to the shared-port issue described above, structurally, not
   just by choosing a "safe-looking" value.

`http.Server.ReadTimeout`/`WriteTimeout` (`internal/distributed/proxy/service.go`)
are left unset (Go zero value, `0` = disabled), restoring the pre-existing,
deliberate behavior for the shared gRPC/REST server.

### Alternatives considered

**`http.Server.ReadTimeout`/`WriteTimeout`** (the original version of this change).
Rejected: regresses gRPC in the default deployment, as detailed in the Revision
note above.

**Per-route deadlines set inside `wrapperPost` directly**, rather than as a global
gin middleware. Equivalent in effect, since gRPC never reaches gin either way, but
would require touching each of the ~30 route registrations in `handler_v2.go`
instead of one `Use()` call, for no additional safety benefit. Rejected as
unnecessary surface area.

**Documentation-only fix** (tell operators to raise `readTimeout` themselves).
Rejected as insufficient on its own -- it leaves the default installation exposed.

## Non-goals

- Per-route/per-endpoint timeout *values* (all REST routes share one budget via
  `proxy.http.readTimeout`/`writeTimeout`). Per-route deadlines were considered
  above only as an alternative *placement*, not as differing values.
- A request body size cap (`http.MaxBytesReader` or similar). This is a related but
  separate axis of the same general "unbounded request" class of problem (bytes vs.
  time) and is not addressed by this change.
- Any change to gRPC proxy timeouts. This is now enforced structurally (see
  Design), not just by convention.

## Changes

- `internal/distributed/proxy/httpserver/body_deadline_middleware.go` (new):
  `BodyDeadlineMiddleware()`.
- `internal/distributed/proxy/httpserver/body_deadline_middleware_test.go` (new):
  unit test for the unsupported-writer path, and an integration test that opens a
  real connection, sends a declared-but-incomplete body, and asserts the
  server-side handler goroutine is actually released and the connection closes --
  not just that a client-visible response eventually appears.
- `internal/distributed/proxy/service.go`: register `BodyDeadlineMiddleware()` as
  the first gin middleware; removed `ReadTimeout`/`WriteTimeout` from the
  `http.Server{}` literal (left at the Go zero value, `0`/disabled).
- `pkg/util/paramtable/http_param.go`: `ReadTimeout` default `0s` → `30s`,
  `WriteTimeout` default `0s` → `60s`; doc strings rewritten to describe the
  per-request/`ResponseController` mechanism and why it is not on `http.Server`.
- `configs/milvus.yaml`: regenerated reference values/comments for the two keys
  (generated via `make generate-yaml`; hand-mirrored here to match the generator's
  output format, since this environment's C++ core build was unavailable to run the
  generator directly -- should be double-checked with `make generate-yaml` in a full
  build environment before merge).
- `internal/distributed/proxy/service_test.go`: `Test_NewServer_HTTPServer_TimeoutDefaults`
  and `Test_NewServer_HTTPServer_TimeoutConfigOverrides` now assert
  `ReadTimeout`/`WriteTimeout` stay `0` on the shared `http.Server` regardless of
  config value -- a regression guard against re-wiring these into `http.Server{}`.

## Test Plan

### Unit tests

- `TestHTTPConfig_Init` (`pkg/util/paramtable/http_param_test.go`) -- asserts the
  `30s`/`60s` config defaults.
- `Test_NewServer_HTTPServer_TimeoutDefaults` /
  `Test_NewServer_HTTPServer_TimeoutConfigOverrides`
  (`internal/distributed/proxy/service_test.go`) -- assert `ReadTimeout`/`WriteTimeout`
  never reach the shared `http.Server`, regardless of config value.
- `TestBodyDeadlineMiddleware_UnsupportedWriterDoesNotBreakRequest`
  (`internal/distributed/proxy/httpserver/body_deadline_middleware_test.go`) --
  a writer that doesn't support deadlines (e.g. `httptest.ResponseRecorder`) must
  not break the request.
- `TestBodyDeadlineMiddleware_ReleasesHandlerGoroutineOnStalledBody`
  (same file) -- opens a real TCP connection to a real `httptest.Server`, sends
  headers plus a declared-but-incomplete body, and asserts (a) the handler's
  blocked body read actually returns within bounded time (proving the goroutine is
  released, not just that the client eventually sees a response) and (b) the
  connection is subsequently closed. **This closes the gap both PR reviews
  flagged** -- the previous version of this doc listed this as "not yet done."

### Verified in this environment

Both `body_deadline_middleware_test.go` tests were run successfully in an isolated
copy of the package (this sandbox cannot build `internal/distributed/proxy/httpserver`
directly, since sibling files in the same package transitively require the compiled
C++ core / rocksdb / rdkafka cgo dependencies, which are unavailable here). The
isolated copy exercises identical code and passed, including the real
connection/goroutine-release assertions above; it should still be re-run as part of
the package proper in a full build environment before merge.

### Not yet done

- Confirm no existing large-payload REST integration test (e.g. bulk `insert`) is
  slow enough on CI infrastructure to trip the `30s`/`60s` bounds.
- `configs/milvus.yaml` regeneration should be confirmed with `make generate-yaml`
  in a full build environment (see Changes above).

## References

- Issue: https://github.com/milvus-io/milvus/issues/53074
- Raised during review of PR #52986 (DQL admission / post-handler drain discussion).
- PR #49951: prior discussion establishing the deliberate `0s`/`0s` default for the
  shared gRPC/REST `http.Server`.
