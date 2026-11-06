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

## Revision history

**Revision 1** set `readTimeout`/`writeTimeout` directly on the shared
`http.Server{}` (`internal/distributed/proxy/service.go`). PR review (both a
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

**Revision 2** replaced the `Server`-level fields with `BodyDeadlineMiddleware`, a
**gin middleware** applying the deadline per-request via `http.ResponseController`,
registered as the first global `Use()` middleware (before `timeoutMiddleware` swaps
the response writer). The reasoning: gRPC requests are diverted to
`grpcExternalServer.ServeHTTP` inside `httpHandler` *before* gin's `ServeHTTP` is
ever invoked, so gin middleware structurally cannot affect gRPC traffic, regardless
of registration order.

That gRPC-safety reasoning was correct, but a follow-up review (two more P1
findings, with real-connection reproducers) found it incomplete in the *other*
direction: not every non-gRPC request actually reaches gin's middleware chain
either.

1. **gin's own trailing-slash/fixed-path redirect** (`RedirectTrailingSlash`,
   default `true`) is handled inside `Engine.handleHTTPRequest` *before* the
   middleware chain runs -- confirmed in gin's source (`gin.go`): the global
   `Use()` chain only executes via `c.Next()` on the `value.handlers != nil`
   (exact match) branch; the `value.tsr` (trailing-slash) branch calls
   `redirectTrailingSlash(c)` and returns directly, `c.Next()` is never called.
   A request whose path only differs from a registered route by a trailing slash
   never reaches `BodyDeadlineMiddleware` at all. Reproduced with a real
   connection: `POST /v2/vectordb/entities/search/` (trailing slash), declared
   `Content-Length: 100`, 1 byte sent -- zero middleware invocations, still
   blocked past 720ms, versus ~120ms release with the Revision-1-equivalent
   `Server.ReadTimeout`.
2. **`golang.org/x/net/http2/h2c`'s own upgrade-handshake handling** reads the
   entire request body itself, before invoking the wrapped handler at all --
   confirmed in the vendored source (`h2c.go`): `h2cHandler.ServeHTTP` calls
   `h2cUpgrade(w, r)` for any request matching `isH2CUpgrade(r.Header)` (i.e.
   carrying `Upgrade: h2c` / `Connection: Upgrade` / `HTTP2-Settings`), and
   `h2cUpgrade` does `io.ReadAll(r.Body)` directly, before `s.Handler.ServeHTTP`
   (which is where `httpHandler` → gin → `BodyDeadlineMiddleware` all live) is
   ever called. `h2c.NewHandler` is the *outermost* layer
   (`Handler: h2c.NewHandler(s.httpHandler(ginHandler), http2Server)`), so
   nothing registered inside gin can ever run early enough. Reproduced the same
   way: an `Upgrade: h2c` handshake request with a declared-but-withheld body
   stayed blocked past 720ms with zero middleware invocations.

Neither of these two paths is gRPC traffic: a trailing-slash mismatch is a REST
routing artifact, and the legacy `Upgrade: h2c` handshake is not how real gRPC
clients connect (grpc-go and essentially all production gRPC implementations use
"prior knowledge" -- sending the HTTP/2 client preface directly -- never this
HTTP/1.1-based upgrade negotiation). Critically, at the exact moment each of these
reads happens, the request is still `r.ProtoMajor == 1`: gin's redirect only fires
for a request that already reached gin as HTTP/1.1, and the h2c Upgrade handshake
is by definition still HTTP/1.1 until/unless it succeeds. Real gRPC, by contrast,
always arrives as already-negotiated HTTP/2 (`r.ProtoMajor == 2`) from the first
instant any handler sees it.

**Revision 3** (current) is the corrected version: `WrapWithBodyDeadline`
(`internal/distributed/proxy/httpserver/body_deadline_handler.go`) is a plain
`http.Handler` wrapper -- not gin middleware -- applied as the **outermost** layer
of `http.Server.Handler`, outside `h2c.NewHandler` entirely:

```go
Handler: httpserver.WrapWithBodyDeadline(h2c.NewHandler(s.httpHandler(ginHandler), http2Server)),
```

It checks `r.ProtoMajor`: if `2` (already-negotiated HTTP/2 -- real gRPC, always),
it does nothing and calls through immediately. Otherwise (still HTTP/1.1 -- plain
REST, gin's own redirects, or an in-progress h2c Upgrade attempt), it arms the
`http.ResponseController` read/write deadlines *before* calling through, so the
deadline is already active on the connection before `h2c.NewHandler`'s own upgrade
handling, before `httpHandler`'s dispatch, and before gin's routing (redirect
included) ever run. `BodyDeadlineMiddleware` (the gin-level version) was removed
entirely -- it is now redundant, and worse, incomplete on its own.

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

Add `httpserver.WrapWithBodyDeadline`
(`internal/distributed/proxy/httpserver/body_deadline_handler.go`), a plain
`http.Handler` wrapper (not gin middleware) that, per request, calls
`http.NewResponseController(w).SetReadDeadline` / `.SetWriteDeadline` using
`proxy.http.readTimeout` (`30s`) / `proxy.http.writeTimeout` (`60s`) -- but only
when `r.ProtoMajor != 2`.

It wraps the **entire** existing `Handler` value, outermost:

```go
Handler: httpserver.WrapWithBodyDeadline(h2c.NewHandler(s.httpHandler(ginHandler), http2Server)),
```

placing it outside `h2c.NewHandler`, `httpHandler`, and gin entirely, for two
reasons:

1. **Correctness / completeness**: this is the earliest point in the entire
   request lifecycle where `http.ResponseController` can be used at all -- before
   `h2c.NewHandler`'s own upgrade-handshake body read, before `httpHandler`'s
   gRPC/REST dispatch, before gin's routing (including gin's own trailing-slash
   redirect). No later insertion point (gin middleware, or even inside
   `httpHandler`) can reach both of the gaps described in Revision 2 above; this
   one can, because nothing runs before it except the raw `net/http` server loop
   itself.
2. **gRPC safety**: `r.ProtoMajor == 2` is true if and only if the connection has
   already been negotiated as HTTP/2 by the time any handler sees it -- which is
   exactly how all real gRPC traffic arrives (gRPC clients send the HTTP/2 client
   preface directly; they do not use the HTTP/1.1-based `Upgrade: h2c` mechanism).
   Both of the Revision-2 gaps (gin's redirect, the h2c Upgrade handshake) are
   still `r.ProtoMajor == 1` at the moment they'd need protecting, so this one
   check correctly separates "real gRPC, leave alone" from "everything else,
   including cases that don't obviously look like plain REST."

`http.Server.ReadTimeout`/`WriteTimeout` remain unset (Go zero value, `0` =
disabled), same as Revision 2, for the same shared gRPC/REST reason.

### Alternatives considered

**`http.Server.ReadTimeout`/`WriteTimeout`** (Revision 1). Rejected: regresses
gRPC in the default deployment.

**gin middleware (`BodyDeadlineMiddleware`, Revision 2)**. Rejected: correctly
avoids touching gRPC, but misses gin's own trailing-slash redirect and
`h2c.NewHandler`'s own upgrade-handshake body read, both of which run before gin's
middleware chain and are not gRPC traffic. See Revision history above.

**A filter placed at `httpHandler` (the existing gRPC/REST dispatch point)
instead of outside `h2c.NewHandler`**. Would fix the gin-redirect gap (everything
non-gRPC passes through `httpHandler` on the way to gin), but not the h2c
Upgrade-handshake gap, since `h2c.NewHandler` is *outside* `httpHandler` and
calls `h2cUpgrade`'s body read before `httpHandler` is ever invoked for that
request. Rejected as still incomplete; the outermost wrapper is not just simpler,
it is the only position that covers both gaps.

**Documentation-only fix** (tell operators to raise `readTimeout` themselves).
Rejected as insufficient on its own -- it leaves the default installation exposed.

## Non-goals

- Per-route/per-endpoint timeout *values* (all REST routes share one budget via
  `proxy.http.readTimeout`/`writeTimeout`).
- A request body size cap (`http.MaxBytesReader` or similar). This is a related but
  separate axis of the same general "unbounded request" class of problem (bytes vs.
  time) and is not addressed by this change.
- Any change to gRPC proxy timeouts. Enforced structurally via `r.ProtoMajor`, not
  by convention or registration order.

## Changes

- `internal/distributed/proxy/httpserver/body_deadline_handler.go` (new, replaces
  the removed `body_deadline_middleware.go`): `WrapWithBodyDeadline`.
- `internal/distributed/proxy/httpserver/body_deadline_handler_test.go` (new,
  replaces the removed `body_deadline_middleware_test.go`): unit tests for the
  `r.ProtoMajor` skip/apply split and the unsupported-writer path, plus two
  real-connection integration tests -- one for a handler that tries to read a
  stalled body (the original leak), and one for a handler that never reads the
  body at all (reproducing gin's own redirect behavior) -- both asserting the
  connection/goroutine is actually released, not just that a response eventually
  appears.
- `internal/distributed/proxy/service.go`: removed
  `ginHandler.Use(httpserver.BodyDeadlineMiddleware())`; wrapped the `Handler`
  field with `httpserver.WrapWithBodyDeadline(...)` outside `h2c.NewHandler`.
  `ReadTimeout`/`WriteTimeout` remain absent from the `http.Server{}` literal.
- `pkg/util/paramtable/http_param.go`: `ReadTimeout` default `0s` → `30s`,
  `WriteTimeout` default `0s` → `60s`; doc strings describe the
  per-request/`ResponseController` mechanism and why it is not on `http.Server`
  (unchanged by Revision 3 -- only the enforcement point moved).
- `configs/milvus.yaml`: regenerated reference values/comments for the two keys
  (generated via `make generate-yaml`; hand-mirrored here to match the generator's
  output format, since this environment's C++ core build was unavailable to run the
  generator directly -- should be double-checked with `make generate-yaml` in a full
  build environment before merge).
- `internal/distributed/proxy/service_test.go`: `Test_NewServer_HTTPServer_TimeoutDefaults`
  and `Test_NewServer_HTTPServer_TimeoutConfigOverrides` assert
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
- `TestWrapWithBodyDeadline_SkipsAlreadyNegotiatedHTTP2` /
  `TestWrapWithBodyDeadline_AppliesDeadlineForHTTP1`
  (`internal/distributed/proxy/httpserver/body_deadline_handler_test.go`) -- assert
  the `r.ProtoMajor` split directly, via a fake `http.ResponseWriter` that records
  whether `SetReadDeadline`/`SetWriteDeadline` were called, with no real connection
  needed.
- `TestWrapWithBodyDeadline_UnsupportedWriterDoesNotBreakRequest` -- a writer that
  doesn't support deadlines (e.g. `httptest.ResponseRecorder`) must not break the
  request.
- `TestWrapWithBodyDeadline_ReleasesHandlerGoroutineOnStalledBody` -- opens a real
  TCP connection to a real `httptest.Server`, sends headers plus a
  declared-but-incomplete body, and asserts the handler's blocked body read
  actually returns within bounded time -- proving the goroutine is released, not
  just that the client eventually sees a response. (This is the original test from
  Revision 2, still valid and still passing against the new handler.)
- `TestWrapWithBodyDeadline_BoundsPostHandlerDrainWhenHandlerNeverReadsBody` --
  reproduces the gin-redirect gap directly: a handler that responds immediately
  without ever reading the body (exactly what gin's `redirectTrailingSlash` does),
  and asserts the connection still closes cleanly within the deadline instead of
  hanging on `net/http`'s post-handler drain. **This is a direct reproducer for
  the P1 finding that Revision 2 missed.**

### Verified in this environment

All five `body_deadline_handler_test.go` tests were run successfully in an
isolated copy of the package (this sandbox cannot build
`internal/distributed/proxy/httpserver` directly, since sibling files in the same
package transitively require the compiled C++ core / rocksdb / rdkafka cgo
dependencies, which are unavailable here). The isolated copy exercises identical
code and passed, including both real-connection tests above; it should still be
re-run as part of the package proper in a full build environment before merge.

The h2c-Upgrade-handshake reproducer (the second P1 finding) was not additionally
re-verified end-to-end through the real `h2c.NewHandler` in this environment --
the fix for it follows directly from the same `r.ProtoMajor` mechanism verified by
`TestWrapWithBodyDeadline_AppliesDeadlineForHTTP1` (an h2c Upgrade attempt is
`r.ProtoMajor == 1`, like any other HTTP/1.1 request, until/unless it succeeds),
and from confirming in the vendored source that `WrapWithBodyDeadline` now sits
outside `h2c.NewHandler` and therefore runs before `h2cUpgrade`'s body read either
way -- but this specific claim should still be exercised with a real `Upgrade: h2c`
handshake test in a full build environment before merge, rather than taken only on
this structural argument.

### Not yet done

- A real-connection test exercising the actual `Upgrade: h2c` handshake path
  through the real `h2c.NewHandler` (see above) -- the current tests prove the
  general mechanism, not this exact path end-to-end.
- Confirm no existing large-payload REST integration test (e.g. bulk `insert`) is
  slow enough on CI infrastructure to trip the `30s`/`60s` bounds.
- `configs/milvus.yaml` regeneration should be confirmed with `make generate-yaml`
  in a full build environment (see Changes above).

## References

- Issue: https://github.com/milvus-io/milvus/issues/53074
- Raised during review of PR #52986 (DQL admission / post-handler drain discussion).
- PR #49951: prior discussion establishing the deliberate `0s`/`0s` default for the
  shared gRPC/REST `http.Server`.
