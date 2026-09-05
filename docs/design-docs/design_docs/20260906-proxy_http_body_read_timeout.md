# MEP: Bound proxy REST body reads with a finite readTimeout/writeTimeout

Current state: In Progress

ISSUE: [[Enhancement]: Bound proxy REST body reads with a read budget (readTimeout ships as 0s) #53074](https://github.com/milvus-io/milvus/issues/53074)

Keywords: Proxy, REST, HTTP server, DoS, Timeout

Released: N/A

## Summary

`proxy.http.readHeaderTimeout` defaults to `5s`, but `proxy.http.readTimeout` defaults
to `0s` (disabled). `0s` means "no deadline" for the entire request body read on
`internal/distributed/proxy/service.go`'s `http.Server`. A client that sends valid
headers with a declared `Content-Length` and then withholds (or trickles) the body
pins one goroutine, one TCP connection, and one file descriptor on the proxy
indefinitely, for the cost of a few bytes and an idle connection. This happens on
every REST route, independent of any single endpoint or admission check, because
every route is wrapped by the same `wrapperPost` → `gCtx.ShouldBindBodyWith(...)`
body-read call.

`proxy.http.writeTimeout` has the identical `0s` gap on the response-writing side and
is fixed alongside `readTimeout` here, since it is the same class of problem with the
same fix shape.

## Why the existing per-request timeout doesn't already cover this

`timeoutMiddleware` (`internal/distributed/proxy/httpserver/timeout_middleware.go`)
races the handler against `proxy.http.requestTimeoutMs` (default `30s`) in a
goroutine, and on timeout writes a response to the client and returns. It does
**not** stop the spawned goroutine — Go has no API to forcibly kill a goroutine, and
a raw `net.Conn.Read()` is not `context.Context`-aware, so cancelling the request's
context does nothing to a read already blocked on the socket. The client sees a
prompt timeout response; the leaked goroutine/connection/fd underneath is unaffected.
`readTimeout`, enforced by `net/http` itself at the connection level below all
gin/middleware code, is the layer that can actually close the connection and free
the resource.

The same missing deadline also affects early-return paths (a validation error, or
the pre-decode 429 from DQL admission, `dqlAdmission` in `handler_v2.go`): after the
handler returns, `net/http`'s own `finishRequest` drains any unread body so the
connection can be reused for keep-alive — another unbounded socket read on the same
underlying connection.

## Design

Set both `readTimeout` and `writeTimeout` to sane, generous, but finite global
defaults, matching the existing pattern already used for every other timeout on this
`http.Server` (`readHeaderTimeout: 5s`, `idleTimeout: 300s`, `maxHeaderBytes: 16MiB`
are all already finite; only `readTimeout`/`writeTimeout` were left at `0s`).

- `proxy.http.readTimeout`: `0s` → `30s`. Chosen to match
  `proxy.http.requestTimeoutMs` (also `30s`): a client that can't finish sending its
  declared body is disconnected around the same time an in-budget request would
  already be timing out anyway, rather than being allowed to hang forever.
- `proxy.http.writeTimeout`: `0s` → `60s`. Kept comfortably above
  `requestTimeoutMs` + `readTimeout` so `timeoutMiddleware`'s graceful JSON timeout
  response always has a chance to be written before this raw, connection-level
  timeout would otherwise abort the connection mid-write.

A deadline set on the connection this way persists for any subsequent read on that
connection — including `finishRequest`'s post-handler drain — so both failure modes
described above are covered by the same two config values, with no separate code
path needed for the early-return case.

### Alternatives considered

**Per-route read deadlines**, set at the body-read call site via
`http.ResponseController(w).SetReadDeadline(...)` inside `wrapperPost`, would allow
tighter budgets for `search`/`query` and looser ones for `insert`. Rejected for now:
it does not fit any existing pattern in this file (every other timeout here is a flat
global value), and it does not actually work as a direct drop-in — `wrapperPost` runs
inside `timeoutMiddleware`'s goroutine with `gCtx.Writer` already swapped to
`timeoutResponseRecorder`, which has no `Unwrap()` method, so
`http.ResponseController` cannot reach the real connection-backed writer from inside
`wrapperPost` at all. Making per-route deadlines work would require restructuring
where in the middleware chain the deadline is set (outside `timeoutMiddleware`, before
the writer is swapped), which is more machinery than there is a current product
requirement for. Left as documented future work if a real need for per-route budgets
ever arises.

**Documentation-only fix** (tell operators to set `readTimeout` themselves) was
rejected as insufficient on its own — it leaves the default installation exposed.

## Non-goals

- Per-route/per-endpoint timeout budgets (see Alternatives).
- A request body size cap (`http.MaxBytesReader` or similar). This is a related but
  separate axis of the same general "unbounded request" class of problem (bytes vs.
  time) and is not addressed by this change.
- Any change to gRPC proxy timeouts; this issue and fix are scoped to the REST
  (`internal/distributed/proxy/httpserver`) path only.

## Changes

- `pkg/util/paramtable/http_param.go`: `ReadTimeout` default `0s` → `30s`,
  `WriteTimeout` default `0s` → `60s`, doc strings updated to explain the reasoning.
- `configs/milvus.yaml`: regenerated reference values/comments for the two keys
  (generated via `make generate-yaml`; hand-mirrored here to match the generator's
  output format, since this environment's C++ core build was unavailable to run the
  generator directly — should be double-checked with `make generate-yaml` in a full
  build environment before merge).
- `pkg/util/paramtable/http_param_test.go`,
  `internal/distributed/proxy/service_test.go`: updated default-value assertions
  from `0s`/`0s` to `30s`/`60s`.

## Test Plan

### Unit tests

- `TestHTTPConfig_Init` (`pkg/util/paramtable/http_param_test.go`) — asserts the new
  defaults.
- `Test_NewServer_HTTPServer_TimeoutDefaults`
  (`internal/distributed/proxy/service_test.go`) — asserts the new defaults reach the
  real `http.Server`.
- `Test_NewServer_HTTPServer_TimeoutConfigOverrides` — unaffected; already exercises
  explicit overrides and continues to pass unchanged.

### Follow-up verification (not yet done — flagged rather than claimed)

- An end-to-end test that opens a real connection, sends headers plus a
  declared-but-incomplete body, waits past `readTimeout`, and asserts the
  server-side connection/goroutine is actually released — not just that the client
  receives an error response. This is the gap the issue is actually about (a
  resource leak, not a client-visible symptom), and the existing tests only check
  config plumbing, not the runtime leak itself.
- Confirm no existing large-payload REST integration test (e.g. bulk `insert`) is
  slow enough on CI infrastructure to trip the new `30s`/`60s` bounds.

## References

- Issue: https://github.com/milvus-io/milvus/issues/53074
- Raised during review of PR #52986 (DQL admission / post-handler drain discussion).
