# HTTP/2 transport snapshot for REST request deadlines

This directory is a scoped copy of `golang.org/x/net@v0.58.0` (module
checksum `h1:ynWG7rqYi4ccpTEuPZ2QGWHktVEM9DMCj9yzDE0Q7To=`). The upstream
license and patent grant are in `LICENSE` and `PATENTS`. Only Proxy's HTTP
server imports this copy; other Milvus packages still use the original module.

Copied production sources: `http2/*.go`, `http2/h2c/h2c.go`,
`internal/httpcommon/*.go`, and `internal/httpsfv/*.go`. Upstream tests were
not copied. The import paths for these copied packages were mechanically
rewritten to this directory. `golang.org/x/net/http2/hpack`, `http/httpguts`,
and `idna` remain upstream imports.

Milvus-specific changes are intentionally limited to:

- `http2/frame.go`: record the first byte read for a frame so an incomplete
  frame header or HEADERS/CONTINUATION block can be guarded.
- `http2/server.go`, `server_common.go`, `request_start.go`: enforce a short
  connection read deadline while a frame header or header block is incomplete;
  attach the completed initial-header timestamp to the resulting request
  context, including while its handler is queued. The shared pre-header guard
  is independent of the post-header REST budget. Complete requests keep
  per-stream deadlines; an indefinitely incomplete header may close the
  connection because the frame parser cannot advance past it.

When upgrading x/net, compare these files with the new upstream version,
reapply only the listed hooks, and rerun the requestbudget HTTP/2 transport
tests (including race), TLS/h2c tests, and the full Proxy integration suite.
The Go stdlib's native HTTP/2 implementation is not patched by this copy.
