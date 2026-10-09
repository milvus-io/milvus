# REST timeout: large JSON response encoding baseline

On 2026-10-09, the current timeout middleware returned HTTP 408 while the original `jsonRender` continued encoding a large response. The renderer returned only after finishing its work and attempting a write to the recorder, which had already closed. A CPU profile started after request cancellation contains actual Sonic encoder samples. This is a diagnostic of the existing behavior, not a runtime fix or a production performance result.

- Base commit: `0acf11fde8a215668b131f932159fc8d522c1670`; Go 1.26.6, darwin/arm64, Sonic 1.15.2.
- Diagnostic: `internal/distributed/proxy/httpserver/timeout_encode_probe_test.go`, compiled with the original `timeout_middleware.go`, original `json_render.go`, and baseline test helpers. It has `//go:build ignore` and is excluded from normal package builds.
- Input: 32,768 rows of 128 `float64` values; one immutable vector is shared in memory before the request begins, but the real encoder still serializes every row. The same response type is warmed before timing.
- The server test setting is `requestTimeoutMs=20ms`. The handler calls Gin `c.Render(200, jsonRender{Data: payload})`; `jsonRender.Render` calls the repository's `internal/json.NewEncoder(w).Encode(payload)`, which uses Sonic. No custom marshaler, sleep, fake encoder, or slow client is used.
- The test observes the request deadline, context cancellation, outer middleware completion with HTTP 408, and the actual `c.Render` return. It requires the real renderer to start before the deadline and remain active until after 408. A 30s hang guard makes an inconclusive run fail. The handler is joined before test cleanup.

Three unprofiled repetitions returned HTTP 408 at approximately **21ms**, while the renderer returned at **350.458ms**, **338.165ms**, and **335.656ms**. Its final write returned `response writer closed: service internal error`; it did not send the large response after 408. This means client-visible timeout occurred, while server-side encoding continued for about **315–330ms past the 20ms deadline**.

In a separate run, CPU profiling began only after cancellation. `go tool pprof -top -cum` attributed **250ms cumulative sampled CPU** to `github.com/bytedance/sonic/internal/encoder.(*StreamEncoder).Encode`, including `EncodeInto`, `vm.Execute`, and float formatting. Other process activity, including garbage collection, was sampled separately. The retained profile is `.superpowers/sdd/2026-10-08-rest-timeout-unification/decode-cpu.X56Rvj/sonic-encoding.pprof` in this worktree. The profiled run's renderer returned at 343.383ms. Samples and wall times are diagnostic evidence, not production CPU accounting or latency estimates.

Focused race run passed with no Go race detector report; it took 4.779s for the renderer to return under race instrumentation. The existing complete-body [decoding baseline](rest-timeout-decoding-baseline.md) separately proves decoding continues after 408.

Reproduce from `internal/distributed/proxy/httpserver`:

```bash
env LOCAL_STORAGE_SIZE=1 go test \
  -tags dynamic,test,sonic,bytedance_tango -ldflags=-checklinkname=0 \
  -gcflags='all=-N -l' -count=3 -v \
  timeout_middleware.go json_render.go \
  timeout_baseline_probe_test.go timeout_decode_probe_test.go timeout_encode_probe_test.go \
  -run '^TestBaseline_TimeoutDoesNotStopLargeJSONEncoding$'
```

All runs used `-gcflags='all=-N -l'` per repository test rules. The profile run additionally set `MILVUS_TIMEOUT_ENCODE_CPU_PROFILE` to a new absolute file path, and the race run added `-race -count=1`. The test does not boot a complete Milvus server, exercise RPCs, or measure optimized production performance.

This result proves the original middleware's request timeout does not terminate big JSON encoding. A future timeout implementation must verify both the client-visible outcome and bounded server-side work completion; a context deadline alone does not stop the current non-context-aware encoder.
