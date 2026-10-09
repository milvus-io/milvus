# REST timeout: complete-body JSON decoding baseline

## Conclusion and scope

On 2026-10-09, the existing Milvus timeout middleware did **not** stop an in-progress real JSON decoder when the request context expired. The middleware returned HTTP 408, but decoding continued to completion with no error and produced every requested row. CPU profiles started only after cancellation contain decoder stack samples.

This is a component-level characterization of the current implementation, not a fix, full Milvus E2E test, production latency benchmark, or acceptance of the new overall-budget design. PASS means the coverage gap was reproduced. The diagnostic deliberately fails if it can no longer reproduce that gap; it is excluded from ordinary package builds.

- Base: `0acf11fde8a215668b131f932159fc8d522c1670`.
- Baseline checkout: `0acf11fde8a215668b131f932159fc8d522c1670`.
- Test: `internal/distributed/proxy/httpserver/timeout_decode_probe_test.go`.
- Go 1.26.6, darwin/arm64; Gin 1.11.0, Sonic 1.15.2.
- All runs use `-gcflags='all=-N -l'`; the Sonic runs select the production JSON backend and plugin-synchronization tag, but are **not** optimized production Milvus builds.
- No production code, decoder dependency, runtime timeout configuration, or protocol adapter was changed by this diagnostic.

## What the test isolates

1. Construct a valid insert-shaped JSON body before starting the request: **13,828,139 bytes (13.19 MiB), 32,768 rows**, each with a 64-element numeric vector and a string field.
2. Warm the same concrete request type with one row before timing. The shape mirrors `CollectionDataReq` from `request_v2.go`; the actual request type imports native dependencies unavailable on this Mac, so it is not compiled into this fixture.
3. Use the original `timeout_middleware.go` with an explicit **20ms** test-only `requestTimeoutMs`. The original renderer and baseline helper file are compiled unchanged.
4. Set Gin's cached `BodyBytesKey` to the complete JSON, then call the same `ShouldBindBodyWith(..., binding.JSON)` used by REST V2 `wrapperPost`. This deliberately isolates the already-buffered-body path: **no socket upload/read, fake slow reader, sleep in the decoder, or custom UnmarshalJSON**.
5. Decorate Gin's selected codec only to timestamp the exact `Decoder.Decode` entry and return. Every operation delegates to the real selected codec, including decoder options; validation after Decode is excluded from the measured decode duration. The decorator is restored after both handler and middleware completion. Tests are serial.
6. Require Decode to start before its request deadline; observe cancellation while Decode is outstanding; require the middleware's 408 before Decode returns. Then assert nil decoding/binding errors, the full row count, and literal values from the first and last vectors.
7. Optional CPU profiling begins **after** observing `ctx.Done()`. Samples are process-wide, so only samples containing actual decoder stacks support the decoding-CPU claim; process-wide GC/kernel samples are not counted as exclusively decoder work.

The test has a 30s diagnostic hang guard and joins its workers on normal/assertion-failure paths. If scheduling or machine speed prevents observing the overlap, it fails as inconclusive rather than silently passing. This is not a portable performance threshold for CI.

## Results

Unprofiled repeated runs, measured from the request's invocation:

| Backend | Run | HTTP 408 observed | Decode returned | Work beyond absolute deadline |
| --- | --- | --- | --- | --- |
| Sonic | 1 | 23.989ms | 132.795ms | 112.630ms |
| Sonic | 2 | 42.294ms | 128.796ms | 108.744ms |
| Sonic | 3 | 21.418ms | 129.611ms | 109.555ms |
| encoding/json | 1 | 21.549ms | 447.238ms | 427.075ms |
| encoding/json | 2 | 21.217ms | 434.782ms | 414.735ms |
| encoding/json | 3 | 21.181ms | 434.847ms | 414.792ms |

All six runs completed the entire payload after expiration. Do not interpret the backend timing differences as a production performance comparison; these are unoptimized, single-machine diagnostic runs.

Separate post-cancel CPU profiles:

- Sonic: **130ms cumulative sampled CPU** under `github.com/bytedance/sonic/internal/decoder/api.(*StreamDecoder).Decode`, including `optdec.Decode` and descendants. The profiled run returned from Decode at 261.435ms. Profiling and host load change timing.
- encoding/json: **160ms cumulative sampled CPU** under `encoding/json.(*Decoder).Decode`. The profiled run returned from Decode at 444.088ms.
- These cumulative values include descendants and must not be summed across nested frames. Sampling is evidence of continued execution, not exact per-request CPU accounting.

Local retained profiles (ignored diagnostic artifacts, not source files):

```text
.superpowers/sdd/2026-10-08-rest-timeout-unification/decode-cpu.X56Rvj/sonic.pprof
.superpowers/sdd/2026-10-08-rest-timeout-unification/decode-cpu.X56Rvj/stdlib.pprof
```

Verification outcomes:

- Sonic focused test, three repeats: PASS, `ok command-line-arguments 6.023s`.
- Standard-library focused test, three repeats: PASS, `ok command-line-arguments 6.964s`.
- Sonic focused race run: PASS, `ok command-line-arguments 7.651s`; no Go race detector report. This is not a claim that all native/JIT code is race-instrumented.
- Combined original baseline plus new decoding probe, Sonic: PASS, `ok command-line-arguments 7.782s`.
- Existing fixture initialization logs a local etcd-unavailable warning before the measured request. The combined suite also logs expected broken-pipe/write-timeout warnings from its deliberate network failures. These are not decoder failures.

## Reproduce

From `internal/distributed/proxy/httpserver` in the worktree:

```bash
env LOCAL_STORAGE_SIZE=1 go test \
  -tags dynamic,test,sonic,bytedance_tango -ldflags=-checklinkname=0 \
  -gcflags='all=-N -l' -count=3 -v \
  timeout_middleware.go json_render.go \
  timeout_baseline_probe_test.go timeout_decode_probe_test.go \
  -run '^TestBaseline_TimeoutDoesNotStopCompleteBodyDecoding$'
```

For the standard-library control, use `-tags dynamic,test` and omit `-ldflags`. For the focused race check, add `-race` and use `-count=1`. To run all diagnostic probes together, omit `-run` and use `-count=1`.

To retain a post-cancel profile, set `MILVUS_TIMEOUT_PROBE_CPU_PROFILE` to a **new absolute file path** and use `-count=1`. The parent directory must exist; the test refuses to overwrite an existing file. Inspect it with:

```bash
go tool pprof -top -cum -nodecount=20 /absolute/path/to/new-profile.pprof
```

## Source audit and remaining work

- REST V2 `handler_v2.go:440` calls `ShouldBindBodyWith` under the existing timeout middleware.
- Gin `context.go:878` takes cached body bytes or reads the body, then calls `BindBody`. Gin `binding/json.go` uses the selected codec's `NewDecoder` and `Decode` without a context parameter, then validates.
- Gin's default build selects `encoding/json`; its `sonic` build selects `sonic.ConfigStd`. Milvus `Makefile` selects `sonic,bytedance_tango`, so the default-tag diagnostic alone would not be sufficient production-backend evidence. A repo-wide search found no production override of Gin's codec API or decoder-option globals.
- Sonic's `internal/decoder/api/stream.go` decodes from its reader and invokes its actual decoder without request-context cancellation. The post-expiry profile confirms execution in this path and `optdec` on this ARM64 machine.
- The timeout middleware cancels the context and writes 408, but does not forcibly terminate its handler goroutine or the decoder. The new policy helper is still unwired and does not change this result.

This closes the **current complete-body decoding gap reproduction**, not Task 4's cancellation implementation. A future fix must demonstrate bounded decoder exit and resource cleanup after cancellation. Socket read/write deadlines alone do not address the in-memory work tested here. Full server routing, real request types, backend RPCs, peak RSS, Linux/amd64 behavior, and optimized production timings remain unverified by this fixture.
