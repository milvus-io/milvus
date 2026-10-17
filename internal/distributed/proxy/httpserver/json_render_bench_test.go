// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Keep the old renderer for a same-binary comparison with the ownership path.
type legacyRecorderJSONRender struct{ jsonRender }

func (r legacyRecorderJSONRender) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	return json.NewEncoder(w).Encode(r.Data)
}

// Count committed bytes without allocating another full response in the sink.
type recorderBenchmarkSink struct {
	header http.Header
	status int
	bytes  int
}

func (w *recorderBenchmarkSink) Header() http.Header         { return w.header }
func (w *recorderBenchmarkSink) WriteHeader(code int)        { w.status = code }
func (w *recorderBenchmarkSink) Write(p []byte) (int, error) { w.bytes += len(p); return len(p), nil }

func recorderBenchmarkCPU(b *testing.B) int64 {
	b.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return usage.Utime.Nano() + usage.Stime.Nano()
}

// BenchmarkTimeoutMiddlewareJSON includes Gin, the worker/timer, the real
// recorder pool, encoding, commit and close. It excludes query execution and
// network I/O. Run optimized, without -race, for performance measurements.
func BenchmarkTimeoutMiddlewareJSON(b *testing.B) {
	paramtable.Init()
	gin.SetMode(gin.TestMode)
	for _, size := range []struct {
		name string
		rows int
		dim  int
	}{{"small", 1, 128}, {"100MiB", 2500, 3840}} {
		b.Run(size.name, func(b *testing.B) {
			// Independent row/vector allocations keep the live input graph realistic.
			rows := make([]map[string]any, size.rows)
			seed := uint32(42)
			for i := range rows {
				vector := make([]float32, size.dim)
				for j := range vector {
					seed = seed*1664525 + 1013904223
					vector[j] = float32(seed%10000000) / 10000000
				}
				rows[i] = map[string]any{"id": i, "text": "<>&", "vector": vector}
			}
			value := gin.H{"code": 0, "data": rows}
			for _, mode := range []string{"encoder", "owned"} {
				b.Run(mode, func(b *testing.B) {
					for _, pool := range []string{"cold", "reuse"} {
						b.Run(pool, func(b *testing.B) {
							var renderer render.Render = jsonRender{Data: value}
							if mode == "encoder" {
								renderer = legacyRecorderJSONRender{jsonRender{Data: value}}
							}
							handler := func(c *gin.Context) { c.Render(http.StatusOK, renderer) }
							wrapped := timeoutMiddleware(handler)
							engine := gin.New()
							engine.POST("/", func(c *gin.Context) {
								if pool == "cold" {
									timeoutMiddleware(handler)(c) // Empty recorder pool per request.
								} else {
									wrapped(c) // Production sync.Pool reuse, including GC eviction.
								}
							})
							request := httptest.NewRequest(http.MethodPost, "/", nil)
							request.Header.Set("Request-Timeout", "120")
							sink := &recorderBenchmarkSink{header: make(http.Header)}
							engine.ServeHTTP(sink, request) // Warm Sonic/JIT and the reusable pool.
							if sink.status != http.StatusOK || sink.bytes == 0 {
								b.Fatalf("warmup failed: status=%d bytes=%d", sink.status, sink.bytes)
							}
							wantBytes := sink.bytes
							b.SetBytes(int64(wantBytes))
							b.ReportAllocs()
							b.ResetTimer()
							cpu := recorderBenchmarkCPU(b)
							for i := 0; i < b.N; i++ {
								clear(sink.header)
								sink.status, sink.bytes = 0, 0
								engine.ServeHTTP(sink, request)
								if sink.status != http.StatusOK || sink.bytes != wantBytes {
									b.Fatalf("response failed: status=%d bytes=%d want=%d", sink.status, sink.bytes, wantBytes)
								}
							}
							cpu = recorderBenchmarkCPU(b) - cpu
							b.StopTimer()
							b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
						})
					}
				})
			}
		})
	}
}
