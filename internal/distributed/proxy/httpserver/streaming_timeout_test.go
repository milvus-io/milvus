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
	"context"
	"crypto/tls"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Only recorder-based value tests use this deadline capability. Real transport
// tests deliberately bypass it so unsupported or missing deadlines fail tests.
type streamingRecorderWriter struct{ gin.ResponseWriter }

func (w streamingRecorderWriter) SetWriteDeadline(time.Time) error { return nil }
func (w streamingRecorderWriter) SetReadDeadline(time.Time) error  { return nil }

func streamingRecorderMiddleware(c *gin.Context) {
	c.Writer = streamingRecorderWriter{c.Writer}
	c.Next()
}

func streamingTestServer(t *testing.T, protocol string, handler http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	if protocol == "h2c" {
		server := httptest.NewServer(h2c.NewHandler(handler, &http2.Server{}))
		transport := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
		t.Cleanup(transport.CloseIdleConnections)
		t.Cleanup(server.Close)
		return server, client
	}
	server := httptest.NewUnstartedServer(handler)
	if protocol == "h2" {
		server.EnableHTTP2 = true
		server.StartTLS()
	} else {
		server.Start()
	}
	client := server.Client()
	client.Timeout = 3 * time.Second
	t.Cleanup(server.Close)
	return server, client
}

func streamingTestTimeout(t *testing.T, timeout string) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.RequestTimeoutMs.Key
	require.NoError(t, paramtable.Get().Save(key, timeout))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

type streamingStalledBody struct {
	io.ReadCloser
	reading chan struct{}
	reads   int
}

func (b *streamingStalledBody) Read(p []byte) (int, error) {
	b.reads++
	if b.reads == 2 {
		close(b.reading) // The partial JSON was consumed; this read must wait.
	}
	return b.ReadCloser.Read(p)
}

func TestStreamingRequestBodyTimeout(t *testing.T) {
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		for _, cancellation := range []string{"deadline", "parent_cancel"} {
			// -1 means unknown length (chunked for HTTP/1, no Content-Length for HTTP/2).
			for _, contentLength := range []int64{1024, -1} {
				t.Run(protocol+"/"+cancellation+"/content_length="+strconv.FormatInt(contentLength, 10), func(t *testing.T) {
					budget := "200"
					if cancellation == "parent_cancel" {
						budget = "5000" // Cancellation must interrupt before this deadline.
					}
					streamingTestTimeout(t, budget)
					readFinished := make(chan error, 1)
					cancelRequest := make(chan context.CancelFunc, 1)
					readingBody := make(chan struct{})
					engine := gin.New()
					engine.POST("/slow", streamingTimeoutMiddleware(func(c *gin.Context) {
						var req struct {
							Data []int `json:"data"`
						}
						err := c.ShouldBindBodyWith(&req, binding.JSON)
						readFinished <- err
						if err != nil {
							// Match the binding error path: the timeout middleware must
							// prevent this from committing a misleading JSON error.
							HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(merr.ErrParameterInvalid)})
						}
					}))
					engine.GET("/healthy", streamingTimeoutMiddleware(func(c *gin.Context) {
						HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: 0})
					}))
					wrapped := StreamingHTTPHandler(engine)
					server, client := streamingTestServer(t, protocol, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/slow" {
							ctx, cancel := context.WithCancel(r.Context())
							defer cancel()
							cancelRequest <- cancel
							r = r.WithContext(ctx)
							r.Body = &streamingStalledBody{ReadCloser: r.Body, reading: readingBody}
						}
						wrapped.ServeHTTP(w, r)
					}))
					reader, upload := io.Pipe()
					t.Cleanup(func() { _ = upload.Close(); _ = reader.Close() })
					request, err := http.NewRequest(http.MethodPost, server.URL+"/slow", reader)
					require.NoError(t, err)
					request.Header.Set("Content-Type", "application/json")
					request.ContentLength = contentLength
					type outcome struct {
						response *http.Response
						err      error
					}
					completed := make(chan outcome, 1)
					go func() {
						response, err := client.Do(request)
						completed <- outcome{response, err}
					}()
					_, err = upload.Write([]byte(`{"data":[1,`))
					require.NoError(t, err)
					select {
					case cancel := <-cancelRequest:
						if cancellation == "parent_cancel" {
							select {
							case <-readingBody:
							case <-time.After(time.Second):
								t.Fatal("body read did not start")
							}
							cancel()
						}
					case <-time.After(time.Second):
						t.Fatal("handler did not start")
					}
					select {
					case err := <-readFinished:
						require.ErrorIs(t, err, os.ErrDeadlineExceeded)
					case <-time.After(time.Second):
						t.Fatal("body read remained blocked after cancellation; client has not closed its upload")
					}
					select {
					case result := <-completed:
						require.NoError(t, result.err)
						defer result.response.Body.Close()
						body, err := io.ReadAll(result.response.Body)
						require.NoError(t, err)
						require.Equal(t, http.StatusRequestTimeout, result.response.StatusCode)
						require.EqualValues(t, merr.TimeoutCode, gjson.GetBytes(body, HTTPReturnCode).Int())
					case <-time.After(time.Second):
						t.Fatal("handler did not return a timeout response")
					}
					var reused bool
					healthy, err := http.NewRequest(http.MethodGet, server.URL+"/healthy", nil)
					require.NoError(t, err)
					healthy = healthy.WithContext(httptrace.WithClientTrace(healthy.Context(), &httptrace.ClientTrace{
						GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
					}))
					response, err := client.Do(healthy)
					require.NoError(t, err)
					defer response.Body.Close()
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					require.JSONEq(t, `{"code":0}`, string(body))
					if protocol != "http1" {
						require.True(t, reused, "body timeout must not close the shared HTTP/2 connection")
					}
				})
			}
		}
	}
}

func TestStreamingCompletedBodyKeepsConnection(t *testing.T) {
	streamingTestTimeout(t, "100")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			engine := gin.New()
			engine.POST("/", streamingTimeoutMiddleware(func(c *gin.Context) {
				var req map[string]interface{}
				if err := c.ShouldBindBodyWith(&req, binding.JSON); err != nil {
					HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(merr.ErrParameterInvalid)})
					return
				}
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: 0})
			}))
			server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
			for i, body := range []string{`{"data":[1]}`, `{"data":`, `{"data":[2]}`} {
				var reused bool
				request, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(body))
				require.NoError(t, err)
				request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
					GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
				}))
				response, err := client.Do(request)
				require.NoError(t, err)
				result, err := io.ReadAll(response.Body)
				response.Body.Close()
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				wantCode := int64(0)
				if i == 1 {
					wantCode = int64(merr.Code(merr.ErrParameterInvalid))
				}
				require.Equal(t, wantCode, gjson.GetBytes(result, HTTPReturnCode).Int())
				if i > 0 {
					require.True(t, reused, "a finished request's read timer must not poison keep-alive")
				}
				time.Sleep(150 * time.Millisecond) // Let the previous request's deadline pass.
			}
		})
	}
}

func requireStreamingAbort(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.Error(t, err, "the transport must report an interrupted response")
	require.False(t, json.Valid(body), string(body))
	require.NotContains(t, string(body), `"message"`)
	require.NotContains(t, string(body), "]}\n")
	return body
}

func TestStreamingResponseDeliveredBeforeHandlerReturns(t *testing.T) {
	paramtable.Init()
	release := make(chan struct{})
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/", streamingTimeoutMiddleware(func(c *gin.Context) {
		_, _ = c.Writer.Write([]byte(strings.Repeat("x", 8192)))
		c.Writer.Flush()
		<-release
	}))
	server := httptest.NewServer(StreamingHTTPHandler(engine))
	defer server.Close()
	defer close(release)
	client := server.Client()
	client.Timeout = time.Second
	resp, err := client.Get(server.URL)
	require.NoError(t, err, "the first batch must be visible while the handler is still running")
	defer resp.Body.Close()
	first := make([]byte, 8192)
	_, err = io.ReadFull(resp.Body, first)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("x", 8192), string(first))
}

func TestStreamingTransportFailures(t *testing.T) {
	streamingTestTimeout(t, "80")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			for _, mode := range []string{"timeout", "panic"} {
				t.Run(mode, func(t *testing.T) {
					engine := gin.New()
					engine.Use(gin.Recovery())
					unwound := make(chan struct{})
					engine.Use(func(c *gin.Context) { c.Next(); close(unwound) })
					engine.GET("/", streamingTimeoutMiddleware(func(c *gin.Context) {
						_, _ = c.Writer.Write([]byte(`{"data":[1`))
						c.Writer.Flush()
						if mode == "panic" {
							panic("committed failure")
						}
						<-c.Request.Context().Done()
					}))
					server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
					resp, err := client.Get(server.URL)
					require.NoError(t, err)
					requireStreamingAbort(t, resp)
					select {
					case <-unwound:
					case <-time.After(time.Second):
						t.Fatal("Gin middleware did not unwind before transport abort")
					}
				})
			}
		})
	}
}

func TestStreamingCommittedPanicDiagnostics(t *testing.T) {
	streamingTestTimeout(t, "1000")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			for _, ordinaryPanic := range []bool{true, false} {
				t.Run(strconv.FormatBool(ordinaryPanic), func(t *testing.T) {
					logs := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug", Format: "json", DisableTimestamp: true, DisableCaller: true, DisableStacktrace: true})
					type accounting struct {
						code    any
						message any
						errors  string
					}
					observed := make(chan accounting, 1)
					engine := gin.New()
					engine.Use(gin.Recovery())
					engine.Use(func(c *gin.Context) {
						c.Set("traceID", "panic-diagnostics-trace")
						c.Next()
						code, _ := c.Get(HTTPReturnCode)
						message, _ := c.Get(HTTPReturnMessage)
						observed <- accounting{code, message, c.Errors.ByType(gin.ErrorTypePrivate).String()}
					})
					engine.GET("/", streamingTimeoutMiddleware(func(c *gin.Context) {
						c.Set(ContextRequest, "REQUEST_PAYLOAD_SENTINEL")
						c.Set(HTTPReturnCode, merr.Code(nil))
						_, _ = c.Writer.Write([]byte(`{"data":[1`))
						c.Writer.Flush()
						if ordinaryPanic {
							panic("original committed panic")
						}
						panic(http.ErrAbortHandler)
					}))
					server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
					resp, err := client.Get(server.URL)
					require.NoError(t, err)
					requireStreamingAbort(t, resp)
					var got accounting
					select {
					case got = <-observed:
					case <-time.After(time.Second):
						t.Fatal("request accounting did not unwind after panic")
					}
					if ordinaryPanic {
						require.Equal(t, merr.Code(merr.ErrServiceInternal), got.code)
						require.Contains(t, got.message, "panicked")
						require.Contains(t, got.errors, "panicked")
						require.Contains(t, logs.String(), "original committed panic")
						require.Contains(t, logs.String(), "TestStreamingCommittedPanicDiagnostics.func")
						require.Contains(t, logs.String(), `"panic_stack"`)
						require.Contains(t, logs.String(), `"error"`)
						require.Contains(t, strings.ToLower(logs.String()), `"level":"error"`)
					} else {
						require.Empty(t, got.errors, "transport abort signal is not an ordinary handler panic")
						require.NotContains(t, logs.String(), "panicked")
					}
					require.NotContains(t, logs.String(), "REQUEST_PAYLOAD_SENTINEL")
				})
			}
		})
	}
}

func TestStreamingPrecommitErrorsAndKeepalive(t *testing.T) {
	streamingTestTimeout(t, "80")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			engine := gin.New()
			engine.Use(gin.Recovery())
			engine.GET("/:mode", streamingTimeoutMiddleware(func(c *gin.Context) {
				switch c.Param("mode") {
				case "timeout":
					<-c.Request.Context().Done()
				case "panic":
					panic("uncommitted panic")
				case "encoding":
					HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: 0, HTTPReturnCost: func() {}, HTTPReturnData: []gin.H{{"value": 1}}})
				case "nested encoding":
					HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: 0, HTTPReturnData: []gin.H{{"value": math.NaN()}}})
				default:
					HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: 0, HTTPReturnData: "healthy"})
				}
			}))
			server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
			for index, mode := range []string{"timeout", "panic", "encoding", "nested encoding", "healthy"} {
				reused := false
				req, err := http.NewRequest(http.MethodGet, server.URL+"/"+mode, nil)
				require.NoError(t, err)
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
				resp, err := client.Do(req)
				require.NoError(t, err)
				if index > 0 {
					require.True(t, reused, "uncommitted errors must preserve a healthy connection")
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.NoError(t, err)
				switch mode {
				case "timeout":
					require.Equal(t, http.StatusRequestTimeout, resp.StatusCode)
					require.Contains(t, string(body), "request timeout")
					require.True(t, json.Valid(body))
				case "panic":
					require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
					require.JSONEq(t, `{"code":500}`, string(body))
				case "encoding", "nested encoding":
					require.Equal(t, int64(merr.Code(merr.ErrInvalidSearchResult)), gjson.GetBytes(body, HTTPReturnCode).Int())
					require.True(t, json.Valid(body))
				default:
					require.Contains(t, string(body), "healthy")
				}
			}
		})
	}
}

func TestStreamingClientDisconnect(t *testing.T) {
	streamingTestTimeout(t, "1000")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			finished := make(chan struct{})
			rejectedWrite := make(chan error, 1)
			engine := gin.New()
			engine.Use(gin.Recovery())
			engine.GET("/", streamingTimeoutMiddleware(func(c *gin.Context) {
				defer close(finished)
				_, _ = c.Writer.Write([]byte(`{"data":[1`))
				c.Writer.Flush()
				<-c.Request.Context().Done()
				_, err := c.Writer.Write([]byte("]}\n"))
				rejectedWrite <- err
			}))
			server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
			resp, err := client.Get(server.URL)
			require.NoError(t, err)
			resp.Body.Close()
			select {
			case <-finished:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("handler failed to stop after client disconnect")
			}
			require.Error(t, <-rejectedWrite)
		})
	}
}

func TestStreamingUnsupportedDeadlinesFailClosed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			engine := gin.New()
			engine.Use(gin.Recovery())
			called := false
			engine.Handle(method, "/", streamingTimeoutMiddleware(func(c *gin.Context) {
				called = true
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: 0})
			}))
			var body io.Reader
			if method == http.MethodPost {
				body = strings.NewReader(`{"data":[1]}`)
			}
			w := httptest.NewRecorder()
			require.PanicsWithValue(t, http.ErrAbortHandler, func() {
				StreamingHTTPHandler(engine).ServeHTTP(w, httptest.NewRequest(method, "/", body))
			})
			require.Empty(t, w.Body.String(), "unsupported deadlines must not permit unbounded I/O")
			if method == http.MethodPost {
				require.False(t, called, "unsupported read deadlines must fail before entering the body reader")
			}
		})
	}
}

func TestStreamingCommittedFailureReturnsToHandler(t *testing.T) {
	streamingTestTimeout(t, "1000")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			continued := make(chan error, 1)
			engine := gin.New()
			engine.Use(gin.Recovery())
			engine.GET("/", streamingTimeoutMiddleware(func(c *gin.Context) {
				_, _ = c.Writer.Write([]byte(`{"data":[1`))
				c.Writer.Flush()
				HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: 0, HTTPReturnData: math.NaN()})
				// Real route wrappers must regain control to record their normal
				// accounting, but cannot write more bytes after renderer failure.
				_, err := c.Writer.Write([]byte("]}\n"))
				continued <- err
			}))
			server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
			resp, err := client.Get(server.URL)
			require.NoError(t, err)
			requireStreamingAbort(t, resp)
			select {
			case err := <-continued:
				require.Error(t, err)
			default:
				t.Fatal("ordinary renderer failure skipped its caller's accounting")
			}
		})
	}
}

func TestStreamingImmediateTimeoutHeader(t *testing.T) {
	streamingTestTimeout(t, "80")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			engine := gin.New()
			engine.Use(gin.Recovery())
			engine.GET("/", streamingTimeoutMiddleware(func(c *gin.Context) {
				HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: 0, HTTPReturnData: []gin.H{{"value": 1}}})
			}))
			server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			require.NoError(t, err)
			req.Header.Set("Request-Timeout", "0")
			resp, err := client.Do(req)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusRequestTimeout, resp.StatusCode)
			require.True(t, json.Valid(body))
			require.Contains(t, string(body), "request timeout")
		})
	}
}

func TestStreamingNestedAggregationRoute(t *testing.T) {
	streamingTestTimeout(t, "80")
	key := paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			for _, mode := range []string{"success", "encoding", "timeout"} {
				t.Run(mode, func(t *testing.T) {
					mp := mocks.NewMockProxy(t)
					mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
						CollectionName: DefaultCollectionName, Schema: generateCollectionSchema(schemapb.DataType_Int64, false, true), Status: &StatusSuccess,
					}, nil).Once()
					metric := 12.5
					if mode == "encoding" {
						metric = math.NaN()
					}
					result := &milvuspb.SearchResults{Status: &StatusSuccess, Results: &schemapb.SearchResultData{
						NumQueries: 1, AggTopks: []int64{1}, AggBuckets: []*schemapb.AggBucket{{
							Key:   []*schemapb.BucketKeyEntry{{FieldName: "brand", Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "<tag>&"}}},
							Count: 3, Metrics: map[string]*schemapb.MetricValue{"avg_price": {Value: &schemapb.MetricValue_DoubleVal{DoubleVal: metric}}},
						}},
					}}
					mp.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
						if mode == "timeout" {
							<-ctx.Done()
						}
						return result, nil
					}).Once()
					engine := gin.New()
					engine.Use(gin.Recovery())
					NewHandlersV2(mp).RegisterRoutesToV2(engine.Group("/v2/vectordb", genAuthMiddleWare(false)))
					server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
					resp, err := client.Post(server.URL+versionalV2(EntityCategory, SearchAction), "application/json", strings.NewReader(`{"collectionName":"book","data":[[0.1,0.2]],"annsField":"book_intro","limit":10,"searchAggregation":{"fields":["brand"],"size":2,"metrics":{"avg_price":{"op":"avg","fieldName":"price"}}}}`))
					require.NoError(t, err)
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					require.NoError(t, err)
					require.True(t, json.Valid(body), string(body))
					switch mode {
					case "timeout":
						require.Equal(t, http.StatusRequestTimeout, resp.StatusCode)
						require.Equal(t, int64(merr.TimeoutCode), gjson.GetBytes(body, HTTPReturnCode).Int())
					case "encoding":
						require.Equal(t, int64(merr.Code(merr.ErrInvalidSearchResult)), gjson.GetBytes(body, HTTPReturnCode).Int())
					default:
						require.Equal(t, int64(0), gjson.GetBytes(body, HTTPReturnCode).Int())
						require.Contains(t, string(body), `\u003ctag\u003e\u0026`)
						require.Equal(t, 12.5, gjson.GetBytes(body, "data.0.buckets.0.metrics.avg_price").Float())
					}
				})
			}
		})
	}
}

func TestStreamingBlockedOutputAndHealthyStream(t *testing.T) {
	streamingTestTimeout(t, "100")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			finished := make(chan struct{})
			engine := gin.New()
			engine.Use(gin.Recovery())
			engine.GET("/blocked", streamingTimeoutMiddleware(func(c *gin.Context) {
				defer close(finished)
				_, _ = c.Writer.Write([]byte(`{"data":["`))
				c.Writer.Flush()
				batch := []byte(strings.Repeat("x", 1<<20))
				for i := 0; i < 128; i++ {
					if _, err := c.Writer.Write(batch); err != nil {
						return
					}
				}
				t.Error("client did not exert output backpressure")
			}))
			engine.GET("/healthy", streamingTimeoutMiddleware(func(c *gin.Context) {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: 0, HTTPReturnData: "healthy"})
			}))
			server, client := streamingTestServer(t, protocol, StreamingHTTPHandler(engine))
			resp, err := client.Get(server.URL + "/blocked")
			require.NoError(t, err)
			if protocol != "http1" {
				reused := false
				req, err := http.NewRequest(http.MethodGet, server.URL+"/healthy", nil)
				require.NoError(t, err)
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
				healthy, err := client.Do(req)
				require.NoError(t, err)
				body, err := io.ReadAll(healthy.Body)
				healthy.Body.Close()
				require.NoError(t, err)
				require.Equal(t, 2, healthy.ProtoMajor)
				require.True(t, reused, "the healthy stream must share the blocked stream's connection")
				require.Contains(t, string(body), "healthy")
			}
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("blocked socket write outlived the request deadline")
			}
			requireStreamingAbort(t, resp)
			// A new request must not inherit a stopped callback or expired deadline.
			healthy, err := client.Get(server.URL + "/healthy")
			require.NoError(t, err)
			body, err := io.ReadAll(healthy.Body)
			healthy.Body.Close()
			require.NoError(t, err)
			require.Contains(t, string(body), "healthy")
		})
	}
}

// Observe accesses to the real writer after ServeHTTP exits, including Header
// calls that would otherwise silently race with a reused Gin context.
type streamingLifetimeWriter struct {
	http.ResponseWriter
	returned atomic.Bool
	late     atomic.Int32
}

func (w *streamingLifetimeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *streamingLifetimeWriter) observe() {
	if w.returned.Load() {
		w.late.Add(1)
	}
}
func (w *streamingLifetimeWriter) Header() http.Header {
	w.observe()
	return w.ResponseWriter.Header()
}
func (w *streamingLifetimeWriter) WriteHeader(code int) {
	w.observe()
	w.ResponseWriter.WriteHeader(code)
}
func (w *streamingLifetimeWriter) Write(p []byte) (int, error) {
	w.observe()
	return w.ResponseWriter.Write(p)
}

type streamingLifetimeBody struct {
	io.ReadCloser
	owner *streamingLifetimeWriter
}

func (b streamingLifetimeBody) Read(p []byte) (int, error) {
	b.owner.observe()
	return b.ReadCloser.Read(p)
}

func (b streamingLifetimeBody) Close() error {
	b.owner.observe()
	return b.ReadCloser.Close()
}

func TestStreamingTimeoutDoesNotWaitForWorker(t *testing.T) {
	streamingTestTimeout(t, "100")
	for _, protocol := range []string{"http1", "h2", "h2c"} {
		for _, committed := range []bool{false, true} {
			t.Run(protocol+"/committed="+strconv.FormatBool(committed), func(t *testing.T) {
				release := make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				lateWrite := make(chan error, 1)
				lateRead := make(chan error, 1)
				lateClose := make(chan error, 1)
				workerDone := make(chan struct{})
				returned := make(chan *streamingLifetimeWriter, 1)
				engine := gin.New()
				engine.Use(gin.Recovery())
				engine.POST("/slow", streamingTimeoutMiddleware(func(c *gin.Context) {
					defer close(workerDone)
					if committed {
						_, _ = c.Writer.Write([]byte(`{"data":[1`))
						c.Writer.Flush()
					}
					<-release // Deliberately ignore context cancellation.
					_, readErr := c.Request.Body.Read(make([]byte, 1))
					lateRead <- readErr
					lateClose <- c.Request.Body.Close()
					c.Writer.Header().Set("X-Late-Worker", "must-not-reach-client")
					c.Writer.WriteHeader(http.StatusCreated)
					_, err := c.Writer.WriteString("LATE_WORKER_BYTES")
					lateWrite <- err
					c.Writer.Flush()
				}))
				engine.GET("/healthy", streamingTimeoutMiddleware(func(c *gin.Context) {
					HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: 0})
				}))
				wrapped := StreamingHTTPHandler(engine)
				server, client := streamingTestServer(t, protocol, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/slow" {
						wrapped.ServeHTTP(w, r)
						return
					}
					tracked := &streamingLifetimeWriter{ResponseWriter: w}
					r.Body = streamingLifetimeBody{ReadCloser: r.Body, owner: tracked}
					defer func() { tracked.returned.Store(true); returned <- tracked }()
					wrapped.ServeHTTP(tracked, r)
				}))
				// Release before httptest's server cleanup, even when the old
				// synchronous implementation fails the timeout assertion below.
				t.Cleanup(unblock)
				client.Timeout = time.Second
				response, err := client.Post(server.URL+"/slow", "application/json", strings.NewReader(`{}`))
				require.NoError(t, err, "HTTP must finish without releasing the blocked worker")
				if committed {
					requireStreamingAbort(t, response)
				} else {
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					require.NoError(t, err)
					require.Equal(t, http.StatusRequestTimeout, response.StatusCode)
					require.EqualValues(t, merr.TimeoutCode, gjson.GetBytes(body, HTTPReturnCode).Int())
				}
				var tracked *streamingLifetimeWriter
				select {
				case tracked = <-returned:
				case <-time.After(time.Second):
					t.Fatal("HTTP handler still waits for the worker")
				}
				select {
				case <-workerDone:
					t.Fatal("test worker must remain blocked until explicitly released")
				default:
				}
				unblock()
				select {
				case <-workerDone:
				case <-time.After(time.Second):
					t.Fatal("late writes or flush must not block after HTTP exit")
				}
				require.Error(t, <-lateWrite)
				require.Error(t, <-lateRead)
				require.Error(t, <-lateClose)
				require.Zero(t, tracked.late.Load(), "worker accessed the real writer after HTTP exit")
				healthy, err := client.Get(server.URL + "/healthy")
				require.NoError(t, err)
				body, err := io.ReadAll(healthy.Body)
				healthy.Body.Close()
				require.NoError(t, err)
				require.JSONEq(t, `{"code":0}`, string(body))
				require.Empty(t, healthy.Header.Get("X-Late-Worker"))
			})
		}
	}
}

func TestStreamingBatchOwnershipOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exchange := &streamingExchange{ctx: ctx, calls: make(chan streamingIOCall)}
	writer := &streamingWorkerWriter{exchange: exchange, header: make(http.Header), status: http.StatusOK, size: -1}
	batch := []byte("encoded batch")
	returned := make(chan error, 1)
	go func() {
		_, err := writer.Write(batch)
		batch[0] = 'X' // An encoder may immediately reuse its bytes after Write.
		returned <- err
	}()
	call := <-exchange.calls
	defer func() {
		select {
		case call.done <- streamingIOResult{err: http.ErrAbortHandler}:
		default:
		}
	}()
	require.Same(t, &batch[0], &call.data[0], "handoff must not copy the encoded batch")
	cancel()
	select {
	case <-returned:
		t.Fatal("worker reused bytes while the HTTP goroutine still owned them")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, "encoded batch", string(call.data))
	call.done <- streamingIOResult{err: context.Canceled}
	select {
	case err := <-returned:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("acknowledgement did not release the worker")
	}
	_, err := writer.Write([]byte("late batch"))
	require.ErrorIs(t, err, context.Canceled)
}
