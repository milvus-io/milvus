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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryJSONValue func() ([]byte, error)

func (f queryJSONValue) MarshalJSON() ([]byte, error) { return f() }

func TestDQLSingleBatchBuffered(t *testing.T) {
	for _, count := range []int{0, 1, dqlJSONBatchRows, dqlJSONBatchRows + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rows := make([]map[string]interface{}, count)
			for i := range rows {
				rows[i] = map[string]interface{}{"id": i, "text": "<>&"}
			}
			result := gin.H{"code": 0, "data": rows}
			w := newDQLResponseWriter(context.Background(), &bytes.Buffer{})
			defer w.Close()
			require.NoError(t, w.renderDQLResult(result))
			if count > dqlJSONBatchRows {
				require.NotNil(t, w.result)
				require.Zero(t, w.body.Len(), "multiple batches must not accumulate in the recorder")
				return
			}
			require.Nil(t, w.result, "a single batch must be encoded by the worker")
			expected, err := json.Marshal(result)
			require.NoError(t, err)
			require.Equal(t, string(expected)+"\n", w.body.String())
		})
	}
}

func TestDQLSingleBatchEncodingTimeout(t *testing.T) {
	setQueryTestBudget(t, "100")
	started, release := make(chan struct{}), make(chan struct{})
	var unblock, signalStarted sync.Once
	defer unblock.Do(func() { close(release) })
	late := make(chan error, 1)
	var writer *dqlResponseWriter
	var buffer *bytes.Buffer
	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/query", dqlTimeoutMiddleware(func(c *gin.Context) {
		writer = c.Writer.(*dqlResponseWriter)
		writer.mu.Lock()
		buffer = writer.body
		writer.mu.Unlock()
		value := queryJSONValue(func() ([]byte, error) {
			signalStarted.Do(func() { close(started) })
			<-release // Sonic does not observe cancellation during an encoding call.
			return []byte(`1`), nil
		})
		late <- writer.renderDQLResult(gin.H{"data": []map[string]interface{}{{"id": value}}})
	}))
	response := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		DQLStreamingHTTPHandler(r).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/query", nil))
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("encoder did not start")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("single-batch encoding blocked the timeout response")
	}
	require.Equal(t, http.StatusRequestTimeout, response.Code)
	require.Contains(t, response.Body.String(), "request timeout")
	require.Nil(t, writer.body, "the closed recorder must relinquish its pooled buffer before returning")
	// Reuse the exact released buffer; sync.Pool.Get itself is not guaranteed
	// to return this object. A late encoder must not corrupt its next owner.
	buffer.WriteString("next response")
	unblock.Do(func() { close(release) })
	select {
	case err := <-late:
		require.Error(t, err, "a late encoder must not write after its buffer has been returned")
	case <-time.After(time.Second):
		t.Fatal("late encoder did not exit")
	}
	require.Equal(t, "next response", buffer.String())
}

func TestQueryStreamEncodingAfterWorker(t *testing.T) {
	setQueryTestBudget(t, "5000")
	var workerFinished, encodedAfterWorker atomic.Bool
	s := queryStreamServer(t, func(c *gin.Context) {
		value := queryJSONValue(func() ([]byte, error) {
			encodedAfterWorker.Store(workerFinished.Load())
			return []byte(`1`), nil
		})
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": queryRowsWithLast(value)})
		workerFinished.Store(true)
	}, false)
	resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, json.Valid(body))
	require.True(t, encodedAfterWorker.Load(), "encoding must start only after the handler finishes preparing its response")
}

func TestQueryStreamEncodingOutlivesRequestTimeout(t *testing.T) {
	setQueryTestBudget(t, "200")
	for _, h2 := range []bool{false, true} {
		for _, firstBatch := range []bool{false, true} {
			t.Run(fmt.Sprintf("h2=%t/firstBatch=%t", h2, firstBatch), func(t *testing.T) {
				s := queryStreamServer(t, func(c *gin.Context) {
					deadline, _ := c.Request.Context().Deadline()
					value := queryJSONValue(func() ([]byte, error) {
						// Encoding, including the first batch, can outlive preparation's budget.
						time.Sleep(time.Until(deadline) + 20*time.Millisecond)
						return []byte(`1`), nil
					})
					rows := queryRowsWithLast(value)
					if firstBatch {
						rows[0]["value"] = value
					}
					HTTPReturnStream(c, 200, gin.H{"code": 0, "data": rows})
				}, h2)
				resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
				require.NoError(t, err)
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
				var result struct {
					Code int              `json:"code"`
					Data []map[string]any `json:"data"`
				}
				require.NoError(t, json.Unmarshal(body, &result))
				require.Zero(t, result.Code)
				require.Len(t, result.Data, dqlJSONBatchRows+1)
				require.Equal(t, float64(1), result.Data[len(result.Data)-1]["value"])
			})
		}
	}
}

type queryDelayedWriter struct {
	gin.ResponseWriter
	once sync.Once
}

func (w *queryDelayedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { time.Sleep(300 * time.Millisecond) })
	return w.ResponseWriter.Write(p)
}

func TestQueryStreamSendingOutlivesRequestTimeout(t *testing.T) {
	setQueryTestBudget(t, "200")
	r := gin.New()
	r.Use(gin.Recovery(), func(c *gin.Context) { c.Writer = &queryDelayedWriter{ResponseWriter: c.Writer} })
	r.POST("/query", dqlTimeoutMiddleware(func(c *gin.Context) {
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"id": 1}}})
	}))
	s := httptest.NewServer(DQLStreamingHTTPHandler(r))
	defer s.Close()
	resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.JSONEq(t, `{"code":0,"data":[{"id":1}]}`, string(body))
}

func TestQueryRenderFallback(t *testing.T) {
	for _, data := range []any{
		map[string]interface{}{"data": []map[string]interface{}{{"id": 1}}},
		gin.H{"code": 1, "message": "no rows"},
		gin.H{"data": nil},
		gin.H{"data": []gin.H{{"id": 1}}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		w := newDQLResponseWriter(ctx, &bytes.Buffer{})
		// Unsupported envelopes must stay in the recorder.
		err := w.renderDQLResult(data)
		cancel()
		require.NoError(t, err)
		require.Nil(t, w.result)
		expected, err := json.Marshal(data)
		require.NoError(t, err)
		require.Equal(t, string(expected)+"\n", w.body.String())
		w.Close()
	}
}

func queryStreamServer(t *testing.T, h gin.HandlerFunc, h2 bool, configure ...func(*http.Server)) *httptest.Server {
	t.Helper()
	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/query", dqlTimeoutMiddleware(h))
	s := httptest.NewUnstartedServer(DQLStreamingHTTPHandler(r))
	for _, config := range configure {
		config(s.Config)
	}
	s.EnableHTTP2 = h2
	if h2 {
		s.StartTLS()
	} else {
		s.Start()
	}
	t.Cleanup(s.Close)
	return s
}

func queryParentTimeout(timeout time.Duration) func(*http.Server) {
	return func(s *http.Server) {
		next := s.Handler
		s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func setQueryTestBudget(t *testing.T, value string) {
	t.Helper()
	paramtable.Init()
	p := &paramtable.Get().HTTPCfg.RequestTimeoutMs
	old := p.GetValue()
	require.NoError(t, paramtable.Get().Save(p.Key, value))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(p.Key, old)) })
}

func TestQueryStreamResponse(t *testing.T) {
	setQueryTestBudget(t, "5000")
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprint(h2), func(t *testing.T) {
			s := queryStreamServer(t, func(c *gin.Context) {
				rows := make([]map[string]interface{}, 600)
				for i := range rows {
					rows[i] = map[string]interface{}{"id": i, "text": "<>&"}
				}
				c.Header("X-Custom", "kept")
				HTTPReturnStream(c, 200, gin.H{"code": 0, "data": rows})
			}, h2)
			for i := 0; i < 2; i++ {
				resp, err := s.Client().Post(s.URL+"/query", "application/json", strings.NewReader("{}"))
				require.NoError(t, err)
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.NoError(t, err)
				require.Equal(t, 200, resp.StatusCode)
				require.Equal(t, "kept", resp.Header.Get("X-Custom"))
				require.Contains(t, string(body), `\u003c\u003e\u0026`)
				require.True(t, strings.HasSuffix(string(body), "]}\n"))
			}
		})
	}
}

func TestQueryStreamTimeoutDoesNotWaitForWorker(t *testing.T) {
	setQueryTestBudget(t, "80")
	release, late := make(chan struct{}), make(chan error, 1)
	s := queryStreamServer(t, func(c *gin.Context) {
		<-release // Deliberately ignore cancellation in the downstream computation.
		err := c.Writer.(*dqlResponseWriter).renderDQLResult(gin.H{"data": []map[string]interface{}{{"id": 1}}})
		late <- err
	}, false)
	defer close(release)
	resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, 408, resp.StatusCode)
	require.Contains(t, string(body), "request timeout")
	// Release after response completion, then prove a late worker cannot write.
	go func() { release <- struct{}{} }()
	select {
	case err := <-late:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("late worker stuck")
	}
}

func TestQueryStreamSmallResponseContentLength(t *testing.T) {
	setQueryTestBudget(t, "100")
	for _, outcome := range []string{"success", "error", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			s := queryStreamServer(t, func(c *gin.Context) {
				switch outcome {
				case "success":
					HTTPReturnStream(c, http.StatusOK, gin.H{"code": 0, "data": []map[string]interface{}{{"id": 1}}})
				case "error":
					HTTPAbortReturn(c, http.StatusBadRequest, gin.H{"code": 400, "message": "invalid query"})
				case "timeout":
					<-c.Request.Context().Done()
				}
			}, false)
			resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			status := map[string]int{"success": 200, "error": 400, "timeout": 408}[outcome]
			require.Equal(t, status, resp.StatusCode)
			require.NotEmpty(t, body)
			// A final explicit Flush would prevent net/http from determining the
			// length of these complete small responses and force HTTP/1 chunking.
			require.Empty(t, resp.TransferEncoding)
			require.Equal(t, int64(len(body)), resp.ContentLength)
			require.Equal(t, fmt.Sprint(len(body)), resp.Header.Get("Content-Length"))
		})
	}
}

func TestQueryStreamHTTP1ConnectionSurvivesTimeout(t *testing.T) {
	setQueryTestBudget(t, "80")
	s := queryStreamServer(t, func(c *gin.Context) {
		if _, err := io.Copy(io.Discard, c.Request.Body); err != nil {
			return
		}
		if c.Request.URL.Query().Get("slow") != "" {
			<-c.Request.Context().Done()
			return
		}
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"id": 1}}})
	}, false)
	// Use one raw connection: automatic reconnect must not hide cancellation of
	// the connection context after a timeout with an already-consumed body.
	conn, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	reader := bufio.NewReader(conn)
	for range 10 {
		for _, slow := range []bool{true, false} {
			path, timeout, status := "/query", "10", http.StatusOK
			if slow {
				path, timeout, status = "/query?slow=1", "", http.StatusRequestTimeout
			}
			header := ""
			if timeout != "" {
				header = "Request-Timeout: " + timeout + "\r\n"
			}
			_, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 2\r\n%s\r\n{}", path, header)
			require.NoError(t, err)
			resp, err := http.ReadResponse(reader, nil)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, err)
			require.Equal(t, status, resp.StatusCode, "slow=%t body=%s", slow, body)
			require.False(t, resp.Close, "pre-commit timeout must preserve keep-alive")
			if !slow {
				require.JSONEq(t, `{"code":0,"data":[{"id":1}]}`, string(body))
			}
		}
	}
}

// A full first batch ensures the following value is encoded after commitment.
func queryRowsWithLast(value any) []map[string]interface{} {
	rows := make([]map[string]interface{}, dqlJSONBatchRows+1)
	for i := range rows {
		rows[i] = map[string]interface{}{"text": strings.Repeat("x", 64)}
	}
	rows[len(rows)-1]["value"] = value
	return rows
}

func TestQueryStreamPrecommitFailure(t *testing.T) {
	setQueryTestBudget(t, "5000")
	for _, failure := range []string{"worker panic", "encoder panic", "encode error"} {
		t.Run(failure, func(t *testing.T) {
			s := queryStreamServer(t, func(c *gin.Context) {
				if failure == "worker panic" {
					panic("query worker panic")
				}
				var value any = queryJSONValue(func() ([]byte, error) { panic("query encoder panic") })
				if failure == "encode error" {
					value = math.NaN()
				}
				HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"id": value}}})
			}, false)
			resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
			require.JSONEq(t, `{"code":500}`, string(body))
		})
	}
}

func TestQueryStreamPartialFailure(t *testing.T) {
	setQueryTestBudget(t, "5000")
	for _, h2 := range []bool{false, true} {
		for _, failure := range []string{"parent timeout", "panic", "encode"} {
			t.Run(fmt.Sprintf("h2=%t/%s", h2, failure), func(t *testing.T) {
				s := queryStreamServer(t, func(c *gin.Context) {
					var value any = queryJSONValue(func() ([]byte, error) {
						if failure == "panic" {
							panic("query test panic")
						}
						time.Sleep(300 * time.Millisecond)
						return []byte(`1`), nil
					})
					if failure == "encode" {
						value = math.NaN()
					}
					HTTPReturnStream(c, 200, gin.H{"code": 0, "data": queryRowsWithLast(value)})
				}, h2, queryParentTimeout(200*time.Millisecond))
				resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
				if err == nil {
					_, err = io.ReadAll(resp.Body)
					resp.Body.Close()
				}
				require.Error(t, err, "partial response must not end with a clean EOF")
			})
		}
	}
}

func TestQueryStreamParentDeadlineBeforeCommit(t *testing.T) {
	setQueryTestBudget(t, "5000")
	s := queryStreamServer(t, func(c *gin.Context) {
		value := queryJSONValue(func() ([]byte, error) {
			time.Sleep(300 * time.Millisecond)
			return []byte(`1`), nil
		})
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"id": value}}})
	}, false, queryParentTimeout(200*time.Millisecond))
	resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestTimeout, resp.StatusCode)
	require.Contains(t, string(body), "request timeout")
}

func TestQueryStreamHTTP2PrecommitTimeout(t *testing.T) {
	setQueryTestBudget(t, "100")
	s := queryStreamServer(t, func(c *gin.Context) { <-c.Request.Context().Done() }, true)
	resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, 2, resp.ProtoMajor)
	require.Equal(t, 408, resp.StatusCode)
	require.Contains(t, string(body), "request timeout")
}

func TestQueryStreamClientCancellation(t *testing.T) {
	setQueryTestBudget(t, "5000")
	for _, h2 := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("h2=%t/committed=%t", h2, committed), func(t *testing.T) {
				started := make(chan struct{})
				type outcome struct {
					code    any
					message any
					written bool
				}
				reported := make(chan outcome, 1)
				r := gin.New()
				r.Use(gin.Recovery(), func(c *gin.Context) {
					c.Next()
					code, _ := c.Get(HTTPReturnCode)
					message, _ := c.Get(HTTPReturnMessage)
					reported <- outcome{code, message, c.Writer.Written()}
				})
				r.POST("/query", func(c *gin.Context) {
					parentCtx := c.Request.Context()
					dqlTimeoutMiddleware(func(c *gin.Context) {
						if committed {
							var once sync.Once
							value := queryJSONValue(func() ([]byte, error) {
								once.Do(func() { close(started) })
								<-parentCtx.Done()
								return []byte(`1`), nil
							})
							HTTPReturnStream(c, 200, gin.H{"code": 0, "data": queryRowsWithLast(value)})
							return
						}
						close(started)
						<-c.Request.Context().Done()
					})(c)
				})
				s := httptest.NewUnstartedServer(DQLStreamingHTTPHandler(r))
				s.EnableHTTP2 = h2
				if h2 {
					s.StartTLS()
				} else {
					s.Start()
				}
				defer s.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/query", nil)
				require.NoError(t, err)
				clientDone := make(chan error, 1)
				go func() {
					resp, err := s.Client().Do(req)
					if err == nil {
						_, err = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
					clientDone <- err
				}()
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("handler did not start")
				}
				cancel() // Actual disconnect/stream reset, not a request-budget expiry.
				select {
				case got := <-reported:
					require.EqualValues(t, merr.CanceledCode, got.code)
					require.Equal(t, context.Canceled.Error(), got.message)
					require.Equal(t, committed, got.written, "cancellation must not write a 408 envelope")
				case <-time.After(2 * time.Second):
					t.Fatal("client cancellation did not stop the request")
				}
				require.Error(t, <-clientDone)
			})
		}
	}
}

func TestQueryStreamPreservesServerWriteTimeout(t *testing.T) {
	setQueryTestBudget(t, "5000")
	exit := make(chan struct{})
	r := gin.New()
	r.Use(gin.Recovery(), func(c *gin.Context) { defer close(exit); c.Next() })
	r.POST("/query", dqlTimeoutMiddleware(func(c *gin.Context) {
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"text": strings.Repeat("x", 32<<20)}}})
	}))
	s := httptest.NewUnstartedServer(DQLStreamingHTTPHandler(r))
	s.Config.WriteTimeout = 200 * time.Millisecond
	s.Start()
	defer s.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.(*net.TCPConn).SetReadBuffer(1024))
	_, err = fmt.Fprintf(conn, "POST /query HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\n\r\n")
	require.NoError(t, err)
	// The shorter server write timeout must interrupt the socket write, despite
	// the longer request budget, and release the HTTP handler.
	select {
	case <-exit:
	case <-time.After(2 * time.Second):
		t.Fatal("socket write stuck")
	}
}

func TestQueryStreamPreservesServerReadTimeout(t *testing.T) {
	setQueryTestBudget(t, "5000")
	exit := make(chan error, 1)
	s := queryStreamServer(t, func(c *gin.Context) {
		_, err := io.ReadAll(c.Request.Body)
		exit <- err
	}, false, func(s *http.Server) { s.ReadTimeout = 200 * time.Millisecond })
	conn, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	_, err = fmt.Fprintf(conn, "POST /query HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\n{")
	require.NoError(t, err)
	select {
	case err := <-exit:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("request budget extended the server read timeout")
	}
}

type queryPanicWriter struct{ gin.ResponseWriter }

func (w queryPanicWriter) Write(p []byte) (int, error) {
	_, _ = w.ResponseWriter.Write(p)
	panic("response write panic")
}

func TestQueryStreamWritePanicAbortsResponse(t *testing.T) {
	setQueryTestBudget(t, "5000")
	r := gin.New()
	r.Use(gin.Recovery(), func(c *gin.Context) { c.Writer = queryPanicWriter{c.Writer} })
	r.POST("/query", dqlTimeoutMiddleware(func(c *gin.Context) {
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"id": 1}}})
	}))
	s := httptest.NewServer(DQLStreamingHTTPHandler(r))
	defer s.Close()
	resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
	if err == nil {
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	require.Error(t, err)
}

func TestQueryStreamHTTP2ConnectionSurvivesTimeout(t *testing.T) {
	setQueryTestBudget(t, "5000")
	started := make(chan struct{})
	s := queryStreamServer(t, func(c *gin.Context) {
		if c.Request.URL.Query().Get("slow") != "" {
			var once sync.Once
			value := queryJSONValue(func() ([]byte, error) {
				once.Do(func() { close(started) })
				time.Sleep(400 * time.Millisecond)
				return []byte(`1`), nil
			})
			HTTPReturnStream(c, 200, gin.H{"code": 0, "data": queryRowsWithLast(value)})
			return
		}
		HTTPReturnStream(c, 200, gin.H{"code": 0, "data": []map[string]interface{}{{"id": 1}}})
	}, true, queryParentTimeout(300*time.Millisecond))
	// Prime the single HTTP/2 connection before starting the stalled stream.
	client := s.Client()
	var firstConn, lastConn net.Conn
	post := func(conn *net.Conn) (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, s.URL+"/query", nil)
		if err != nil {
			return nil, err
		}
		trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { *conn = info.Conn }}
		return client.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	}
	resp, err := post(&firstConn)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	slowDone := make(chan error, 1)
	go func() {
		resp, err := client.Post(s.URL+"/query?slow=1", "application/json", nil)
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		slowDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow stream did not start")
	}
	require.Error(t, <-slowDone)
	resp, err = post(&lastConn)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 2, resp.ProtoMajor)
	require.Equal(t, firstConn, lastConn, "failed stream must not close the shared HTTP/2 connection")
	require.JSONEq(t, `{"code":0,"data":[{"id":1}]}`, string(body))
}
