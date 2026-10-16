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
	"bytes"
	"context"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestJSONRenderOwnedBody(t *testing.T) {
	buffer := &bytes.Buffer{}
	recorder := newTimeoutResponseRecorder(buffer)
	value := struct {
		Text string `json:"text"`
		ID   int64  `json:"id"`
	}{Text: "<>&", ID: 9223372036854775807}
	require.NoError(t, (jsonRender{Data: value}).Render(recorder))
	// The existing buffer must hold only the encoder's newline, not a second
	// copy of the JSON document. Replacing ownership transfer with Write fails.
	require.Equal(t, "\n", buffer.String())
	want := "{\"text\":\"\\u003c\\u003e\\u0026\",\"id\":9223372036854775807}\n"
	require.Equal(t, len(want), recorder.Size())
	output := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(output)
	require.NoError(t, recorder.CommitTo(c.Writer))
	require.Equal(t, want, output.Body.String())
	require.Equal(t, "application/json; charset=utf-8", output.Header().Get("Content-Type"))
	recorder.Close()

	// Non-recorder callers retain the same JSON and trailing newline.
	direct := httptest.NewRecorder()
	require.NoError(t, (jsonRender{Data: value}).Render(direct))
	require.Equal(t, want, direct.Body.String())
}

type ownedBodyCapture struct {
	*httptest.ResponseRecorder
	first []byte
}

func (w *ownedBodyCapture) Write(data []byte) (int, error) {
	if w.first == nil {
		w.first = data
	}
	return w.ResponseRecorder.Write(data)
}

func TestTimeoutRecorderOwnedBody(t *testing.T) {
	for _, prefix := range []string{"", "prefix:"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			buffer := &bytes.Buffer{}
			recorder := newTimeoutResponseRecorder(buffer)
			if prefix != "" {
				_, err := recorder.WriteString(prefix)
				require.NoError(t, err)
			}
			data := []byte(`{"data":[1]}`)
			n, err := recorder.WriteOwned(data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			borrowed := []byte("\n")
			_, err = recorder.Write(borrowed)
			require.NoError(t, err)
			borrowed[0] = 'x' // Ordinary Write must not retain caller-owned bytes.
			_, err = recorder.WriteOwned([]byte(`{"data":[2]}`))
			require.NoError(t, err)
			want := prefix + "{\"data\":[1]}\n{\"data\":[2]}"
			require.Equal(t, len(want), recorder.Size())
			output := &ownedBodyCapture{ResponseRecorder: httptest.NewRecorder()}
			c, _ := gin.CreateTestContext(output)
			require.NoError(t, recorder.CommitTo(c.Writer))
			require.Equal(t, want, output.Body.String())
			if prefix == "" {
				require.Same(t, &data[0], &output.first[0], "CommitTo must send the transferred slice without copying it")
			}
			recorder.CloseForTimeout()
			require.Nil(t, recorder.ownedBody)
			// Simulate reuse of the pooled buffer by the next request. A closed
			// recorder must never adopt bytes or modify that buffer again.
			next := newTimeoutResponseRecorder(buffer)
			_, err = next.WriteString("next request")
			require.NoError(t, err)
			n, err = recorder.WriteOwned([]byte("late result"))
			require.Error(t, err)
			require.Zero(t, n)
			require.Equal(t, "next request", buffer.String())
		})
	}
}

type delayedJSON struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (v *delayedJSON) MarshalJSON() ([]byte, error) {
	// Sonic's race instrumentation marshals the value a second time.
	v.once.Do(func() { close(v.started) })
	<-v.release
	return []byte(`{"data":[1]}`), nil
}

func TestTimeoutRecorderLateJSON(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.RequestTimeoutMs.Key
	require.NoError(t, paramtable.Get().Save(key, "50"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, duringEncode := range []bool{true, false} {
		name := "after_encoding"
		if duringEncode {
			name = "during_encoding"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			engine := gin.New()
			engine.GET("/", timeoutMiddleware(func(c *gin.Context) {
				if duringEncode {
					done <- (jsonRender{Data: &delayedJSON{started: started, release: release}}).Render(c.Writer)
					return
				}
				err := (jsonRender{Data: gin.H{"data": []int{1}}}).Render(c.Writer)
				if err != nil {
					done <- err
					return
				}
				close(started)
				<-release
				done <- (jsonRender{Data: gin.H{"late": true}}).Render(c.Writer)
			}))
			response := httptest.NewRecorder()
			returned := make(chan struct{})
			go func() {
				engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
				close(returned)
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("encoding did not start")
			}
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("timeout waited for the encoder or handler")
			}
			require.Equal(t, http.StatusRequestTimeout, response.Code)
			require.JSONEq(t, `{"code":10001,"message":"request timeout"}`, response.Body.String())
			before := response.Body.String()
			// Let the worker finish only after the HTTP invocation returned.
			unblock()
			select {
			case err := <-done:
				require.ErrorIs(t, err, merr.ErrServiceInternal)
			case <-time.After(time.Second):
				t.Fatal("late result was not rejected")
			}
			require.Equal(t, before, response.Body.String())
		})
	}
}

func TestJSONRenderEncodingError(t *testing.T) {
	buffer := &bytes.Buffer{}
	recorder := newTimeoutResponseRecorder(buffer)
	require.Error(t, (jsonRender{Data: math.NaN()}).Render(recorder))
	require.False(t, recorder.Written())
	require.Equal(t, -1, recorder.Size())
	require.Empty(t, buffer.Bytes())
}

type delayedResponseWriter struct {
	*httptest.ResponseRecorder
	started chan struct{}
	release chan struct{}
	once    bool
}

func (w *delayedResponseWriter) Write(data []byte) (int, error) {
	if !w.once {
		w.once = true
		close(w.started)
		<-w.release
	}
	return w.ResponseRecorder.Write(data)
}

func TestTimeoutRecorderCompletedHandlerCanFinishSending(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.RequestTimeoutMs.Key
	require.NoError(t, paramtable.Get().Save(key, "100"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	ctxs := make(chan context.Context, 1)
	engine := gin.New()
	engine.GET("/", timeoutMiddleware(func(c *gin.Context) {
		ctxs <- c.Request.Context()
		HTTPReturnStream(c, http.StatusOK, gin.H{"code": 0, "data": []int{1}})
	}))
	response := &delayedResponseWriter{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(response.release) })
	returned := make(chan struct{})
	go func() {
		engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
		close(returned)
	}()
	select {
	case <-response.started:
	case <-time.After(time.Second):
		t.Fatal("completed response was not committed")
	}
	ctx := <-ctxs
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("request deadline did not expire")
	}
	response.release <- struct{}{}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("response did not finish")
	}
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"code":0,"data":[1]}`, response.Body.String())
}

var testResult = gin.H{
	"message": "This is a test message",
	"data":    make(map[string]interface{}),
}

func init() {
	const chunkSize = 1024
	rs := randomString(chunkSize)
	var sb strings.Builder
	for sb.Len() < 10*1024*1024 {
		sb.WriteString(rs)
	}
	testResult["data"] = sb.String()
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteByte(charset[rand.Intn(len(charset))])
	}
	return result.String()
}

func BenchmarkHTTPReturn(b *testing.B) {
	// Set Gin to test mode to prevent output to stdout
	gin.SetMode(gin.TestMode)

	b.Run("test HTTPReturn", func(b *testing.B) {
		router := gin.New()
		router.GET("/test1", func(c *gin.Context) {
			HTTPReturn(c, http.StatusOK, testResult)
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest("GET", "/test1", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
		}

		b.ReportAllocs()
	})

	b.Run("test HTTPReturnStream", func(b *testing.B) {
		router := gin.New()
		router.GET("/test2", func(c *gin.Context) {
			HTTPReturnStream(c, http.StatusOK, testResult)
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest("GET", "/test2", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
		}

		b.ReportAllocs()
	})
}

// goos: linux
// goarch: amd64
// pkg: github.com/milvus-io/milvus/internal/distributed/proxy/httpserver
// cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
// BenchmarkHTTPReturn
// BenchmarkHTTPReturn/test_HTTPReturn
// BenchmarkHTTPReturn/test_HTTPReturn-12         	      87	  13127452 ns/op	27992718 B/op	      34 allocs/op
// BenchmarkHTTPReturn/test_HTTPReturnStream
// BenchmarkHTTPReturn/test_HTTPReturnStream-12   	      87	  12804875 ns/op	14361636 B/op	      31 allocs/op
// PASS
//
// Process finished with the exit code 0
