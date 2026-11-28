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

package httpcompress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	gin.SetMode(gin.ReleaseMode)
	m.Run()
}

// stack mirrors the proxy: response compression outside gin, request decoding
// as a gin middleware, then the route.
func stack(h http.Handler) http.Handler {
	engine := gin.New()
	engine.Use(RequestBodyMiddleware((*gin.Context).Abort))
	engine.Any("/*path", gin.WrapH(h))
	return Handler(engine)
}

// setCompression saves the compression params for one test and restores them.
func setCompression(t *testing.T, enabled bool, minSize int, maxDecoded int64) {
	t.Helper()
	cfg := &paramtable.Get().HTTPCfg
	paramtable.Get().Save(cfg.CompressionEnabled.Key, strconv.FormatBool(enabled))
	paramtable.Get().Save(cfg.CompressionMinResponseSize.Key, strconv.Itoa(minSize))
	maxRecv := paramtable.Get().ProxyGrpcServerCfg.ServerMaxRecvSize.Key
	paramtable.Get().Save(maxRecv, strconv.FormatInt(maxDecoded, 10))
	t.Cleanup(func() {
		paramtable.Get().Reset(cfg.CompressionEnabled.Key)
		paramtable.Get().Reset(cfg.CompressionMinResponseSize.Key)
		paramtable.Get().Reset(maxRecv)
	})
}

// jsonPayload is a RESTful-looking body of roughly n bytes.
func jsonPayload(n int) []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"code":0,"data":[`)
	for i := 0; buf.Len() < n; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"id":%d,"title":"document number %d","tags":["a","b"]}`, i, i)
	}
	buf.WriteString(`]}`)
	return buf.Bytes()
}

func zstdBytes(t *testing.T, p []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer enc.Close()
	return enc.EncodeAll(p, nil)
}

func gzipBytes(t *testing.T, p []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(p)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func decodeBody(t *testing.T, coding string, body []byte) []byte {
	t.Helper()
	switch coding {
	case "":
		return body
	case ContentEncodingZstd:
		dec, err := zstd.NewReader(bytes.NewReader(body))
		require.NoError(t, err)
		defer dec.Close()
		out, err := io.ReadAll(dec)
		require.NoError(t, err)
		return out
	case ContentEncodingGzip:
		r, err := gzip.NewReader(bytes.NewReader(body))
		require.NoError(t, err)
		out, err := io.ReadAll(r)
		require.NoError(t, err)
		return out
	}
	t.Fatalf("unexpected coding %q", coding)
	return nil
}

// echoHandler answers with the request body it read, so one round trip
// checks both directions. It may run off the test goroutine, so it reports a
// failure with assert rather than require.
func echoHandler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Seen-Content-Encoding", r.Header.Get("Content-Encoding"))
		w.Header().Set("X-Seen-Content-Length", strconv.FormatInt(r.ContentLength, 10))
		_, _ = w.Write(body)
	})
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// The switch only governs response compression. A compressed request body is
// the client's choice and is decoded either way.
func TestDisabledOnlyStopsResponseCompression(t *testing.T) {
	setCompression(t, false, 0, 1<<20)
	payload := jsonPayload(4096)

	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/query", bytes.NewReader(zstdBytes(t, payload)))
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := serve(stack(echoHandler(t)), req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("Content-Encoding"), "responses stay uncompressed")
	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
	assert.Equal(t, SupportedRequestEncodings, rec.Header().Get("Accept-Encoding"), "request compression is still accepted")
	assert.Empty(t, rec.Header().Get("X-Seen-Content-Encoding"))
	assert.Equal(t, payload, rec.Body.Bytes(), "the request body was decoded")

	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("x")))
	req.Header.Set("Content-Encoding", "br")
	assertRejected(t, serve(stack(echoHandler(t)), req), http.StatusUnsupportedMediaType,
		merr.Code(merr.ErrParameterInvalid), `"br"`)
}

func TestPlainClientUnchanged(t *testing.T) {
	setCompression(t, true, 0, 1<<20)
	payload := jsonPayload(64 << 10)
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/query", bytes.NewReader(payload))
	rec := serve(stack(echoHandler(t)), req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, payload, rec.Body.Bytes())
	assert.Equal(t, SupportedRequestEncodings, rec.Header().Get("Accept-Encoding"))
	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
	assert.Equal(t, strconv.Itoa(len(payload)), rec.Header().Get("X-Seen-Content-Length"))
}

func TestResponseNegotiation(t *testing.T) {
	setCompression(t, true, 0, 1<<20)
	payload := jsonPayload(8192)
	cases := []struct {
		accept string
		want   string
	}{
		{"zstd", ContentEncodingZstd},
		{"gzip", ContentEncodingGzip},
		{"x-gzip", ContentEncodingGzip},
		{"gzip, deflate, br, zstd", ContentEncodingZstd},
		{"gzip;q=1.0, zstd;q=0.5", ContentEncodingGzip},
		{"GZIP ; Q=0.8", ContentEncodingGzip},
		{"*", ContentEncodingZstd},
		{"zstd;q=0, *", ContentEncodingGzip},
		{"zstd;q=0, gzip;q=0", ""},
		{"gzip;q=0.5, identity;q=1", ""},
		{"identity;q=0.5, gzip;q=0.5", ContentEncodingGzip},
		{"*;q=0, identity;q=0.5", ""},
		{"identity", ""},
		{"br, deflate", ""},
		{"zstd;q=abc, gzip", ContentEncodingGzip},
		{"zstd;level=3;q=0, gzip;x", ContentEncodingGzip},
		{"", ""},
		{"<empty header>", ""},
	}
	for _, tc := range cases {
		t.Run(tc.accept, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
			switch tc.accept {
			case "":
			case "<empty header>":
				// cpp-httplib built without zlib/zstd sends "Accept-Encoding:".
				req.Header["Accept-Encoding"] = []string{""}
			default:
				req.Header.Set("Accept-Encoding", tc.accept)
			}
			rec := serve(stack(echoHandler(t)), req)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tc.want, rec.Header().Get("Content-Encoding"))
			assert.Empty(t, rec.Header().Get("Content-Length"))
			assert.Equal(t, payload, decodeBody(t, tc.want, rec.Body.Bytes()))
			if tc.want != "" {
				assert.Less(t, rec.Body.Len(), len(payload))
			}
		})
	}
}

func TestRequiredResponseCoding(t *testing.T) {
	setCompression(t, true, 1024, 1<<20)
	body := []byte(`{"code":0}`)
	for _, tc := range []struct {
		accept string
		coding string
	}{
		{"gzip, identity;q=0", ContentEncodingGzip},
		{"gzip, *;q=0", ContentEncodingGzip},
		{"zstd, identity;q=0", ContentEncodingZstd},
	} {
		t.Run(tc.accept, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("Accept-Encoding", tc.accept)
			rec := serve(Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write(body)
			})), req)
			require.Equal(t, http.StatusCreated, rec.Code)
			require.Equal(t, tc.coding, rec.Header().Get("Content-Encoding"))
			assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
			assert.Equal(t, body, decodeBody(t, tc.coding, rec.Body.Bytes()))
			assert.Zero(t, len(compressSlots))
		})
	}

	for _, coding := range []string{ContentEncodingGzip, ContentEncodingZstd} {
		t.Run("empty 200 "+coding, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("Accept-Encoding", coding+", identity;q=0")
			rec := serve(Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})), req)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, coding, rec.Header().Get("Content-Encoding"))
			require.NotZero(t, rec.Body.Len(), "an empty representation still needs a valid encoded frame")
			assert.Empty(t, decodeBody(t, coding, rec.Body.Bytes()))
			assert.Zero(t, len(compressSlots))
		})
	}

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, identity;q=0")
	for _, status := range []int{http.StatusNoContent, http.StatusResetContent, http.StatusNotModified} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rec := serve(Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			})), req)
			assert.Equal(t, status, rec.Code)
			assert.Empty(t, rec.Header().Get("Content-Encoding"))
			assert.Zero(t, rec.Body.Len())
			assert.Zero(t, len(compressSlots))
		})
	}
	t.Run("abort before first write", func(t *testing.T) {
		assert.PanicsWithValue(t, http.ErrAbortHandler, func() {
			serve(Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic(http.ErrAbortHandler)
			})), req)
		})
		assert.Zero(t, len(compressSlots))
	})
}

func TestUnacceptableResponseCoding(t *testing.T) {
	for _, tc := range []struct {
		accept  string
		enabled bool
	}{
		{"*;q=0", true},
		{"br, identity;q=0", true},
		{"gzip, identity;q=0", false},
	} {
		t.Run(tc.accept, func(t *testing.T) {
			setCompression(t, tc.enabled, 1024, 1<<20)
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("Accept-Encoding", tc.accept)
			rec := serve(Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("handler ran without an acceptable response coding")
			})), req)
			assert.Equal(t, http.StatusNotAcceptable, rec.Code)
			assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
			assert.Empty(t, rec.Header().Get("Content-Encoding"))
			assert.Zero(t, rec.Body.Len())
		})
	}
}

func TestBodylessMethodsSkipResponseNegotiation(t *testing.T) {
	setCompression(t, true, 0, 1<<20)
	for _, method := range []string{http.MethodHead, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/", nil)
			req.Header.Set("Accept-Encoding", "*;q=0")
			rec := serve(Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})), req)
			assert.Equal(t, http.StatusNoContent, rec.Code)
			assert.Empty(t, rec.Header().Get("Content-Encoding"))
			assert.Zero(t, rec.Body.Len())
		})
	}
}

func TestMinResponseSize(t *testing.T) {
	setCompression(t, true, 1024, 1<<20)
	for _, size := range []int{0, 1, 1023, 1024, 1025, 100 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte("a"), size)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
			req.Header.Set("Accept-Encoding", "zstd")
			rec := serve(stack(echoHandler(t)), req)
			want := ""
			if size >= 1024 {
				want = ContentEncodingZstd
			}
			assert.Equal(t, want, rec.Header().Get("Content-Encoding"))
			assert.True(t, bytes.Equal(payload, decodeBody(t, want, rec.Body.Bytes())))
		})
	}
}

func TestRequestDecoding(t *testing.T) {
	setCompression(t, true, 1<<30, 1<<20)
	payload := jsonPayload(32 << 10)
	cases := []struct {
		header string
		body   []byte
	}{
		{"zstd", zstdBytes(t, payload)},
		{"ZSTD", zstdBytes(t, payload)},
		{"gzip", gzipBytes(t, payload)},
		{"x-gzip", gzipBytes(t, payload)},
		{"identity", payload},
		{"identity, gzip", gzipBytes(t, payload)},
		// Not content codings: ignored, as the server always did.
		{"UTF-8", payload},
		{"utf-8, gzip", gzipBytes(t, payload)},
		{"json", payload},
	}
	for _, tc := range cases {
		t.Run(tc.header, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tc.body))
			req.Header.Set("Content-Encoding", tc.header)
			rec := serve(stack(echoHandler(t)), req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, payload, rec.Body.Bytes())
			assert.Equal(t, strconv.Itoa(len(payload)), rec.Header().Get("X-Seen-Content-Length"))
			if strings.Contains(strings.ToLower(tc.header), "zip") || strings.Contains(strings.ToLower(tc.header), "zstd") {
				assert.Empty(t, rec.Header().Get("X-Seen-Content-Encoding"), "a decoded body loses its coding")
			} else {
				assert.Equal(t, tc.header, rec.Header().Get("X-Seen-Content-Encoding"), "an ignored value is left untouched")
			}
		})
	}
}

func assertRejected(t *testing.T, rec *httptest.ResponseRecorder, status int, code int32, contains string) {
	t.Helper()
	require.Equal(t, status, rec.Code)
	assert.Equal(t, SupportedRequestEncodings, rec.Header().Get("Accept-Encoding"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	assert.EqualValues(t, code, body["code"])
	assert.Contains(t, body["message"], contains)
}

func TestRequestRejections(t *testing.T) {
	setCompression(t, true, 0, 1024)
	called := false
	var loggedCode int32
	abortCalls := 0
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Next()
		loggedCode = c.GetInt32(info.ContextReturnCode)
	}, RequestBodyMiddleware(func(c *gin.Context) {
		abortCalls++
		assert.Equal(t, merr.CanceledCode, c.GetInt32(info.ContextReturnCode))
		c.Abort()
	}))
	engine.POST("/", func(*gin.Context) { called = true })
	h := Handler(engine)
	invalid := merr.Code(merr.ErrParameterInvalid)

	newReq := func(coding string, body []byte) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("Content-Encoding", coding)
		return req
	}

	t.Run("unsupported", func(t *testing.T) {
		rec := serve(h, newReq("br", []byte("x")))
		assertRejected(t, rec, http.StatusUnsupportedMediaType, invalid, `"br"`)
	})
	t.Run("deflate", func(t *testing.T) {
		rec := serve(h, newReq("deflate", []byte("x")))
		assertRejected(t, rec, http.StatusUnsupportedMediaType, invalid, "supported: zstd, gzip")
	})
	t.Run("stacked", func(t *testing.T) {
		rec := serve(h, newReq("gzip, zstd", gzipBytes(t, zstdBytes(t, []byte("{}")))))
		assertRejected(t, rec, http.StatusUnsupportedMediaType, invalid, "stacked")
	})
	t.Run("corrupt zstd", func(t *testing.T) {
		rec := serve(h, newReq("zstd", []byte(`{"not":"zstd"}`)))
		assertRejected(t, rec, http.StatusBadRequest, invalid, "invalid zstd request body")
	})
	t.Run("corrupt gzip", func(t *testing.T) {
		rec := serve(h, newReq("gzip", []byte(`{"not":"gzip"}`)))
		assertRejected(t, rec, http.StatusBadRequest, invalid, "invalid gzip request body")
	})
	t.Run("gzip without a frame", func(t *testing.T) {
		assertRejected(t, serve(h, newReq("gzip", nil)), http.StatusBadRequest, invalid, "invalid gzip request body")
	})
	t.Run("invalid HTTP chunk length", func(t *testing.T) {
		body := httputil.NewChunkedReader(strings.NewReader("invalid\r\n"))
		req := httptest.NewRequest(http.MethodPost, "/", body)
		req.Header.Set("Content-Encoding", "gzip")
		assertRejected(t, serve(h, req), http.StatusBadRequest, invalid, "")
	})
	timeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for coding, encode := range map[string]func(*testing.T, []byte) []byte{
		ContentEncodingGzip: gzipBytes, ContentEncodingZstd: zstdBytes,
	} {
		full := encode(t, jsonPayload(512))
		half := full[:len(full)/2]
		for name, cause := range map[string]error{
			"caller canceled": context.Canceled,
			"HTTP2 CANCEL":    http2.StreamError{Code: http2.ErrCodeCancel},
		} {
			t.Run(name+" "+coding, func(t *testing.T) {
				before := abortCalls
				req := httptest.NewRequestWithContext(canceled, http.MethodPost, "/", iotest.ErrReader(cause))
				req.Header.Set("Content-Encoding", coding)
				assert.Empty(t, serve(h, req).Body.Bytes())
				assert.Equal(t, merr.CanceledCode, loggedCode)
				assert.Equal(t, before+1, abortCalls)
			})
		}
		for _, tc := range []struct {
			name   string
			ctx    context.Context
			prefix []byte
			err    error
			status int
			code   int32
		}{
			{"truncated", t.Context(), half, io.EOF, http.StatusBadRequest, invalid},
			{"timeout before header", t.Context(), nil, timeout, http.StatusRequestTimeout, merr.TimeoutCode},
			// A failed read can also cancel the request context.
			{"timeout mid-body", canceled, half, timeout, http.StatusRequestTimeout, merr.TimeoutCode},
			{"HTTP2 INTERNAL_ERROR", canceled, half, http2.StreamError{Code: http2.ErrCodeInternal}, http.StatusServiceUnavailable, merr.Code(merr.ErrServiceUnavailable)},
			{"deadline mid-body", t.Context(), half, context.DeadlineExceeded, http.StatusRequestTimeout, merr.TimeoutCode},
			{"reset mid-body", t.Context(), half, reset, http.StatusServiceUnavailable, merr.Code(merr.ErrServiceUnavailable)},
			{"incomplete HTTP body after full frame", t.Context(), full, io.ErrUnexpectedEOF, http.StatusServiceUnavailable, merr.Code(merr.ErrServiceUnavailable)},
		} {
			t.Run(tc.name+" "+coding, func(t *testing.T) {
				body := io.MultiReader(bytes.NewReader(tc.prefix), iotest.ErrReader(tc.err))
				req := httptest.NewRequestWithContext(tc.ctx, http.MethodPost, "/", body)
				req.Header.Set("Content-Encoding", coding)
				assertRejected(t, serve(h, req), tc.status, tc.code, "")
				assert.Equal(t, tc.code, loggedCode)
			})
		}
		t.Run("too large "+coding, func(t *testing.T) {
			body := encode(t, bytes.Repeat([]byte("a"), 1025))
			for _, ctx := range []context.Context{t.Context(), canceled} {
				rec := serve(h, newReq(coding, body).WithContext(ctx))
				assertRejected(t, rec, http.StatusRequestEntityTooLarge, merr.Code(merr.ErrParameterTooLarge), "proxy.grpc.serverMaxRecvSize")
			}
		})
	}
	assert.False(t, called, "a rejected body never reaches the handler")

	t.Run("exactly the limit", func(t *testing.T) {
		rec := serve(h, newReq("zstd", zstdBytes(t, bytes.Repeat([]byte("a"), 1024))))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, called)
	})
}

// math.MaxInt64 is a valid receive limit and means no limit. Reading one byte
// past it must not wrap around into a limit that drops every body.
func TestRequestLimitAtMaxInt64(t *testing.T) {
	setCompression(t, true, 1<<30, math.MaxInt64)
	payload := jsonPayload(32 << 10)
	for _, coding := range []string{ContentEncodingZstd, ContentEncodingGzip} {
		body := zstdBytes(t, payload)
		if coding == ContentEncodingGzip {
			body = gzipBytes(t, payload)
		}
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("Content-Encoding", coding)
		rec := serve(stack(echoHandler(t)), req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, payload, rec.Body.Bytes(), coding)
	}
}

// The decoded body is the largest thing a request holds, and gin's pooled
// context keeps it until the context is reused. It is allocated at its own
// size, without the slack a growing buffer leaves behind.
func TestDecodedBodyHasNoSlack(t *testing.T) {
	setCompression(t, true, 1<<30, 64<<20)
	payload := jsonPayload(3 << 20)
	var seenLen, seenCap int
	engine := gin.New()
	engine.Use(RequestBodyMiddleware((*gin.Context).Abort))
	engine.POST("/insert", func(c *gin.Context) {
		cached, _ := c.Get(gin.BodyBytesKey)
		seenLen, seenCap = len(cached.([]byte)), cap(cached.([]byte))
		c.Status(http.StatusNoContent)
	})
	for _, coding := range []string{ContentEncodingZstd, ContentEncodingGzip} {
		body := zstdBytes(t, payload)
		if coding == ContentEncodingGzip {
			body = gzipBytes(t, payload)
		}
		req := httptest.NewRequest(http.MethodPost, "/insert", bytes.NewReader(body))
		req.Header.Set("Content-Encoding", coding)
		rec := serve(Handler(engine), req)
		require.Equal(t, http.StatusNoContent, rec.Code, coding)
		assert.Equal(t, len(payload), seenLen, coding)
		assert.Less(t, seenCap-seenLen, 64<<10, "%s: no growth slack kept with the body", coding)
	}
}

// A single-segment frame carries no window descriptor: its window is its whole
// content (RFC 8878 §3.1.1.1.2). Up to 8 MiB it is accepted; above that it
// breaks the 8 MiB window rule of RFC 9659 and is rejected, however far below
// the decoded-size limit it is. This also pins the window cap.
func TestRequestZstdSingleSegmentWindow(t *testing.T) {
	setCompression(t, true, 0, 64<<20)
	encode := func(n int) []byte {
		enc, err := zstd.NewWriter(nil, zstd.WithSingleSegment(true))
		require.NoError(t, err)
		frame := enc.EncodeAll(bytes.Repeat([]byte("a"), n), nil)
		var h zstd.Header
		require.NoError(t, h.Decode(frame))
		require.True(t, h.SingleSegment)
		require.EqualValues(t, n, h.FrameContentSize)
		return frame
	}
	post := func(frame []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(frame))
		req.Header.Set("Content-Encoding", "zstd")
		return serve(stack(echoHandler(t)), req)
	}

	rec := post(encode(compressionZstdMaxDecodeWindow))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, compressionZstdMaxDecodeWindow, rec.Body.Len())

	rec = post(encode(compressionZstdMaxDecodeWindow + 1))
	assertRejected(t, rec, http.StatusBadRequest, merr.Code(merr.ErrParameterInvalid), "invalid zstd request body")
}

func TestLongCodingHeaders(t *testing.T) {
	setCompression(t, true, 1<<20, 1<<20)
	payload := []byte(`{"id":1}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(zstdBytes(t, payload)))
	padding := strings.Repeat(" ", 2048)
	req.Header.Set("Content-Encoding", "identity,"+padding+"zstd")
	req.Header.Set("Accept-Encoding", "identity;q=0,"+padding+"gzip")

	rec := serve(stack(echoHandler(t)), req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, ContentEncodingGzip, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, payload, decodeBody(t, ContentEncodingGzip, rec.Body.Bytes()))
}

// A frame may declare a window far larger than its content. The decoder sizes
// its history from the header, so the window cap has to reject it even though
// the decoded-size limit would not.
func TestRequestRejectsOversizedZstdWindow(t *testing.T) {
	setCompression(t, true, 0, 64<<20)
	var frame bytes.Buffer
	enc, err := zstd.NewWriter(&frame, zstd.WithWindowSize(64<<20), zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	data := make([]byte, 9<<20)
	rand.New(rand.NewSource(1)).Read(data)
	_, err = enc.Write(data)
	require.NoError(t, err)
	require.NoError(t, enc.Close())

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(frame.Bytes()))
	req.Header.Set("Content-Encoding", "zstd")
	rec := serve(stack(echoHandler(t)), req)
	assertRejected(t, rec, http.StatusBadRequest, merr.Code(merr.ErrParameterInvalid), "window size exceeded")
}

func TestResponseStatusAndHeaders(t *testing.T) {
	setCompression(t, true, 16, 1<<20)
	payload := jsonPayload(4096)

	run := func(h http.HandlerFunc, method string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/", nil)
		req.Header.Set("Accept-Encoding", "gzip, zstd")
		return serve(stack(h), req)
	}

	t.Run("error status keeps its code", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write(payload)
		}, http.MethodPost)
		assert.Equal(t, http.StatusTooManyRequests, rec.Code)
		assert.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
	t.Run("no content", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}, http.MethodPost)
		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.Zero(t, rec.Body.Len())
	})
	t.Run("nothing written", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {}, http.MethodPost)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
	})
	t.Run("already encoded", func(t *testing.T) {
		pre := gzipBytes(t, payload)
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(pre)
		}, http.MethodPost)
		assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
		assert.Equal(t, pre, rec.Body.Bytes(), "a body the handler encoded itself is not encoded twice")
	})
	t.Run("head", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(payload)
		}, http.MethodHead)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
	})
	t.Run("many small writes", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			text := string(payload)
			for i := 0; i < len(text); i += 7 {
				_, _ = io.WriteString(w, text[i:min(i+7, len(text))])
			}
		}, http.MethodPost)
		assert.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
	t.Run("flush", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(payload[:8])
			w.(http.Flusher).Flush()
			_, _ = w.Write(payload[8:])
		}, http.MethodPost)
		assert.True(t, rec.Flushed)
		assert.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
	t.Run("response controller", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(payload)
			require.NoError(t, http.NewResponseController(w).Flush())
		}, http.MethodPost)
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
}

// DQLStreamingHTTPHandler aborts a response by panicking with
// http.ErrAbortHandler after it has started writing. The panic has to reach
// net/http unchanged, and the pooled encoder has to come back usable.
func TestAbortPanicPassesThrough(t *testing.T) {
	setCompression(t, true, 0, 1<<20)
	payload := jsonPayload(4096)
	aborting := stack(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
		panic(http.ErrAbortHandler)
	}))
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		assert.PanicsWithValue(t, http.ErrAbortHandler, func() { serve(aborting, req) })
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set("Accept-Encoding", "zstd")
	rec := serve(stack(echoHandler(t)), req)
	assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
}

func TestRefreshable(t *testing.T) {
	setCompression(t, false, 0, 1<<20)
	payload := jsonPayload(4096)
	h := stack(echoHandler(t))
	send := func() string {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
		req.Header.Set("Accept-Encoding", "zstd")
		return serve(h, req).Header().Get("Content-Encoding")
	}
	assert.Empty(t, send())
	paramtable.Get().Save(paramtable.Get().HTTPCfg.CompressionEnabled.Key, "true")
	assert.Equal(t, ContentEncodingZstd, send())
	paramtable.Get().Save(paramtable.Get().HTTPCfg.CompressionEnabled.Key, "false")
	assert.Empty(t, send())
}

// Pooled encoders and decoders are shared across requests; run them
// concurrently under -race and check that no request sees another's bytes.
func TestConcurrentRequests(t *testing.T) {
	setCompression(t, true, 0, 4<<20)
	h := stack(echoHandler(t))
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := jsonPayload(1024 * (1 + i%40))
			coding := []string{ContentEncodingZstd, ContentEncodingGzip}[i%2]
			var body []byte
			var err error
			if coding == ContentEncodingZstd {
				enc, _ := zstd.NewWriter(nil)
				body = enc.EncodeAll(payload, nil)
				enc.Close()
			} else {
				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				_, _ = gw.Write(payload)
				_ = gw.Close()
				body = buf.Bytes()
			}
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Encoding", coding)
			req.Header.Set("Accept-Encoding", []string{"gzip", "zstd"}[i%2])
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			var got []byte
			switch rec.Header().Get("Content-Encoding") {
			case "":
				// A response that finds no free encoder slot is sent plain.
				got = rec.Body.Bytes()
			case ContentEncodingZstd:
				dec, _ := zstd.NewReader(bytes.NewReader(rec.Body.Bytes()))
				got, err = io.ReadAll(dec)
				dec.Close()
			case ContentEncodingGzip:
				var gr *gzip.Reader
				if gr, err = gzip.NewReader(bytes.NewReader(rec.Body.Bytes())); err == nil {
					got, err = io.ReadAll(gr)
				}
			}
			if err == nil && !bytes.Equal(got, payload) {
				err = fmt.Errorf("request %d: body mismatch", i)
			}
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	assert.Zero(t, len(compressSlots), "every slot is returned")
}

// Real clients over a real connection: Go's default transport asks for gzip on
// its own and decodes it transparently; one that disables it, and one that
// asks for zstd, must each get what they asked for.
func TestRealHTTPClients(t *testing.T) {
	setCompression(t, true, 1024, 1<<20)
	payload := jsonPayload(32 << 10)
	srv := httptest.NewServer(stack(echoHandler(t)))
	defer srv.Close()

	post := func(client *http.Client, header http.Header, body []byte) (*http.Response, []byte) {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v2/vectordb/entities/query", bytes.NewReader(body))
		require.NoError(t, err)
		for k, v := range header {
			req.Header[k] = v
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp, out
	}

	t.Run("default transport", func(t *testing.T) {
		resp, out := post(http.DefaultClient, nil, payload)
		assert.True(t, resp.Uncompressed, "gzip negotiated and decoded by the transport")
		assert.Equal(t, payload, out)
	})
	t.Run("compression disabled in client", func(t *testing.T) {
		client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
		resp, out := post(client, nil, payload)
		assert.Empty(t, resp.Header.Get("Content-Encoding"))
		assert.Equal(t, payload, out)
		assert.Equal(t, SupportedRequestEncodings, resp.Header.Get("Accept-Encoding"))
	})
	t.Run("zstd both ways", func(t *testing.T) {
		header := http.Header{}
		header.Set("Accept-Encoding", "zstd")
		header.Set("Content-Encoding", "zstd")
		resp, out := post(http.DefaultClient, header, zstdBytes(t, payload))
		assert.Equal(t, ContentEncodingZstd, resp.Header.Get("Content-Encoding"))
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, out))
	})
	t.Run("unsupported coding", func(t *testing.T) {
		header := http.Header{}
		header.Set("Content-Encoding", "br")
		resp, _ := post(http.DefaultClient, header, []byte("x"))
		assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
		assert.True(t, strings.Contains(resp.Header.Get("Accept-Encoding"), "zstd"))
	})
}

func TestRequestEdgeCases(t *testing.T) {
	setCompression(t, true, 0, 1<<20)

	t.Run("empty decoded body", func(t *testing.T) {
		zstdFrame := zstdBytes(t, nil)
		require.NotEmpty(t, zstdFrame)
		for _, tc := range []struct {
			coding string
			wire   []byte
		}{{"zstd", nil}, {"zstd", zstdFrame}, {"gzip", gzipBytes(t, nil)}} {
			req := httptest.NewRequest(http.MethodDelete, "/", bytes.NewReader(tc.wire))
			req.Header.Set("Content-Encoding", tc.coding)
			rec := serve(stack(echoHandler(t)), req)
			assert.Equal(t, http.StatusOK, rec.Code, tc.coding)
			assert.Zero(t, rec.Body.Len(), tc.coding)
		}
	})

	t.Run("unmatched route never decodes", func(t *testing.T) {
		engine := gin.New()
		engine.Use(RequestBodyMiddleware((*gin.Context).Abort))
		engine.POST("/v2/vectordb/entities/insert", func(c *gin.Context) {})
		req := httptest.NewRequest(http.MethodPost, "/no/such/route", bytes.NewReader([]byte("not gzip")))
		req.Header.Set("Content-Encoding", "gzip")
		rec := serve(Handler(engine), req)
		assert.Equal(t, http.StatusNotFound, rec.Code, "a 404, not a 400 from decoding")
	})

	t.Run("decoded body is cached for ShouldBindBodyWith", func(t *testing.T) {
		payload := []byte(`{"collectionName":"book"}`)
		engine := gin.New()
		engine.Use(RequestBodyMiddleware((*gin.Context).Abort))
		var bound struct {
			CollectionName string `json:"collectionName"`
		}
		engine.POST("/insert", func(c *gin.Context) {
			cached, ok := c.Get(gin.BodyBytesKey)
			assert.True(t, ok)
			assert.Equal(t, payload, cached)
			require.NoError(t, c.ShouldBindBodyWithJSON(&bound))
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequest(http.MethodPost, "/insert", bytes.NewReader(zstdBytes(t, payload)))
		req.Header.Set("Content-Encoding", "zstd")
		rec := serve(Handler(engine), req)
		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, "book", bound.CollectionName)
	})

	t.Run("rejections are recorded for the access log", func(t *testing.T) {
		engine := gin.New()
		var code any
		engine.Use(func(c *gin.Context) { c.Next(); code, _ = c.Get("code") })
		engine.Use(RequestBodyMiddleware((*gin.Context).Abort))
		engine.POST("/insert", func(c *gin.Context) {})
		req := httptest.NewRequest(http.MethodPost, "/insert", strings.NewReader("x"))
		req.Header.Set("Content-Encoding", "br")
		rec := serve(Handler(engine), req)
		assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
		assert.EqualValues(t, merr.Code(merr.ErrParameterInvalid), code)
	})
}

func TestResponseWriterDetails(t *testing.T) {
	setCompression(t, true, 64, 1<<20)
	payload := jsonPayload(4096)
	run := func(h http.HandlerFunc) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		return serve(Handler(h), req)
	}

	t.Run("sniffs Content-Type net/http would no longer sniff", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) })
		assert.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	})
	t.Run("handler Content-Length is dropped when compressing", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload)
		})
		assert.Empty(t, rec.Header().Get("Content-Length"))
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
	t.Run("one large write after a small one", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(payload[:10])
			_, _ = w.Write(payload[10:])
		})
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
	t.Run("informational status passes through", func(t *testing.T) {
		// A ResponseRecorder treats a 1xx as the final status, so this uses
		// the connection fake, which records every status it is sent.
		conn := &connection{}
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusEarlyHints)
			_, _ = w.Write(payload)
		})).ServeHTTP(conn, req)
		assert.Equal(t, []int{http.StatusEarlyHints, http.StatusOK}, conn.statuses)
		assert.Equal(t, ContentEncodingZstd, conn.header.Get("Content-Encoding"))
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, conn.body.Bytes()))
	})
	t.Run("no free encoder slot", func(t *testing.T) {
		require.Zero(t, len(compressSlots))
		for len(compressSlots) < cap(compressSlots) {
			compressSlots <- struct{}{}
		}
		defer func() {
			for len(compressSlots) > 0 {
				<-compressSlots
			}
		}()
		rec := run(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) })
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, payload, rec.Body.Bytes())

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "gzip, identity;q=0")
		rec = serve(Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("handler ran without the required encoder slot")
		})), req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.Zero(t, rec.Body.Len())
		assert.Equal(t, cap(compressSlots), len(compressSlots), "rejected requests must leave other responses' slots alone")
	})
	t.Run("slots are returned", func(t *testing.T) {
		for i := 0; i < cap(compressSlots)+5; i++ {
			rec := run(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) })
			require.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"), i)
		}
		assert.Zero(t, len(compressSlots))
	})
	t.Run("an aborted response is not a valid stream", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		assert.Panics(t, func() {
			Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(payload)
				w.(http.Flusher).Flush()
				_, _ = w.Write(payload)
				panic(http.ErrAbortHandler)
			})).ServeHTTP(rec, req)
		})
		dec, err := zstd.NewReader(bytes.NewReader(rec.Body.Bytes()))
		require.NoError(t, err)
		defer dec.Close()
		_, err = io.ReadAll(dec)
		assert.Error(t, err, "the frame was never finished, so a client cannot mistake it for a whole body")
		assert.Zero(t, len(compressSlots))
	})
}

// connection is the http.ResponseWriter under the compressing one. It records
// what reaches the client. With writeErr set it fails body writes the way a
// client that went away does: all of them, or only the first failOnly ones.
type connection struct {
	header   http.Header
	statuses []int
	body     bytes.Buffer
	flushes  int
	writeErr error
	failOnly int
	failed   int
}

func (c *connection) Header() http.Header {
	if c.header == nil {
		c.header = http.Header{}
	}
	return c.header
}

func (c *connection) WriteHeader(code int) { c.statuses = append(c.statuses, code) }

func (c *connection) Write(p []byte) (int, error) {
	if c.writeErr != nil && (c.failOnly == 0 || c.failed < c.failOnly) {
		c.failed++
		return 0, c.writeErr
	}
	return c.body.Write(p)
}

func (c *connection) Flush() { c.flushes++ }

func TestResponseOnTheConnection(t *testing.T) {
	setCompression(t, true, 64, 1<<20)
	payload := jsonPayload(4096)
	run := func(h http.HandlerFunc) *connection {
		conn := &connection{}
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		Handler(h).ServeHTTP(conn, req)
		return conn
	}

	t.Run("a handler that writes nothing leaves the response to net/http", func(t *testing.T) {
		conn := run(func(w http.ResponseWriter, r *http.Request) {})
		assert.Empty(t, conn.statuses)
		assert.Zero(t, conn.body.Len())
	})
	t.Run("a status after the header went out is ignored", func(t *testing.T) {
		conn := run(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(payload)
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusInternalServerError)
		})
		assert.Equal(t, []int{http.StatusOK}, conn.statuses)
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, conn.body.Bytes()))
	})
	t.Run("flush before the first write", func(t *testing.T) {
		conn := run(func(w http.ResponseWriter, r *http.Request) {
			w.(http.Flusher).Flush()
			_, _ = w.Write(payload)
		})
		assert.Equal(t, []int{http.StatusOK}, conn.statuses)
		assert.Equal(t, 1, conn.flushes)
		assert.Equal(t, ContentEncodingZstd, conn.header.Get("Content-Encoding"))
		assert.NotContains(t, conn.header, "Content-Type", "no bytes to sniff a type from yet, as in net/http")
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, conn.body.Bytes()))
	})
}

// With minResponseSize 0 a zero-length first write must not decide the
// response before any byte is there to sniff a type from.
func TestZeroLengthFirstWrite(t *testing.T) {
	setCompression(t, true, 0, 1<<20)
	payload := jsonPayload(4096)
	run := func(h http.HandlerFunc) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		return serve(Handler(h), req)
	}

	t.Run("then a body", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(nil)
			_, _ = w.Write(payload)
		})
		assert.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"), "sniffed from the body, as net/http would")
		assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, rec.Body.Bytes()))
	})
	t.Run("and nothing else", func(t *testing.T) {
		rec := run(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, "")
		})
		assert.Equal(t, http.StatusAccepted, rec.Code)
		assert.Empty(t, rec.Header().Get("Content-Encoding"), "an empty body is not worth an encoder")
		assert.Zero(t, rec.Body.Len())
	})
}

func TestSniffSample(t *testing.T) {
	big := bytes.Repeat([]byte("b"), 1<<20)
	for _, tc := range []struct {
		name          string
		pending, next []byte
		want          []byte
	}{
		{"nothing written yet", nil, nil, nil},
		{"short body", []byte("a"), []byte("b"), []byte("ab")},
		{"one large write", nil, big, big[:512]},
		{"held-back bytes, then a large write", []byte("aaa"), big, append([]byte("aaa"), big[:509]...)},
		{"held-back bytes past the sample", big[:600], big, big[:512]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			sample := sniffSample(tc.pending, tc.next)
			runtime.ReadMemStats(&after)
			assert.True(t, bytes.Equal(tc.want, sample))
			assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10),
				"a buffered handler hands over its whole body in one write; sniffing must not copy it")
		})
	}
}

// A client that goes away fails the writes to it. The error has to reach the
// handler, nothing may panic, and the encoder slot has to come back.
func TestClientWriteFailures(t *testing.T) {
	setCompression(t, true, 64, 1<<20)
	small := []byte("0123456789") // below minSize, so held back
	payload := jsonPayload(4096)  // less than one zstd block, so buffered by the encoder
	gone := errors.New("write: broken pipe")
	run := func(t *testing.T, h http.HandlerFunc) *connection {
		conn := &connection{writeErr: gone}
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		assert.NotPanics(t, func() { Handler(h).ServeHTTP(conn, req) })
		assert.Zero(t, len(compressSlots), "the encoder slot is returned")
		return conn
	}
	preEncoded := func(w http.ResponseWriter) { w.Header().Set("Content-Encoding", "br") }

	t.Run("the write that sends the held-back bytes", func(t *testing.T) {
		var first, second error
		run(t, func(w http.ResponseWriter, r *http.Request) {
			preEncoded(w)
			_, first = w.Write(small)
			_, second = w.Write(payload)
		})
		assert.NoError(t, first, "held back: nothing was sent yet")
		assert.ErrorIs(t, second, gone)
	})
	t.Run("losing the held-back bytes fails the write, whatever happens next", func(t *testing.T) {
		conn := &connection{writeErr: gone, failOnly: 1}
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		var n int
		var err error
		Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			preEncoded(w)
			_, _ = w.Write(small)
			n, err = w.Write(payload)
		})).ServeHTTP(conn, req)
		assert.ErrorIs(t, err, gone)
		assert.Zero(t, n)
		assert.Zero(t, conn.body.Len(), "nothing follows the bytes that were lost")
	})
	t.Run("a compressed write of more than one block", func(t *testing.T) {
		var err error
		run(t, func(w http.ResponseWriter, r *http.Request) {
			_, err = w.Write(jsonPayload(1 << 20))
		})
		assert.ErrorIs(t, err, gone)
	})
	t.Run("flush of held-back bytes", func(t *testing.T) {
		conn := run(t, func(w http.ResponseWriter, r *http.Request) {
			preEncoded(w)
			_, _ = w.Write(small)
			w.(http.Flusher).Flush()
		})
		assert.Zero(t, conn.flushes, "nothing reached the client, so there is nothing to flush")
	})
	t.Run("flush of compressed bytes", func(t *testing.T) {
		conn := run(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(payload)
			w.(http.Flusher).Flush()
		})
		assert.Zero(t, conn.flushes, "nothing reached the client, so there is nothing to flush")
	})
	t.Run("held-back bytes sent when the handler returns", func(t *testing.T) {
		run(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(small) })
	})
	t.Run("the end of a compressed body", func(t *testing.T) {
		run(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) })
	})
}

// http.ResponseController reaches the connection through Unwrap, for what the
// compressing writer has no reason to implement itself.
func TestResponseControllerReachesConnection(t *testing.T) {
	setCompression(t, true, 0, 1<<20)
	payload := jsonPayload(4096)
	deadlineErr := make(chan error, 1)
	srv := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadlineErr <- http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Minute))
		_, _ = w.Write(payload)
	})))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "zstd")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.NoError(t, <-deadlineErr)
	assert.Equal(t, ContentEncodingZstd, resp.Header.Get("Content-Encoding"))
	assert.Equal(t, payload, decodeBody(t, ContentEncodingZstd, body))
}

// gin's ResponseWriter asserts http.CloseNotifier without checking, and
// c.Stream calls it first, so a streaming handler must keep working when the
// client asked for compression.
func TestGinStreamCompressed(t *testing.T) {
	setCompression(t, true, 64, 1<<20)
	event := strings.Repeat("data: event\n\n", 100)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/stream", func(c *gin.Context) {
		c.Stream(func(w io.Writer) bool {
			_, _ = io.WriteString(w, event)
			return false
		})
	})
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req.Header.Set("Accept-Encoding", "zstd")
	rec := serve(Handler(engine), req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, ContentEncodingZstd, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, event, string(decodeBody(t, ContentEncodingZstd, rec.Body.Bytes())))
}
