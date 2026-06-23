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
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const testTraceID = "0123456789abcdef0123456789abcdef"

var traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func withTestTracerProvider(t *testing.T, opts ...sdktrace.TracerProviderOption) {
	t.Helper()
	oldProvider := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		require.NoError(t, tp.Shutdown(context.Background()))
		otel.SetTracerProvider(oldProvider)
	})
}

func withTraceContextPropagator(t *testing.T) {
	t.Helper()
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTextMapPropagator(oldPropagator)
	})
}

func newTraceIDTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/traceid", nil)
	c.Set("traceID", testTraceID)
	return c, w
}

func assertTraceIDHeader(t *testing.T, headers http.Header, expected string) {
	t.Helper()
	assert.Equal(t, expected, headers.Get(HTTPHeaderMilvusTraceID))
}

func assertValidTraceIDHeader(t *testing.T, headers http.Header) string {
	t.Helper()
	traceID := headers.Get(HTTPHeaderMilvusTraceID)
	require.Regexp(t, traceIDPattern, traceID)
	assertTraceIDHeader(t, headers, traceID)
	return traceID
}

func TestHTTPReturnWritesTraceIDHeader(t *testing.T) {
	tests := []struct {
		name  string
		write func(*gin.Context)
	}{
		{
			name: "json",
			write: func(c *gin.Context) {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
			},
		},
		{
			name: "stream",
			write: func(c *gin.Context) {
				HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
			},
		},
		{
			name: "abort",
			write: func(c *gin.Context) {
				HTTPAbortReturn(c, http.StatusUnauthorized, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrNeedAuthenticate),
					HTTPReturnMessage: merr.ErrNeedAuthenticate.Error(),
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, w := newTraceIDTestContext()

			test.write(c)

			assertTraceIDHeader(t, w.Header(), testTraceID)
		})
	}
}

func TestHTTPReturnSkipsTraceIDHeaderWhenMissing(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/traceid", nil)

	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})

	assert.Empty(t, w.Header().Get(HTTPHeaderMilvusTraceID))
}

func TestTraceIDHandlerFuncCreatesRequestTraceID(t *testing.T) {
	withTestTracerProvider(t)
	app := gin.New()
	app.Use(TraceIDHandlerFunc)

	var ctxTraceID string
	app.POST("/v2/vectordb/entities/search", func(c *gin.Context) {
		spanCtx := oteltrace.SpanFromContext(c.Request.Context()).SpanContext()
		require.True(t, spanCtx.TraceID().IsValid())
		ctxTraceID = spanCtx.TraceID().String()

		traceID, ok := c.Get("traceID")
		require.True(t, ok)
		assert.Equal(t, ctxTraceID, traceID)

		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
	app.ServeHTTP(w, req)

	returnedTraceID := assertValidTraceIDHeader(t, w.Header())
	assert.Equal(t, ctxTraceID, returnedTraceID)
}

func TestTraceIDHandlerFuncContinuesIncomingTrace(t *testing.T) {
	withTestTracerProvider(t)
	withTraceContextPropagator(t)

	parentTraceID, err := oteltrace.TraceIDFromHex("11111111111111111111111111111111")
	require.NoError(t, err)
	parentSpanID, err := oteltrace.SpanIDFromHex("2222222222222222")
	require.NoError(t, err)
	parentSpan := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    parentTraceID,
		SpanID:     parentSpanID,
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})

	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.POST("/v2/vectordb/entities/search", func(c *gin.Context) {
		spanCtx := oteltrace.SpanFromContext(c.Request.Context()).SpanContext()
		assert.Equal(t, parentTraceID, spanCtx.TraceID())
		assert.NotEqual(t, parentSpanID, spanCtx.SpanID())
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
	otel.GetTextMapPropagator().Inject(
		oteltrace.ContextWithRemoteSpanContext(context.Background(), parentSpan),
		propagation.HeaderCarrier(req.Header),
	)
	app.ServeHTTP(w, req)

	assertTraceIDHeader(t, w.Header(), parentTraceID.String())
}

func TestTraceIDHandlerFuncCoversV1AndV2RestRoutes(t *testing.T) {
	withTestTracerProvider(t)
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "v1",
			method: http.MethodGet,
			path:   "/v1/vector/collections",
		},
		{
			name:   "v2",
			method: http.MethodPost,
			path:   "/v2/vectordb/collections/list",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := gin.New()
			app.Use(TraceIDHandlerFunc)
			app.Handle(test.method, test.path, func(c *gin.Context) {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
			})

			w := httptest.NewRecorder()
			req := httptest.NewRequest(test.method, test.path, nil)
			app.ServeHTTP(w, req)

			assertValidTraceIDHeader(t, w.Header())
		})
	}
}

func TestTraceIDHandlerFuncCreatesTraceIDWhenUnsampled(t *testing.T) {
	withTestTracerProvider(t, sdktrace.WithSampler(sdktrace.NeverSample()))
	app := gin.New()
	app.Use(TraceIDHandlerFunc)

	var sampled bool
	app.POST("/v2/vectordb/entities/search", func(c *gin.Context) {
		spanCtx := oteltrace.SpanFromContext(c.Request.Context()).SpanContext()
		require.True(t, spanCtx.TraceID().IsValid())
		sampled = spanCtx.IsSampled()

		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
	app.ServeHTTP(w, req)

	assert.False(t, sampled)
	assertValidTraceIDHeader(t, w.Header())
}

func TestTraceIDHandlerFuncReturnsUniqueTraceID(t *testing.T) {
	withTestTracerProvider(t)
	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.POST("/v2/vectordb/entities/search", func(c *gin.Context) {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
	})

	seen := make(map[string]struct{})
	for i := 0; i < 20; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
		app.ServeHTTP(w, req)

		traceID := assertValidTraceIDHeader(t, w.Header())
		require.NotContains(t, seen, traceID)
		seen[traceID] = struct{}{}
	}
}

func TestTraceIDHandlerFuncCoversEarlyAbort(t *testing.T) {
	withTestTracerProvider(t)
	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.GET("/auth", func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrNeedAuthenticate),
			HTTPReturnMessage: merr.ErrNeedAuthenticate.Error(),
		})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth", nil)
	app.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assertValidTraceIDHeader(t, w.Header())
}

func TestWrapperPostBadJSONReturnsTraceIDHeader(t *testing.T) {
	withTestTracerProvider(t)
	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.POST("/v2/vectordb/entities/search", wrapperPost(func() any { return &DefaultReq{} },
		func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
			return nil, nil
		}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", strings.NewReader(`{"dbName"}`))
	app.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assertValidTraceIDHeader(t, w.Header())
}

func TestWrapperPostKeepsRequestTraceID(t *testing.T) {
	withTestTracerProvider(t)
	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.Use(func(c *gin.Context) {
		c.Set(ContextUsername, "")
		c.Next()
	})

	var handlerTraceID string
	app.POST("/v2/vectordb/entities/search", wrapperPost(func() any { return &DefaultReq{} },
		func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
			handlerTraceID = oteltrace.SpanFromContext(ctx).SpanContext().TraceID().String()
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
			return nil, nil
		}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", strings.NewReader(`{}`))
	app.ServeHTTP(w, req)

	returnedTraceID := assertValidTraceIDHeader(t, w.Header())
	assert.Equal(t, handlerTraceID, returnedTraceID)
}

func TestWrapperPostUsesRequestSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	withTestTracerProvider(t, sdktrace.WithSpanProcessor(recorder))

	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.Use(func(c *gin.Context) {
		c.Set(ContextUsername, "")
		c.Next()
	})
	app.POST("/v2/vectordb/entities/search", wrapperPost(func() any { return &DefaultReq{} },
		func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil)})
			return nil, nil
		}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", strings.NewReader(`{}`))
	app.ServeHTTP(w, req)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "/v2/vectordb/entities/search", spans[0].Name())
}

func TestTimeoutMiddlewareReturnsTraceIDHeader(t *testing.T) {
	withTestTracerProvider(t)
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "10")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	app := gin.New()
	app.Use(TraceIDHandlerFunc)
	app.POST("/timeout", timeoutMiddleware(func(c *gin.Context) {
		time.Sleep(50 * time.Millisecond)
	}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/timeout", nil)
	app.ServeHTTP(w, req)

	assert.Equal(t, http.StatusRequestTimeout, w.Code)
	assertValidTraceIDHeader(t, w.Header())
}
