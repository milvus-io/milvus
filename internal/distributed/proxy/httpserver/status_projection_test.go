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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/iotest"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestErrToHTTPStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, http.StatusOK},
		{"need authenticate", merr.ErrNeedAuthenticate, http.StatusUnauthorized},
		{"not authenticated", merr.ErrPrivilegeNotAuthenticated, http.StatusUnauthorized},
		{"not permitted", merr.ErrPrivilegeNotPermitted, http.StatusForbidden},
		{"downstream queue full", merr.ErrServiceTooManyRequests, http.StatusInternalServerError},
		{"downstream rate limit", merr.ErrServiceRateLimit, http.StatusInternalServerError},
		{"downstream quota exceeded", merr.ErrServiceQuotaExceeded, http.StatusInternalServerError},
		{"http pre-execution rate limit", merr.ErrHTTPRateLimit, http.StatusTooManyRequests},
		{"input sentinel", merr.ErrParameterInvalid, http.StatusBadRequest},
		{"boundary-marked input", merr.WrapErrAsInputError(merr.ErrCollectionNotFound), http.StatusBadRequest},
		{"collection not loaded", merr.ErrCollectionNotLoaded, http.StatusInternalServerError},
		{"schema mismatch", merr.ErrCollectionSchemaMismatch, http.StatusInternalServerError},
		{"retriable system", merr.ErrServiceUnavailable, http.StatusInternalServerError},
		{"non-retriable system", merr.ErrServiceInternal, http.StatusInternalServerError},
		{"non-merr error", assert.AnError, http.StatusInternalServerError},
		{"deadline exceeded", context.DeadlineExceeded, http.StatusInternalServerError},
		{"canceled without provenance", context.Canceled, http.StatusInternalServerError},
		{"raw grpc InvalidArgument", status.Error(codes.InvalidArgument, "x"), http.StatusBadRequest},
		{"raw grpc Unauthenticated", status.Error(codes.Unauthenticated, "x"), http.StatusUnauthorized},
		{"raw grpc PermissionDenied", status.Error(codes.PermissionDenied, "x"), http.StatusForbidden},
		{"raw grpc Unavailable", status.Error(codes.Unavailable, "x"), http.StatusInternalServerError},
		{"raw grpc DeadlineExceeded", status.Error(codes.DeadlineExceeded, "x"), http.StatusInternalServerError},
		{"raw grpc Canceled without provenance", status.Error(codes.Canceled, "x"), http.StatusInternalServerError},
		{"raw grpc NotFound", status.Error(codes.NotFound, "x"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, errToHTTPStatus(tc.err))
		})
	}
}

func TestErrorTypeForAccessLog(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want merr.ErrorType
	}{
		{"merr input", merr.ErrParameterInvalid, merr.InputError},
		{"merr system", merr.ErrServiceInternal, merr.SystemError},
		{"raw grpc InvalidArgument", status.Error(codes.InvalidArgument, "x"), merr.InputError},
		{"raw grpc Unauthenticated", status.Error(codes.Unauthenticated, "x"), merr.InputError},
		{"raw grpc PermissionDenied", status.Error(codes.PermissionDenied, "x"), merr.InputError},
		{"raw grpc Unavailable", status.Error(codes.Unavailable, "x"), merr.SystemError},
		{"raw grpc Canceled", status.Error(codes.Canceled, "x"), merr.SystemError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, errorTypeForAccessLog(tc.err))
		})
	}
}

func TestErrToHTTPStatusSurvivesWire(t *testing.T) {
	roundTrip := func(err error) error { return merr.Error(merr.Status(err)) }

	assert.Equal(t, http.StatusBadRequest, errToHTTPStatus(roundTrip(merr.ErrParameterInvalid)))
	assert.Equal(t, http.StatusBadRequest, errToHTTPStatus(roundTrip(merr.WrapErrAsInputError(merr.ErrCollectionNotFound))))
	assert.Equal(t, http.StatusInternalServerError, errToHTTPStatus(roundTrip(merr.ErrServiceRateLimit)))
	assert.Equal(t, http.StatusInternalServerError, errToHTTPStatus(roundTrip(merr.ErrServiceUnavailable)))

	legacyInput := &commonpb.Status{ErrorCode: commonpb.ErrorCode_IllegalArgument, Reason: "legacy input"}
	assert.Equal(t, http.StatusBadRequest, errToHTTPStatus(errorFromStatusForHTTP(legacyInput)))

	legacySystem := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "legacy system"}
	assert.Equal(t, http.StatusInternalServerError, errToHTTPStatus(errorFromStatusForHTTP(legacySystem)))
}

func TestProjectedStatusGate(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key

	assert.Equal(t, http.StatusOK, projectedStatus(merr.ErrServiceRateLimit))
	assert.Equal(t, http.StatusOK, projectedStatus(merr.ErrParameterInvalid))

	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)
	assert.Equal(t, http.StatusInternalServerError, projectedStatus(merr.ErrServiceRateLimit))
	assert.Equal(t, http.StatusBadRequest, projectedStatus(merr.ErrParameterInvalid))
	assert.Equal(t, http.StatusOK, projectedStatus(nil))
}

func TestProjectedStatusForRequest(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	newGinCtx := func(canceled bool) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		req := httptest.NewRequest(http.MethodPost, "/x", nil)
		if canceled {
			reqCtx, cancel := context.WithCancel(req.Context())
			cancel()
			req = req.WithContext(reqCtx)
		}
		c.Request = req
		return c
	}

	assert.Equal(t, statusClientClosedRequest, projectedStatusForRequest(newGinCtx(true), context.Canceled))
	assert.Equal(t, statusClientClosedRequest, projectedStatusForRequest(newGinCtx(true), status.Error(codes.Canceled, "x")))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(false), context.Canceled))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(false), status.Error(codes.Canceled, "x")))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(true), merr.ErrServiceUnavailable))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(true), context.DeadlineExceeded))
}

func TestProjectedAuthorizationStatus(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/x", nil)

	assert.Equal(t, http.StatusForbidden, projectedAuthorizationStatus(c, assert.AnError))

	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)
	assert.Equal(t, http.StatusForbidden, projectedAuthorizationStatus(c, status.Error(codes.PermissionDenied, "denied")))
	assert.Equal(t, http.StatusUnauthorized, projectedAuthorizationStatus(c, status.Error(codes.Unauthenticated, "missing")))
	assert.Equal(t, http.StatusInternalServerError, projectedAuthorizationStatus(c, assert.AnError))
	canceledCtx, cancel := context.WithCancel(c.Request.Context())
	cancel()
	c.Request = c.Request.WithContext(canceledCtx)
	assert.Equal(t, statusClientClosedRequest, projectedAuthorizationStatus(c, context.Canceled))
}

func TestMiddlewareTimeoutStatus(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key

	assert.Equal(t, http.StatusRequestTimeout, middlewareTimeoutStatus(true))
	assert.Equal(t, http.StatusRequestTimeout, middlewareTimeoutStatus(false))

	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)
	assert.Equal(t, http.StatusRequestTimeout, middlewareTimeoutStatus(false))
	assert.Equal(t, http.StatusInternalServerError, middlewareTimeoutStatus(true))
}

func TestBodyReadTracker(t *testing.T) {
	t.Run("complete", func(t *testing.T) {
		recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
		body := &bodyReadTracker{
			ReadCloser:   io.NopCloser(bytes.NewReader([]byte(`{"bad json`))),
			bodyReceived: &recorder.bodyReceived,
		}
		_, err := io.ReadAll(body)
		assert.NoError(t, err)
		assert.True(t, recorder.bodyReceived.Load())
	})

	t.Run("read failure", func(t *testing.T) {
		recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
		body := &bodyReadTracker{
			ReadCloser:   io.NopCloser(iotest.ErrReader(assert.AnError)),
			bodyReceived: &recorder.bodyReceived,
		}
		_, err := io.ReadAll(body)
		assert.Error(t, err)
		assert.False(t, recorder.bodyReceived.Load())
	})
}

func TestTimeoutMiddlewareClassifiesBodyReceipt(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.StandardErrorStatus.Key, "true")
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "20")
	defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.StandardErrorStatus.Key)
	defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)

	ginHandler := gin.New()
	ginHandler.Use(func(c *gin.Context) {
		c.Set(ContextUsername, "root")
		c.Next()
	})
	ginHandler.POST("/complete", timeoutMiddleware(wrapperPost(func() any { return &DefaultReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})))
	ginHandler.POST("/incomplete", timeoutMiddleware(wrapperPost(func() any { return &DefaultReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	})))

	completeReq := httptest.NewRequest(http.MethodPost, "/complete", bytes.NewReader([]byte(`{}`)))
	completeResp := httptest.NewRecorder()
	ginHandler.ServeHTTP(completeResp, completeReq)
	assert.Equal(t, http.StatusInternalServerError, completeResp.Code)

	reader, writer := io.Pipe()
	incompleteReq := httptest.NewRequest(http.MethodPost, "/incomplete", reader)
	incompleteResp := httptest.NewRecorder()
	ginHandler.ServeHTTP(incompleteResp, incompleteReq)
	assert.Equal(t, http.StatusRequestTimeout, incompleteResp.Code)
	assert.NoError(t, writer.Close())
}

func TestProjectedStatusThroughHandlers(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	h := &HandlersV2{metaCache: func() proxy.Cache { return nil }}
	ginHandler := gin.Default()
	app := ginHandler.Group("", genAuthMiddleWare(false))
	app.POST("/param", wrapperPost(func() any { return &CollectionNameReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	}))
	rpcErrRoute := func(rpcErr error) gin.HandlerFunc {
		return wrapperPost(func() any { return &DefaultReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
			return h.wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/Search", func(reqCtx context.Context, req any) (any, error) {
				return nil, rpcErr
			})
		})
	}
	app.POST("/v2/vectordb/entities/hybrid_search", rpcErrRoute(merr.ErrServiceRateLimit))
	app.POST("/v2/vectordb/entities/search", rpcErrRoute(merr.ErrServiceUnavailable))

	cases := []struct {
		path       string
		body       string
		wantStatus int
		wantCode   int32
	}{
		{"/param", `{}`, http.StatusBadRequest, merr.Code(merr.ErrMissingRequiredParameters)},
		{"/v2/vectordb/entities/hybrid_search", `{}`, http.StatusInternalServerError, merr.Code(merr.ErrServiceRateLimit)},
		{"/v2/vectordb/entities/search", `{}`, http.StatusInternalServerError, merr.Code(merr.ErrServiceUnavailable)},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader([]byte(tc.body)))
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, tc.wantStatus, w.Code)
			returnBody := &ReturnErrMsg{}
			assert.NoError(t, json.Unmarshal(w.Body.Bytes(), returnBody))
			assert.Equal(t, tc.wantCode, returnBody.Code)
		})
	}
}
