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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type reportedAction struct {
	ctx        context.Context
	req        interface{}
	fullMethod string
	action     string
}

type actionRecorder struct {
	hookutil.DefaultExtension
	actions []reportedAction
}

func (r *actionRecorder) ReportAction(ctx context.Context, req interface{}, _ interface{}, _ error, fullMethod string, action string) error {
	r.actions = append(r.actions, reportedAction{ctx: ctx, req: req, fullMethod: fullMethod, action: action})
	return nil
}

func (r *actionRecorder) authorizeActions() []reportedAction {
	var result []reportedAction
	for _, action := range r.actions {
		if action.action == hookutil.ActionAuthorize {
			result = append(result, action)
		}
	}
	return result
}

func recordActions(t *testing.T) *actionRecorder {
	original := hookutil.GetExtension()
	t.Cleanup(func() { hookutil.SetTestExtension(original) })
	recorder := &actionRecorder{}
	hookutil.SetTestExtension(recorder)
	return recorder
}

// A refusal reported as an authorization event carries the gin context, the
// only place the hook extension can read the HTTP caller from.
func TestCheckAuthorizationV2ReportsWithGinContext(t *testing.T) {
	denied := status.Error(codes.PermissionDenied, "PrivilegeSearch: permission deny to alice in the `default` database")
	privilege := mockey.Mock(proxy.PrivilegeInterceptorWithMetaCache).Return(
		proxy.PrivilegeFunc(func(ctx context.Context, _ interface{}) (context.Context, error) {
			return ctx, denied
		})).Build()
	defer privilege.UnPatch()

	for _, tc := range []struct {
		name     string
		username string
		status   int
	}{
		{name: "no authenticated user", username: "", status: http.StatusUnauthorized},
		{name: "privilege denied", username: "alice", status: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := recordActions(t)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
			c.Set(ContextUsername, tc.username)
			req := &milvuspb.SearchRequest{DbName: "default", CollectionName: "collection"}

			err := (&HandlersV2{}).checkAuthorizationV2(context.Background(), c, false, req)

			assert.Error(t, err)
			assert.Equal(t, tc.status, w.Code)
			reports := recorder.authorizeActions()
			require.Len(t, reports, 1)
			assert.Same(t, c, reports[0].ctx.Value(hook.GinParamsKey))
			assert.Same(t, req, reports[0].req)
		})
	}
}

// A rate limit rejection is not an authorization decision. The gRPC limiter
// does not report it as one, so the RESTful entry must not either.
func TestRateLimitRejectionIsNotReportedAsAuthorization(t *testing.T) {
	paramtable.Init()
	checkLimiter := mockey.Mock(CheckLimiter).Return(nil, merr.ErrServiceRateLimit).Build()
	defer checkLimiter.UnPatch()
	recorder := recordActions(t)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
	handled := false

	_, err := (&HandlersV2{}).wrapperProxyWithLimit(context.Background(), c, &milvuspb.SearchRequest{}, false, false,
		"/milvus.proto.milvus.MilvusService/Search", true, nil,
		func(context.Context, any) (any, error) { handled = true; return nil, nil })

	assert.ErrorIs(t, err, RestRequestInterceptorErr)
	assert.False(t, handled)
	code, _ := c.Get(HTTPReturnCode)
	assert.EqualValues(t, merr.Code(merr.ErrHTTPRateLimit), code)
	assert.Empty(t, recorder.authorizeActions())
}

type restQuotaTestHook struct {
	hookutil.DefaultHook
	mockResponse any
	before       func(context.Context, any) (context.Context, error)
	after        func(any, error) error
}

func (h restQuotaTestHook) Mock(context.Context, any, string) (bool, any, error) {
	return h.mockResponse != nil, h.mockResponse, nil
}

func (h restQuotaTestHook) Before(ctx context.Context, req any, _ string) (context.Context, error) {
	if h.before != nil {
		return h.before(ctx, req)
	}
	return ctx, nil
}

func (h restQuotaTestHook) After(_ context.Context, result any, err error, _ string) error {
	return h.after(result, err)
}

func setRESTQuotaTestHook(t *testing.T, testHook hook.Hook) {
	t.Helper()
	original := hookutil.GetHook()
	hookutil.SetTestHook(testHook)
	t.Cleanup(func() { hookutil.SetTestHook(original) })
}

func TestRESTProxyHookCanStopBeforeQuota(t *testing.T) {
	paramtable.Init()
	mockResponse := &milvuspb.SearchResults{Status: merr.Success()}
	for _, tc := range []struct {
		name     string
		mock     any
		before   func(context.Context, any) (context.Context, error)
		wantCode codes.Code
	}{
		{name: "mock response", mock: mockResponse, wantCode: codes.OK},
		{
			name: "before error",
			before: func(ctx context.Context, _ any) (context.Context, error) {
				return ctx, merr.ErrServiceUnavailable
			},
			wantCode: codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRESTQuotaTestHook(t, restQuotaTestHook{mockResponse: tc.mock, before: tc.before, after: func(any, error) error {
				t.Fatal("After must not run when Mock or Before ends the request")
				return nil
			}})
			limiter := mockey.Mock(CheckLimiter).To(func(context.Context, any, types.ProxyComponent) (any, error) {
				t.Fatal("quota must not be checked when Mock or Before ends the request")
				return nil, nil
			}).Build()
			defer limiter.UnPatch()

			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
			resp, err := (&HandlersV2{}).wrapperProxyWithLimit(context.Background(), c, &milvuspb.SearchRequest{}, false, false,
				"/milvus.proto.milvus.MilvusService/Search", true, nil,
				func(context.Context, any) (any, error) {
					t.Fatal("business handler must not run")
					return nil, nil
				})
			assert.Equal(t, tc.wantCode, status.Code(err))
			assert.Equal(t, tc.mock, resp)
		})
	}
}

func TestRESTProxyHookAfterReceivesQuotaRejection(t *testing.T) {
	paramtable.Init()
	type hookContextKey struct{}
	for _, tc := range []struct {
		name      string
		req       any
		method    string
		rejection any
		afterErr  error
	}{
		{
			name:      "search native response",
			req:       &milvuspb.SearchRequest{},
			method:    "/milvus.proto.milvus.MilvusService/Search",
			rejection: &milvuspb.SearchResults{Status: merr.Status(merr.ErrServiceQuotaExceeded)},
		},
		{
			name:   "refresh status fallback",
			req:    &milvuspb.RefreshExternalCollectionRequest{},
			method: "/milvus.proto.milvus.MilvusService/RefreshExternalCollection",
		},
		{
			name:     "after error takes precedence",
			req:      &milvuspb.SearchRequest{},
			method:   "/milvus.proto.milvus.MilvusService/Search",
			afterErr: merr.ErrServiceUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var phases []string
			setRESTQuotaTestHook(t, restQuotaTestHook{
				before: func(ctx context.Context, req any) (context.Context, error) {
					phases = append(phases, "before")
					switch req := req.(type) {
					case *milvuspb.SearchRequest:
						req.CollectionName = "from_hook"
					case *milvuspb.RefreshExternalCollectionRequest:
						req.CollectionName = "from_hook"
					}
					return context.WithValue(ctx, hookContextKey{}, true), nil
				},
				after: func(rejection any, err error) error {
					phases = append(phases, "after")
					require.NoError(t, err)
					if tc.rejection != nil {
						require.Same(t, tc.rejection, rejection)
					} else {
						failure, ok := rejection.(*commonpb.Status)
						require.True(t, ok)
						assert.ErrorIs(t, merr.Error(failure), merr.ErrServiceQuotaExceeded)
					}
					return tc.afterErr
				},
			})
			limiter := mockey.Mock(CheckLimiter).To(func(ctx context.Context, req any, _ types.ProxyComponent) (any, error) {
				phases = append(phases, "quota")
				assert.Equal(t, true, ctx.Value(hookContextKey{}))
				assert.Same(t, tc.req, req)
				assert.Equal(t, "from_hook", req.(interface{ GetCollectionName() string }).GetCollectionName())
				return tc.rejection, merr.ErrServiceQuotaExceeded
			}).Build()
			defer limiter.UnPatch()

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			previous := &milvuspb.DescribeCollectionResponse{Status: merr.Success()}
			c.Set(ContextResponse, previous)
			resp, err := (&HandlersV2{}).wrapperProxyWithLimit(context.Background(), c, tc.req, false, false,
				tc.method, true, nil, func(context.Context, any) (any, error) {
					t.Fatal("business handler must not run when quota rejects the request")
					return nil, nil
				})
			assert.Nil(t, resp)
			assert.Equal(t, []string{"before", "quota", "after"}, phases)
			assert.Equal(t, http.StatusOK, w.Code)
			actualResponse, _ := c.Get(ContextResponse)
			assert.Same(t, previous, actualResponse)
			code, _ := c.Get(HTTPReturnCode)
			if tc.afterErr == nil {
				assert.ErrorIs(t, err, RestRequestInterceptorErr)
				assert.EqualValues(t, merr.Code(merr.ErrHTTPRateLimit), code)
				assert.Contains(t, w.Body.String(), merr.ErrServiceQuotaExceeded.Error())
			} else {
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				assert.EqualValues(t, merr.Code(err), code)
				assert.Contains(t, w.Body.String(), tc.afterErr.Error())
			}
		})
	}
}
