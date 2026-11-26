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

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
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
