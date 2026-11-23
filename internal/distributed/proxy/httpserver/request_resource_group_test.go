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
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type resourceGroupHook struct {
	hookutil.DefaultHook
	group               string
	beforeErr, afterErr error
	mock                bool
}

func (h resourceGroupHook) Mock(context.Context, any, string) (bool, any, error) {
	return h.mock, &milvuspb.SearchResults{Status: merr.Success()}, nil
}

func (h resourceGroupHook) Before(ctx context.Context, req any, method string) (context.Context, error) {
	return extension.WithQueryResourceGroup(ctx, h.group), h.beforeErr
}
func (h resourceGroupHook) After(context.Context, any, error, string) error { return h.afterErr }

func TestRESTRequestCounterResourceGroup(t *testing.T) {
	paramtable.Init()
	hookutil.InitOnceHook()
	t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}); metrics.ProxyFunctionCall.Reset() })
	for _, tc := range []struct {
		name, group, expected, outcome, cause string
		beforeErr, afterErr                   error
		mock                                  bool
	}{
		{name: "stock", outcome: metrics.SuccessLabel, cause: metrics.CauseNA},
		{name: "scoped", group: "rg-a", expected: "rg-a", outcome: metrics.SuccessLabel, cause: metrics.CauseNA},
		{name: "before refusal", group: "rg-a", beforeErr: status.Error(codes.InvalidArgument, "refused"), outcome: metrics.RejectedLabel, cause: metrics.CauseUser},
		{name: "after refusal", group: "rg-a", expected: "rg-a", afterErr: status.Error(codes.InvalidArgument, "refused"), outcome: metrics.RejectedLabel, cause: metrics.CauseUser},
		{name: "mock", group: "rg-a", mock: true, outcome: metrics.SuccessLabel, cause: metrics.CauseNA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.ProxyFunctionCall.Reset()
			hookutil.SetTestHook(resourceGroupHook{group: tc.group, beforeErr: tc.beforeErr, afterErr: tc.afterErr, mock: tc.mock})
			r := gin.New()
			r.Use(func(c *gin.Context) { c.Set(ContextUsername, "user"); c.Next() })
			path := versionalV2(EntityCategory, SearchAction)
			r.POST(path, wrapperPost(func() any { return &CollectionNameReq{} }, func(ctx context.Context, c *gin.Context, req any, db string) (any, error) {
				return proxy.HookInterceptor(ctx, &milvuspb.SearchRequest{DbName: db, CollectionName: "coll"}, "user", milvuspb.MilvusService_Search_FullMethodName, func(ctx context.Context, _ any) (any, error) {
					assert.Equal(t, tc.group, extension.QueryResourceGroupFromContext(ctx))
					return &milvuspb.SearchResults{Status: merr.Success()}, nil
				})
			}))
			postJSONBody(r, path, `{"dbName":"db","collectionName":"coll"}`)
			require.Equal(t, 2, testutil.CollectAndCount(metrics.ProxyFunctionCall))
			for _, labels := range [][2]string{{metrics.TotalLabel, metrics.CauseNA}, {tc.outcome, tc.cause}} {
				assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(paramtable.GetStringNodeID(), "Search", labels[0], labels[1], "db", "coll", tc.expected)))
			}
			require.Equal(t, 2, testutil.CollectAndCount(metrics.ProxyFunctionCall))
		})
	}
}
