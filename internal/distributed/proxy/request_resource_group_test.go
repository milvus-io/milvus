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

package grpcproxy

import (
	"context"
	"testing"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
}

func (h resourceGroupHook) Before(ctx context.Context, req any, method string) (context.Context, error) {
	return extension.WithQueryResourceGroup(ctx, h.group), h.beforeErr
}
func (h resourceGroupHook) After(context.Context, any, error, string) error { return h.afterErr }

func TestRequestCounterResourceGroup(t *testing.T) {
	paramtable.Init()
	hookutil.InitOnceHook()
	t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}); metrics.ProxyFunctionCall.Reset() })
	chain := grpc_middleware.ChainUnaryServer(UnaryRequestStatsInterceptor, proxy.UnaryServerHookInterceptor())
	info := &grpc.UnaryServerInfo{FullMethod: milvuspb.MilvusService_Search_FullMethodName}
	req := &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}
	for _, tc := range []struct {
		name, group, expected, outcome, cause string
		beforeErr, afterErr, handlerErr       error
	}{
		{name: "stock", outcome: metrics.SuccessLabel, cause: metrics.CauseNA},
		{name: "rg-a", group: "rg-a", expected: "rg-a", outcome: metrics.SuccessLabel, cause: metrics.CauseNA},
		{name: "rg-b", group: "rg-b", expected: "rg-b", outcome: metrics.SuccessLabel, cause: metrics.CauseNA},
		{name: "handler failure", group: "rg-a", expected: "rg-a", handlerErr: merr.WrapErrServiceInternalMsg("failed"), outcome: metrics.FailLabel, cause: metrics.CauseSystem},
		{name: "before refusal", group: "rg-a", beforeErr: status.Error(codes.InvalidArgument, "refused"), outcome: metrics.RejectedLabel, cause: metrics.CauseUser},
		{name: "after refusal", group: "rg-a", expected: "rg-a", afterErr: status.Error(codes.InvalidArgument, "refused"), outcome: metrics.RejectedLabel, cause: metrics.CauseUser},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.ProxyFunctionCall.Reset()
			hookutil.SetTestHook(resourceGroupHook{group: tc.group, beforeErr: tc.beforeErr, afterErr: tc.afterErr})
			_, _ = chain(context.Background(), req, info, func(ctx context.Context, _ any) (any, error) {
				assert.Equal(t, tc.group, extension.QueryResourceGroupFromContext(ctx))
				return &milvuspb.SearchResults{Status: merr.Status(tc.handlerErr)}, nil
			})
			require.Equal(t, 2, testutil.CollectAndCount(metrics.ProxyFunctionCall))
			for _, labels := range [][2]string{{metrics.TotalLabel, metrics.CauseNA}, {tc.outcome, tc.cause}} {
				assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(paramtable.GetStringNodeID(), "Search", labels[0], labels[1], "db", "coll", tc.expected)))
			}
			require.Equal(t, 2, testutil.CollectAndCount(metrics.ProxyFunctionCall), "no unscoped duplicate")
		})
	}
	metrics.ProxyFunctionCall.Reset()
	for _, group := range []string{"rg-a", "rg-b", "rg-a"} {
		hookutil.SetTestHook(resourceGroupHook{group: group})
		_, _ = chain(context.Background(), req, info, func(context.Context, any) (any, error) { return &milvuspb.SearchResults{Status: merr.Success()}, nil })
	}
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(paramtable.GetStringNodeID(), "Search", metrics.TotalLabel, metrics.CauseNA, "db", "coll", "rg-a")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(paramtable.GetStringNodeID(), "Search", metrics.TotalLabel, metrics.CauseNA, "db", "coll", "rg-b")))
	assert.Zero(t, metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{"rg": ""}))
}
