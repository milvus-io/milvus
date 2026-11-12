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

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

type StatsInterceptorSuite struct {
	testutils.PromMetricsSuite
}

func (suite *StatsInterceptorSuite) TestUnaryRequestStatsInterceptor() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type testCase struct {
		tag          string
		req          any
		info         *grpc.UnaryServerInfo
		handler      grpc.UnaryHandler
		expectLabels [][]string
	}

	dbName := "default"
	collection := "test"

	cases := []testCase{
		{
			tag: "normal",
			req: &milvuspb.CreateCollectionRequest{
				DbName:         dbName,
				CollectionName: collection,
			},
			info: &grpc.UnaryServerInfo{
				FullMethod: milvuspb.MilvusService_CreateCollection_FullMethodName,
			},
			handler: func(ctx context.Context, req any) (interface{}, error) {
				return merr.Success(), nil
			},
			expectLabels: [][]string{
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.TotalLabel, metrics.CauseNA, dbName, collection},
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.SuccessLabel, metrics.CauseNA, dbName, collection},
			},
		},
		{
			tag: "service_internal",
			req: &milvuspb.CreateCollectionRequest{
				DbName:         dbName,
				CollectionName: collection,
			},
			info: &grpc.UnaryServerInfo{
				FullMethod: milvuspb.MilvusService_CreateCollection_FullMethodName,
			},
			handler: func(ctx context.Context, req any) (interface{}, error) {
				return merr.Status(merr.WrapErrServiceInternal("unexpcted")), nil
			},
			expectLabels: [][]string{
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.TotalLabel, metrics.CauseNA, dbName, collection},
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.FailLabel, metrics.CauseSystem, dbName, collection},
			},
		},
		{
			tag: "rate_limited",
			req: &milvuspb.InsertRequest{
				DbName:         dbName,
				CollectionName: collection,
			},
			info: &grpc.UnaryServerInfo{
				FullMethod: milvuspb.MilvusService_Insert_FullMethodName,
			},
			handler: func(ctx context.Context, req any) (interface{}, error) {
				return &milvuspb.MutationResult{
					Status: merr.Status(merr.ErrServiceRateLimit),
				}, nil
			},
			expectLabels: [][]string{
				{paramtable.GetStringNodeID(), "Insert", metrics.TotalLabel, metrics.CauseNA, dbName, collection},
				{paramtable.GetStringNodeID(), "Insert", metrics.RetryLabel, metrics.CauseNA, dbName, collection},
			},
		},
		{
			tag: "not_authorized",
			req: &milvuspb.CreateCollectionRequest{
				DbName:         dbName,
				CollectionName: collection,
			},
			info: &grpc.UnaryServerInfo{
				FullMethod: milvuspb.MilvusService_CreateCollection_FullMethodName,
			},
			handler: func(ctx context.Context, req any) (interface{}, error) {
				return nil, status.Error(codes.Unauthenticated, "auth check failure, please check api key is correct")
			},
			expectLabels: [][]string{
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.TotalLabel, metrics.CauseNA, dbName, collection},
				// Unauthenticated is the caller's fault -> rejected, cause=user.
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.RejectedLabel, metrics.CauseUser, dbName, collection},
			},
		},
	}

	for _, tc := range cases {
		suite.Run(tc.tag, func() {
			UnaryRequestStatsInterceptor(ctx, tc.req, tc.info, tc.handler)
			for _, labels := range tc.expectLabels {
				suite.MetricsEqual(metrics.ProxyFunctionCall.WithLabelValues(labels...), 1)
			}
			metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{})
		})
	}
}

type mockAnswerHook struct {
	hookutil.DefaultHook
	resp any
}

func (h mockAnswerHook) Mock(ctx context.Context, req any, fullMethod string) (bool, any, error) {
	return true, h.resp, nil
}

type beforeRefusalHook struct {
	hookutil.DefaultHook
}

func (h beforeRefusalHook) Before(ctx context.Context, req any, fullMethod string) (context.Context, error) {
	return ctx, errors.New("refused by hook")
}

type afterRefusalHook struct {
	hookutil.DefaultHook
}

func (h afterRefusalHook) After(ctx context.Context, resp any, err error, fullMethod string) error {
	return errors.New("refused by hook")
}

// A request the hook answers or refuses is counted once, by this interceptor,
// with the outcome the client sees.
func (suite *StatsInterceptorSuite) TestHookVerdictCountedOnce() {
	chain := grpc_middleware.ChainUnaryServer(UnaryRequestStatsInterceptor, proxy.UnaryServerHookInterceptor())
	info := &grpc.UnaryServerInfo{FullMethod: milvuspb.MilvusService_GetVersion_FullMethodName}
	handlerRuns := 0
	handler := func(ctx context.Context, req any) (any, error) {
		handlerRuns++
		return &milvuspb.GetVersionResponse{Status: merr.Success(), Version: "real"}, nil
	}
	nodeID := paramtable.GetStringNodeID()
	hookutil.InitOnceHook()
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	type testCase struct {
		tag         string
		hook        hook.Hook
		handlerRuns int
		outcome     [2]string
	}
	cases := []testCase{
		{
			tag:     "mock_answer",
			hook:    mockAnswerHook{resp: &milvuspb.GetVersionResponse{Status: merr.Success(), Version: "mock"}},
			outcome: [2]string{metrics.SuccessLabel, metrics.CauseNA},
		},
		{
			// a classified refusal: the hook answers from Mock with a merr.Status
			tag:     "mock_refusal_status",
			hook:    mockAnswerHook{resp: &milvuspb.GetVersionResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("refused by hook"))}},
			outcome: [2]string{metrics.FailLabel, metrics.CauseUser},
		},
		{
			tag:     "before_refusal",
			hook:    beforeRefusalHook{},
			outcome: [2]string{metrics.RejectedLabel, metrics.CauseUser},
		},
		{
			tag:         "after_refusal",
			hook:        afterRefusalHook{},
			handlerRuns: 1,
			outcome:     [2]string{metrics.RejectedLabel, metrics.CauseUser},
		},
	}
	for _, tc := range cases {
		suite.Run(tc.tag, func() {
			metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{})
			handlerRuns = 0
			hookutil.SetTestHook(tc.hook)
			_, _ = chain(context.Background(), &milvuspb.GetVersionRequest{}, info, handler)
			suite.Equal(tc.handlerRuns, handlerRuns)
			// one total and one outcome, under any labels; checked before WithLabelValues creates series
			suite.CollectCntEqual(metrics.ProxyFunctionCall, 2)
			suite.MetricsEqual(metrics.ProxyFunctionCall.WithLabelValues(nodeID, "GetVersion", metrics.TotalLabel, metrics.CauseNA, "", ""), 1)
			suite.MetricsEqual(metrics.ProxyFunctionCall.WithLabelValues(nodeID, "GetVersion", tc.outcome[0], tc.outcome[1], "", ""), 1)
		})
	}
	metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{})
}

func TestUnaryRequestStatsInterceptor(t *testing.T) {
	suite.Run(t, new(StatsInterceptorSuite))
}
