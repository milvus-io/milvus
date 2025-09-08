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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
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
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.TotalLabel, dbName, collection},
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.SuccessLabel, dbName, collection},
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
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.TotalLabel, dbName, collection},
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.FailLabel, dbName, collection},
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
				{paramtable.GetStringNodeID(), "Insert", metrics.TotalLabel, dbName, collection},
				{paramtable.GetStringNodeID(), "Insert", metrics.RetryLabel, dbName, collection},
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
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.TotalLabel, dbName, collection},
				{paramtable.GetStringNodeID(), "CreateCollection", metrics.RejectedLabel, dbName, collection},
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

func TestUnaryRequestStatsInterceptor(t *testing.T) {
	suite.Run(t, new(StatsInterceptorSuite))
}
