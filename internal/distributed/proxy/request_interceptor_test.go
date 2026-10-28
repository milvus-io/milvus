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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
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
	// the collection exists: every request that resolves it caches it
	cache := proxy.NewMockCache(suite.T())
	cache.EXPECT().HasCachedCollection(dbName, collection).Return(true).Maybe()
	getMetaCache := func() proxy.Cache { return cache }

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
			UnaryRequestStatsInterceptor(getMetaCache)(ctx, tc.req, tc.info, tc.handler)
			for _, labels := range tc.expectLabels {
				suite.MetricsEqual(metrics.ProxyFunctionCall.WithLabelValues(labels...), 1)
			}
			metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{})
		})
	}
}

// A request naming a collection or database that does not exist is counted
// without the name: nothing would ever delete a series labeled with it, and the
// name is the client's to vary.
func (suite *StatsInterceptorSuite) TestNamesThatDoNotExistAreNotLabels() {
	ctx := context.Background()
	nodeID := paramtable.GetStringNodeID()
	cache := proxy.NewMockCache(suite.T())
	cache.EXPECT().HasCachedCollection("default", "ghost").Return(false).Maybe()
	cache.EXPECT().HasCachedCollection("ghostdb", "ghost").Return(false).Maybe()
	cache.EXPECT().HasCachedDatabase("default").Return(true).Maybe()
	cache.EXPECT().HasCachedDatabase("ghostdb").Return(false).Maybe()
	interceptor := UnaryRequestStatsInterceptor(func() proxy.Cache { return cache })
	notFound := func(ctx context.Context, req any) (interface{}, error) {
		return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrCollectionNotFound("ghost"))}, nil
	}
	search := &grpc.UnaryServerInfo{FullMethod: milvuspb.MilvusService_Search_FullMethodName}

	for _, db := range []string{"default", "ghostdb"} {
		suite.Run(db, func() {
			defer metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{})
			expectDB := db
			if db == "ghostdb" {
				expectDB = ""
			}
			interceptor(ctx, &milvuspb.SearchRequest{DbName: db, CollectionName: "ghost"}, search, notFound)

			suite.MetricsEqual(metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.TotalLabel, metrics.CauseNA, expectDB, ""), 1)
			suite.MetricsEqual(metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.FailLabel, metrics.CauseSystem, expectDB, ""), 1)
			suite.Equal(0, metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{"collection_name": "ghost"}))
		})
	}
}

// The received bytes are recorded once the response is written, from the names
// the handler put into the RPC stats; the interceptor keeps them only while
// they exist.
func (suite *StatsInterceptorSuite) TestStatsNamesThatDoNotExistAreCleared() {
	cache := proxy.NewMockCache(suite.T())
	cache.EXPECT().HasCachedCollection("default", "ghost").Return(false).Maybe()
	cache.EXPECT().HasCachedDatabase("default").Return(true).Maybe()
	interceptor := UnaryRequestStatsInterceptor(func() proxy.Cache { return cache })

	ctx := metrics.WrapRestfulContext(context.Background(), 1)
	interceptor(ctx, &milvuspb.SearchRequest{DbName: "default", CollectionName: "ghost"},
		&grpc.UnaryServerInfo{FullMethod: milvuspb.MilvusService_Search_FullMethodName},
		func(ctx context.Context, req any) (interface{}, error) {
			metrics.GetStats(ctx).SetDatabaseName("default").SetCollectionName("ghost")
			return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrCollectionNotFound("ghost"))}, nil
		})

	suite.Equal("default", metrics.GetStats(ctx).DatabaseName())
	suite.Equal("", metrics.GetStats(ctx).CollectionName())
	metrics.ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{})
}

func TestUnaryRequestStatsInterceptor(t *testing.T) {
	suite.Run(t, new(StatsInterceptorSuite))
}
