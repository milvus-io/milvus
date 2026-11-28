// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

func (s *ReadSuite) TestSearchAggregateOverflowStopsRetry() {
	// MockSuiteBase constructs the client with New and the default retry
	// interceptors, talking to a real gRPC server over bufconn.
	for _, tc := range []struct {
		name        string
		unavailable bool
		wantRPCs    int32
	}{
		{name: "overflow", wantRPCs: 1},
		// Positive control: the default transport retry must still work,
		// but stop as soon as an aggregate-overflow response is received.
		{name: "unavailable_then_overflow", unavailable: true, wantRPCs: 2},
	} {
		s.Run(tc.name, func() {
			collectionName := "aggregate_" + tc.name
			schema := entity.NewSchema().
				WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2)).
				WithField(entity.NewField().WithName("g").WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName("v").WithDataType(entity.FieldTypeInt64))
			// Keep metadata lookup separate from the Search RPC count.
			s.setupCache(collectionName, schema)
			var searchCalls atomic.Int32
			const reason = `metric "sum_v": integer SUM result exceeds int64 range`
			s.mock.EXPECT().Search(mock.Anything, mock.MatchedBy(func(req *milvuspb.SearchRequest) bool {
				return req.GetCollectionName() == collectionName && req.GetSearchAggregation().GetMetrics()["sum_v"].GetOp() == "sum"
			})).RunAndReturn(func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				if searchCalls.Add(1) == 1 && tc.unavailable {
					return nil, status.Error(codes.Unavailable, "temporary transport failure")
				}
				return &milvuspb.SearchResults{Status: &commonpb.Status{
					Code:      2203,
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    reason,
					Detail:    reason,
					Retriable: false,
				}}, nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := s.client.Search(ctx, NewSearchOption(collectionName, 2, []entity.Vector{entity.FloatVector{1, 2}}).
				WithSearchAggregation(NewSearchAggregation([]string{"g"}, 1).WithMetric("sum_v", "sum", "v")))
			s.Require().Error(err)
			s.Equal(tc.wantRPCs, searchCalls.Load())
			s.Nil(result)
			s.EqualValues(2203, ErrorCode(err))
			s.False(IsRetryableError(err))
			s.EqualError(err, reason)
			var rpcErr *RPCError
			s.Require().ErrorAs(err, &rpcErr)
			s.False(rpcErr.IsInputError())
			s.Equal(reason, rpcErr.Detail())
		})
	}
}
