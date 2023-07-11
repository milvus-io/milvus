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

package proxy

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type limiterMock struct {
	limit             bool
	rate              float64
	quotaStates       []milvuspb.QuotaState
	quotaStateReasons []commonpb.ErrorCode
}

func (l *limiterMock) Check(collection int64, rt internalpb.RateType, n int) commonpb.ErrorCode {
	if l.rate == 0 {
		return commonpb.ErrorCode_ForceDeny
	}
	if l.limit {
		return commonpb.ErrorCode_RateLimit
	}
	return commonpb.ErrorCode_Success
}

func TestRateLimitInterceptor(t *testing.T) {
	t.Run("test getRequestInfo", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(0), nil)
		globalMetaCache = mockCache
		collection, rt, size, err := getRequestInfo(&milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.InsertRequest{}), size)
		assert.Equal(t, internalpb.RateType_DMLInsert, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.UpsertRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.InsertRequest{}), size)
		assert.Equal(t, internalpb.RateType_DMLUpsert, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.DeleteRequest{}), size)
		assert.Equal(t, internalpb.RateType_DMLDelete, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.ImportRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.ImportRequest{}), size)
		assert.Equal(t, internalpb.RateType_DMLBulkLoad, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.SearchRequest{Nq: 5})
		assert.NoError(t, err)
		assert.Equal(t, 5, size)
		assert.Equal(t, internalpb.RateType_DQLSearch, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DQLQuery, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)
		assert.Equal(t, collection, int64(0))

		collection, rt, size, err = getRequestInfo(&milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)
		assert.Equal(t, collection, int64(0))

		_, rt, size, err = getRequestInfo(&milvuspb.FlushRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLFlush, rt)

		collection, rt, size, err = getRequestInfo(&milvuspb.ManualCompactionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCompaction, rt)
		assert.Equal(t, collection, int64(0))
	})

	t.Run("test getFailedResponse", func(t *testing.T) {
		testGetFailedResponse := func(req interface{}, rt internalpb.RateType, errCode commonpb.ErrorCode, fullMethod string) {
			rsp := getFailedResponse(req, rt, errCode, fullMethod)
			assert.NotNil(t, rsp)
		}

		testGetFailedResponse(&milvuspb.DeleteRequest{}, internalpb.RateType_DMLDelete, commonpb.ErrorCode_ForceDeny, "delete")
		testGetFailedResponse(&milvuspb.ImportRequest{}, internalpb.RateType_DMLBulkLoad, commonpb.ErrorCode_MemoryQuotaExhausted, "import")
		testGetFailedResponse(&milvuspb.SearchRequest{}, internalpb.RateType_DQLSearch, commonpb.ErrorCode_DiskQuotaExhausted, "search")
		testGetFailedResponse(&milvuspb.QueryRequest{}, internalpb.RateType_DQLQuery, commonpb.ErrorCode_ForceDeny, "query")
		testGetFailedResponse(&milvuspb.CreateCollectionRequest{}, internalpb.RateType_DDLCollection, commonpb.ErrorCode_RateLimit, "createCollection")
		testGetFailedResponse(&milvuspb.FlushRequest{}, internalpb.RateType_DDLFlush, commonpb.ErrorCode_RateLimit, "flush")
		testGetFailedResponse(&milvuspb.ManualCompactionRequest{}, internalpb.RateType_DDLCompaction, commonpb.ErrorCode_RateLimit, "compaction")

		// test illegal
		rsp := getFailedResponse(&milvuspb.SearchResults{}, internalpb.RateType_DQLSearch, commonpb.ErrorCode_UnexpectedError, "method")
		assert.Nil(t, rsp)
		rsp = getFailedResponse(nil, internalpb.RateType_DQLSearch, commonpb.ErrorCode_UnexpectedError, "method")
		assert.Nil(t, rsp)
	})

	t.Run("test RateLimitInterceptor", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(0), nil)
		globalMetaCache = mockCache

		limiter := limiterMock{rate: 100}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockFullMethod"}

		limiter.limit = true
		interceptorFun := RateLimitInterceptor(&limiter)
		rsp, err := interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_RateLimit, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)

		limiter.limit = false
		interceptorFun = RateLimitInterceptor(&limiter)
		rsp, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)

		// test 0 rate, force deny
		limiter.rate = 0
		interceptorFun = RateLimitInterceptor(&limiter)
		rsp, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_ForceDeny, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)
	})
}
