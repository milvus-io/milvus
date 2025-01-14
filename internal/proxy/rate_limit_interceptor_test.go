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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type limiterMock struct {
	limit             bool
	rate              float64
	quotaStates       []milvuspb.QuotaState
	quotaStateReasons []commonpb.ErrorCode
}

func (l *limiterMock) Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error {
	if l.rate == 0 {
		return merr.ErrServiceQuotaExceeded
	}
	if l.limit {
		return merr.ErrServiceRateLimit
	}
	return nil
}

func (l *limiterMock) Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error {
	return l.Check(dbID, collectionIDToPartIDs, rt, n)
}

func TestRateLimitInterceptor(t *testing.T) {
	t.Run("test getRequestInfo", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			name:                "p1",
			partitionID:         10,
			createdTimestamp:    10001,
			createdUtcTimestamp: 10002,
		}, nil)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil)
		globalMetaCache = mockCache
		database, col2part, rt, size, err := GetRequestInfo(context.Background(), &milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}), size)
		assert.Equal(t, internalpb.RateType_DMLInsert, rt)
		assert.Equal(t, database, int64(100))
		assert.True(t, len(col2part) == 1)
		assert.Equal(t, int64(10), col2part[1][0])

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.UpsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}), size)
		assert.Equal(t, internalpb.RateType_DMLInsert, rt)
		assert.Equal(t, database, int64(100))
		assert.True(t, len(col2part) == 1)
		assert.Equal(t, int64(10), col2part[1][0])

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.DeleteRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.DeleteRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}), size)
		assert.Equal(t, internalpb.RateType_DMLDelete, rt)
		assert.Equal(t, database, int64(100))
		assert.True(t, len(col2part) == 1)
		assert.Equal(t, int64(10), col2part[1][0])

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.ImportRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.ImportRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}), size)
		assert.Equal(t, internalpb.RateType_DMLBulkLoad, rt)
		assert.Equal(t, database, int64(100))
		assert.True(t, len(col2part) == 1)
		assert.Equal(t, int64(10), col2part[1][0])

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.SearchRequest{
			Nq: 5,
			PartitionNames: []string{
				"p1",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 5, size)
		assert.Equal(t, internalpb.RateType_DQLSearch, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 1, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.QueryRequest{
			CollectionName: "foo",
			PartitionNames: []string{
				"p1",
			},
			DbName: "db1",
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DQLQuery, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 1, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.FlushRequest{
			CollectionNames: []string{
				"col1",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLFlush, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))

		database, _, rt, size, err = GetRequestInfo(context.Background(), &milvuspb.ManualCompactionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCompaction, rt)
		assert.Equal(t, database, int64(100))

		_, _, _, _, err = GetRequestInfo(context.Background(), nil)
		assert.Error(t, err)

		_, _, _, _, err = GetRequestInfo(context.Background(), &milvuspb.CalcDistanceRequest{})
		assert.NoError(t, err)
	})

	t.Run("test GetFailedResponse", func(t *testing.T) {
		testGetFailedResponse := func(req interface{}, rt internalpb.RateType, err error, fullMethod string) {
			rsp := GetFailedResponse(req, err)
			assert.NotNil(t, rsp)
		}

		testGetFailedResponse(&milvuspb.DeleteRequest{}, internalpb.RateType_DMLDelete, merr.ErrServiceQuotaExceeded, "delete")
		testGetFailedResponse(&milvuspb.UpsertRequest{}, internalpb.RateType_DMLUpsert, merr.ErrServiceQuotaExceeded, "upsert")
		testGetFailedResponse(&milvuspb.ImportRequest{}, internalpb.RateType_DMLBulkLoad, merr.ErrServiceMemoryLimitExceeded, "import")
		testGetFailedResponse(&milvuspb.SearchRequest{}, internalpb.RateType_DQLSearch, merr.ErrServiceDiskLimitExceeded, "search")
		testGetFailedResponse(&milvuspb.QueryRequest{}, internalpb.RateType_DQLQuery, merr.ErrServiceQuotaExceeded, "query")
		testGetFailedResponse(&milvuspb.CreateCollectionRequest{}, internalpb.RateType_DDLCollection, merr.ErrServiceRateLimit, "createCollection")
		testGetFailedResponse(&milvuspb.FlushRequest{}, internalpb.RateType_DDLFlush, merr.ErrServiceRateLimit, "flush")
		testGetFailedResponse(&milvuspb.ManualCompactionRequest{}, internalpb.RateType_DDLCompaction, merr.ErrServiceRateLimit, "compaction")

		// test illegal
		rsp := GetFailedResponse(&milvuspb.SearchResults{}, merr.OldCodeToMerr(commonpb.ErrorCode_UnexpectedError))
		assert.Nil(t, rsp)
		rsp = GetFailedResponse(nil, merr.OldCodeToMerr(commonpb.ErrorCode_UnexpectedError))
		assert.Nil(t, rsp)
	})

	t.Run("test RateLimitInterceptor", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			name:                "p1",
			partitionID:         10,
			createdTimestamp:    10001,
			createdUtcTimestamp: 10002,
		}, nil)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{}, nil)
		globalMetaCache = mockCache

		limiter := limiterMock{rate: 100}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return &milvuspb.MutationResult{
				Status: merr.Success(),
			}, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockFullMethod"}

		limiter.limit = true
		interceptorFun := RateLimitInterceptor(&limiter)
		rsp, err := interceptorFun(context.Background(), &milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_RateLimit, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)

		limiter.limit = false
		interceptorFun = RateLimitInterceptor(&limiter)
		rsp, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)

		// test 0 rate, force deny
		limiter.rate = 0
		interceptorFun = RateLimitInterceptor(&limiter)
		rsp, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_ForceDeny, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)
	})

	t.Run("request info fail", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("mock error: get database info"))
		originCache := globalMetaCache
		globalMetaCache = mockCache
		defer func() {
			globalMetaCache = originCache
		}()

		limiter := limiterMock{rate: 100}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return &milvuspb.MutationResult{
				Status: merr.Success(),
			}, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockFullMethod"}

		limiter.limit = true
		interceptorFun := RateLimitInterceptor(&limiter)
		rsp, err := interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)
	})
}

func TestGetInfo(t *testing.T) {
	mockCache := NewMockCache(t)
	ctx := context.Background()
	originCache := globalMetaCache
	globalMetaCache = mockCache
	defer func() {
		globalMetaCache = originCache
	}()

	t.Run("fail to get database", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("mock error: get database info")).Times(5)
		{
			_, _, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.Error(t, err)
		}
		{
			_, _, err := getCollectionAndPartitionIDs(ctx, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.Error(t, err)
		}
		{
			_, _, _, _, err := GetRequestInfo(ctx, &milvuspb.FlushRequest{
				DbName: "foo",
			})
			assert.Error(t, err)
		}
		{
			_, _, _, _, err := GetRequestInfo(ctx, &milvuspb.ManualCompactionRequest{})
			assert.Error(t, err)
		}
		{
			dbID, collectionIDInfos := getCollectionID(&milvuspb.CreateCollectionRequest{})
			assert.Equal(t, util.InvalidDBID, dbID)
			assert.Equal(t, 0, len(collectionIDInfos))
		}
	})

	t.Run("fail to get collection", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil).Times(3)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(0), errors.New("mock error: get collection id")).Times(3)
		{
			_, _, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.Error(t, err)
		}
		{
			_, _, err := getCollectionAndPartitionIDs(ctx, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.Error(t, err)
		}
		{
			_, _, _, _, err := GetRequestInfo(ctx, &milvuspb.FlushRequest{
				DbName:          "foo",
				CollectionNames: []string{"coo"},
			})
			assert.Error(t, err)
		}
	})

	t.Run("fail to get collection schema", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil).Once()
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()

		_, _, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
			DbName:         "foo",
			CollectionName: "coo",
		})
		assert.Error(t, err)
	})

	t.Run("partition key mode", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil).Once()
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{
			hasPartitionKeyField: true,
		}, nil).Once()

		db, col2par, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
			DbName:         "foo",
			CollectionName: "coo",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), db)
		assert.NotNil(t, col2par[1])
		assert.Equal(t, 0, len(col2par[1]))
	})

	t.Run("fail to get partition", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil).Twice()
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Twice()
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error: get partition info")).Twice()
		{
			_, _, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.Error(t, err)
		}
		{
			_, _, err := getCollectionAndPartitionIDs(ctx, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.Error(t, err)
		}
	})

	t.Run("success", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			dbID:             100,
			createdTimestamp: 1,
		}, nil).Times(3)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{}, nil).Times(1)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(10), nil).Times(3)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			name:        "p1",
			partitionID: 100,
		}, nil).Times(3)
		{
			db, col2par, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.NoError(t, err)
			assert.Equal(t, int64(100), db)
			assert.NotNil(t, col2par[10])
			assert.Equal(t, int64(100), col2par[10][0])
		}
		{
			db, col2par, err := getCollectionAndPartitionID(ctx, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
			})
			assert.NoError(t, err)
			assert.Equal(t, int64(100), db)
			assert.NotNil(t, col2par[10])
			assert.Equal(t, int64(100), col2par[10][0])
		}
		{
			db, col2par, err := getCollectionAndPartitionIDs(ctx, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.NoError(t, err)
			assert.Equal(t, int64(100), db)
			assert.NotNil(t, col2par[10])
			assert.Equal(t, int64(100), col2par[10][0])
		}
	})
}
