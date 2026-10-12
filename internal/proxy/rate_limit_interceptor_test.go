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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
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

type snapshotLimiterCheck struct {
	dbID            int64
	collectionCount int
	rateType        internalpb.RateType
	n               int
}

type rejectingSnapshotLimiter struct {
	checks []snapshotLimiterCheck
}

func (l *rejectingSnapshotLimiter) Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rateType internalpb.RateType, n int) error {
	l.checks = append(l.checks, snapshotLimiterCheck{
		dbID:            dbID,
		collectionCount: len(collectionIDToPartIDs),
		rateType:        rateType,
		n:               n,
	})
	if n <= 0 {
		return nil
	}
	return merr.ErrServiceRateLimit
}

func (l *rejectingSnapshotLimiter) Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rateType internalpb.RateType, n int) error {
	return l.Check(dbID, collectionIDToPartIDs, rateType, n)
}

func TestRateLimitInterceptor(t *testing.T) {
	t.Run("test getRequestInfo", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			Name:                "p1",
			PartitionID:         10,
			CreatedTimestamp:    10001,
			CreatedUtcTimestamp: 10002,
		}, nil)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil)
		database, col2part, rt, size, err := GetRequestInfo(context.Background(), mockCache, &milvuspb.InsertRequest{
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

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.UpsertRequest{
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

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.DeleteRequest{
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

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.ImportRequest{
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

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.SearchRequest{
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

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.QueryRequest{
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

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.RestoreExternalSnapshotRequest{
			DbName:               "db1",
			TargetCollectionName: "restored",
			SnapshotMetadataUri:  "s3://bucket/export-root/snapshots/100/metadata/1.json",
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Empty(t, col2part)

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.ExportSnapshotRequest{
			DbName:         "db1",
			CollectionName: "foo",
			Name:           "snapshot",
			TargetS3Path:   "s3://bucket/export-root",
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))
		assert.Equal(t, 0, len(col2part[1]))

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.FlushRequest{
			CollectionNames: []string{
				"col1",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLFlush, rt)
		assert.Equal(t, database, int64(100))
		assert.Equal(t, 1, len(col2part))

		database, _, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.ManualCompactionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCompaction, rt)
		assert.Equal(t, database, int64(100))

		_, _, _, _, err = GetRequestInfo(context.Background(), mockCache, nil)
		assert.Error(t, err)

		_, _, _, _, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.CalcDistanceRequest{})
		assert.NoError(t, err)
	})

	t.Run("namespace partition mode request info", func(t *testing.T) {
		namespace := "tenant_partition"
		schema := &schemapb.CollectionSchema{
			EnableNamespace: true,
			Properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
			},
		}
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil).Times(4)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Times(4)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(mustNewSchemaInfo(schema), nil).Times(4)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, namespace).Return(&partitionInfo{
			Name:                namespace,
			PartitionID:         20,
			CreatedTimestamp:    10001,
			CreatedUtcTimestamp: 10002,
		}, nil).Times(4)
		database, col2part, rt, _, err := GetRequestInfo(context.Background(), mockCache, &milvuspb.InsertRequest{
			CollectionName: "foo",
			DbName:         "db1",
			Namespace:      &namespace,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), database)
		assert.Equal(t, internalpb.RateType_DMLInsert, rt)
		assert.Equal(t, []int64{20}, col2part[1])

		database, col2part, rt, _, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.DeleteRequest{
			CollectionName: "foo",
			DbName:         "db1",
			Namespace:      &namespace,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), database)
		assert.Equal(t, internalpb.RateType_DMLDelete, rt)
		assert.Equal(t, []int64{20}, col2part[1])

		database, col2part, rt, size, err := GetRequestInfo(context.Background(), mockCache, &milvuspb.SearchRequest{
			CollectionName: "foo",
			DbName:         "db1",
			Nq:             5,
			Namespace:      &namespace,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), database)
		assert.Equal(t, internalpb.RateType_DQLSearch, rt)
		assert.Equal(t, 5, size)
		assert.Equal(t, []int64{20}, col2part[1])

		database, col2part, rt, size, err = GetRequestInfo(context.Background(), mockCache, &milvuspb.HybridSearchRequest{
			CollectionName: "foo",
			DbName:         "db1",
			Namespace:      &namespace,
			Requests: []*milvuspb.SearchRequest{
				{Nq: 2},
				{Nq: 3},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), database)
		assert.Equal(t, internalpb.RateType_DQLSearch, rt)
		assert.Equal(t, 5, size)
		assert.Equal(t, []int64{20}, col2part[1])
	})

	t.Run("test GetFailedResponse", func(t *testing.T) {
		testGetFailedResponse := func(req interface{}, rt internalpb.RateType, err error, fullMethod string) {
			rsp := GetFailedResponse(req, err)
			assert.NotNil(t, rsp)
		}

		testGetFailedResponse(&milvuspb.DeleteRequest{}, internalpb.RateType_DMLDelete, merr.ErrServiceQuotaExceeded, "delete")
		testGetFailedResponse(&milvuspb.UpsertRequest{}, internalpb.RateType_DMLInsert, merr.ErrServiceQuotaExceeded, "upsert")
		testGetFailedResponse(&milvuspb.ImportRequest{}, internalpb.RateType_DMLBulkLoad, merr.ErrServiceMemoryLimitExceeded, "import")
		testGetFailedResponse(&milvuspb.SearchRequest{}, internalpb.RateType_DQLSearch, merr.ErrServiceDiskLimitExceeded, "search")
		testGetFailedResponse(&milvuspb.QueryRequest{}, internalpb.RateType_DQLQuery, merr.ErrServiceQuotaExceeded, "query")
		testGetFailedResponse(&milvuspb.CreateCollectionRequest{}, internalpb.RateType_DDLCollection, merr.ErrServiceRateLimit, "createCollection")
		testGetFailedResponse(&milvuspb.RestoreExternalSnapshotRequest{}, internalpb.RateType_DDLCollection, merr.ErrServiceRateLimit, "restoreExternalSnapshot")
		testGetFailedResponse(&milvuspb.ExportSnapshotRequest{}, internalpb.RateType_DDLCollection, merr.ErrServiceRateLimit, "exportSnapshot")
		testGetFailedResponse(&milvuspb.FlushRequest{}, internalpb.RateType_DDLFlush, merr.ErrServiceRateLimit, "flush")
		testGetFailedResponse(&milvuspb.ManualCompactionRequest{}, internalpb.RateType_DDLCompaction, merr.ErrServiceRateLimit, "compaction")
		testGetFailedResponse(&milvuspb.AddFileResourceRequest{}, internalpb.RateType_DDLCollection, merr.ErrServiceRateLimit, "addFileResource")
		testGetFailedResponse(&milvuspb.RemoveFileResourceRequest{}, internalpb.RateType_DDLCollection, merr.ErrServiceRateLimit, "removeFileResource")

		// test illegal
		rsp := GetFailedResponse(&milvuspb.SearchResults{}, merr.OldCodeToMerr(commonpb.ErrorCode_UnexpectedError))
		assert.Nil(t, rsp)
		rsp = GetFailedResponse(nil, merr.OldCodeToMerr(commonpb.ErrorCode_UnexpectedError))
		assert.Nil(t, rsp)
	})

	t.Run("snapshot mutations are rate limited", func(t *testing.T) {
		mockCache := NewMockCache(t)
		databaseNames := make([]string, 0)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).
			Run(func(_ context.Context, database string) {
				databaseNames = append(databaseNames, database)
			}).
			Return(&databaseInfo{
				DBID:             100,
				CreatedTimestamp: 1,
			}, nil)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)

		testCases := []struct {
			name            string
			ctx             context.Context
			request         proto.Message
			expectedDBID    int64
			expectedCollNum int
		}{
			{
				name: "create snapshot",
				request: &milvuspb.CreateSnapshotRequest{
					DbName:         "db1",
					CollectionName: "source",
				},
				expectedDBID:    100,
				expectedCollNum: 1,
			},
			{
				name: "drop snapshot",
				request: &milvuspb.DropSnapshotRequest{
					DbName:         "db1",
					CollectionName: "source",
				},
				expectedDBID:    100,
				expectedCollNum: 1,
			},
			{
				name: "restore snapshot",
				request: &milvuspb.RestoreSnapshotRequest{
					DbName:               "source_db",
					CollectionName:       "source",
					TargetDbName:         "target_db",
					TargetCollectionName: "target",
				},
				expectedDBID: 100,
			},
			{
				name: "restore snapshot to active database",
				ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
					util.HeaderDBName, "active_db",
				)),
				request: &milvuspb.RestoreSnapshotRequest{
					DbName:               "source_db",
					CollectionName:       "source",
					TargetCollectionName: "target",
				},
				expectedDBID: 100,
			},
			{
				name: "pin snapshot",
				request: &milvuspb.PinSnapshotDataRequest{
					DbName:         "db1",
					CollectionName: "source",
				},
				expectedDBID:    100,
				expectedCollNum: 1,
			},
			{
				name:         "unpin snapshot",
				request:      &milvuspb.UnpinSnapshotDataRequest{PinId: 1},
				expectedDBID: util.InvalidDBID,
			},
		}

		limiter := &rejectingSnapshotLimiter{}
		handlerCalls := 0
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalls++
			return merr.Success(), nil
		}
		interceptor := RateLimitInterceptorWithMetaCache(func() Cache { return mockCache }, limiter)
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockSnapshotMethod"}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				testCtx := testCase.ctx
				if testCtx == nil {
					testCtx = context.Background()
				}
				response, err := interceptor(testCtx, testCase.request, serverInfo, handler)
				require.NoError(t, err)
				status, ok := requestutil.GetStatusFromResponse(response)
				require.True(t, ok)
				assert.Equal(t, commonpb.ErrorCode_RateLimit, status.GetErrorCode())
			})
		}

		assert.Zero(t, handlerCalls)
		require.Len(t, limiter.checks, len(testCases))
		for i, testCase := range testCases {
			assert.Equal(t, snapshotLimiterCheck{
				dbID:            testCase.expectedDBID,
				collectionCount: testCase.expectedCollNum,
				rateType:        internalpb.RateType_DDLCollection,
				n:               1,
			}, limiter.checks[i])
		}
		assert.Equal(t, []string{"db1", "db1", "target_db", "active_db", "db1"}, databaseNames)
	})

	t.Run("test RateLimitInterceptor", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			Name:                "p1",
			PartitionID:         10,
			CreatedTimestamp:    10001,
			CreatedUtcTimestamp: 10002,
		}, nil)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{}, nil)

		limiter := limiterMock{rate: 100}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return &milvuspb.MutationResult{
				Status: merr.Success(),
			}, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockFullMethod"}

		limiter.limit = true
		interceptorFun := RateLimitInterceptorWithMetaCache(func() Cache { return mockCache }, &limiter)
		rsp, err := interceptorFun(context.Background(), &milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_RateLimit, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)

		limiter.limit = false
		interceptorFun = RateLimitInterceptorWithMetaCache(func() Cache { return mockCache }, &limiter)
		rsp, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{
			CollectionName: "foo",
			PartitionName:  "p1",
			DbName:         "db1",
		}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)

		// test 0 rate, force deny
		limiter.rate = 0
		interceptorFun = RateLimitInterceptorWithMetaCache(func() Cache { return mockCache }, &limiter)
		rsp, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_ForceDeny, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)
	})

	t.Run("request info fail", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("mock error: get database info"))
		limiter := limiterMock{rate: 100}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return &milvuspb.MutationResult{
				Status: merr.Success(),
			}, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockFullMethod"}

		limiter.limit = true
		interceptorFun := RateLimitInterceptorWithMetaCache(func() Cache { return mockCache }, &limiter)
		rsp, err := interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.(*milvuspb.MutationResult).GetStatus().GetErrorCode())
		assert.NoError(t, err)
	})
}

func TestGetInfo(t *testing.T) {
	mockCache := NewMockCache(t)
	ctx := context.Background()

	t.Run("fail to get database", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("mock error: get database info")).Times(5)
		{
			_, _, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.Error(t, err)
		}
		{
			_, _, err := getCollectionAndPartitionIDs(ctx, mockCache, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.Error(t, err)
		}
		{
			_, _, _, _, err := GetRequestInfo(ctx, mockCache, &milvuspb.FlushRequest{
				DbName: "foo",
			})
			assert.Error(t, err)
		}
		{
			_, _, _, _, err := GetRequestInfo(ctx, mockCache, &milvuspb.ManualCompactionRequest{})
			assert.Error(t, err)
		}
		{
			dbID, collectionIDInfos := getCollectionID(mockCache, &milvuspb.CreateCollectionRequest{})
			assert.Equal(t, util.InvalidDBID, dbID)
			assert.Equal(t, 0, len(collectionIDInfos))
		}
	})

	t.Run("fail to get collection", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil).Times(3)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(0), errors.New("mock error: get collection id")).Times(3)
		{
			_, _, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.Error(t, err)
		}
		{
			_, _, err := getCollectionAndPartitionIDs(ctx, mockCache, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.Error(t, err)
		}
		{
			_, _, _, _, err := GetRequestInfo(ctx, mockCache, &milvuspb.FlushRequest{
				DbName:          "foo",
				CollectionNames: []string{"coo"},
			})
			assert.Error(t, err)
		}
	})

	t.Run("fail to get collection schema", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil).Once()
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()

		_, _, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
			DbName:         "foo",
			CollectionName: "coo",
		})
		assert.Error(t, err)
	})

	t.Run("partition key mode", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil).Once()
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{
			HasPartitionKeyField: true,
		}, nil).Once()

		db, col2par, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
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
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil).Twice()
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Twice()
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error: get partition info")).Twice()
		{
			_, _, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionName:  "p1",
			})
			assert.Error(t, err)
		}
		{
			_, _, err := getCollectionAndPartitionIDs(ctx, mockCache, &milvuspb.SearchRequest{
				DbName:         "foo",
				CollectionName: "coo",
				PartitionNames: []string{"p1"},
			})
			assert.Error(t, err)
		}
	})

	t.Run("success", func(t *testing.T) {
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{
			DBID:             100,
			CreatedTimestamp: 1,
		}, nil).Times(3)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{}, nil).Times(1)
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(10), nil).Times(3)
		mockCache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			Name:        "p1",
			PartitionID: 100,
		}, nil).Times(3)
		{
			db, col2par, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
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
			db, col2par, err := getCollectionAndPartitionID(ctx, mockCache, &milvuspb.InsertRequest{
				DbName:         "foo",
				CollectionName: "coo",
			})
			assert.NoError(t, err)
			assert.Equal(t, int64(100), db)
			assert.NotNil(t, col2par[10])
			assert.Equal(t, int64(100), col2par[10][0])
		}
		{
			db, col2par, err := getCollectionAndPartitionIDs(ctx, mockCache, &milvuspb.SearchRequest{
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

	t.Run("get db request info", func(t *testing.T) {
		{
			dbID, collectionInfos, rateType, cost, err := GetRequestInfo(ctx, mockCache, &milvuspb.CreateDatabaseRequest{
				DbName: "foo",
			})
			assert.NoError(t, err)
			assert.Equal(t, util.InvalidDBID, dbID)
			assert.Equal(t, 0, len(collectionInfos))
			assert.Equal(t, internalpb.RateType_DDLDB, rateType)
			assert.Equal(t, 1, cost)
		}
		{
			dbID, collectionInfos, rateType, cost, err := GetRequestInfo(ctx, mockCache, &milvuspb.DropDatabaseRequest{
				DbName: "foo",
			})
			assert.NoError(t, err)
			assert.Equal(t, util.InvalidDBID, dbID)
			assert.Equal(t, 0, len(collectionInfos))
			assert.Equal(t, internalpb.RateType_DDLDB, rateType)
			assert.Equal(t, 1, cost)
		}
		{
			dbID, collectionInfos, rateType, cost, err := GetRequestInfo(ctx, mockCache, &milvuspb.AlterDatabaseRequest{
				DbName: "foo",
			})
			assert.NoError(t, err)
			assert.Equal(t, util.InvalidDBID, dbID)
			assert.Equal(t, 0, len(collectionInfos))
			assert.Equal(t, internalpb.RateType_DDLDB, rateType)
			assert.Equal(t, 1, cost)
		}
	})

	t.Run("get file resource request info", func(t *testing.T) {
		requests := []proto.Message{
			&milvuspb.AddFileResourceRequest{},
			&milvuspb.RemoveFileResourceRequest{},
		}
		for _, request := range requests {
			dbID, collectionInfos, rateType, cost, err := GetRequestInfo(ctx, mockCache, request)
			assert.NoError(t, err)
			assert.Equal(t, util.InvalidDBID, dbID)
			assert.Empty(t, collectionInfos)
			assert.Equal(t, internalpb.RateType_DDLCollection, rateType)
			assert.Equal(t, 1, cost)
		}

		dbID, collectionInfos, _, cost, err := GetRequestInfo(ctx, mockCache, &milvuspb.ListFileResourcesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, util.InvalidDBID, dbID)
		assert.Empty(t, collectionInfos)
		assert.Zero(t, cost)
	})
}
