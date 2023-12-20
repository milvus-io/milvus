package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_GetExpr(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:        "test_delete",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}
	t.Run("delelte with complex pk expr", func(t *testing.T) {
		expr := "pk < 4"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr)
		assert.NoError(t, err)
		isSimple, _ := getExpr(plan)
		assert.False(t, isSimple)
	})

	t.Run("delete with no-pk field expr", func(t *testing.T) {
		expr := "non_pk in [1, 2, 3]"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr)
		assert.NoError(t, err)
		isSimple, _ := getExpr(plan)
		assert.False(t, isSimple)
	})

	t.Run("delete with simple expr", func(t *testing.T) {
		expr := "pk in [1, 2, 3]"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr)
		assert.NoError(t, err)
		isSimple, _ := getExpr(plan)
		assert.True(t, isSimple)
	})
}

func TestDeleteTask_GetChannels(t *testing.T) {
	collectionID := UniqueID(0)
	collectionName := "col-0"
	channels := []pChan{"mock-chan-0", "mock-chan-1"}
	cache := NewMockCache(t)
	cache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)
	globalMetaCache = cache
	chMgr := NewMockChannelsMgr(t)
	chMgr.EXPECT().getChannels(mock.Anything).Return(channels, nil)
	dt := deleteTask{
		ctx: context.Background(),
		req: &milvuspb.DeleteRequest{
			CollectionName: collectionName,
		},
		chMgr: chMgr,
	}
	err := dt.setChannels()
	assert.NoError(t, err)
	resChannels := dt.getChannels()
	assert.ElementsMatch(t, channels, resChannels)
	assert.ElementsMatch(t, channels, dt.pChannels)
}

func TestDeleteTask_PreExecute(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:        "test_delete",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	t.Run("empty collection name", func(t *testing.T) {
		dt := deleteTask{}
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("fail to get collection id", func(t *testing.T) {
		dt := deleteTask{
			req: &milvuspb.DeleteRequest{
				CollectionName: "foo",
			},
		}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(0), errors.New("mock GetCollectionID err"))
		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("fail partition key mode", func(t *testing.T) {
		dt := deleteTask{req: &milvuspb.DeleteRequest{
			CollectionName: "foo",
			DbName:         "db_1",
		}}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(10000), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(nil, errors.New("mock GetCollectionSchema err"))

		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("invalid partition name", func(t *testing.T) {
		dt := deleteTask{req: &milvuspb.DeleteRequest{
			CollectionName: "foo",
			DbName:         "db_1",
			PartitionName:  "aaa",
		}}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(10000), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{
			Name:        "test_delete",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:        common.StartOfUserFieldID,
					Name:           "pk",
					IsPrimaryKey:   true,
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: true,
				},
			},
		}, nil)

		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("invalie partition", func(t *testing.T) {
		dt := deleteTask{
			req: &milvuspb.DeleteRequest{
				CollectionName: "foo",
				DbName:         "db_1",
				PartitionName:  "aaa",
				Expr:           "non_pk in [1, 2, 3]",
			},
		}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(10000), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(schema, nil)
		cache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(0), errors.New("mock GetPartitionID err"))

		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))

		dt.req.PartitionName = "aaa"
		assert.Error(t, dt.PreExecute(context.Background()))

		cache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(100001), nil)
		assert.Error(t, dt.PreExecute(context.Background()))
	})
}

func TestDeleteTask_Execute(t *testing.T) {
	collectionName := "test_delete"
	collectionID := int64(111)
	partitionName := "default"
	partitionID := int64(222)
	channels := []string{"test_channel"}
	dbName := "test_1"

	schema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}
	t.Run("empty expr", func(t *testing.T) {
		dt := deleteTask{}
		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("get channel failed", func(t *testing.T) {
		mockMgr := NewMockChannelsMgr(t)
		dt := deleteTask{
			chMgr: mockMgr,
			req: &milvuspb.DeleteRequest{
				Expr: "pk in [1,2]",
			},
		}

		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(nil, errors.New("mock error"))
		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("create plan failed", func(t *testing.T) {
		mockMgr := NewMockChannelsMgr(t)
		dt := deleteTask{
			chMgr:  mockMgr,
			schema: schema,
			req: &milvuspb.DeleteRequest{
				Expr: "????",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("alloc failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		assert.NoError(t, err)
		allocator.Close()

		dt := deleteTask{
			chMgr:        mockMgr,
			schema:       schema,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk in [1,2]",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)

		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("simple delete failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(
			&rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     0,
				Count:  1,
			}, nil)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		allocator.Start()
		assert.NoError(t, err)

		dt := deleteTask{
			chMgr:        mockMgr,
			schema:       schema,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk in [1,2]",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().Produce(mock.Anything).Return(errors.New("mock error"))
		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("complex delete query rpc failed", func(t *testing.T) {
		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := NewMockLBPolicy(t)

		dt := deleteTask{
			chMgr:        mockMgr,
			schema:       schema,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			lb:           lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk < 3",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload CollectionWorkLoad) error {
			return workload.exec(ctx, 1, qn)
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		assert.Error(t, dt.Execute(context.Background()))
		assert.Equal(t, int64(0), dt.result.DeleteCnt)
	})

	t.Run("complex delete query failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := NewMockLBPolicy(t)
		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(
			&rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     0,
				Count:  1,
			}, nil)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		allocator.Start()
		assert.NoError(t, err)

		dt := deleteTask{
			chMgr:        mockMgr,
			schema:       schema,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			lb:           lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk < 3",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload CollectionWorkLoad) error {
			return workload.exec(ctx, 1, qn)
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Status(errors.New("mock error")),
				})
				return client
			}, nil)
		stream.EXPECT().Produce(mock.Anything).Return(nil)

		assert.Error(t, dt.Execute(context.Background()))
		// query failed but still delete some data before failed.
		assert.Equal(t, int64(3), dt.result.DeleteCnt)
	})

	t.Run("complex delete produce failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := NewMockLBPolicy(t)
		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(
			&rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     0,
				Count:  1,
			}, nil)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		allocator.Start()
		assert.NoError(t, err)

		dt := deleteTask{
			chMgr:        mockMgr,
			schema:       schema,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			lb:           lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk < 3",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload CollectionWorkLoad) error {
			return workload.exec(ctx, 1, qn)
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)
		stream.EXPECT().Produce(mock.Anything).Return(errors.New("mock error"))

		assert.Error(t, dt.Execute(context.Background()))
		assert.Equal(t, int64(0), dt.result.DeleteCnt)
	})

	t.Run("complex delete success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := NewMockLBPolicy(t)
		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(
			&rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     0,
				Count:  1,
			}, nil)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		allocator.Start()
		assert.NoError(t, err)

		dt := deleteTask{
			chMgr:        mockMgr,
			schema:       schema,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			lb:           lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk < 3",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload CollectionWorkLoad) error {
			return workload.exec(ctx, 1, qn)
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)
		stream.EXPECT().Produce(mock.Anything).Return(nil)

		assert.NoError(t, dt.Execute(context.Background()))
		assert.Equal(t, int64(3), dt.result.DeleteCnt)
	})

	schema.Fields[1].IsPartitionKey = true
	partitionMaps := make(map[string]int64)
	partitionMaps["test_0"] = 1
	partitionMaps["test_1"] = 2
	partitionMaps["test_2"] = 3
	indexedPartitions := []string{"test_0", "test_1", "test_2"}

	t.Run("complex delete with partitionKey mode success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := NewMockLBPolicy(t)

		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(
			partitionMaps, nil)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(
			schema, nil)
		mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(indexedPartitions, nil)
		globalMetaCache = mockCache
		defer func() { globalMetaCache = nil }()

		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(
			&rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     0,
				Count:  1,
			}, nil)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		allocator.Start()
		assert.NoError(t, err)

		dt := deleteTask{
			chMgr:            mockMgr,
			schema:           schema,
			collectionID:     collectionID,
			partitionID:      int64(-1),
			vChannels:        channels,
			idAllocator:      allocator,
			lb:               lb,
			partitionKeyMode: true,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  "",
				DbName:         dbName,
				Expr:           "non_pk in [2, 3]",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		mockMgr.EXPECT().getOrCreateDmlStream(mock.Anything).Return(stream, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload CollectionWorkLoad) error {
			return workload.exec(ctx, 1, qn)
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()
				assert.Greater(t, len(in.Req.PartitionIDs), 0)
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)

		stream.EXPECT().Produce(mock.Anything).Return(nil)
		assert.NoError(t, dt.Execute(context.Background()))
		assert.Equal(t, int64(3), dt.result.DeleteCnt)
	})
}

func TestDeleteTask_StreamingQueryAndDelteFunc(t *testing.T) {
	collectionName := "test_delete"
	collectionID := int64(111)
	channels := []string{"test_channel"}
	dbName := "test_1"

	schema := &schemapb.CollectionSchema{
		Name:        "test_delete",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	// test partitionKey mode
	schema.Fields[1].IsPartitionKey = true
	partitionMaps := make(map[string]int64)
	partitionMaps["test_0"] = 1
	partitionMaps["test_1"] = 2
	partitionMaps["test_2"] = 3
	indexedPartitions := []string{"test_0", "test_1", "test_2"}
	t.Run("partitionKey mode parse plan failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dt := deleteTask{
			schema:           schema,
			collectionID:     collectionID,
			partitionID:      int64(-1),
			vChannels:        channels,
			partitionKeyMode: true,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  "",
				DbName:         dbName,
				Expr:           "non_pk in [2, 3]",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		qn := mocks.NewMockQueryNodeClient(t)
		queryFunc := dt.getStreamingQueryAndDelteFunc(stream, nil)
		assert.Error(t, queryFunc(ctx, 1, qn))
	})

	t.Run("partitionKey mode get meta failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dt := deleteTask{
			schema:           schema,
			collectionID:     collectionID,
			partitionID:      int64(-1),
			vChannels:        channels,
			partitionKeyMode: true,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  "",
				DbName:         dbName,
				Expr:           "non_pk in [2, 3]",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		qn := mocks.NewMockQueryNodeClient(t)

		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("mock error"))
		globalMetaCache = mockCache
		defer func() { globalMetaCache = nil }()

		plan, err := planparserv2.CreateRetrievePlan(dt.schema, dt.req.Expr)
		assert.NoError(t, err)
		queryFunc := dt.getStreamingQueryAndDelteFunc(stream, plan)
		assert.Error(t, queryFunc(ctx, 1, qn))
	})

	t.Run("partitionKey mode get partition ID failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dt := deleteTask{
			schema:           schema,
			collectionID:     collectionID,
			partitionID:      int64(-1),
			vChannels:        channels,
			partitionKeyMode: true,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  "",
				DbName:         dbName,
				Expr:           "non_pk in [2, 3]",
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		qn := mocks.NewMockQueryNodeClient(t)

		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(indexedPartitions, nil)
		mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(
			schema, nil)
		mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(
			nil, fmt.Errorf("mock error"))
		globalMetaCache = mockCache
		defer func() { globalMetaCache = nil }()

		plan, err := planparserv2.CreateRetrievePlan(dt.schema, dt.req.Expr)
		assert.NoError(t, err)
		queryFunc := dt.getStreamingQueryAndDelteFunc(stream, plan)
		assert.Error(t, queryFunc(ctx, 1, qn))
	})
}

func TestDeleteTask_SimpleDelete(t *testing.T) {
	collectionName := "test_delete"
	collectionID := int64(111)
	partitionName := "default"
	partitionID := int64(222)
	dbName := "test_1"

	schema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	task := deleteTask{
		schema:       schema,
		collectionID: collectionID,
		partitionID:  partitionID,
		req: &milvuspb.DeleteRequest{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			DbName:         dbName,
		},
	}
	t.Run("get PK failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expr := &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{
					DataType: schemapb.DataType_BinaryVector,
				},
			},
		}
		stream := msgstream.NewMockMsgStream(t)
		err := task.simpleDelete(ctx, expr, stream)
		assert.Error(t, err)
	})
}
