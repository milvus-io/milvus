package dml

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

func TestTask_Int64PrimaryKey(t *testing.T) {
	t.Skip("TODO: full insert/upsert/delete lifecycle executes real streaming; needs mockey on WAL/streaming-node")
	streaming.SetupNoopWALForTest()
	var err error

	qc := mocks.NewMockMixCoordClient(t)
	ctx := context.Background()

	cache := newTestCache()
	collectionID := int64(1000)
	mockey.Mock((*metacache.MetaCache).GetCollectionID).Return(collectionID, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(&collectionInfo{CollID: collectionID}, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetPartitions).Return(map[string]int64{"_default": 2000}, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetPartitionInfo).Return(&metacache.PartitionInfo{Name: "_default", PartitionID: 2000, IsDefault: true}, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetPartitionID).Return(int64(2000), nil).Build()

	prefix := "TestTask_int64pk"
	dbName := "int64PK"
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	mockey.Mock((*metacache.MetaCache).GetCollectionSchema).Return(mustNewSchemaInfo(schema), nil).Build()
	qc.EXPECT().CreatePartition(mock.Anything, mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Maybe()
	_, _ = qc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})

	collectionID, err = cache.GetCollectionID(ctx, dbName, collectionName)
	assert.NoError(t, err)

	chMgr := channelmgr.NewMockChannelsMgr(t)
	chMgr.EXPECT().GetChannels(mock.Anything).Return([]string{"test-channel"}, nil).Maybe()
	chMgr.EXPECT().GetVChannels(mock.Anything).Return([]string{"test-channel"}, nil).Maybe()
	_, err = chMgr.GetChannels(collectionID)
	assert.NoError(t, err)

	idAllocator, err := allocator.NewIDAllocator(ctx, qc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		task := &InsertTask{
			baseTask: baseTask{MetaCache: cache},
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator: idAllocator,
			chMgr:       chMgr,
			vChannels:   nil,
			pChannels:   nil,
			schema:      nil,
		}

		for fieldName, dataType := range fieldName2Types {
			task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, generateFieldData(dataType, fieldName, nb))
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("simple delete", func(t *testing.T) {
		task := &DeleteTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				Expr:           "int64 in [0, 1]",
			},
			idAllocator: idAllocator,
			ctx:         ctx,
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}},
			},
			chMgr:        chMgr,
			collectionID: collectionID,
			vChannels:    []string{"test-ch"},
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})
}

func TestTask_VarCharPrimaryKey(t *testing.T) {
	t.Skip("TODO: full insert/upsert/delete lifecycle executes real streaming; needs mockey on WAL/streaming-node")
	streaming.SetupNoopWALForTest()
	var err error
	mixc := mocks.NewMockMixCoordClient(t)

	ctx := context.Background()

	cache := newTestCache()
	collectionID := int64(1000)
	mockey.Mock((*metacache.MetaCache).GetCollectionID).Return(collectionID, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(&collectionInfo{CollID: collectionID}, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetPartitions).Return(map[string]int64{"_default": 2000}, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetPartitionInfo).Return(&metacache.PartitionInfo{Name: "_default", PartitionID: 2000, IsDefault: true}, nil).Build()
	mockey.Mock((*metacache.MetaCache).GetPartitionID).Return(int64(2000), nil).Build()

	prefix := "TestTask_all"
	dbName := "testvarchar"
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testVarCharField, false)
	mockey.Mock((*metacache.MetaCache).GetCollectionSchema).Return(mustNewSchemaInfo(schema), nil).Build()
	mixc.EXPECT().CreatePartition(mock.Anything, mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Maybe()
	_, _ = mixc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})

	collectionID, err = cache.GetCollectionID(ctx, dbName, collectionName)
	assert.NoError(t, err)

	chMgr := channelmgr.NewMockChannelsMgr(t)
	chMgr.EXPECT().GetChannels(mock.Anything).Return([]string{"test-channel"}, nil).Maybe()
	chMgr.EXPECT().GetVChannels(mock.Anything).Return([]string{"test-channel"}, nil).Maybe()
	_, err = chMgr.GetChannels(collectionID)
	assert.NoError(t, err)

	idAllocator, err := allocator.NewIDAllocator(ctx, mixc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		task := &InsertTask{
			baseTask: baseTask{MetaCache: cache},
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator: idAllocator,
			chMgr:       chMgr,
			vChannels:   nil,
			pChannels:   nil,
			schema:      nil,
		}

		fieldID := common.StartOfUserFieldID
		for fieldName, dataType := range fieldName2Types {
			task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("upsert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		task := &UpsertTask{
			baseTask: baseTask{MetaCache: cache},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &BaseInsertTask{
					BaseMsg: msgstream.BaseMsg{
						HashValues: hash,
					},
					InsertRequest: &msgpb.InsertRequest{
						Base: &commonpb.MsgBase{
							MsgType:  commonpb.MsgType_Insert,
							MsgID:    0,
							SourceID: paramtable.GetNodeID(),
						},
						DbName:         dbName,
						CollectionName: collectionName,
						PartitionName:  partitionName,
						NumRows:        uint64(nb),
						Version:        msgpb.InsertDataVersion_ColumnBased,
					},
				},
				DeleteMsg: &msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						HashValues: hash,
					},
					DeleteRequest: &msgpb.DeleteRequest{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_Delete,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  paramtable.GetNodeID(),
						},
						DbName:         dbName,
						CollectionName: collectionName,
						PartitionName:  partitionName,
					},
				},
			},

			Condition: NewTaskCondition(ctx),
			req: &milvuspb.UpsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Insert,
					MsgID:    0,
					SourceID: paramtable.GetNodeID(),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  partitionName,
				HashKeys:       hash,
				NumRows:        uint32(nb),
			},
			ctx: ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator: idAllocator,
			chMgr:       chMgr,
			vChannels:   nil,
			pChannels:   nil,
			schema:      nil,
		}

		fieldID := common.StartOfUserFieldID
		for fieldName, dataType := range fieldName2Types {
			task.req.FieldsData = append(task.req.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("simple delete", func(t *testing.T) {
		task := &DeleteTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				Expr:           "varChar in [\"milvus\", \"test\"]",
			},
			idAllocator: idAllocator,
			ctx:         ctx,
			chMgr:       chMgr,
			vChannels:   []string{"test-channel"},
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"milvus", "test"}}},
			},
			collectionID: collectionID,
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})
}
