package dml

import (
	"context"
	"fmt"
	"testing"

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
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

func TestPartitionKey(t *testing.T) {
	streaming.SetupNoopWALForTest()
	qc := mocks.NewMockMixCoordClient(t)
	qc.EXPECT().Close().Return(nil).Maybe()
	qc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     0,
		Count:  1000000,
	}, nil).Maybe()
	ctx := context.Background()

	cache := newTestCache()
	collectionID := int64(1000)
	mockTest(t, (*metacache.MetaCache).GetCollectionID, collectionID, nil)
	mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{CollID: collectionID}, nil)
	defaultPartitions := make(map[string]int64, common.DefaultPartitionsWithPartitionKey)
	for i := int64(0); i < common.DefaultPartitionsWithPartitionKey; i++ {
		defaultPartitions[fmt.Sprintf("_default_%d", i)] = i
	}
	mockTest(t, (*metacache.MetaCache).GetPartitions, defaultPartitions, nil)
	mockTest(t, (*metacache.MetaCache).GetPartitionID, int64(2000), nil)
	mockTest(t, (*metacache.MetaCache).GetPartitionInfo, &metacache.PartitionInfo{Name: "_default", PartitionID: 2000, IsDefault: true}, nil)
	mockTest(t, (*metacache.MetaCache).GetPartitionInfos, &metacache.PartitionInfos{
		PartitionInfos: nil,
		Name2Info:      map[string]*metacache.PartitionInfo{},
		Name2ID:        map[string]int64{},
	}, nil)

	prefix := "TestInsertTaskWithPartitionKey"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key_field",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	fieldName2Type["partition_key_field"] = schemapb.DataType_Int64
	schema.Fields = append(schema.Fields, partitionKeyField)
	mockTest(t, (*metacache.MetaCache).GetCollectionSchema, mustNewSchemaInfo(schema), nil)
	collectionID, err := cache.GetCollectionID(ctx, "", collectionName)
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

	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, cache, "", collectionName)
	assert.NoError(t, err)
	assert.Equal(t, common.DefaultPartitionsWithPartitionKey, int64(len(partitionNames)))

	nb := 10
	fieldID := common.StartOfUserFieldID
	fieldDatas := make([]*schemapb.FieldData, 0)
	for fieldName, dataType := range fieldName2Type {
		fieldData := generateFieldData(dataType, fieldName, nb)
		fieldData.FieldId = int64(fieldID)
		fieldDatas = append(fieldDatas, generateFieldData(dataType, fieldName, nb))
		fieldID++
	}

	t.Run("Insert", func(t *testing.T) {
		it := &InsertTask{
			baseTask: baseTask{MetaCache: cache},
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					FieldsData:     fieldDatas,
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

		// don't support specify partition name if use partition key
		it.insertMsg.PartitionName = partitionNames[0]
		assert.Error(t, it.PreExecute(ctx))

		it.insertMsg.PartitionName = ""
		assert.NoError(t, it.OnEnqueue())
		assert.NoError(t, it.PreExecute(ctx))
		assert.NoError(t, it.Execute(ctx))
		assert.NoError(t, it.PostExecute(ctx))
	})

	t.Run("Upsert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		ut := &UpsertTask{
			baseTask:  baseTask{MetaCache: cache},
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			baseMsg: msgstream.BaseMsg{
				HashValues: hash,
			},
			req: &milvuspb.UpsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: collectionName,
				FieldsData:     fieldDatas,
				NumRows:        uint32(nb),
			},

			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			idAllocator: idAllocator,
			chMgr:       chMgr,
		}

		// don't support specify partition name if use partition key
		ut.req.PartitionName = partitionNames[0]
		assert.Error(t, ut.PreExecute(ctx))

		ut.req.PartitionName = ""
		assert.NoError(t, ut.OnEnqueue())
		assert.NoError(t, ut.PreExecute(ctx))
		assert.NoError(t, ut.Execute(ctx))
		assert.NoError(t, ut.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		dt := &DeleteTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           "int64_field in [0, 1]",
			},
			ctx: ctx,
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}},
			},
			idAllocator:  idAllocator,
			chMgr:        chMgr,
			collectionID: collectionID,
			vChannels:    []string{"test-channel"},
		}

		dt.req.PartitionName = ""
		assert.NoError(t, dt.PreExecute(ctx))
		assert.NoError(t, dt.Execute(ctx))
		assert.NoError(t, dt.PostExecute(ctx))
	})
}

func TestDefaultPartition(t *testing.T) {
	t.Skip("TODO: rewrite with mockey for non-partition-key default-partition streaming path")

	streaming.SetupNoopWALForTest()
	qc := mocks.NewMockMixCoordClient(t)
	qc.EXPECT().Close().Return(nil).Maybe()
	qc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     0,
		Count:  1000000,
	}, nil).Maybe()
	ctx := context.Background()

	cache := newTestCache()
	collectionID := int64(1000)
	mockTest(t, (*metacache.MetaCache).GetCollectionID, collectionID, nil)
	mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{CollID: collectionID}, nil)
	defaultPartitions := make(map[string]int64, common.DefaultPartitionsWithPartitionKey)
	for i := int64(0); i < common.DefaultPartitionsWithPartitionKey; i++ {
		defaultPartitions[fmt.Sprintf("_default_%d", i)] = i
	}
	mockTest(t, (*metacache.MetaCache).GetPartitions, defaultPartitions, nil)
	mockTest(t, (*metacache.MetaCache).GetPartitionID, int64(2000), nil)
	mockTest(t, (*metacache.MetaCache).GetPartitionInfo, &metacache.PartitionInfo{Name: "_default", PartitionID: 2000, IsDefault: true}, nil)
	mockTest(t, (*metacache.MetaCache).GetPartitionInfos, &metacache.PartitionInfos{
		PartitionInfos: nil,
		Name2Info:      map[string]*metacache.PartitionInfo{},
		Name2ID:        map[string]int64{},
	}, nil)

	prefix := "TestInsertTaskWithPartitionKey"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
	mockTest(t, (*metacache.MetaCache).GetCollectionSchema, mustNewSchemaInfo(schema), nil)
	collectionID, err := cache.GetCollectionID(ctx, "", collectionName)
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

	nb := 10
	fieldID := common.StartOfUserFieldID
	fieldDatas := make([]*schemapb.FieldData, 0)
	for fieldName, dataType := range fieldName2Type {
		fieldData := generateFieldData(dataType, fieldName, nb)
		fieldData.FieldId = int64(fieldID)
		fieldDatas = append(fieldDatas, generateFieldData(dataType, fieldName, nb))
		fieldID++
	}

	t.Run("Insert", func(t *testing.T) {
		it := &InsertTask{
			baseTask: baseTask{MetaCache: cache},
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					FieldsData:     fieldDatas,
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

		it.insertMsg.PartitionName = ""
		assert.NoError(t, it.OnEnqueue())
		assert.NoError(t, it.PreExecute(ctx))
		assert.NoError(t, it.Execute(ctx))
		assert.NoError(t, it.PostExecute(ctx))
	})

	t.Run("Upsert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		ut := &UpsertTask{
			baseTask:  baseTask{MetaCache: cache},
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			baseMsg: msgstream.BaseMsg{
				HashValues: hash,
			},
			req: &milvuspb.UpsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: collectionName,
				FieldsData:     fieldDatas,
				NumRows:        uint32(nb),
			},

			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			idAllocator: idAllocator,
			chMgr:       chMgr,
		}

		ut.req.PartitionName = ""
		assert.NoError(t, ut.OnEnqueue())
		assert.NoError(t, ut.PreExecute(ctx))
		assert.NoError(t, ut.Execute(ctx))
		assert.NoError(t, ut.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		dt := &DeleteTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           "int64_field in [0, 1]",
			},
			ctx: ctx,
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}},
			},
			idAllocator:  idAllocator,
			chMgr:        chMgr,
			collectionID: collectionID,
			vChannels:    []string{"test-channel"},
		}

		dt.req.PartitionName = ""
		assert.NoError(t, dt.PreExecute(ctx))
		assert.NoError(t, dt.Execute(ctx))
		assert.NoError(t, dt.PostExecute(ctx))
	})
}
