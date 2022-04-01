package proxy

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestQueryTask_all(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.RetrieveResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestQueryTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

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

	expr := fmt.Sprintf("%s > 0", testInt64Field)
	hitNum := 10

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID:         0,
		CollectionID: collectionID,
		Schema:       nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	task := &queryTask{
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Retrieve,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			ResultChannelID:    strconv.Itoa(int(Params.ProxyCfg.ProxyID)),
			DbID:               0,
			CollectionID:       collectionID,
			PartitionIDs:       nil,
			SerializedExprPlan: nil,
			OutputFieldsId:     make([]int64, len(fieldName2Types)),
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		},
		ctx:       ctx,
		resultBuf: make(chan []*internalpb.RetrieveResults),
		result: &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			FieldsData: nil,
		},
		query: &milvuspb.QueryRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Retrieve,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			DbName:             dbName,
			CollectionName:     collectionName,
			Expr:               expr,
			OutputFields:       nil,
			PartitionNames:     nil,
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		},
		chMgr: chMgr,
		qc:    qc,
		ids:   nil,
	}
	for i := 0; i < len(fieldName2Types); i++ {
		task.RetrieveRequest.OutputFieldsId[i] = int64(common.StartOfUserFieldID + i)
	}

	// simple mock for query node
	// TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?

	err = chMgr.createDQLStream(collectionID)
	assert.NoError(t, err)
	stream, err := chMgr.getDQLStream(collectionID)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	consumeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case pack, ok := <-stream.Chan():
				assert.True(t, ok)

				if pack == nil {
					continue
				}

				for _, msg := range pack.Msgs {
					_, ok := msg.(*msgstream.RetrieveMsg)
					assert.True(t, ok)
					// TODO(dragondriver): construct result according to the request

					result1 := &internalpb.RetrieveResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_RetrieveResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID: strconv.Itoa(int(Params.ProxyCfg.ProxyID)),
						Ids: &schemapb.IDs{
							IdField: &schemapb.IDs_IntId{
								IntId: &schemapb.LongArray{
									Data: generateInt64Array(hitNum),
								},
							},
						},
						SealedSegmentIDsRetrieved: nil,
						ChannelIDsRetrieved:       nil,
						GlobalSealedSegmentIDs:    nil,
					}

					fieldID := common.StartOfUserFieldID
					for fieldName, dataType := range fieldName2Types {
						result1.FieldsData = append(result1.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), hitNum))
						fieldID++
					}

					// send search result
					task.resultBuf <- []*internalpb.RetrieveResults{result1}
				}
			}
		}
	}()

	assert.NoError(t, task.OnEnqueue())

	// test query task with timeout
	ctx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	// before preExecute
	assert.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)
	task.ctx = ctx1
	assert.NoError(t, task.PreExecute(ctx))
	// after preExecute
	assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)
	task.ctx = ctx

	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	cancel()
	wg.Wait()
}
