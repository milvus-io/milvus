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

package querynode

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func genSimpleQueryCollection(ctx context.Context, cancel context.CancelFunc) (*queryCollection, error) {
	tSafe := newTSafeReplica()
	historical, err := genSimpleHistorical(ctx, tSafe)
	if err != nil {
		return nil, err
	}

	streaming, err := genSimpleStreaming(ctx, tSafe)
	if err != nil {
		return nil, err
	}

	fac := genFactory()

	localCM, err := genLocalChunkManager()
	if err != nil {
		return nil, err
	}

	remoteCM, err := genRemoteChunkManager(ctx)
	if err != nil {
		return nil, err
	}

	queryCollection, err := newQueryCollection(ctx, cancel,
		defaultCollectionID,
		historical,
		streaming,
		fac,
		localCM,
		remoteCM,
	)
	return queryCollection, err
}

func genSimpleSegmentInfo() *querypb.SegmentInfo {
	return &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
	}
}

func genSimpleSealedSegmentsChangeInfo() *querypb.SealedSegmentsChangeInfo {
	changeInfo := &querypb.SegmentChangeInfo{
		OnlineNodeID:    Params.QueryNodeCfg.QueryNodeID,
		OnlineSegments:  []*querypb.SegmentInfo{},
		OfflineNodeID:   Params.QueryNodeCfg.QueryNodeID,
		OfflineSegments: []*querypb.SegmentInfo{},
	}
	return &querypb.SealedSegmentsChangeInfo{
		Base:  genCommonMsgBase(commonpb.MsgType_SealedSegmentsChangeInfo),
		Infos: []*querypb.SegmentChangeInfo{changeInfo},
	}
}

func genSimpleSealedSegmentsChangeInfoMsg() *msgstream.SealedSegmentsChangeInfoMsg {
	return &msgstream.SealedSegmentsChangeInfoMsg{
		BaseMsg:                  genMsgStreamBaseMsg(),
		SealedSegmentsChangeInfo: *genSimpleSealedSegmentsChangeInfo(),
	}
}

func updateTSafe(queryCollection *queryCollection, timestamp Timestamp) error {
	// register
	queryCollection.tSafeWatchers[defaultDMLChannel] = newTSafeWatcher()
	queryCollection.tSafeWatchers[defaultDeltaChannel] = newTSafeWatcher()

	err := queryCollection.streaming.tSafeReplica.setTSafe(defaultDMLChannel, timestamp)
	if err != nil {
		return err
	}
	return queryCollection.historical.tSafeReplica.setTSafe(defaultDeltaChannel, timestamp)
}

func TestQueryCollection_withoutVChannel(t *testing.T) {
	ctx := context.Background()
	factory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	schema := genTestCollectionSchema(0, false, 2)
	historicalReplica := newCollectionReplica(etcdKV)
	tsReplica := newTSafeReplica()
	streamingReplica := newCollectionReplica(etcdKV)
	historical := newHistorical(context.Background(), historicalReplica, tsReplica)

	//add a segment to historical data
	historical.replica.addCollection(0, schema)
	err = historical.replica.addPartition(0, 1)
	assert.Nil(t, err)
	err = historical.replica.addSegment(2, 1, 0, "testChannel", segmentTypeSealed, true)
	assert.Nil(t, err)
	segment, err := historical.replica.getSegmentByID(2)
	assert.Nil(t, err)
	const N = 2
	rowID := []int32{1, 2}
	timeStamp := []int64{0, 1}
	age := []int64{10, 20}
	vectorData := []float32{1, 2, 3, 4}
	err = segment.segmentLoadFieldData(0, N, rowID)
	assert.Nil(t, err)
	err = segment.segmentLoadFieldData(1, N, timeStamp)
	assert.Nil(t, err)
	err = segment.segmentLoadFieldData(101, N, age)
	assert.Nil(t, err)
	err = segment.segmentLoadFieldData(100, N, vectorData)
	assert.Nil(t, err)

	//create a streaming
	streaming := newStreaming(ctx, streamingReplica, factory, etcdKV, tsReplica)
	streaming.replica.addCollection(0, schema)
	err = streaming.replica.addPartition(0, 1)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	queryCollection, err := newQueryCollection(ctx, cancel, 0, historical, streaming, factory, nil, nil)
	assert.NoError(t, err)

	// producerChannels := []string{"testResultChannel"}
	// queryCollection.queryResultMsgStream.AsProducer(producerChannels)
	sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
	sessionManager.AddSession(&NodeInfo{
		NodeID:  1,
		Address: "",
	})
	queryCollection.sessionManager = sessionManager

	dim := 2
	// generate search rawData
	var vec = make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"
	var searchRawData1 []byte
	var searchRawData2 []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData1 = append(searchRawData1, buf...)
	}
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*4)))
		searchRawData2 = append(searchRawData2, buf...)
	}

	// generate placeholder
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: [][]byte{searchRawData1, searchRawData2},
	}
	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	assert.Nil(t, err)

	queryMsg := &msgstream.SearchMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: 10,
			EndTimestamp:   10,
		},
		SearchRequest: internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     1,
				Timestamp: Timestamp(10),
				SourceID:  1,
			},
			CollectionID:       0,
			ResultChannelID:    "testResultChannel",
			Dsl:                dslString,
			PlaceholderGroup:   placeGroupByte,
			TravelTimestamp:    10,
			GuaranteeTimestamp: 10,
		},
	}
	err = queryCollection.receiveQueryMsg(queryMsg)
	assert.Nil(t, err)

	queryCollection.cancel()
	queryCollection.close()
	historical.close()
	streaming.close()
}

func TestQueryCollection_unsolvedMsg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	qm, err := genSimpleSearchMsg(IndexFaissIDMap)
	assert.NoError(t, err)

	queryCollection.addToUnsolvedMsg(qm)

	res := queryCollection.popAllUnsolvedMsg()
	assert.NotNil(t, res)
	assert.Len(t, res, 1)
}

func TestQueryCollection_consumeQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	runConsumeQuery := func(msg msgstream.TsMsg) {
		queryCollection, err := genSimpleQueryCollection(ctx, cancel)
		assert.NoError(t, err)

		queryChannel := genQueryChannel()
		queryCollection.queryMsgStream.AsConsumer([]Channel{queryChannel}, defaultSubName)
		queryCollection.queryMsgStream.Start()

		sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
		sessionManager.AddSession(&NodeInfo{
			NodeID:  1,
			Address: "",
		})
		queryCollection.sessionManager = sessionManager

		go queryCollection.consumeQuery()

		producer, err := genQueryMsgStream(ctx)
		assert.NoError(t, err)
		producer.AsProducer([]Channel{queryChannel})
		producer.Start()
		msgPack := &msgstream.MsgPack{
			BeginTs: 0,
			EndTs:   10,
			Msgs:    []msgstream.TsMsg{msg},
		}
		err = producer.Produce(msgPack)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
	}

	t.Run("consume search", func(t *testing.T) {
		msg, err := genSimpleSearchMsg(IndexFaissIDMap)
		assert.NoError(t, err)
		runConsumeQuery(msg)
	})

	t.Run("consume retrieve", func(t *testing.T) {
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		runConsumeQuery(msg)
	})

	t.Run("consume SimpleSealedSegmentsChangeInfoMsg", func(t *testing.T) {
		// test is success if it doesn't block
		msg := genSimpleSealedSegmentsChangeInfoMsg()
		simpleInfo := genSimpleSegmentInfo()
		simpleInfo.CollectionID = 1000
		msg.Infos[0].OnlineSegments = append(msg.Infos[0].OnlineSegments, simpleInfo)
		runConsumeQuery(msg)
	})

	t.Run("consume invalid msg", func(t *testing.T) {
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		msg.Base.MsgType = commonpb.MsgType_CreateCollection
		runConsumeQuery(msg)
	})

	t.Run("consume timeout msg", func(t *testing.T) {
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		msg.TimeoutTimestamp = tsoutil.GetCurrentTime() - Timestamp(time.Second<<18)
		runConsumeQuery(msg)
	})
}

func TestQueryCollection_TranslateHits(t *testing.T) {
	fieldID := FieldID(0)
	fieldIDs := []FieldID{fieldID}

	genRawHits := func(dataType schemapb.DataType) [][]byte {
		// ids
		ids := make([]int64, 0)
		for i := 0; i < defaultMsgLength; i++ {
			ids = append(ids, int64(i))
		}

		// raw data
		rawData := make([][]byte, 0)
		switch dataType {
		case schemapb.DataType_Bool:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, true)
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int8:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, int8(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int16:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, int16(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int32:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, int32(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int64:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, int64(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Float:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, float32(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Double:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, common.Endian, float64(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		}
		hit := &milvuspb.Hits{
			IDs:     ids,
			RowData: rawData,
		}
		hits := []*milvuspb.Hits{hit}
		rawHits := make([][]byte, 0)
		for _, h := range hits {
			rawHit, err := proto.Marshal(h)
			assert.NoError(t, err)
			rawHits = append(rawHits, rawHit)
		}
		return rawHits
	}

	genSchema := func(dataType schemapb.DataType) *typeutil.SchemaHelper {
		schema := &schemapb.CollectionSchema{
			Name:   defaultCollectionName,
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				genConstantField(constFieldParam{
					id:       fieldID,
					dataType: dataType,
				}),
			},
		}
		schemaHelper, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)
		return schemaHelper
	}

	t.Run("test bool field", func(t *testing.T) {
		dataType := schemapb.DataType_Bool
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int8 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int8
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int16 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int16
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int32 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int32
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int64 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int64
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test float field", func(t *testing.T) {
		dataType := schemapb.DataType_Float
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test double field", func(t *testing.T) {
		dataType := schemapb.DataType_Double
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test field with error type", func(t *testing.T) {
		dataType := schemapb.DataType_FloatVector
		_, err := translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.Error(t, err)

		dataType = schemapb.DataType_BinaryVector
		_, err = translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.Error(t, err)
	})
}

func TestQueryCollection_serviceableTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	st := Timestamp(1000)
	queryCollection.setServiceableTime(st)

	gracefulTimeInMilliSecond := Params.QueryNodeCfg.GracefulTime
	gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
	resST := queryCollection.getServiceableTime()
	assert.Equal(t, st+gracefulTime, resST)
}

func TestQueryCollection_tSafeWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	err = queryCollection.addTSafeWatcher(defaultDMLChannel)
	assert.NoError(t, err)

	err = queryCollection.removeTSafeWatcher(defaultDMLChannel)
	assert.NoError(t, err)

	// no tSafe watcher
	err = queryCollection.removeTSafeWatcher(defaultDMLChannel)
	assert.Error(t, err)
}

func TestQueryCollection_waitNewTSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	err = updateTSafe(queryCollection, timestamp)
	assert.NoError(t, err)

	resTimestamp, err := queryCollection.waitNewTSafe()
	assert.NoError(t, err)
	assert.Equal(t, timestamp, resTimestamp)
}

func TestQueryCollection_mergeRetrieveResults(t *testing.T) {
	const (
		Dim                  = 8
		Int64FieldName       = "Int64Field"
		FloatVectorFieldName = "FloatVectorField"
		Int64FieldID         = common.StartOfUserFieldID + 1
		FloatVectorFieldID   = common.StartOfUserFieldID + 2
	)
	Int64Array := []int64{11, 22}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	result1 := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{0, 1},
				},
			},
		},
		Offset:     []int64{0, 1},
		FieldsData: fieldDataArray1,
	}
	result2 := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{0, 1},
				},
			},
		},
		Offset:     []int64{0, 1},
		FieldsData: fieldDataArray2,
	}

	result, err := mergeRetrieveResults([]*segcorepb.RetrieveResults{result1, result2})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.FieldsData[0].GetScalars().GetLongData().Data))
	assert.Equal(t, 2*Dim, len(result.FieldsData[1].GetVectors().GetFloatVector().Data))

	_, err = mergeRetrieveResults(nil)
	assert.NoError(t, err)
}

func TestQueryCollection_doUnsolvedQueryMsg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	t.Run("test doUnsolvedQueryMsg", func(t *testing.T) {
		queryCollection, err := genSimpleQueryCollection(ctx, cancel)
		assert.NoError(t, err)

		sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
		sessionManager.AddSession(&NodeInfo{
			NodeID:  1,
			Address: "",
		})
		queryCollection.sessionManager = sessionManager

		timestamp := Timestamp(1000)
		err = updateTSafe(queryCollection, timestamp)
		assert.NoError(t, err)

		go queryCollection.doUnsolvedQueryMsg()

		msg, err := genSimpleSearchMsg(IndexFaissIDMap)
		assert.NoError(t, err)
		queryCollection.addToUnsolvedMsg(msg)

		time.Sleep(200 * time.Millisecond)
	})

	t.Run("test doUnsolvedQueryMsg timeout", func(t *testing.T) {
		queryCollection, err := genSimpleQueryCollection(ctx, cancel)
		assert.NoError(t, err)

		sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
		sessionManager.AddSession(&NodeInfo{
			NodeID:  1,
			Address: "",
		})
		queryCollection.sessionManager = sessionManager

		timestamp := Timestamp(1000)
		err = updateTSafe(queryCollection, timestamp)
		assert.NoError(t, err)

		go queryCollection.doUnsolvedQueryMsg()

		msg, err := genSimpleSearchMsg(IndexFaissIDMap)
		assert.NoError(t, err)
		msg.TimeoutTimestamp = tsoutil.GetCurrentTime() - Timestamp(time.Second<<18)
		queryCollection.addToUnsolvedMsg(msg)

		time.Sleep(2000 * time.Millisecond)
	})
}

func TestQueryCollection_search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	// queryChannel := genQueryChannel()
	// queryCollection.queryResultMsgStream.AsProducer([]Channel{queryChannel})
	// queryCollection.queryResultMsgStream.Start()
	sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
	sessionManager.AddSession(&NodeInfo{
		NodeID:  0,
		Address: "",
	})
	queryCollection.sessionManager = sessionManager

	err = queryCollection.streaming.replica.removeSegment(defaultSegmentID)
	assert.NoError(t, err)

	err = queryCollection.historical.replica.removeSegment(defaultSegmentID)
	assert.NoError(t, err)

	msg, err := genSimpleSearchMsg(IndexFaissIDMap)
	assert.NoError(t, err)

	err = queryCollection.search(msg)
	assert.NoError(t, err)
}

func TestQueryCollection_retrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	// queryChannel := genQueryChannel()
	// queryCollection.queryResultMsgStream.AsProducer([]Channel{queryChannel})
	// queryCollection.queryResultMsgStream.Start()
	sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
	sessionManager.AddSession(&NodeInfo{
		NodeID:  0,
		Address: "",
	})
	queryCollection.sessionManager = sessionManager

	vecCM, err := genVectorChunkManager(ctx)
	assert.NoError(t, err)

	queryCollection.vectorChunkManager = vecCM

	msg, err := genSimpleRetrieveMsg()
	assert.NoError(t, err)

	err = queryCollection.retrieve(msg)
	assert.NoError(t, err)
}

func TestQueryCollection_AddPopUnsolvedMsg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	qCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.Nil(t, err)
	var i int64
	for i = 0; i < 3; i++ {
		qCollection.addToUnsolvedMsg(&msgstream.RetrieveMsg{
			RetrieveRequest: internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{MsgID: i},
			},
		})
	}

	unsolved := qCollection.popAllUnsolvedMsg()
	assert.EqualValues(t, 3, len(unsolved))
	for i := 0; i < 3; i++ {
		assert.EqualValues(t, i, unsolved[i].ID())
	}

	// add new msg to unsolved msgs and check old unsolved msg
	for i := 0; i < 3; i++ {
		qCollection.addToUnsolvedMsg(&msgstream.RetrieveMsg{
			RetrieveRequest: internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{MsgID: 4},
			},
		})
	}

	for i := 0; i < 3; i++ {
		assert.EqualValues(t, i, unsolved[i].ID())
	}
}

func TestQueryCollection_adjustByChangeInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("test adjustByChangeInfo", func(t *testing.T) {
		defer wg.Done()
		qc, err := genSimpleQueryCollection(ctx, cancel)
		assert.Nil(t, err)

		segmentChangeInfos := genSimpleSealedSegmentsChangeInfoMsg()

		// test online
		segmentChangeInfos.Infos[0].OnlineSegments = append(segmentChangeInfos.Infos[0].OnlineSegments, genSimpleSegmentInfo())
		qc.adjustByChangeInfo(segmentChangeInfos)
		ids := qc.globalSegmentManager.getGlobalSegmentIDs()
		assert.Len(t, ids, 1)

		// test offline
		segmentChangeInfos.Infos[0].OnlineSegments = make([]*querypb.SegmentInfo, 0)
		segmentChangeInfos.Infos[0].OfflineSegments = append(segmentChangeInfos.Infos[0].OfflineSegments, genSimpleSegmentInfo())
		qc.adjustByChangeInfo(segmentChangeInfos)
		ids = qc.globalSegmentManager.getGlobalSegmentIDs()
		assert.Len(t, ids, 0)
	})

	wg.Add(1)
	t.Run("test mismatch collectionID when adjustByChangeInfo", func(t *testing.T) {
		defer wg.Done()
		qc, err := genSimpleQueryCollection(ctx, cancel)
		assert.Nil(t, err)

		segmentChangeInfos := genSimpleSealedSegmentsChangeInfoMsg()

		// test online
		simpleInfo := genSimpleSegmentInfo()
		simpleInfo.CollectionID = 1000
		segmentChangeInfos.Infos[0].OnlineSegments = append(segmentChangeInfos.Infos[0].OnlineSegments, simpleInfo)
		qc.adjustByChangeInfo(segmentChangeInfos)
	})

	wg.Add(1)
	t.Run("test no segment when adjustByChangeInfo", func(t *testing.T) {
		defer wg.Done()
		qc, err := genSimpleQueryCollection(ctx, cancel)
		assert.Nil(t, err)

		err = qc.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		segmentChangeInfos := genSimpleSealedSegmentsChangeInfoMsg()
		segmentChangeInfos.Infos[0].OfflineSegments = append(segmentChangeInfos.Infos[0].OfflineSegments, genSimpleSegmentInfo())

		qc.adjustByChangeInfo(segmentChangeInfos)
	})
	wg.Wait()
}

func TestQueryCollection_search_while_release(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wgAll sync.WaitGroup
	wgAll.Add(1)
	t.Run("test search while release collection", func(t *testing.T) {
		defer wgAll.Done()
		queryCollection, err := genSimpleQueryCollection(ctx, cancel)
		assert.NoError(t, err)

		// queryChannel := genQueryChannel()
		// queryCollection.queryResultMsgStream.AsProducer([]Channel{queryChannel})
		// queryCollection.queryResultMsgStream.Start()
		sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
		sessionManager.AddSession(&NodeInfo{
			NodeID:  1,
			Address: "",
		})
		queryCollection.sessionManager = sessionManager

		msg, err := genSimpleSearchMsg(IndexFaissIDMap)
		assert.NoError(t, err)

		// To prevent data race in search trackCtx
		searchMu := &sync.Mutex{}

		runSearchWhileReleaseCollection := func(wg *sync.WaitGroup) {
			go func() {
				_ = queryCollection.streaming.replica.removeCollection(defaultCollectionID)
				wg.Done()
			}()

			go func() {
				searchMu.Lock()
				_ = queryCollection.search(msg)
				searchMu.Unlock()
				wg.Done()
			}()
		}

		wg := &sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			log.Debug("runSearchWhileReleaseCollection", zap.Any("time", i))
			wg.Add(2)
			go runSearchWhileReleaseCollection(wg)
		}
		wg.Wait()
	})

	wgAll.Add(1)
	t.Run("test search while release partition", func(t *testing.T) {
		defer wgAll.Done()
		queryCollection, err := genSimpleQueryCollection(ctx, cancel)
		assert.NoError(t, err)

		// queryChannel := genQueryChannel()
		// queryCollection.queryResultMsgStream.AsProducer([]Channel{queryChannel})
		// queryCollection.queryResultMsgStream.Start()
		sessionManager := NewSessionManager(withSessionCreator(mockProxyCreator()))
		sessionManager.AddSession(&NodeInfo{
			NodeID:  1,
			Address: "",
		})
		queryCollection.sessionManager = sessionManager

		msg, err := genSimpleSearchMsg(IndexFaissIDMap)
		assert.NoError(t, err)

		// To prevent data race in search trackCtx
		searchMu := &sync.Mutex{}

		runSearchWhileReleasePartition := func(wg *sync.WaitGroup) {
			go func() {
				_ = queryCollection.streaming.replica.removePartition(defaultPartitionID)
				wg.Done()
			}()

			go func() {
				searchMu.Lock()
				_ = queryCollection.search(msg)
				searchMu.Unlock()
				wg.Done()
			}()
		}

		wg := &sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			log.Debug("runSearchWhileReleasePartition", zap.Any("time", i))
			wg.Add(2)
			go runSearchWhileReleasePartition(wg)
		}
		wg.Wait()
	})
	wgAll.Wait()
}
