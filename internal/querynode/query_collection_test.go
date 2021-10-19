package querynode

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func genSimpleQueryCollection(ctx context.Context, cancel context.CancelFunc) (*queryCollection, error) {
	historical, err := genSimpleHistorical(ctx)
	if err != nil {
		return nil, err
	}

	streaming, err := genSimpleStreaming(ctx)
	if err != nil {
		return nil, err
	}

	fac, err := genFactory()
	if err != nil {
		return nil, err
	}

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
		false)
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
	return &querypb.SealedSegmentsChangeInfo{
		Base:            genCommonMsgBase(commonpb.MsgType_SealedSegmentsChangeInfo),
		OnlineNodeID:    Params.QueryNodeID,
		OnlineSegments:  []*querypb.SegmentInfo{},
		OfflineNodeID:   Params.QueryNodeID,
		OfflineSegments: []*querypb.SegmentInfo{},
	}
}

func genSimpleSealedSegmentsChangeInfoMsg() *msgstream.SealedSegmentsChangeInfoMsg {
	return &msgstream.SealedSegmentsChangeInfoMsg{
		BaseMsg:                  genMsgStreamBaseMsg(),
		SealedSegmentsChangeInfo: *genSimpleSealedSegmentsChangeInfo(),
	}
}

func updateTSafe(queryCollection *queryCollection, timestamp Timestamp) {
	// register
	queryCollection.tSafeWatchers[defaultVChannel] = newTSafeWatcher()
	queryCollection.streaming.tSafeReplica.addTSafe(defaultVChannel)
	queryCollection.streaming.tSafeReplica.registerTSafeWatcher(defaultVChannel, queryCollection.tSafeWatchers[defaultVChannel])
	queryCollection.addTSafeWatcher(defaultVChannel)

	queryCollection.streaming.tSafeReplica.setTSafe(defaultVChannel, defaultCollectionID, timestamp)
}

func TestQueryCollection_withoutVChannel(t *testing.T) {
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	factory := msgstream.NewPmsFactory()
	err := factory.SetParams(m)
	assert.Nil(t, err)
	etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)

	schema := genTestCollectionSchema(0, false, 2)
	historical := newHistorical(context.Background(), nil, nil, factory, etcdKV)

	//add a segment to historical data
	err = historical.replica.addCollection(0, schema)
	assert.Nil(t, err)
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
	streaming := newStreaming(context.Background(), factory, etcdKV)
	err = streaming.replica.addCollection(0, schema)
	assert.Nil(t, err)
	err = streaming.replica.addPartition(0, 1)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	queryCollection, err := newQueryCollection(ctx, cancel, 0, historical, streaming, factory, nil, nil, false)
	assert.NoError(t, err)

	producerChannels := []string{"testResultChannel"}
	queryCollection.queryResultMsgStream.AsProducer(producerChannels)

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
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData1 = append(searchRawData1, buf...)
	}
	for i, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*4)))
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

func TestGetSegmentsByPKs(t *testing.T) {
	buf := make([]byte, 8)
	filter1 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		filter1.Add(buf)
	}
	filter2 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 3; i < 5; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		filter2.Add(buf)
	}
	segment1 := &Segment{
		segmentID: 1,
		pkFilter:  filter1,
	}
	segment2 := &Segment{
		segmentID: 2,
		pkFilter:  filter1,
	}
	segment3 := &Segment{
		segmentID: 3,
		pkFilter:  filter1,
	}
	segment4 := &Segment{
		segmentID: 4,
		pkFilter:  filter2,
	}
	segment5 := &Segment{
		segmentID: 5,
		pkFilter:  filter2,
	}
	segments := []*Segment{segment1, segment2, segment3, segment4, segment5}
	results, err := getSegmentsByPKs([]int64{0, 1, 2, 3, 4}, segments)
	assert.Nil(t, err)
	expected := map[int64][]int64{
		1: {0, 1, 2},
		2: {0, 1, 2},
		3: {0, 1, 2},
		4: {3, 4},
		5: {3, 4},
	}
	assert.Equal(t, expected, results)

	_, err = getSegmentsByPKs(nil, segments)
	assert.NotNil(t, err)
	_, err = getSegmentsByPKs([]int64{0, 1, 2, 3, 4}, nil)
	assert.NotNil(t, err)
}

func TestQueryCollection_unsolvedMsg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	qm, err := genSimpleSearchMsg()
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
		msg, err := genSimpleSearchMsg()
		assert.NoError(t, err)
		runConsumeQuery(msg)
	})

	t.Run("consume retrieve", func(t *testing.T) {
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		runConsumeQuery(msg)
	})

	t.Run("consume load balance", func(t *testing.T) {
		msg := &msgstream.LoadBalanceSegmentsMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			LoadBalanceSegmentsRequest: internalpb.LoadBalanceSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
					MsgID:   rand.Int63(), // TODO: random msgID?
				},
				SegmentIDs: []UniqueID{defaultSegmentID},
			},
		}
		runConsumeQuery(msg)
	})

	t.Run("consume SimpleSealedSegmentsChangeInfoMsg", func(t *testing.T) {
		// test is success if it doesn't block
		msg := genSimpleSealedSegmentsChangeInfoMsg()
		simpleInfo := genSimpleSegmentInfo()
		simpleInfo.CollectionID = 1000
		msg.OnlineSegments = append(msg.OnlineSegments, simpleInfo)
		runConsumeQuery(msg)
	})

	t.Run("consume invalid msg", func(t *testing.T) {
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		msg.Base.MsgType = commonpb.MsgType_CreateCollection
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
				err := binary.Write(&buf, binary.LittleEndian, true)
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int8:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int8(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int16:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int16(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int32:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int32(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int64:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int64(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Float:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, float32(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Double:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, float64(i))
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

	gracefulTimeInMilliSecond := Params.GracefulTime
	gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
	resST := queryCollection.getServiceableTime()
	assert.Equal(t, st+gracefulTime, resST)
}

func TestQueryCollection_addTSafeWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	queryCollection.addTSafeWatcher(defaultVChannel)
}

func TestQueryCollection_waitNewTSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	updateTSafe(queryCollection, timestamp)

	resTimestamp, err := queryCollection.waitNewTSafe()
	assert.NoError(t, err)
	assert.Equal(t, timestamp, resTimestamp)
}

func TestQueryCollection_mergeRetrieveResults(t *testing.T) {
	fieldData := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_FloatVector,
			FieldName: defaultVecFieldName,
			FieldId:   simpleVecField.id,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: defaultDim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{1.1, 2.2, 3.3, 4.4},
						},
					},
				},
			},
		},
	}
	result := &segcorepb.RetrieveResults{
		Ids:        &schemapb.IDs{},
		Offset:     []int64{0},
		FieldsData: fieldData,
	}

	_, err := mergeRetrieveResults([]*segcorepb.RetrieveResults{result})
	assert.NoError(t, err)

	_, err = mergeRetrieveResults(nil)
	assert.NoError(t, err)
}

func TestQueryCollection_doUnsolvedQueryMsg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	updateTSafe(queryCollection, timestamp)

	go queryCollection.doUnsolvedQueryMsg()

	msg, err := genSimpleSearchMsg()
	assert.NoError(t, err)
	queryCollection.addToUnsolvedMsg(msg)

	time.Sleep(200 * time.Millisecond)
}

func TestQueryCollection_search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	queryChannel := genQueryChannel()
	queryCollection.queryResultMsgStream.AsProducer([]Channel{queryChannel})
	queryCollection.queryResultMsgStream.Start()

	err = queryCollection.streaming.replica.removeSegment(defaultSegmentID)
	assert.NoError(t, err)

	err = queryCollection.historical.replica.removeSegment(defaultSegmentID)
	assert.NoError(t, err)

	msg, err := genSimpleSearchMsg()
	assert.NoError(t, err)

	err = queryCollection.search(msg)
	assert.NoError(t, err)
}

func TestQueryCollection_receive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queryCollection, err := genSimpleQueryCollection(ctx, cancel)
	assert.NoError(t, err)

	queryChannel := genQueryChannel()
	queryCollection.queryResultMsgStream.AsProducer([]Channel{queryChannel})
	queryCollection.queryResultMsgStream.Start()

	vecCM, err := genVectorChunkManager(ctx)
	assert.NoError(t, err)

	queryCollection.vectorChunkManager = vecCM

	err = queryCollection.streaming.replica.removeSegment(defaultSegmentID)
	assert.NoError(t, err)

	err = queryCollection.historical.replica.removeSegment(defaultSegmentID)
	assert.NoError(t, err)

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

	t.Run("test adjustByChangeInfo", func(t *testing.T) {
		qc, err := genSimpleQueryCollection(ctx, cancel)
		assert.Nil(t, err)

		info := genSimpleSealedSegmentsChangeInfoMsg()

		// test online
		info.OnlineSegments = append(info.OnlineSegments, genSimpleSegmentInfo())
		err = qc.adjustByChangeInfo(info)
		assert.NoError(t, err)
		ids := qc.globalSegmentManager.getGlobalSegmentIDs()
		assert.Len(t, ids, 1)

		// test offline
		info.OnlineSegments = make([]*querypb.SegmentInfo, 0)
		info.OfflineSegments = append(info.OfflineSegments, genSimpleSegmentInfo())
		err = qc.adjustByChangeInfo(info)
		assert.NoError(t, err)
		ids = qc.globalSegmentManager.getGlobalSegmentIDs()
		assert.Len(t, ids, 0)
	})

	t.Run("test mismatch collectionID when adjustByChangeInfo", func(t *testing.T) {
		qc, err := genSimpleQueryCollection(ctx, cancel)
		assert.Nil(t, err)

		info := genSimpleSealedSegmentsChangeInfoMsg()

		// test online
		simpleInfo := genSimpleSegmentInfo()
		simpleInfo.CollectionID = 1000
		info.OnlineSegments = append(info.OnlineSegments, simpleInfo)
		err = qc.adjustByChangeInfo(info)
		assert.Error(t, err)
	})

	t.Run("test no segment when adjustByChangeInfo", func(t *testing.T) {
		qc, err := genSimpleQueryCollection(ctx, cancel)
		assert.Nil(t, err)

		err = qc.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		info := genSimpleSealedSegmentsChangeInfoMsg()
		info.OfflineSegments = append(info.OfflineSegments, genSimpleSegmentInfo())

		err = qc.adjustByChangeInfo(info)
		assert.Error(t, err)
	})
}
