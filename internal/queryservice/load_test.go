package queryservice

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"

	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

//generate insert data
const msgLength = 100
const receiveBufSize = 1024
const pulsarBufSize = 1024
const DIM = 16

func genInsert(collectionID int64, partitionID int64, segmentID int64, timeStart int) (*msgstream.MsgPack, *msgstream.MsgPack) {
	msgs := make([]msgstream.TsMsg, 0)
	for n := timeStart; n < timeStart+msgLength; n++ {
		rowData := make([]byte, 0)
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, uint64(n))
		rowData = append(rowData, id...)
		time := make([]byte, 8)
		binary.BigEndian.PutUint64(time, uint64(n))
		rowData = append(rowData, time...)
		for i := 0; i < DIM; i++ {
			vec := make([]byte, 4)
			binary.BigEndian.PutUint32(vec, math.Float32bits(float32(n*i)))
			rowData = append(rowData, vec...)
		}
		age := make([]byte, 4)
		binary.BigEndian.PutUint32(age, 1)
		rowData = append(rowData, age...)
		blob := &commonpb.Blob{
			Value: rowData,
		}

		var insertMsg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{uint32(n)},
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     0,
					Timestamp: uint64(n),
					SourceID:  0,
				},
				CollectionID: collectionID,
				PartitionID:  partitionID,
				SegmentID:    segmentID,
				ChannelID:    "0",
				Timestamps:   []uint64{uint64(n)},
				RowIDs:       []int64{int64(n)},
				RowData:      []*commonpb.Blob{blob},
			},
		}
		msgs = append(msgs, insertMsg)
	}

	insertMsgPack := &msgstream.MsgPack{
		BeginTs: uint64(timeStart),
		EndTs:   uint64(timeStart + msgLength),
		Msgs:    msgs,
	}

	// generate timeTick
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     0,
				Timestamp: uint64(timeStart + msgLength),
				SourceID:  0,
			},
		},
	}
	timeTickMsgPack := &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{timeTickMsg},
	}
	return insertMsgPack, timeTickMsgPack
}

func genSchema(collectionID int64) *schemapb.CollectionSchema {
	fieldID := schemapb.FieldSchema{
		FieldID:      UniqueID(0),
		Name:         "RowID",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT64,
	}

	fieldTime := schemapb.FieldSchema{
		FieldID:      UniqueID(1),
		Name:         "Timestamp",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT64,
	}

	fieldVec := schemapb.FieldSchema{
		FieldID:      UniqueID(100),
		Name:         "vec",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		FieldID:      UniqueID(101),
		Name:         "age",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT32,
	}

	return &schemapb.CollectionSchema{
		Name:   "collection-" + strconv.FormatInt(collectionID, 10),
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldID, &fieldTime, &fieldVec, &fieldInt,
		},
	}
}

func genCreateCollection(collectionID int64) *msgstream.MsgPack {
	schema := genSchema(collectionID)

	byteSchema, err := proto.Marshal(schema)
	if err != nil {
		panic(err)
	}

	request := internalpb2.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kCreateCollection,
			Timestamp: uint64(10),
		},
		DbID:         0,
		CollectionID: collectionID,
		Schema:       byteSchema,
	}

	msg := &msgstream.CreateCollectionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		},
		CreateCollectionRequest: request,
	}

	return &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
}

func genCreatePartition(collectionID int64, partitionID int64) *msgstream.MsgPack {
	request := internalpb2.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kCreatePartition,
			Timestamp: uint64(20),
		},
		DbID:         0,
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}

	msg := &msgstream.CreatePartitionMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		},
		CreatePartitionRequest: request,
	}
	return &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
}

func getMinioKV(ctx context.Context) (*minioKV.MinIOKV, error) {
	minioAddress := "localhost:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false
	bucketName := "a-bucket"

	option := &minioKV.Option{
		Address:           minioAddress,
		AccessKeyID:       accessKeyID,
		SecretAccessKeyID: secretAccessKey,
		UseSSL:            useSSL,
		BucketName:        bucketName,
		CreateBucket:      true,
	}

	return minioKV.NewMinIOKV(ctx, option)
}

func TestLoadCollection(t *testing.T) {
	//// produce msg
	//insertChannels := []string{"insert-0"}
	//ddChannels := []string{"data-definition-0"}
	//pulsarAddress := "pulsar://127.0.0.1:6650"
	//
	//insertStream := pulsarms.NewPulsarMsgStream(context.Background(), receiveBufSize)
	//insertStream.SetPulsarClient(pulsarAddress)
	//insertStream.CreatePulsarProducers(insertChannels)
	//ddStream := pulsarms.NewPulsarMsgStream(context.Background(), receiveBufSize)
	//ddStream.SetPulsarClient(pulsarAddress)
	//ddStream.CreatePulsarProducers(ddChannels)
	//
	//var insertMsgStream msgstream.MsgStream = insertStream
	//insertMsgStream.Start()
	//var ddMsgStream msgstream.MsgStream = ddStream
	//ddMsgStream.Start()
	//
	//createCollectionMsgPack := genCreateCollection(1)
	//createPartitionMsgPack := genCreatePartition(1, 1)
	//ddMsgStream.Produce(createCollectionMsgPack)
	//ddMsgStream.Produce(createPartitionMsgPack)
	//
	//consumeStream := pulsarms.NewPulsarTtMsgStream(context.Background(), receiveBufSize)
	//consumeStream.SetPulsarClient(pulsarAddress)
	//unmarshalDispatcher := util.NewUnmarshalDispatcher()
	//consumeStream.CreatePulsarConsumers(insertChannels, "test", unmarshalDispatcher, pulsarBufSize)
	//consumeStream.Start()
	//
	//for i := 0; i < 10; i++ {
	//	insertMsgPack, timeTickMsgPack := genInsert(1, 1, int64(i), i*msgLength+1)
	//	err := insertMsgStream.Produce(insertMsgPack)
	//	assert.NoError(t, err)
	//	err = insertMsgStream.Broadcast(timeTickMsgPack)
	//	assert.NoError(t, err)
	//	err = ddMsgStream.Broadcast(timeTickMsgPack)
	//	assert.NoError(t, err)
	//}
	//
	////consume msg
	//segPosition := make(map[int64][]*internalpb2.MsgPosition)
	//segmentData := make([]*storage.InsertData, 0)
	//indexRowDatas := make([][]float32, 0)
	//for i := 0; i < 10; i++ {
	//	msgPack := consumeStream.Consume()
	//	idData := make([]int64, 0)
	//	timestamps := make([]int64, 0)
	//	fieldAgeData := make([]int32, 0)
	//	fieldVecData := make([]float32, 0)
	//	for n := 0; n < msgLength; n++ {
	//		blob := msgPack.Msgs[n].(*msgstream.InsertMsg).RowData[0].Value
	//		id := binary.BigEndian.Uint64(blob[0:8])
	//		idData = append(idData, int64(id))
	//		time := binary.BigEndian.Uint64(blob[8:16])
	//		timestamps = append(timestamps, int64(time))
	//		for i := 0; i < DIM; i++ {
	//			bits := binary.BigEndian.Uint32(blob[16+4*i : 16+4*(i+1)])
	//			floatVec := math.Float32frombits(bits)
	//			fieldVecData = append(fieldVecData, floatVec)
	//		}
	//		ageValue := binary.BigEndian.Uint32(blob[80:84])
	//		fieldAgeData = append(fieldAgeData, int32(ageValue))
	//	}
	//
	//	insertData := &storage.InsertData{
	//		Data: map[int64]storage.FieldData{
	//			0: &storage.Int64FieldData{
	//				NumRows: msgLength,
	//				Data:    idData,
	//			},
	//			1: &storage.Int64FieldData{
	//				NumRows: msgLength,
	//				Data:    timestamps,
	//			},
	//			100: &storage.FloatVectorFieldData{
	//				NumRows: msgLength,
	//				Data:    fieldVecData,
	//				Dim:     DIM,
	//			},
	//			101: &storage.Int32FieldData{
	//				NumRows: msgLength,
	//				Data:    fieldAgeData,
	//			},
	//		},
	//	}
	//	segPosition[int64(i)] = msgPack.StartPositions
	//	segmentData = append(segmentData, insertData)
	//	indexRowDatas = append(indexRowDatas, fieldVecData)
	//}
	//
	////gen inCodec
	//collectionMeta := &etcdpb.CollectionMeta{
	//	ID:           1,
	//	Schema:       genSchema(1),
	//	CreateTime:   0,
	//	PartitionIDs: []int64{1},
	//	SegmentIDs:   []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	//}
	//inCodec := storage.NewInsertCodec(collectionMeta)
	//
	//// get minio client
	//minioKV, err := getMinioKV(context.Background())
	//assert.Nil(t, err)
	//
	//// write binlog minio
	//collectionStr := strconv.FormatInt(1, 10)
	//for i := 0; i < 9; i++ {
	//	binLogs, err := inCodec.Serialize(1, storage.UniqueID(i), segmentData[i])
	//	assert.Nil(t, err)
	//	assert.Equal(t, len(binLogs), 4)
	//	keyPrefix := "distributed-query-test-binlog"
	//	segmentStr := strconv.FormatInt(int64(i), 10)
	//
	//	for _, blob := range binLogs {
	//		key := path.Join(keyPrefix, collectionStr, segmentStr, blob.Key)
	//		err = minioKV.Save(key, string(blob.Value[:]))
	//		assert.Nil(t, err)
	//	}
	//}
	//
	//// gen index build's indexParams
	//indexParams := make(map[string]string)
	//indexParams["index_type"] = "IVF_PQ"
	//indexParams["index_mode"] = "cpu"
	//indexParams["dim"] = "16"
	//indexParams["k"] = "10"
	//indexParams["nlist"] = "100"
	//indexParams["nprobe"] = "10"
	//indexParams["m"] = "4"
	//indexParams["nbits"] = "8"
	//indexParams["metric_type"] = "L2"
	//indexParams["SLICE_SIZE"] = "400"
	//
	//var indexParamsKV []*commonpb.KeyValuePair
	//for key, value := range indexParams {
	//	indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
	//		Key:   key,
	//		Value: value,
	//	})
	//}
	//
	//// generator index and  write index to minio
	//for i := 0; i < 9; i++ {
	//	typeParams := make(map[string]string)
	//	typeParams["dim"] = "16"
	//	index, err := indexnode.NewCIndex(typeParams, indexParams)
	//	assert.Nil(t, err)
	//	err = index.BuildFloatVecIndexWithoutIds(indexRowDatas[i])
	//	assert.Equal(t, err, nil)
	//	binarySet, err := index.Serialize()
	//	assert.Equal(t, len(binarySet), 1)
	//	assert.Nil(t, err)
	//	keyPrefix := "distributed-query-test-index"
	//	segmentStr := strconv.FormatInt(int64(i), 10)
	//	indexStr := strconv.FormatInt(int64(i), 10)
	//	key := path.Join(keyPrefix, collectionStr, segmentStr, indexStr)
	//	minioKV.Save(key, string(binarySet[0].Value))
	//}
	//
	////generate query service
	//service, err := NewQueryService(context.Background())
	//assert.Nil(t, err)
	//collectionID := UniqueID(1)
	//partitions := []UniqueID{1}
	//col2partition := make(map[UniqueID][]UniqueID)
	//col2partition[collectionID] = partitions
	//segments := []UniqueID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	//partition2segment := make(map[UniqueID][]UniqueID)
	//partition2segment[UniqueID(1)] = segments
	//masterMock := &masterMock{
	//	collectionIDs:     []UniqueID{1},
	//	col2partition:     col2partition,
	//	partition2segment: partition2segment,
	//}
	//service.SetMasterService(masterMock)
	//segStates := make(map[UniqueID]*datapb.SegmentStatesResponse)
	//for i := 0; i < 10; i++ {
	//	if i != 9 {
	//		state := &datapb.SegmentStatesResponse{
	//			State:          datapb.SegmentState_SegmentFlushed,
	//			StartPositions: segPosition[int64(i)],
	//		}
	//		segStates[UniqueID(i)] = state
	//	} else {
	//		state := &datapb.SegmentStatesResponse{
	//			State:          datapb.SegmentState_SegmentGrowing,
	//			StartPositions: segPosition[int64(i)],
	//		}
	//		segStates[UniqueID(i)] = state
	//	}
	//}
	//dataMock := &dataMock{
	//	segmentIDs:    []UniqueID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	//	segmentStates: segStates,
	//}
	//
	//service.SetDataService(dataMock)
	//service.SetEnableGrpc(true)
	//
	//loadCollectionRequest := &querypb.LoadCollectionRequest{
	//	Base: &commonpb.MsgBase{
	//		MsgType: commonpb.MsgType_kCreateCollection,
	//	},
	//	DbID:         UniqueID(0),
	//	CollectionID: collectionID,
	//}
	//
	//registerRequest := &querypb.RegisterNodeRequest{
	//	Address: &commonpb.Address{
	//		Ip:   "localhost",
	//		Port: 20010,
	//	},
	//}
	//response, err := service.RegisterNode(registerRequest)
	//assert.Nil(t, err)
	//assert.Equal(t, response.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
	//
	//status, err := service.LoadCollection(loadCollectionRequest)
	//assert.Nil(t, err)
	//assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_SUCCESS)
}
