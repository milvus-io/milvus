package querynode

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

func generateInsertBinLog(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, keyPrefix string) ([]*internalPb.StringList, []int64, error) {
	const (
		msgLength = 1000
		DIM       = 16
	)

	idData := make([]int64, 0)
	for n := 0; n < msgLength; n++ {
		idData = append(idData, int64(n))
	}

	var timestamps []int64
	for n := 0; n < msgLength; n++ {
		timestamps = append(timestamps, int64(n+1))
	}

	var fieldAgeData []int32
	for n := 0; n < msgLength; n++ {
		fieldAgeData = append(fieldAgeData, int32(n))
	}

	fieldVecData := make([]float32, 0)
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM; i++ {
			fieldVecData = append(fieldVecData, float32(n*i)*0.1)
		}
	}

	insertData := &storage.InsertData{
		Data: map[int64]storage.FieldData{
			0: &storage.Int64FieldData{
				NumRows: msgLength,
				Data:    idData,
			},
			1: &storage.Int64FieldData{
				NumRows: msgLength,
				Data:    timestamps,
			},
			100: &storage.FloatVectorFieldData{
				NumRows: msgLength,
				Data:    fieldVecData,
				Dim:     DIM,
			},
			101: &storage.Int32FieldData{
				NumRows: msgLength,
				Data:    fieldAgeData,
			},
		},
	}

	// buffer data to binLogs
	collMeta := genTestCollectionMeta("collection0", collectionID, false)
	collMeta.Schema.Fields = append(collMeta.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  0,
		Name:     "uid",
		DataType: schemapb.DataType_INT64,
	})
	collMeta.Schema.Fields = append(collMeta.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "timestamp",
		DataType: schemapb.DataType_INT64,
	})
	inCodec := storage.NewInsertCodec(collMeta)
	binLogs, err := inCodec.Serialize(partitionID, segmentID, insertData)

	if err != nil {
		return nil, nil, err
	}

	// create minio client
	bucketName := Params.MinioBucketName
	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, nil, err
	}

	// binLogs -> minIO/S3
	segIDStr := strconv.FormatInt(segmentID, 10)
	keyPrefix = path.Join(keyPrefix, segIDStr)

	paths := make([]*internalPb.StringList, 0)
	fieldIDs := make([]int64, 0)
	fmt.Println(".. saving binlog to MinIO ...", len(binLogs))
	for _, blob := range binLogs {
		uid := rand.Int63n(100000000)
		key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
		err = kv.Save(key, string(blob.Value[:]))
		if err != nil {
			return nil, nil, err
		}
		paths = append(paths, &internalPb.StringList{
			Values: []string{key},
		})
		fieldID, err := strconv.Atoi(blob.Key)
		if err != nil {
			return nil, nil, err
		}
		fieldIDs = append(fieldIDs, int64(fieldID))
	}

	return paths, fieldIDs, nil
}

func generateIndex(segmentID UniqueID) ([]string, indexParam, error) {
	const (
		msgLength = 1000
		DIM       = 16
	)

	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "16"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "4"

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(DIM)
	var indexRowData []float32
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}

	index, err := indexnode.NewCIndex(typeParams, indexParams)
	if err != nil {
		return nil, nil, err
	}

	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	if err != nil {
		return nil, nil, err
	}

	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}

	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, nil, err
	}

	//save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, nil, err
	}

	indexPaths := make([]string, 0)
	for _, index := range binarySet {
		path := strconv.Itoa(int(segmentID)) + "/" + index.Key
		indexPaths = append(indexPaths, path)
		err := kv.Save(path, string(index.Value))
		if err != nil {
			return nil, nil, err
		}
	}

	return indexPaths, indexParams, nil
}

func doInsert(ctx context.Context, collectionName string, partitionTag string, segmentID UniqueID) error {
	const msgLength = 1000
	const DIM = 16

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	// messages generate
	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i),
				},
			},
			InsertRequest: internalPb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     0,
					Timestamp: uint64(i + 1000),
					SourceID:  0,
				},
				CollectionName: collectionName,
				PartitionName:  partitionTag,
				SegmentID:      segmentID,
				ChannelID:      "0",
				Timestamps:     []uint64{uint64(i + 1000)},
				RowIDs:         []int64{int64(i)},
				RowData: []*commonpb.Blob{
					{Value: rawData},
				},
			},
		}
		insertMessages = append(insertMessages, msg)
	}

	msgPack := msgstream.MsgPack{
		BeginTs: timeRange.timestampMin,
		EndTs:   timeRange.timestampMax,
		Msgs:    insertMessages,
	}

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 1000,
		EndTimestamp:   1500,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalPb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kTimeTick,
			MsgID:     0,
			Timestamp: 1000,
			SourceID:  0,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	const receiveBufSize = 1024
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames
	pulsarURL := Params.PulsarAddress

	insertStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(insertChannels)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	insertStream.CreatePulsarConsumers(insertChannels, Params.MsgChannelSubName, unmarshalDispatcher, receiveBufSize)

	ddStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	ddStream.SetPulsarClient(pulsarURL)
	ddStream.CreatePulsarProducers(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err := insertMsgStream.Produce(&msgPack)
	if err != nil {
		return err
	}

	err = insertMsgStream.Broadcast(&timeTickMsgPack)
	if err != nil {
		return err
	}
	err = ddMsgStream.Broadcast(&timeTickMsgPack)
	if err != nil {
		return err
	}

	//messages := insertStream.Consume()
	//for _, msg := range messages.Msgs {
	//
	//}

	return nil
}

func sentTimeTick(ctx context.Context) error {
	timeTickMsgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 1500,
		EndTimestamp:   2000,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalPb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kTimeTick,
			MsgID:     0,
			Timestamp: math.MaxUint64,
			SourceID:  0,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	const receiveBufSize = 1024
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames
	pulsarURL := Params.PulsarAddress

	insertStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(insertChannels)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	insertStream.CreatePulsarConsumers(insertChannels, Params.MsgChannelSubName, unmarshalDispatcher, receiveBufSize)

	ddStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	ddStream.SetPulsarClient(pulsarURL)
	ddStream.CreatePulsarProducers(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err := insertMsgStream.Broadcast(&timeTickMsgPack)
	if err != nil {
		return err
	}
	err = ddMsgStream.Broadcast(&timeTickMsgPack)
	if err != nil {
		return err
	}
	return nil
}

func TestSegmentManager_load_release_and_search(t *testing.T) {
	collectionID := UniqueID(0)
	partitionID := UniqueID(1)
	segmentID := UniqueID(2)
	fieldIDs := []int64{0, 101}

	// mock write insert bin log
	keyPrefix := path.Join("query-node-seg-manager-test-minio-prefix", strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10))
	Params.WriteNodeSegKvSubPath = keyPrefix

	node := newQueryNodeMock()
	defer node.Stop()

	ctx := node.queryNodeLoopCtx
	node.loadIndexService = newLoadIndexService(ctx, node.replica)
	node.segManager = newSegmentManager(ctx, node.replica, nil, node.loadIndexService.loadIndexReqChan)
	go node.loadIndexService.start()

	collectionName := "collection0"
	initTestMeta(t, node, collectionName, collectionID, 0)

	err := node.replica.addPartition(collectionID, partitionID)
	assert.NoError(t, err)

	err = node.replica.addSegment(segmentID, partitionID, collectionID, segTypeSealed)
	assert.NoError(t, err)

	paths, srcFieldIDs, err := generateInsertBinLog(collectionID, partitionID, segmentID, keyPrefix)
	assert.NoError(t, err)

	fieldsMap := node.segManager.filterOutNeedlessFields(paths, srcFieldIDs, fieldIDs)
	assert.Equal(t, len(fieldsMap), 2)

	err = node.segManager.loadSegmentFieldsData(segmentID, fieldsMap)
	assert.NoError(t, err)

	indexPaths, indexParams, err := generateIndex(segmentID)
	assert.NoError(t, err)

	err = node.segManager.loadIndex(segmentID, indexPaths, indexParams)
	assert.NoError(t, err)

	// do search
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"

	const DIM = 16
	var searchRawData []byte
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		searchRawData = append(searchRawData, buf...)
	}
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData},
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeHolderGroupBlob, err := proto.Marshal(&placeholderGroup)
	assert.NoError(t, err)

	searchTimestamp := Timestamp(1020)
	collection, err := node.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	plan, err := createPlan(*collection, dslString)
	assert.NoError(t, err)
	holder, err := parserPlaceholderGroup(plan, placeHolderGroupBlob)
	assert.NoError(t, err)
	placeholderGroups := make([]*PlaceholderGroup, 0)
	placeholderGroups = append(placeholderGroups, holder)

	// wait for segment building index
	time.Sleep(3 * time.Second)

	segment, err := node.replica.getSegmentByID(segmentID)
	assert.NoError(t, err)
	_, err = segment.segmentSearch(plan, placeholderGroups, []Timestamp{searchTimestamp})
	assert.Nil(t, err)

	plan.delete()
	holder.delete()

	<-ctx.Done()
}

//// NOTE: start pulsar before test
//func TestSegmentManager_with_seek(t *testing.T) {
//	collectionID := UniqueID(0)
//	partitionID := UniqueID(1)
//	//segmentID := UniqueID(2)
//	fieldIDs := []int64{0, 101}
//
//	//// mock write insert bin log
//	//keyPrefix := path.Join("query-node-seg-manager-test-minio-prefix", strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10))
//	//Params.WriteNodeSegKvSubPath = keyPrefix + "/"
//	node := newQueryNodeMock()
//
//	ctx := node.queryNodeLoopCtx
//	go node.Start()
//
//	collectionName := "collection0"
//	initTestMeta(t, node, collectionName, collectionID, 0)
//
//	err := node.replica.addPartition(collectionID, partitionID)
//	assert.NoError(t, err)
//
//	//err = node.replica.addSegment(segmentID, partitionID, collectionID, segTypeSealed)
//	//assert.NoError(t, err)
//
//	//paths, srcFieldIDs, err := generateInsertBinLog(collectionID, partitionID, segmentID, keyPrefix)
//	//assert.NoError(t, err)
//
//	//fieldsMap := node.segManager.filterOutNeedlessFields(paths, srcFieldIDs, fieldIDs)
//	//assert.Equal(t, len(fieldsMap), 2)
//
//	segmentIDToInsert := UniqueID(3)
//	err = doInsert(ctx, collectionName, "default", segmentIDToInsert)
//	assert.NoError(t, err)
//
//	startPositions := make([]*internalPb.MsgPosition, 0)
//	for _, ch := range Params.InsertChannelNames {
//		startPositions = append(startPositions, &internalPb.MsgPosition{
//			ChannelName: ch,
//		})
//	}
//	var positions []*internalPb.MsgPosition
//	lastSegStates := &datapb.SegmentStatesResponse{
//		State:          datapb.SegmentState_SegmentGrowing,
//		StartPositions: positions,
//	}
//	loadReq := &querypb.LoadSegmentRequest{
//		CollectionID:     collectionID,
//		PartitionID:      partitionID,
//		SegmentIDs:       []UniqueID{segmentIDToInsert},
//		FieldIDs:         fieldIDs,
//		LastSegmentState: lastSegStates,
//	}
//	_, err = node.LoadSegments(loadReq)
//	assert.NoError(t, err)
//
//	err = sentTimeTick(ctx)
//	assert.NoError(t, err)
//
//	// do search
//	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
//
//	const DIM = 16
//	var searchRawData []byte
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	for _, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
//		searchRawData = append(searchRawData, buf...)
//	}
//	placeholderValue := milvuspb.PlaceholderValue{
//		Tag:    "$0",
//		Type:   milvuspb.PlaceholderType_VECTOR_FLOAT,
//		Values: [][]byte{searchRawData},
//	}
//
//	placeholderGroup := milvuspb.PlaceholderGroup{
//		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
//	}
//
//	placeHolderGroupBlob, err := proto.Marshal(&placeholderGroup)
//	assert.NoError(t, err)
//
//	//searchTimestamp := Timestamp(1020)
//	collection, err := node.replica.getCollectionByID(collectionID)
//	assert.NoError(t, err)
//	plan, err := createPlan(*collection, dslString)
//	assert.NoError(t, err)
//	holder, err := parserPlaceholderGroup(plan, placeHolderGroupBlob)
//	assert.NoError(t, err)
//	placeholderGroups := make([]*PlaceholderGroup, 0)
//	placeholderGroups = append(placeholderGroups, holder)
//
//	// wait for segment building index
//	time.Sleep(3 * time.Second)
//
//	//segment, err := node.replica.getSegmentByID(segmentIDToInsert)
//	//assert.NoError(t, err)
//	//_, err = segment.segmentSearch(plan, placeholderGroups, []Timestamp{searchTimestamp})
//	//assert.Nil(t, err)
//
//	plan.delete()
//	holder.delete()
//
//	<-ctx.Done()
//	err = node.Stop()
//	assert.NoError(t, err)
//}
