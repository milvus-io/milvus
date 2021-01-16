package querynode

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/querynode/client"
)

func TestLoadIndexService_FloatVector(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := rand.Int63n(1000000)
	segmentID := rand.Int63n(1000000)
	initTestMeta(t, node, "collection0", collectionID, segmentID)

	// loadIndexService and statsService
	suffix := "-test-search" + strconv.FormatInt(rand.Int63n(1000000), 10)
	oldSearchChannelNames := Params.SearchChannelNames
	newSearchChannelNames := makeNewChannelNames(oldSearchChannelNames, suffix)
	Params.SearchChannelNames = newSearchChannelNames

	oldSearchResultChannelNames := Params.SearchChannelNames
	newSearchResultChannelNames := makeNewChannelNames(oldSearchResultChannelNames, suffix)
	Params.SearchResultChannelNames = newSearchResultChannelNames

	oldLoadIndexChannelNames := Params.LoadIndexChannelNames
	newLoadIndexChannelNames := makeNewChannelNames(oldLoadIndexChannelNames, suffix)
	Params.LoadIndexChannelNames = newLoadIndexChannelNames

	oldStatsChannelName := Params.StatsChannelName
	newStatsChannelNames := makeNewChannelNames([]string{oldStatsChannelName}, suffix)
	Params.StatsChannelName = newStatsChannelNames[0]
	go node.Start()

	//generate insert data
	const msgLength = 1000
	const receiveBufSize = 1024
	const DIM = 16
	var insertRowBlob []*commonpb.Blob
	var timestamps []uint64
	var rowIDs []int64
	var hashValues []uint32
	for n := 0; n < msgLength; n++ {
		rowData := make([]byte, 0)
		for i := 0; i < DIM; i++ {
			vec := make([]byte, 4)
			binary.LittleEndian.PutUint32(vec, math.Float32bits(float32(n*i)))
			rowData = append(rowData, vec...)
		}
		age := make([]byte, 4)
		binary.LittleEndian.PutUint32(age, 1)
		rowData = append(rowData, age...)
		blob := &commonpb.Blob{
			Value: rowData,
		}
		insertRowBlob = append(insertRowBlob, blob)
		timestamps = append(timestamps, uint64(n))
		rowIDs = append(rowIDs, int64(n))
		hashValues = append(hashValues, uint32(n))
	}

	var insertMsg msgstream.TsMsg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: hashValues,
		},
		InsertRequest: internalpb.InsertRequest{
			MsgType:        commonpb.MsgType_kInsert,
			ReqID:          0,
			CollectionName: "collection0",
			PartitionTag:   "default",
			SegmentID:      segmentID,
			ChannelID:      int64(0),
			ProxyID:        int64(0),
			Timestamps:     timestamps,
			RowIDs:         rowIDs,
			RowData:        insertRowBlob,
		},
	}
	insertMsgPack := msgstream.MsgPack{
		BeginTs: 0,
		EndTs:   math.MaxUint64,
		Msgs:    []msgstream.TsMsg{insertMsg},
	}

	// generate timeTick
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			MsgType:   commonpb.MsgType_kTimeTick,
			PeerID:    UniqueID(0),
			Timestamp: math.MaxUint64,
		},
	}
	timeTickMsgPack := &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{timeTickMsg},
	}

	// pulsar produce
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames

	insertStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	insertStream.SetPulsarClient(Params.PulsarAddress)
	insertStream.CreatePulsarProducers(insertChannels)
	ddStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	ddStream.SetPulsarClient(Params.PulsarAddress)
	ddStream.CreatePulsarProducers(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()
	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err := insertMsgStream.Produce(&insertMsgPack)
	assert.NoError(t, err)
	err = insertMsgStream.Broadcast(timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(timeTickMsgPack)
	assert.NoError(t, err)

	// generator searchRowData
	var searchRowData []float32
	for i := 0; i < DIM; i++ {
		searchRowData = append(searchRowData, float32(42*i))
	}

	//generate search data and send search msg
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
	var searchRowByteData []byte
	for i := range searchRowData {
		vec := make([]byte, 4)
		binary.LittleEndian.PutUint32(vec, math.Float32bits(searchRowData[i]))
		searchRowByteData = append(searchRowByteData, vec...)
	}
	placeholderValue := servicepb.PlaceholderValue{
		Tag:    "$0",
		Type:   servicepb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRowByteData},
	}
	placeholderGroup := servicepb.PlaceholderGroup{
		Placeholders: []*servicepb.PlaceholderValue{&placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}
	query := servicepb.Query{
		CollectionName:   "collection0",
		PartitionTags:    []string{"default"},
		Dsl:              dslString,
		PlaceholderGroup: placeGroupByte,
	}
	queryByte, err := proto.Marshal(&query)
	if err != nil {
		log.Print("marshal query failed")
	}
	blob := commonpb.Blob{
		Value: queryByte,
	}
	fn := func(n int64) *msgstream.MsgPack {
		searchMsg := &msgstream.SearchMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			SearchRequest: internalpb.SearchRequest{
				MsgType:         commonpb.MsgType_kSearch,
				ReqID:           n,
				ProxyID:         int64(1),
				Timestamp:       uint64(msgLength),
				ResultChannelID: int64(0),
				Query:           &blob,
			},
		}
		return &msgstream.MsgPack{
			Msgs: []msgstream.TsMsg{searchMsg},
		}
	}
	searchStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	searchStream.SetPulsarClient(Params.PulsarAddress)
	searchStream.CreatePulsarProducers(newSearchChannelNames)
	searchStream.Start()
	err = searchStream.Produce(fn(1))
	assert.NoError(t, err)

	//get search result
	searchResultStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	searchResultStream.SetPulsarClient(Params.PulsarAddress)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	searchResultStream.CreatePulsarConsumers(newSearchResultChannelNames, "loadIndexTestSubSearchResult", unmarshalDispatcher, receiveBufSize)
	searchResultStream.Start()
	searchResult := searchResultStream.Consume()
	assert.NotNil(t, searchResult)
	unMarshaledHit := servicepb.Hits{}
	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
	assert.Nil(t, err)

	// gen load index message pack
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

	// generator index
	typeParams := make(map[string]string)
	typeParams["dim"] = "16"
	var indexRowData []float32
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}
	index, err := indexnode.NewCIndex(typeParams, indexParams)
	assert.Nil(t, err)
	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	assert.Equal(t, err, nil)

	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}

	minioKV, err := minioKV.NewMinIOKV(node.queryNodeLoopCtx, option)
	assert.Equal(t, err, nil)
	//save index to minio
	binarySet, err := index.Serialize()
	assert.Equal(t, err, nil)
	indexPaths := make([]string, 0)
	for _, index := range binarySet {
		path := strconv.Itoa(int(segmentID)) + "/" + index.Key
		indexPaths = append(indexPaths, path)
		minioKV.Save(path, string(index.Value))
	}

	//test index search result
	indexResult, err := index.QueryOnFloatVecIndexWithParam(searchRowData, indexParams)
	assert.Equal(t, err, nil)

	// create loadIndexClient
	fieldID := UniqueID(100)
	loadIndexChannelNames := Params.LoadIndexChannelNames
	client := client.NewQueryNodeClient(node.queryNodeLoopCtx, Params.PulsarAddress, loadIndexChannelNames)
	client.LoadIndex(indexPaths, segmentID, fieldID, "vec", indexParams)

	// init message stream consumer and do checks
	statsMs := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.StatsReceiveBufSize)
	statsMs.SetPulsarClient(Params.PulsarAddress)
	statsMs.CreatePulsarConsumers([]string{Params.StatsChannelName}, Params.MsgChannelSubName, msgstream.NewUnmarshalDispatcher(), Params.StatsReceiveBufSize)
	statsMs.Start()

	findFiledStats := false
	for {
		receiveMsg := msgstream.MsgStream(statsMs).Consume()
		assert.NotNil(t, receiveMsg)
		assert.NotEqual(t, len(receiveMsg.Msgs), 0)

		for _, msg := range receiveMsg.Msgs {
			statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
			if statsMsg.FieldStats == nil || len(statsMsg.FieldStats) == 0 {
				continue
			}
			findFiledStats = true
			assert.Equal(t, ok, true)
			assert.Equal(t, len(statsMsg.FieldStats), 1)
			fieldStats0 := statsMsg.FieldStats[0]
			assert.Equal(t, fieldStats0.FieldID, fieldID)
			assert.Equal(t, fieldStats0.CollectionID, collectionID)
			assert.Equal(t, len(fieldStats0.IndexStats), 1)
			indexStats0 := fieldStats0.IndexStats[0]
			params := indexStats0.IndexParams
			// sort index params by key
			sort.Slice(indexParamsKV, func(i, j int) bool { return indexParamsKV[i].Key < indexParamsKV[j].Key })
			indexEqual := node.loadIndexService.indexParamsEqual(params, indexParamsKV)
			assert.Equal(t, indexEqual, true)
		}

		if findFiledStats {
			break
		}
	}

	err = searchStream.Produce(fn(2))
	assert.NoError(t, err)
	searchResult = searchResultStream.Consume()
	assert.NotNil(t, searchResult)
	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
	assert.Nil(t, err)

	idsIndex := indexResult.IDs()
	idsSegment := unMarshaledHit.IDs
	assert.Equal(t, len(idsIndex), len(idsSegment))
	for i := 0; i < len(idsIndex); i++ {
		assert.Equal(t, idsIndex[i], idsSegment[i])
	}
	Params.SearchChannelNames = oldSearchChannelNames
	Params.SearchResultChannelNames = oldSearchResultChannelNames
	Params.LoadIndexChannelNames = oldLoadIndexChannelNames
	Params.StatsChannelName = oldStatsChannelName
	fmt.Println("loadIndex floatVector test Done!")

	defer assert.Equal(t, findFiledStats, true)
	<-node.queryNodeLoopCtx.Done()
	node.Close()
}

func TestLoadIndexService_BinaryVector(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := rand.Int63n(1000000)
	segmentID := rand.Int63n(1000000)
	initTestMeta(t, node, "collection0", collectionID, segmentID, true)

	// loadIndexService and statsService
	suffix := "-test-search-binary" + strconv.FormatInt(rand.Int63n(1000000), 10)
	oldSearchChannelNames := Params.SearchChannelNames
	newSearchChannelNames := makeNewChannelNames(oldSearchChannelNames, suffix)
	Params.SearchChannelNames = newSearchChannelNames

	oldSearchResultChannelNames := Params.SearchChannelNames
	newSearchResultChannelNames := makeNewChannelNames(oldSearchResultChannelNames, suffix)
	Params.SearchResultChannelNames = newSearchResultChannelNames

	oldLoadIndexChannelNames := Params.LoadIndexChannelNames
	newLoadIndexChannelNames := makeNewChannelNames(oldLoadIndexChannelNames, suffix)
	Params.LoadIndexChannelNames = newLoadIndexChannelNames

	oldStatsChannelName := Params.StatsChannelName
	newStatsChannelNames := makeNewChannelNames([]string{oldStatsChannelName}, suffix)
	Params.StatsChannelName = newStatsChannelNames[0]
	go node.Start()

	const msgLength = 1000
	const receiveBufSize = 1024
	const DIM = 128

	// generator index data
	var indexRowData []byte
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM/8; i++ {
			indexRowData = append(indexRowData, byte(rand.Intn(8)))
		}
	}

	//generator insert data
	var insertRowBlob []*commonpb.Blob
	var timestamps []uint64
	var rowIDs []int64
	var hashValues []uint32
	offset := 0
	for n := 0; n < msgLength; n++ {
		rowData := make([]byte, 0)
		rowData = append(rowData, indexRowData[offset:offset+(DIM/8)]...)
		offset += DIM / 8
		age := make([]byte, 4)
		binary.LittleEndian.PutUint32(age, 1)
		rowData = append(rowData, age...)
		blob := &commonpb.Blob{
			Value: rowData,
		}
		insertRowBlob = append(insertRowBlob, blob)
		timestamps = append(timestamps, uint64(n))
		rowIDs = append(rowIDs, int64(n))
		hashValues = append(hashValues, uint32(n))
	}

	var insertMsg msgstream.TsMsg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: hashValues,
		},
		InsertRequest: internalpb.InsertRequest{
			MsgType:        commonpb.MsgType_kInsert,
			ReqID:          0,
			CollectionName: "collection0",
			PartitionTag:   "default",
			SegmentID:      segmentID,
			ChannelID:      int64(0),
			ProxyID:        int64(0),
			Timestamps:     timestamps,
			RowIDs:         rowIDs,
			RowData:        insertRowBlob,
		},
	}
	insertMsgPack := msgstream.MsgPack{
		BeginTs: 0,
		EndTs:   math.MaxUint64,
		Msgs:    []msgstream.TsMsg{insertMsg},
	}

	// generate timeTick
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			MsgType:   commonpb.MsgType_kTimeTick,
			PeerID:    UniqueID(0),
			Timestamp: math.MaxUint64,
		},
	}
	timeTickMsgPack := &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{timeTickMsg},
	}

	// pulsar produce
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames

	insertStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	insertStream.SetPulsarClient(Params.PulsarAddress)
	insertStream.CreatePulsarProducers(insertChannels)
	ddStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	ddStream.SetPulsarClient(Params.PulsarAddress)
	ddStream.CreatePulsarProducers(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()
	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err := insertMsgStream.Produce(&insertMsgPack)
	assert.NoError(t, err)
	err = insertMsgStream.Broadcast(timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(timeTickMsgPack)
	assert.NoError(t, err)

	//generate search data and send search msg
	searchRowData := indexRowData[42*(DIM/8) : 43*(DIM/8)]
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"JACCARD\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
	placeholderValue := servicepb.PlaceholderValue{
		Tag:    "$0",
		Type:   servicepb.PlaceholderType_VECTOR_BINARY,
		Values: [][]byte{searchRowData},
	}
	placeholderGroup := servicepb.PlaceholderGroup{
		Placeholders: []*servicepb.PlaceholderValue{&placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}
	query := servicepb.Query{
		CollectionName:   "collection0",
		PartitionTags:    []string{"default"},
		Dsl:              dslString,
		PlaceholderGroup: placeGroupByte,
	}
	queryByte, err := proto.Marshal(&query)
	if err != nil {
		log.Print("marshal query failed")
	}
	blob := commonpb.Blob{
		Value: queryByte,
	}
	fn := func(n int64) *msgstream.MsgPack {
		searchMsg := &msgstream.SearchMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			SearchRequest: internalpb.SearchRequest{
				MsgType:         commonpb.MsgType_kSearch,
				ReqID:           n,
				ProxyID:         int64(1),
				Timestamp:       uint64(msgLength),
				ResultChannelID: int64(0),
				Query:           &blob,
			},
		}
		return &msgstream.MsgPack{
			Msgs: []msgstream.TsMsg{searchMsg},
		}
	}
	searchStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	searchStream.SetPulsarClient(Params.PulsarAddress)
	searchStream.CreatePulsarProducers(newSearchChannelNames)
	searchStream.Start()
	err = searchStream.Produce(fn(1))
	assert.NoError(t, err)

	//get search result
	searchResultStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	searchResultStream.SetPulsarClient(Params.PulsarAddress)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	searchResultStream.CreatePulsarConsumers(newSearchResultChannelNames, "loadIndexTestSubSearchResult2", unmarshalDispatcher, receiveBufSize)
	searchResultStream.Start()
	searchResult := searchResultStream.Consume()
	assert.NotNil(t, searchResult)
	unMarshaledHit := servicepb.Hits{}
	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
	assert.Nil(t, err)

	// gen load index message pack
	indexParams := make(map[string]string)
	indexParams["index_type"] = "BIN_IVF_FLAT"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "128"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "JACCARD"
	indexParams["SLICE_SIZE"] = "4"

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	// generator index
	typeParams := make(map[string]string)
	typeParams["dim"] = "128"
	index, err := indexnode.NewCIndex(typeParams, indexParams)
	assert.Nil(t, err)
	err = index.BuildBinaryVecIndexWithoutIds(indexRowData)
	assert.Equal(t, err, nil)

	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}

	minioKV, err := minioKV.NewMinIOKV(node.queryNodeLoopCtx, option)
	assert.Equal(t, err, nil)
	//save index to minio
	binarySet, err := index.Serialize()
	assert.Equal(t, err, nil)
	indexPaths := make([]string, 0)
	for _, index := range binarySet {
		path := strconv.Itoa(int(segmentID)) + "/" + index.Key
		indexPaths = append(indexPaths, path)
		minioKV.Save(path, string(index.Value))
	}

	//test index search result
	indexResult, err := index.QueryOnBinaryVecIndexWithParam(searchRowData, indexParams)
	assert.Equal(t, err, nil)

	// create loadIndexClient
	fieldID := UniqueID(100)
	loadIndexChannelNames := Params.LoadIndexChannelNames
	client := client.NewQueryNodeClient(node.queryNodeLoopCtx, Params.PulsarAddress, loadIndexChannelNames)
	client.LoadIndex(indexPaths, segmentID, fieldID, "vec", indexParams)

	// init message stream consumer and do checks
	statsMs := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.StatsReceiveBufSize)
	statsMs.SetPulsarClient(Params.PulsarAddress)
	statsMs.CreatePulsarConsumers([]string{Params.StatsChannelName}, Params.MsgChannelSubName, msgstream.NewUnmarshalDispatcher(), Params.StatsReceiveBufSize)
	statsMs.Start()

	findFiledStats := false
	for {
		receiveMsg := msgstream.MsgStream(statsMs).Consume()
		assert.NotNil(t, receiveMsg)
		assert.NotEqual(t, len(receiveMsg.Msgs), 0)

		for _, msg := range receiveMsg.Msgs {
			statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
			if statsMsg.FieldStats == nil || len(statsMsg.FieldStats) == 0 {
				continue
			}
			findFiledStats = true
			assert.Equal(t, ok, true)
			assert.Equal(t, len(statsMsg.FieldStats), 1)
			fieldStats0 := statsMsg.FieldStats[0]
			assert.Equal(t, fieldStats0.FieldID, fieldID)
			assert.Equal(t, fieldStats0.CollectionID, collectionID)
			assert.Equal(t, len(fieldStats0.IndexStats), 1)
			indexStats0 := fieldStats0.IndexStats[0]
			params := indexStats0.IndexParams
			// sort index params by key
			sort.Slice(indexParamsKV, func(i, j int) bool { return indexParamsKV[i].Key < indexParamsKV[j].Key })
			indexEqual := node.loadIndexService.indexParamsEqual(params, indexParamsKV)
			assert.Equal(t, indexEqual, true)
		}

		if findFiledStats {
			break
		}
	}

	err = searchStream.Produce(fn(2))
	assert.NoError(t, err)
	searchResult = searchResultStream.Consume()
	assert.NotNil(t, searchResult)
	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
	assert.Nil(t, err)

	idsIndex := indexResult.IDs()
	idsSegment := unMarshaledHit.IDs
	assert.Equal(t, len(idsIndex), len(idsSegment))
	for i := 0; i < len(idsIndex); i++ {
		assert.Equal(t, idsIndex[i], idsSegment[i])
	}
	Params.SearchChannelNames = oldSearchChannelNames
	Params.SearchResultChannelNames = oldSearchResultChannelNames
	Params.LoadIndexChannelNames = oldLoadIndexChannelNames
	Params.StatsChannelName = oldStatsChannelName
	fmt.Println("loadIndex binaryVector test Done!")

	defer assert.Equal(t, findFiledStats, true)
	<-node.queryNodeLoopCtx.Done()
	node.Close()
}
