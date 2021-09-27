// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"path"
	"strconv"

	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

//func TestLoadService_LoadIndex_FloatVector(t *testing.T) {
//	node := newQueryNodeMock()
//	collectionID := rand.Int63n(1000000)
//	segmentID := rand.Int63n(1000000)
//	initTestMeta(t, node, "collection0", collectionID, segmentID)
//
//	// loadService and statsService
//	suffix := "-test-search" + strconv.FormatInt(rand.Int63n(1000000), 10)
//	oldSearchChannelNames := Params.SearchChannelNames
//	newSearchChannelNames := makeNewChannelNames(oldSearchChannelNames, suffix)
//	Params.SearchChannelNames = newSearchChannelNames
//
//	oldSearchResultChannelNames := Params.SearchChannelNames
//	newSearchResultChannelNames := makeNewChannelNames(oldSearchResultChannelNames, suffix)
//	Params.SearchResultChannelNames = newSearchResultChannelNames
//
//	oldLoadIndexChannelNames := Params.LoadIndexChannelNames
//	newLoadIndexChannelNames := makeNewChannelNames(oldLoadIndexChannelNames, suffix)
//	Params.LoadIndexChannelNames = newLoadIndexChannelNames
//
//	oldStatsChannelName := Params.StatsChannelName
//	newStatsChannelNames := makeNewChannelNames([]string{oldStatsChannelName}, suffix)
//	Params.StatsChannelName = newStatsChannelNames[0]
//	go node.Start()
//
//	//generate insert data
//	const msgLength = 1000
//	const receiveBufSize = 1024
//	const DIM = 16
//	var insertRowBlob []*commonpb.Blob
//	var timestamps []uint64
//	var rowIDs []int64
//	var hashValues []uint32
//	for n := 0; n < msgLength; n++ {
//		rowData := make([]byte, 0)
//		for i := 0; i < DIM; i++ {
//			vec := make([]byte, 4)
//			binary.LittleEndian.PutUint32(vec, math.Float32bits(float32(n*i)))
//			rowData = append(rowData, vec...)
//		}
//		age := make([]byte, 4)
//		binary.LittleEndian.PutUint32(age, 1)
//		rowData = append(rowData, age...)
//		blob := &commonpb.Blob{
//			Value: rowData,
//		}
//		insertRowBlob = append(insertRowBlob, blob)
//		timestamps = append(timestamps, uint64(n))
//		rowIDs = append(rowIDs, int64(n))
//		hashValues = append(hashValues, uint32(n))
//	}
//
//	var insertMsg msgstream.TsMsg = &msgstream.InsertMsg{
//		BaseMsg: msgstream.BaseMsg{
//			HashValues: hashValues,
//		},
//		InsertRequest: internalpb.InsertRequest{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_kInsert,
//				MsgID:     0,
//				Timestamp: timestamps[0],
//				SourceID:  0,
//			},
//			CollectionID: UniqueID(collectionID),
//			PartitionID:  defaultPartitionID,
//			SegmentID:      segmentID,
//			ChannelID:      "0",
//			Timestamps:     timestamps,
//			RowIDs:         rowIDs,
//			RowData:        insertRowBlob,
//		},
//	}
//	insertMsgPack := msgstream.MsgPack{
//		BeginTs: 0,
//		EndTs:   math.MaxUint64,
//		Msgs:    []msgstream.TsMsg{insertMsg},
//	}
//
//	// generate timeTick
//	timeTickMsg := &msgstream.TimeTickMsg{
//		BaseMsg: msgstream.BaseMsg{
//			BeginTimestamp: 0,
//			EndTimestamp:   0,
//			HashValues:     []uint32{0},
//		},
//		TimeTickMsg: internalpb.TimeTickMsg{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_kTimeTick,
//				MsgID:     0,
//				Timestamp: math.MaxUint64,
//				SourceID:  0,
//			},
//		},
//	}
//	timeTickMsgPack := &msgstream.MsgPack{
//		Msgs: []msgstream.TsMsg{timeTickMsg},
//	}
//
//	// pulsar produce
//	insertChannels := Params.InsertChannelNames
//	ddChannels := Params.DDChannelNames
//
//	insertStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	insertStream.SetPulsarClient(Params.PulsarAddress)
//	insertStream.AsProducer(insertChannels)
//	ddStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	ddStream.SetPulsarClient(Params.PulsarAddress)
//	ddStream.AsProducer(ddChannels)
//
//	var insertMsgStream msgstream.MsgStream = insertStream
//	insertMsgStream.Start()
//	var ddMsgStream msgstream.MsgStream = ddStream
//	ddMsgStream.Start()
//
//	err := insertMsgStream.Produce(&insertMsgPack)
//	assert.NoError(t, err)
//	err = insertMsgStream.Broadcast(timeTickMsgPack)
//	assert.NoError(t, err)
//	err = ddMsgStream.Broadcast(timeTickMsgPack)
//	assert.NoError(t, err)
//
//	// generator searchRowData
//	var searchRowData []float32
//	for i := 0; i < DIM; i++ {
//		searchRowData = append(searchRowData, float32(42*i))
//	}
//
//	//generate search data and send search msg
//	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
//	var searchRowByteData []byte
//	for i := range searchRowData {
//		vec := make([]byte, 4)
//		binary.LittleEndian.PutUint32(vec, math.Float32bits(searchRowData[i]))
//		searchRowByteData = append(searchRowByteData, vec...)
//	}
//	placeholderValue := milvuspb.PlaceholderValue{
//		Tag:    "$0",
//		Type:   milvuspb.PlaceholderType_VectorFloat,
//		Values: [][]byte{searchRowByteData},
//	}
//	placeholderGroup := milvuspb.searchRequest{
//		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
//	}
//	placeGroupByte, err := proto.Marshal(&placeholderGroup)
//	if err != nil {
//		log.Print("marshal placeholderGroup failed")
//	}
//	query := milvuspb.SearchRequest{
//		Dsl:              dslString,
//		searchRequest: placeGroupByte,
//	}
//	queryByte, err := proto.Marshal(&query)
//	if err != nil {
//		log.Print("marshal query failed")
//	}
//	blob := commonpb.Blob{
//		Value: queryByte,
//	}
//	fn := func(n int64) *msgstream.MsgPack {
//		searchMsg := &msgstream.SearchMsg{
//			BaseMsg: msgstream.BaseMsg{
//				HashValues: []uint32{0},
//			},
//			SearchRequest: internalpb.SearchRequest{
//				Base: &commonpb.MsgBase{
//					MsgType:   commonpb.MsgType_kSearch,
//					MsgID:     n,
//					Timestamp: uint64(msgLength),
//					SourceID:  1,
//				},
//				ResultChannelID: "0",
//				Query:           &blob,
//			},
//		}
//		return &msgstream.MsgPack{
//			Msgs: []msgstream.TsMsg{searchMsg},
//		}
//	}
//	searchStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	searchStream.SetPulsarClient(Params.PulsarAddress)
//	searchStream.AsProducer(newSearchChannelNames)
//	searchStream.Start()
//	err = searchStream.Produce(fn(1))
//	assert.NoError(t, err)
//
//	//get search result
//	searchResultStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	searchResultStream.SetPulsarClient(Params.PulsarAddress)
//	unmarshalDispatcher := util.NewUnmarshalDispatcher()
//	searchResultStream.AsConsumer(newSearchResultChannelNames, "loadIndexTestSubSearchResult", unmarshalDispatcher, receiveBufSize)
//	searchResultStream.Start()
//	searchResult := searchResultStream.Consume()
//	assert.NotNil(t, searchResult)
//	unMarshaledHit := milvuspb.Hits{}
//	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
//	assert.Nil(t, err)
//
//	// gen load index message pack
//	indexParams := make(map[string]string)
//	indexParams["index_type"] = "IVF_PQ"
//	indexParams["index_mode"] = "cpu"
//	indexParams["dim"] = "16"
//	indexParams["k"] = "10"
//	indexParams["nlist"] = "100"
//	indexParams["nprobe"] = "10"
//	indexParams["m"] = "4"
//	indexParams["nbits"] = "8"
//	indexParams["metric_type"] = "L2"
//	indexParams["SLICE_SIZE"] = "4"
//
//	var indexParamsKV []*commonpb.KeyValuePair
//	for key, value := range indexParams {
//		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
//			Key:   key,
//			Value: value,
//		})
//	}
//
//	// generator index
//	typeParams := make(map[string]string)
//	typeParams["dim"] = "16"
//	var indexRowData []float32
//	for n := 0; n < msgLength; n++ {
//		for i := 0; i < DIM; i++ {
//			indexRowData = append(indexRowData, float32(n*i))
//		}
//	}
//	index, err := indexnode.NewCIndex(typeParams, indexParams)
//	assert.Nil(t, err)
//	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
//	assert.Equal(t, err, nil)
//
//	option := &minioKV.Option{
//		Address:           Params.MinioEndPoint,
//		AccessKeyID:       Params.MinioAccessKeyID,
//		SecretAccessKeyID: Params.MinioSecretAccessKey,
//		UseSSL:            Params.MinioUseSSLStr,
//		BucketName:        Params.MinioBucketName,
//		CreateBucket:      true,
//	}
//
//	minioKV, err := minioKV.NewMinIOKV(node.queryNodeLoopCtx, option)
//	assert.Equal(t, err, nil)
//	//save index to minio
//	binarySet, err := index.Serialize()
//	assert.Equal(t, err, nil)
//	indexPaths := make([]string, 0)
//	var indexCodec storage.IndexCodec
//	binarySet, err = indexCodec.Serialize(binarySet, indexParams)
//	assert.NoError(t, err)
//	for _, index := range binarySet {
//		path := strconv.Itoa(int(segmentID)) + "/" + index.Key
//		indexPaths = append(indexPaths, path)
//		minioKV.Save(path, string(index.Value))
//	}
//
//	//test index search result
//	indexResult, err := index.QueryOnFloatVecIndexWithParam(searchRowData, indexParams)
//	assert.Equal(t, err, nil)
//
//	// create loadIndexClient
//	fieldID := UniqueID(100)
//	loadIndexChannelNames := Params.LoadIndexChannelNames
//	client := client.NewQueryNodeClient(node.queryNodeLoopCtx, Params.PulsarAddress, loadIndexChannelNames)
//	client.LoadIndex(indexPaths, segmentID, fieldID, "vec", indexParams)
//
//	// init message stream consumer and do checks
//	statsMs := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.StatsReceiveBufSize)
//	statsMs.SetPulsarClient(Params.PulsarAddress)
//	statsMs.AsConsumer([]string{Params.StatsChannelName}, Params.MsgChannelSubName, util.NewUnmarshalDispatcher(), Params.StatsReceiveBufSize)
//	statsMs.Start()
//
//	findFiledStats := false
//	for {
//		receiveMsg := msgstream.MsgStream(statsMs).Consume()
//		assert.NotNil(t, receiveMsg)
//		assert.NotEqual(t, len(receiveMsg.Msgs), 0)
//
//		for _, msg := range receiveMsg.Msgs {
//			statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
//			if statsMsg.FieldStats == nil || len(statsMsg.FieldStats) == 0 {
//				continue
//			}
//			findFiledStats = true
//			assert.Equal(t, ok, true)
//			assert.Equal(t, len(statsMsg.FieldStats), 1)
//			fieldStats0 := statsMsg.FieldStats[0]
//			assert.Equal(t, fieldStats0.FieldID, fieldID)
//			assert.Equal(t, fieldStats0.CollectionID, collectionID)
//			assert.Equal(t, len(fieldStats0.IndexStats), 1)
//			indexStats0 := fieldStats0.IndexStats[0]
//			params := indexStats0.IndexParams
//			// sort index params by key
//			sort.Slice(indexParamsKV, func(i, j int) bool { return indexParamsKV[i].Key < indexParamsKV[j].Key })
//			indexEqual := node.loadService.indexParamsEqual(params, indexParamsKV)
//			assert.Equal(t, indexEqual, true)
//		}
//
//		if findFiledStats {
//			break
//		}
//	}
//
//	err = searchStream.Produce(fn(2))
//	assert.NoError(t, err)
//	searchResult = searchResultStream.Consume()
//	assert.NotNil(t, searchResult)
//	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
//	assert.Nil(t, err)
//
//	idsIndex := indexResult.IDs()
//	idsSegment := unMarshaledHit.IDs
//	assert.Equal(t, len(idsIndex), len(idsSegment))
//	for i := 0; i < len(idsIndex); i++ {
//		assert.Equal(t, idsIndex[i], idsSegment[i])
//	}
//	Params.SearchChannelNames = oldSearchChannelNames
//	Params.SearchResultChannelNames = oldSearchResultChannelNames
//	Params.LoadIndexChannelNames = oldLoadIndexChannelNames
//	Params.StatsChannelName = oldStatsChannelName
//	fmt.Println("loadIndex floatVector test Done!")
//
//	defer assert.Equal(t, findFiledStats, true)
//	<-node.queryNodeLoopCtx.Done()
//	node.Stop()
//}
//
//func TestLoadService_LoadIndex_BinaryVector(t *testing.T) {
//	node := newQueryNodeMock()
//	collectionID := rand.Int63n(1000000)
//	segmentID := rand.Int63n(1000000)
//	initTestMeta(t, node, "collection0", collectionID, segmentID, true)
//
//	// loadService and statsService
//	suffix := "-test-search-binary" + strconv.FormatInt(rand.Int63n(1000000), 10)
//	oldSearchChannelNames := Params.SearchChannelNames
//	newSearchChannelNames := makeNewChannelNames(oldSearchChannelNames, suffix)
//	Params.SearchChannelNames = newSearchChannelNames
//
//	oldSearchResultChannelNames := Params.SearchChannelNames
//	newSearchResultChannelNames := makeNewChannelNames(oldSearchResultChannelNames, suffix)
//	Params.SearchResultChannelNames = newSearchResultChannelNames
//
//	oldLoadIndexChannelNames := Params.LoadIndexChannelNames
//	newLoadIndexChannelNames := makeNewChannelNames(oldLoadIndexChannelNames, suffix)
//	Params.LoadIndexChannelNames = newLoadIndexChannelNames
//
//	oldStatsChannelName := Params.StatsChannelName
//	newStatsChannelNames := makeNewChannelNames([]string{oldStatsChannelName}, suffix)
//	Params.StatsChannelName = newStatsChannelNames[0]
//	go node.Start()
//
//	const msgLength = 1000
//	const receiveBufSize = 1024
//	const DIM = 128
//
//	// generator index data
//	var indexRowData []byte
//	for n := 0; n < msgLength; n++ {
//		for i := 0; i < DIM/8; i++ {
//			indexRowData = append(indexRowData, byte(rand.Intn(8)))
//		}
//	}
//
//	//generator insert data
//	var insertRowBlob []*commonpb.Blob
//	var timestamps []uint64
//	var rowIDs []int64
//	var hashValues []uint32
//	offset := 0
//	for n := 0; n < msgLength; n++ {
//		rowData := make([]byte, 0)
//		rowData = append(rowData, indexRowData[offset:offset+(DIM/8)]...)
//		offset += DIM / 8
//		age := make([]byte, 4)
//		binary.LittleEndian.PutUint32(age, 1)
//		rowData = append(rowData, age...)
//		blob := &commonpb.Blob{
//			Value: rowData,
//		}
//		insertRowBlob = append(insertRowBlob, blob)
//		timestamps = append(timestamps, uint64(n))
//		rowIDs = append(rowIDs, int64(n))
//		hashValues = append(hashValues, uint32(n))
//	}
//
//	var insertMsg msgstream.TsMsg = &msgstream.InsertMsg{
//		BaseMsg: msgstream.BaseMsg{
//			HashValues: hashValues,
//		},
//		InsertRequest: internalpb.InsertRequest{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_kInsert,
//				MsgID:     0,
//				Timestamp: timestamps[0],
//				SourceID:  0,
//			},
//			CollectionID: UniqueID(collectionID),
//			PartitionID:  defaultPartitionID,
//			SegmentID:      segmentID,
//			ChannelID:      "0",
//			Timestamps:     timestamps,
//			RowIDs:         rowIDs,
//			RowData:        insertRowBlob,
//		},
//	}
//	insertMsgPack := msgstream.MsgPack{
//		BeginTs: 0,
//		EndTs:   math.MaxUint64,
//		Msgs:    []msgstream.TsMsg{insertMsg},
//	}
//
//	// generate timeTick
//	timeTickMsg := &msgstream.TimeTickMsg{
//		BaseMsg: msgstream.BaseMsg{
//			BeginTimestamp: 0,
//			EndTimestamp:   0,
//			HashValues:     []uint32{0},
//		},
//		TimeTickMsg: internalpb.TimeTickMsg{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_kTimeTick,
//				MsgID:     0,
//				Timestamp: math.MaxUint64,
//				SourceID:  0,
//			},
//		},
//	}
//	timeTickMsgPack := &msgstream.MsgPack{
//		Msgs: []msgstream.TsMsg{timeTickMsg},
//	}
//
//	// pulsar produce
//	insertChannels := Params.InsertChannelNames
//	ddChannels := Params.DDChannelNames
//
//	insertStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	insertStream.SetPulsarClient(Params.PulsarAddress)
//	insertStream.AsProducer(insertChannels)
//	ddStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	ddStream.SetPulsarClient(Params.PulsarAddress)
//	ddStream.AsProducer(ddChannels)
//
//	var insertMsgStream msgstream.MsgStream = insertStream
//	insertMsgStream.Start()
//	var ddMsgStream msgstream.MsgStream = ddStream
//	ddMsgStream.Start()
//
//	err := insertMsgStream.Produce(&insertMsgPack)
//	assert.NoError(t, err)
//	err = insertMsgStream.Broadcast(timeTickMsgPack)
//	assert.NoError(t, err)
//	err = ddMsgStream.Broadcast(timeTickMsgPack)
//	assert.NoError(t, err)
//
//	//generate search data and send search msg
//	searchRowData := indexRowData[42*(DIM/8) : 43*(DIM/8)]
//	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"JACCARD\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
//	placeholderValue := milvuspb.PlaceholderValue{
//		Tag:    "$0",
//		Type:   milvuspb.PlaceholderType_VectorBinary,
//		Values: [][]byte{searchRowData},
//	}
//	placeholderGroup := milvuspb.searchRequest{
//		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
//	}
//	placeGroupByte, err := proto.Marshal(&placeholderGroup)
//	if err != nil {
//		log.Print("marshal placeholderGroup failed")
//	}
//	query := milvuspb.SearchRequest{
//		Dsl:              dslString,
//		searchRequest: placeGroupByte,
//	}
//	queryByte, err := proto.Marshal(&query)
//	if err != nil {
//		log.Print("marshal query failed")
//	}
//	blob := commonpb.Blob{
//		Value: queryByte,
//	}
//	fn := func(n int64) *msgstream.MsgPack {
//		searchMsg := &msgstream.SearchMsg{
//			BaseMsg: msgstream.BaseMsg{
//				HashValues: []uint32{0},
//			},
//			SearchRequest: internalpb.SearchRequest{
//				Base: &commonpb.MsgBase{
//					MsgType:   commonpb.MsgType_kSearch,
//					MsgID:     n,
//					Timestamp: uint64(msgLength),
//					SourceID:  1,
//				},
//				ResultChannelID: "0",
//				Query:           &blob,
//			},
//		}
//		return &msgstream.MsgPack{
//			Msgs: []msgstream.TsMsg{searchMsg},
//		}
//	}
//	searchStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	searchStream.SetPulsarClient(Params.PulsarAddress)
//	searchStream.AsProducer(newSearchChannelNames)
//	searchStream.Start()
//	err = searchStream.Produce(fn(1))
//	assert.NoError(t, err)
//
//	//get search result
//	searchResultStream := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
//	searchResultStream.SetPulsarClient(Params.PulsarAddress)
//	unmarshalDispatcher := util.NewUnmarshalDispatcher()
//	searchResultStream.AsConsumer(newSearchResultChannelNames, "loadIndexTestSubSearchResult2", unmarshalDispatcher, receiveBufSize)
//	searchResultStream.Start()
//	searchResult := searchResultStream.Consume()
//	assert.NotNil(t, searchResult)
//	unMarshaledHit := milvuspb.Hits{}
//	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
//	assert.Nil(t, err)
//
//	// gen load index message pack
//	indexParams := make(map[string]string)
//	indexParams["index_type"] = "BIN_IVF_FLAT"
//	indexParams["index_mode"] = "cpu"
//	indexParams["dim"] = "128"
//	indexParams["k"] = "10"
//	indexParams["nlist"] = "100"
//	indexParams["nprobe"] = "10"
//	indexParams["m"] = "4"
//	indexParams["nbits"] = "8"
//	indexParams["metric_type"] = "JACCARD"
//	indexParams["SLICE_SIZE"] = "4"
//
//	var indexParamsKV []*commonpb.KeyValuePair
//	for key, value := range indexParams {
//		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
//			Key:   key,
//			Value: value,
//		})
//	}
//
//	// generator index
//	typeParams := make(map[string]string)
//	typeParams["dim"] = "128"
//	index, err := indexnode.NewCIndex(typeParams, indexParams)
//	assert.Nil(t, err)
//	err = index.BuildBinaryVecIndexWithoutIds(indexRowData)
//	assert.Equal(t, err, nil)
//
//	option := &minioKV.Option{
//		Address:           Params.MinioEndPoint,
//		AccessKeyID:       Params.MinioAccessKeyID,
//		SecretAccessKeyID: Params.MinioSecretAccessKey,
//		UseSSL:            Params.MinioUseSSLStr,
//		BucketName:        Params.MinioBucketName,
//		CreateBucket:      true,
//	}
//
//	minioKV, err := minioKV.NewMinIOKV(node.queryNodeLoopCtx, option)
//	assert.Equal(t, err, nil)
//	//save index to minio
//	binarySet, err := index.Serialize()
//	assert.Equal(t, err, nil)
//	var indexCodec storage.IndexCodec
//	binarySet, err = indexCodec.Serialize(binarySet, indexParams)
//	assert.NoError(t, err)
//	indexPaths := make([]string, 0)
//	for _, index := range binarySet {
//		path := strconv.Itoa(int(segmentID)) + "/" + index.Key
//		indexPaths = append(indexPaths, path)
//		minioKV.Save(path, string(index.Value))
//	}
//
//	//test index search result
//	indexResult, err := index.QueryOnBinaryVecIndexWithParam(searchRowData, indexParams)
//	assert.Equal(t, err, nil)
//
//	// create loadIndexClient
//	fieldID := UniqueID(100)
//	loadIndexChannelNames := Params.LoadIndexChannelNames
//	client := client.NewQueryNodeClient(node.queryNodeLoopCtx, Params.PulsarAddress, loadIndexChannelNames)
//	client.LoadIndex(indexPaths, segmentID, fieldID, "vec", indexParams)
//
//	// init message stream consumer and do checks
//	statsMs := pulsarms.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.StatsReceiveBufSize)
//	statsMs.SetPulsarClient(Params.PulsarAddress)
//	statsMs.AsConsumer([]string{Params.StatsChannelName}, Params.MsgChannelSubName, util.NewUnmarshalDispatcher(), Params.StatsReceiveBufSize)
//	statsMs.Start()
//
//	findFiledStats := false
//	for {
//		receiveMsg := msgstream.MsgStream(statsMs).Consume()
//		assert.NotNil(t, receiveMsg)
//		assert.NotEqual(t, len(receiveMsg.Msgs), 0)
//
//		for _, msg := range receiveMsg.Msgs {
//			statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
//			if statsMsg.FieldStats == nil || len(statsMsg.FieldStats) == 0 {
//				continue
//			}
//			findFiledStats = true
//			assert.Equal(t, ok, true)
//			assert.Equal(t, len(statsMsg.FieldStats), 1)
//			fieldStats0 := statsMsg.FieldStats[0]
//			assert.Equal(t, fieldStats0.FieldID, fieldID)
//			assert.Equal(t, fieldStats0.CollectionID, collectionID)
//			assert.Equal(t, len(fieldStats0.IndexStats), 1)
//			indexStats0 := fieldStats0.IndexStats[0]
//			params := indexStats0.IndexParams
//			// sort index params by key
//			sort.Slice(indexParamsKV, func(i, j int) bool { return indexParamsKV[i].Key < indexParamsKV[j].Key })
//			indexEqual := node.loadService.indexParamsEqual(params, indexParamsKV)
//			assert.Equal(t, indexEqual, true)
//		}
//
//		if findFiledStats {
//			break
//		}
//	}
//
//	err = searchStream.Produce(fn(2))
//	assert.NoError(t, err)
//	searchResult = searchResultStream.Consume()
//	assert.NotNil(t, searchResult)
//	err = proto.Unmarshal(searchResult.Msgs[0].(*msgstream.SearchResultMsg).Hits[0], &unMarshaledHit)
//	assert.Nil(t, err)
//
//	idsIndex := indexResult.IDs()
//	idsSegment := unMarshaledHit.IDs
//	assert.Equal(t, len(idsIndex), len(idsSegment))
//	for i := 0; i < len(idsIndex); i++ {
//		assert.Equal(t, idsIndex[i], idsSegment[i])
//	}
//	Params.SearchChannelNames = oldSearchChannelNames
//	Params.SearchResultChannelNames = oldSearchResultChannelNames
//	Params.LoadIndexChannelNames = oldLoadIndexChannelNames
//	Params.StatsChannelName = oldStatsChannelName
//	fmt.Println("loadIndex binaryVector test Done!")
//
//	defer assert.Equal(t, findFiledStats, true)
//	<-node.queryNodeLoopCtx.Done()
//	node.Stop()
//}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
func genETCDCollectionMeta(collectionID UniqueID, isBinary bool) *etcdpb.CollectionMeta {
	var fieldVec schemapb.FieldSchema
	if isBinary {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(100),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_BinaryVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "metric_type",
					Value: "JACCARD",
				},
			},
		}
	} else {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(100),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_FloatVector,
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
	}

	fieldInt := schemapb.FieldSchema{
		FieldID:      UniqueID(101),
		Name:         "age",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_Int32,
	}

	schema := schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       &schema,
		CreateTime:   Timestamp(0),
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	return &collectionMeta
}

func generateInsertBinLog(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, keyPrefix string) ([]*internalpb.StringList, []int64, error) {
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
				NumRows: []int64{msgLength},
				Data:    idData,
			},
			1: &storage.Int64FieldData{
				NumRows: []int64{msgLength},
				Data:    timestamps,
			},
			100: &storage.FloatVectorFieldData{
				NumRows: []int64{msgLength},
				Data:    fieldVecData,
				Dim:     DIM,
			},
			101: &storage.Int32FieldData{
				NumRows: []int64{msgLength},
				Data:    fieldAgeData,
			},
		},
	}

	// buffer data to binLogs
	collMeta := genETCDCollectionMeta(collectionID, false)
	collMeta.Schema.Fields = append(collMeta.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  0,
		Name:     "uid",
		DataType: schemapb.DataType_Int64,
	})
	collMeta.Schema.Fields = append(collMeta.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "timestamp",
		DataType: schemapb.DataType_Int64,
	})
	inCodec := storage.NewInsertCodec(collMeta)
	binLogs, _, err := inCodec.Serialize(partitionID, segmentID, insertData)

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

	paths := make([]*internalpb.StringList, 0)
	fieldIDs := make([]int64, 0)
	fmt.Println(".. saving binlog to MinIO ...", len(binLogs))
	for _, blob := range binLogs {
		uid := rand.Int63n(100000000)
		key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
		err = kv.Save(key, string(blob.Value[:]))
		if err != nil {
			return nil, nil, err
		}
		paths = append(paths, &internalpb.StringList{
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

func doInsert(ctx context.Context, collectionID UniqueID, partitionID UniqueID, segmentID UniqueID) error {
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

	// messages generate
	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i),
				},
			},
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     0,
					Timestamp: uint64(i + 1000),
					SourceID:  0,
				},
				CollectionID: collectionID,
				PartitionID:  partitionID,
				SegmentID:    segmentID,
				ShardName:    "0",
				Timestamps:   []uint64{uint64(i + 1000)},
				RowIDs:       []int64{int64(i)},
				RowData: []*commonpb.Blob{
					{Value: rawData},
				},
			},
		}
		insertMessages = append(insertMessages, msg)
	}

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 1000,
		EndTimestamp:   1500,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
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

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	if err != nil {
		return err
	}

	return nil
}

//
//func TestSegmentLoad_Search_Vector(t *testing.T) {
//	collectionID := UniqueID(0)
//	partitionID := UniqueID(1)
//	segmentID := UniqueID(2)
//	fieldIDs := []int64{0, 101}
//
//	// mock write insert bin log
//	keyPrefix := path.Join("query-node-seg-manager-test-minio-prefix", strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10))
//
//	node := newQueryNodeMock()
//	defer node.Stop()
//
//	ctx := node.queryNodeLoopCtx
//	node.historical.loadService = newLoadService(ctx, nil, nil, nil, node.historical.replica)
//
//	initTestMeta(t, node, collectionID, 0)
//
//	err := node.historical.replica.addPartition(collectionID, partitionID)
//	assert.NoError(t, err)
//
//	err = node.historical.replica.addSegment(segmentID, partitionID, collectionID, segmentTypeSealed)
//	assert.NoError(t, err)
//
//	paths, srcFieldIDs, err := generateInsertBinLog(collectionID, partitionID, segmentID, keyPrefix)
//	assert.NoError(t, err)
//
//	fieldsMap, _ := node.historical.loadService.segLoader.checkTargetFields(paths, srcFieldIDs, fieldIDs)
//	assert.Equal(t, len(fieldsMap), 4)
//
//	segment, err := node.historical.replica.getSegmentByID(segmentID)
//	assert.NoError(t, err)
//
//	err = node.historical.loadService.segLoader.loadSegmentFieldsData(segment, fieldsMap)
//	assert.NoError(t, err)
//
//	indexPaths, err := generateIndex(segmentID)
//	assert.NoError(t, err)
//
//	indexInfo := &indexInfo{
//		indexPaths: indexPaths,
//		readyLoad:  true,
//	}
//	err = segment.setIndexInfo(100, indexInfo)
//	assert.NoError(t, err)
//
//	err = node.historical.loadService.segLoader.indexLoader.loadIndex(segment, 100)
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
//		Type:   milvuspb.PlaceholderType_FloatVector,
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
//	searchTimestamp := Timestamp(1020)
//	collection, err := node.historical.replica.getCollectionByID(collectionID)
//	assert.NoError(t, err)
//	plan, err := createPlan(*collection, dslString)
//	assert.NoError(t, err)
//	holder, err := parseSearchRequest(plan, placeHolderGroupBlob)
//	assert.NoError(t, err)
//	placeholderGroups := make([]*searchRequest, 0)
//	placeholderGroups = append(placeholderGroups, holder)
//
//	// wait for segment building index
//	time.Sleep(1 * time.Second)
//
//	_, err = segment.segmentSearch(plan, placeholderGroups, []Timestamp{searchTimestamp})
//	assert.Nil(t, err)
//
//	plan.delete()
//	holder.delete()
//
//	<-ctx.Done()
//}
