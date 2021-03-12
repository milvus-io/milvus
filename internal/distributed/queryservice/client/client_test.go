package grpcqueryserviceclient

//import (
//	"context"
//	"encoding/binary"
//	"fmt"
//	"log"
//	"math"
//	"testing"
//	"time"
//
//	"github.com/golang/protobuf/proto"
//	"github.com/stretchr/testify/assert"
//
//	"github.com/zilliztech/milvus-distributed/internal/msgstream"
//	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
//	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
//	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
//	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
//	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
//	qs "github.com/zilliztech/milvus-distributed/internal/queryservice"
//)
//
//const (
//	debug = false
//	pulsarAddress = "pulsar://127.0.0.1:6650"
//)
//
//func TestClient_LoadCollection(t *testing.T) {
//	var ctx context.Context
//	if debug {
//		ctx = context.Background()
//	} else {
//		var cancel context.CancelFunc
//		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
//		defer cancel()
//	}
//
//	//create queryService client
//	qs.Params.Init()
//	log.Println("QueryService address:", qs.Params.Address)
//	log.Println("Init Query service client ...")
//	client, err := NewClient(qs.Params.Address, 20*time.Second)
//	assert.Nil(t, err)
//	err = client.Init()
//	assert.Nil(t, err)
//	err = client.Start()
//	assert.Nil(t, err)
//
//	insertChannels := []string{"insert-0", "insert-1", "insert-2", "insert-3"}
//	ddChannels := []string{"data-definition"}
//
//	factory := pulsarms.NewFactory(pulsarAddress, receiveBufSize, pulsarBufSize)
//	insertStream, _ := factory.NewTtMsgStream(ctx)
//	insertStream.AsProducer(insertChannels)
//	insertStream.Start()
//
//	ddStream, err := factory.NewTtMsgStream(ctx)
//	assert.NoError(t, err)
//	ddStream.AsProducer(ddChannels)
//	ddStream.Start()
//
//	// showCollection
//	showCollectionRequest := &querypb.ShowCollectionsRequest{
//		DbID: 0,
//	}
//	showCollectionRes, err := client.ShowCollections(showCollectionRequest)
//	fmt.Println("showCollectionRes: ", showCollectionRes)
//	assert.Nil(t, err)
//
//	//load collection
//	loadCollectionRequest := &querypb.LoadCollectionRequest{
//		CollectionID: 1,
//		Schema:       genSchema(1),
//	}
//	loadCollectionRes, err := client.LoadCollection(loadCollectionRequest)
//	fmt.Println("loadCollectionRes: ", loadCollectionRes)
//	assert.Nil(t, err)
//
//	// showCollection
//	showCollectionRes, err = client.ShowCollections(showCollectionRequest)
//	fmt.Println("showCollectionRes: ", showCollectionRes)
//	assert.Nil(t, err)
//
//	//showSegmentInfo
//	getSegmentInfoRequest := &querypb.SegmentInfoRequest{
//		SegmentIDs: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
//	}
//	getSegmentInfoRes, err := client.GetSegmentInfo(getSegmentInfoRequest)
//	fmt.Println("segment info : ", getSegmentInfoRes)
//	assert.Nil(t, err)
//
//	// insert msg
//	for i := 0; i < 12; i++ {
//		insertMsgPack, timeTickMsgPack := genInsert(1, 1, i*msgLength+1, 4, false)
//		err := insertStream.Produce(insertMsgPack)
//		assert.NoError(t, err)
//		err = insertStream.Broadcast(timeTickMsgPack)
//		assert.NoError(t, err)
//		err = ddStream.Broadcast(timeTickMsgPack)
//		assert.NoError(t, err)
//	}
//
//	getSegmentInfoRes, err = client.GetSegmentInfo(getSegmentInfoRequest)
//	assert.Nil(t, err)
//	fmt.Println("segment info : ", getSegmentInfoRes)
//
//}
//
//func TestClient_GetSegmentInfo(t *testing.T) {
//	if !debug {
//		_, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
//		defer cancel()
//	}
//
//	//create queryService client
//	qs.Params.Init()
//	log.Println("QueryService address:", qs.Params.Address)
//	log.Println("Init Query service client ...")
//	client, err := NewClient(qs.Params.Address, 20*time.Second)
//	assert.Nil(t, err)
//	err = client.Init()
//	assert.Nil(t, err)
//	err = client.Start()
//	assert.Nil(t, err)
//
//	//showSegmentInfo
//	getSegmentInfoRequest := &querypb.SegmentInfoRequest{
//		SegmentIDs: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
//	}
//	getSegmentInfoRes, err := client.GetSegmentInfo(getSegmentInfoRequest)
//	assert.Nil(t, err)
//	fmt.Println("segment info : ", getSegmentInfoRes)
//}
//
//func TestClient_LoadPartitions(t *testing.T) {
//	if !debug {
//		_, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
//		defer cancel()
//	}
//
//	//create queryService client
//	qs.Params.Init()
//	log.Println("QueryService address:", qs.Params.Address)
//	log.Println("Init Query service client ...")
//	client, err := NewClient(qs.Params.Address, 20*time.Second)
//	assert.Nil(t, err)
//	err = client.Init()
//	assert.Nil(t, err)
//	err = client.Start()
//	assert.Nil(t, err)
//
//	loadPartitionRequest := &querypb.LoadPartitionsRequest{
//		CollectionID: 1,
//		Schema: genSchema(1),
//	}
//	loadPartitionRes, err := client.LoadPartitions(loadPartitionRequest)
//	fmt.Println("loadCollectionRes: ", loadPartitionRes)
//	assert.Nil(t, err)
//}
//
//func TestClient_GetChannels(t *testing.T) {
//	if !debug {
//		_, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
//		defer cancel()
//	}
//
//	//create queryService client
//	qs.Params.Init()
//	log.Println("QueryService address:", qs.Params.Address)
//	log.Println("Init Query service client ...")
//	client, err := NewClient(qs.Params.Address, 20*time.Second)
//	assert.Nil(t, err)
//	err = client.Init()
//	assert.Nil(t, err)
//	err = client.Start()
//	assert.Nil(t, err)
//
//	getTimeTickChannelRes, err := client.GetTimeTickChannel()
//	fmt.Println("loadCollectionRes: ", getTimeTickChannelRes)
//	assert.Nil(t, err)
//}
//
//func sendSearchRequest(ctx context.Context, searchChannels []string) {
//	// test data generate
//	const msgLength = 10
//	const receiveBufSize = 1024
//	const DIM = 16
//
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	// start search service
//	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
//	var searchRawData1 []byte
//	var searchRawData2 []byte
//	for i, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
//		searchRawData1 = append(searchRawData1, buf...)
//	}
//	for i, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*4)))
//		searchRawData2 = append(searchRawData2, buf...)
//	}
//	placeholderValue := milvuspb.PlaceholderValue{
//		Tag:    "$0",
//		Type:   milvuspb.PlaceholderType_VECTOR_FLOAT,
//		Values: [][]byte{searchRawData1, searchRawData2},
//	}
//
//	placeholderGroup := milvuspb.PlaceholderGroup{
//		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
//	}
//
//	placeGroupByte, err := proto.Marshal(&placeholderGroup)
//	if err != nil {
//		log.Print("marshal placeholderGroup failed")
//	}
//
//	query := milvuspb.SearchRequest{
//		Dsl:              dslString,
//		PlaceholderGroup: placeGroupByte,
//	}
//
//	queryByte, err := proto.Marshal(&query)
//	if err != nil {
//		log.Print("marshal query failed")
//	}
//
//	blob := commonpb.Blob{
//		Value: queryByte,
//	}
//
//	searchMsg := &msgstream.SearchMsg{
//		BaseMsg: msgstream.BaseMsg{
//			HashValues: []uint32{0},
//		},
//		SearchRequest: internalpb.SearchRequest{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_kSearch,
//				MsgID:     1,
//				Timestamp: uint64(10 + 1000),
//				SourceID:  1,
//			},
//			ResultChannelID: "0",
//			Query:           &blob,
//		},
//	}
//
//	msgPackSearch := msgstream.MsgPack{}
//	msgPackSearch.Msgs = append(msgPackSearch.Msgs, searchMsg)
//
//	factory := pulsarms.NewFactory(pulsarAddress, receiveBufSize, 1024)
//	searchStream, _ := factory.NewMsgStream(ctx)
//	searchStream.AsProducer(searchChannels)
//	searchStream.Start()
//	err = searchStream.Produce(&msgPackSearch)
//	if err != nil {
//		panic(err)
//	}
//}
