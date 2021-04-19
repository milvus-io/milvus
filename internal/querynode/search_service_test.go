package querynode

import (
	"context"
	"encoding/binary"
	"log"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

func TestSearch_Search(t *testing.T) {
	ctx := context.Background()

	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)

	pulsarURL := Params.PulsarAddress

	// test data generate
	const msgLength = 10
	const receiveBufSize = 1024
	const DIM = 16
	searchProducerChannels := Params.SearchChannelNames
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// start search service
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
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
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData1, searchRawData2},
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	query := milvuspb.SearchRequest{
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

	searchMsg := &msgstream.SearchMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SearchRequest: internalpb2.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     1,
				Timestamp: uint64(10 + 1000),
				SourceID:  1,
			},
			ResultChannelID: "0",
			Query:           &blob,
		},
	}

	msgPackSearch := msgstream.MsgPack{}
	msgPackSearch.Msgs = append(msgPackSearch.Msgs, searchMsg)

	msFactory := pulsarms.NewFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarURL,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	searchStream, _ := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	searchStream.AsProducer(searchProducerChannels)
	searchStream.Start()
	err = searchStream.Produce(ctx, &msgPackSearch)
	assert.NoError(t, err)

	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica, msFactory)
	go node.searchService.start()

	// start insert
	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		var rawData []byte
		for _, ele := range vec {
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
			rawData = append(rawData, buf...)
		}
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, 1)
		rawData = append(rawData, bs...)

		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i),
				},
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     int64(i),
					Timestamp: uint64(10 + 1000),
					SourceID:  0,
				},
				CollectionID: UniqueID(0),
				PartitionID:  defaultPartitionID,
				SegmentID:    int64(0),
				ChannelID:    "0",
				Timestamps:   []uint64{uint64(i + 1000)},
				RowIDs:       []int64{int64(i)},
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
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb2.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
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
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames

	insertStream, _ := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	insertStream.AsProducer(insertChannels)

	ddStream, _ := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	ddStream.AsProducer(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err = insertMsgStream.Produce(ctx, &msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Broadcast(ctx, &timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(ctx, &timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.replica, msFactory)
	go node.dataSyncService.start()

	time.Sleep(1 * time.Second)

	node.Stop()
}

func TestSearch_SearchMultiSegments(t *testing.T) {
	ctx := context.Background()
	pulsarURL := Params.PulsarAddress
	const receiveBufSize = 1024

	msFactory := pulsarms.NewFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarURL,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	assert.Nil(t, err)

	node := NewQueryNode(context.Background(), 0, msFactory)
	initTestMeta(t, node, 0, 0)

	// test data generate
	const msgLength = 10
	const DIM = 16
	searchProducerChannels := Params.SearchChannelNames
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// start search service
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
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
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData1, searchRawData2},
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	query := milvuspb.SearchRequest{
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

	searchMsg := &msgstream.SearchMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SearchRequest: internalpb2.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     1,
				Timestamp: uint64(10 + 1000),
				SourceID:  1,
			},
			ResultChannelID: "0",
			Query:           &blob,
		},
	}

	msgPackSearch := msgstream.MsgPack{}
	msgPackSearch.Msgs = append(msgPackSearch.Msgs, searchMsg)

	searchStream, _ := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	searchStream.AsProducer(searchProducerChannels)
	searchStream.Start()
	err = searchStream.Produce(ctx, &msgPackSearch)
	assert.NoError(t, err)

	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica, msFactory)
	go node.searchService.start()

	// start insert
	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		segmentID := 0
		if i >= msgLength/2 {
			segmentID = 1
		}
		var rawData []byte
		for _, ele := range vec {
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
			rawData = append(rawData, buf...)
		}
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, 1)
		rawData = append(rawData, bs...)

		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i),
				},
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     int64(i),
					Timestamp: uint64(i + 1000),
					SourceID:  0,
				},
				CollectionID: UniqueID(0),
				PartitionID:  defaultPartitionID,
				SegmentID:    int64(segmentID),
				ChannelID:    "0",
				Timestamps:   []uint64{uint64(i + 1000)},
				RowIDs:       []int64{int64(i)},
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
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb2.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
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
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames

	insertStream, _ := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	insertStream.AsProducer(insertChannels)

	ddStream, _ := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	ddStream.AsProducer(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err = insertMsgStream.Produce(ctx, &msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Broadcast(ctx, &timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(ctx, &timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.replica, msFactory)
	go node.dataSyncService.start()

	time.Sleep(1 * time.Second)

	node.Stop()
}
