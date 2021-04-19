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
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

func TestSearch_Search(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())

	// init query node
	pulsarURL, _ := Params.pulsarAddress()
	node := NewQueryNode(ctx, 0)

	// init meta
	collectionName := "collection0"
	fieldVec := schemapb.FieldSchema{
		Name:         "vec",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:         "age",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).addPartition(collection.ID(), collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	segmentID := UniqueID(0)
	err = (*node.replica).addSegment(segmentID, collectionMeta.PartitionTags[0], UniqueID(0))
	assert.NoError(t, err)

	// test data generate
	const msgLength = 10
	const receiveBufSize = 1024
	const DIM = 16
	insertProducerChannels := Params.insertChannelNames()
	searchProducerChannels := Params.searchChannelNames()
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
	placeholderValue := servicepb.PlaceholderValue{
		Tag:    "$0",
		Type:   servicepb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData1, searchRawData2},
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

	searchMsg := &msgstream.SearchMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SearchRequest: internalpb.SearchRequest{
			MsgType:         internalpb.MsgType_kSearch,
			ReqID:           int64(1),
			ProxyID:         int64(1),
			Timestamp:       uint64(10 + 1000),
			ResultChannelID: int64(0),
			Query:           &blob,
		},
	}

	msgPackSearch := msgstream.MsgPack{}
	msgPackSearch.Msgs = append(msgPackSearch.Msgs, searchMsg)

	searchStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchStream.SetPulsarClient(pulsarURL)
	searchStream.CreatePulsarProducers(searchProducerChannels)
	searchStream.Start()
	err = searchStream.Produce(&msgPackSearch)
	assert.NoError(t, err)

	node.searchService = newSearchService(node.ctx, node.replica)
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
			InsertRequest: internalpb.InsertRequest{
				MsgType:        internalpb.MsgType_kInsert,
				ReqID:          int64(i),
				CollectionName: "collection0",
				PartitionTag:   "default",
				SegmentID:      int64(0),
				ChannelID:      int64(0),
				ProxyID:        int64(0),
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
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb.TimeTickMsg{
		MsgType:   internalpb.MsgType_kTimeTick,
		PeerID:    UniqueID(0),
		Timestamp: math.MaxUint64,
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(insertProducerChannels)
	insertStream.Start()
	err = insertStream.Produce(&msgPack)
	assert.NoError(t, err)
	err = insertStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	node.dataSyncService = newDataSyncService(node.ctx, node.replica)
	go node.dataSyncService.start()

	time.Sleep(1 * time.Second)

	cancel()
	node.Close()
}
