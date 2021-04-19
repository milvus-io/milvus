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
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: collectionName,
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
	const DIM = 16
	const N = 10

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	// messages generate
	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []int32{
					int32(i), int32(i),
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
				Timestamps:     []uint64{uint64(i + 1000), uint64(i + 1000)},
				RowIDs:         []int64{int64(i * 2), int64(i*2 + 1)},
				RowData: []*commonpb.Blob{
					{Value: rawData},
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

	// pulsar produce
	const receiveBufSize = 1024
	insertProducerChannels := []string{"insert"}

	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(insertProducerChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()
	err = insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	// dataSync
	node.dataSyncService = newDataSyncService(node.ctx, node.replica)
	go node.dataSyncService.start()

	time.Sleep(2 * time.Second)

	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"

	searchProducerChannels := []string{"search"}
	searchStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchStream.SetPulsarClient(pulsarURL)
	searchStream.CreatePulsarProducers(searchProducerChannels)

	var vecSearch = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17}
	var searchRawData []byte
	for _, ele := range vecSearch {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		searchRawData = append(searchRawData, buf...)
	}
	placeholderValue := servicepb.PlaceholderValue{
		Tag:    "$0",
		Type:   servicepb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData},
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
			HashValues: []int32{0},
		},
		SearchRequest: internalpb.SearchRequest{
			MsgType:         internalpb.MsgType_kSearch,
			ReqID:           int64(1),
			ProxyID:         int64(1),
			Timestamp:       uint64(20 + 1000),
			ResultChannelID: int64(1),
			Query:           &blob,
		},
	}

	msgPackSearch := msgstream.MsgPack{}
	msgPackSearch.Msgs = append(msgPackSearch.Msgs, searchMsg)

	var searchMsgStream msgstream.MsgStream = searchStream
	searchMsgStream.Start()
	err = searchMsgStream.Produce(&msgPackSearch)
	assert.NoError(t, err)

	node.searchService = newSearchService(node.ctx, node.replica)
	go node.searchService.start()

	time.Sleep(2 * time.Second)

	cancel()
	node.Close()
}
