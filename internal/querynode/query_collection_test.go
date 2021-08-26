package querynode

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

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
	queryCollection := newQueryCollection(ctx, cancel, 0, historical, streaming, factory, nil, nil, false)

	producerChannels := []string{"testResultChannel"}
	queryCollection.queryResultMsgStream.AsProducer(producerChannels)

	dim := 2
	// generate search rawData
	var vec = make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
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
