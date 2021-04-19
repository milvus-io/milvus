package datanode

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

// NOTE: start pulsar before test
func TestDataSyncService_Start(t *testing.T) {
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = true
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	// init data node
	pulsarURL := Params.PulsarAddress

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")

	chanSize := 100
	flushChan := make(chan *flushMsg, chanSize)
	replica := newReplica()
	allocFactory := AllocatorFactory{}
	sync := newDataSyncService(ctx, flushChan, replica, allocFactory)
	sync.replica.addCollection(collMeta.ID, proto.MarshalTextString(collMeta.Schema))
	go sync.start()

	// test data generate
	// GOOSE TODO orgnize
	const DIM = 2
	const N = 1
	var rawData []byte

	// Float vector
	var fvector = [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	var bvector = [4]byte{255, 255, 255, 0}
	for _, ele := range bvector {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(ele))
		rawData = append(rawData, bs...)
	}

	// Bool
	bb := make([]byte, 4)
	var fieldBool = true
	var fieldBoolInt uint32
	if fieldBool {
		fieldBoolInt = 1
	} else {
		fieldBoolInt = 0
	}

	binary.LittleEndian.PutUint32(bb, fieldBoolInt)
	rawData = append(rawData, bb...)

	// int8
	var dataInt8 int8 = 100
	bint8 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint8, uint32(dataInt8))
	rawData = append(rawData, bint8...)

	// int16
	var dataInt16 int16 = 200
	bint16 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint16, uint32(dataInt16))
	rawData = append(rawData, bint16...)

	// int32
	var dataInt32 int32 = 300
	bint32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint32, uint32(dataInt32))
	rawData = append(rawData, bint32...)

	// int64
	var dataInt64 int64 = 300
	bint64 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint64, uint32(dataInt64))
	rawData = append(rawData, bint64...)

	// float32
	var datafloat float32 = 1.1
	bfloat32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bfloat32, math.Float32bits(datafloat))
	rawData = append(rawData, bfloat32...)

	// float64
	var datafloat64 float64 = 2.2
	bfloat64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bfloat64, math.Float64bits(datafloat64))
	rawData = append(rawData, bfloat64...)

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	// messages generate
	const MSGLENGTH = 1
	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < MSGLENGTH; i++ {
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i),
				},
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     UniqueID(0),
					Timestamp: Timestamp(i + 1000),
					SourceID:  0,
				},
				CollectionName: "col1",
				PartitionName:  "default",
				SegmentID:      UniqueID(1),
				ChannelID:      "0",
				Timestamps:     []Timestamp{Timestamp(i + 1000)},
				RowIDs:         []UniqueID{UniqueID(i)},

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

	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(0),
			EndTimestamp:   Timestamp(0),
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     UniqueID(0),
				Timestamp: math.MaxUint64,
				SourceID:  0,
			},
		},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	const receiveBufSize = 1024
	insertChannels := Params.InsertChannelNames
	ddChannels := Params.DDChannelNames

	insertStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(insertChannels)

	ddStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	ddStream.SetPulsarClient(pulsarURL)
	ddStream.CreatePulsarProducers(ddChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err := insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	Params.FlushInsertBufferSize = 1

	sync.close()
}
