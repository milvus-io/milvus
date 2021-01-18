package writenode

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

// NOTE: start pulsar before test
func TestDataSyncService_Start(t *testing.T) {
	newMeta()
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

	// init write node
	pulsarURL := Params.PulsarAddress
	node := NewWriteNode(ctx, 0)

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
					MsgID:     0,
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
				MsgID:     0,
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

	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(insertChannels)

	ddStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
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
	replica := newReplica()
	node.dataSyncService = newDataSyncService(node.ctx, nil, nil, replica)
	go node.dataSyncService.start()

	node.Close()

	<-ctx.Done()
}

func newMeta() *etcdpb.CollectionMeta {
	ETCDAddr := Params.EtcdAddress
	MetaRootPath := Params.MetaRootPath

	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDAddr},
		DialTimeout: 5 * time.Second,
	})
	kvClient := etcdkv.NewEtcdKV(cli, MetaRootPath)
	defer kvClient.Close()

	sch := schemapb.CollectionSchema{
		Name:        "col1",
		Description: "test collection",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     1,
				Name:        "Timestamp",
				Description: "test collection filed 1",
				DataType:    schemapb.DataType_INT64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f1_tk2",
						Value: "col1_f1_tv2",
					},
				},
			},
			{
				FieldID:     0,
				Name:        "RowID",
				Description: "test collection filed 1",
				DataType:    schemapb.DataType_INT64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f1_tk2",
						Value: "col1_f1_tv2",
					},
				},
			},
			{
				FieldID:     100,
				Name:        "col1_f1",
				Description: "test collection filed 1",
				DataType:    schemapb.DataType_VECTOR_FLOAT,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "2",
					},
					{
						Key:   "col1_f1_tk2",
						Value: "col1_f1_tv2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f1_ik1",
						Value: "col1_f1_iv1",
					},
					{
						Key:   "col1_f1_ik2",
						Value: "col1_f1_iv2",
					},
				},
			},
			{
				FieldID:     101,
				Name:        "col1_f2",
				Description: "test collection filed 2",
				DataType:    schemapb.DataType_VECTOR_BINARY,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "32",
					},
					{
						Key:   "col1_f2_tk2",
						Value: "col1_f2_tv2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f2_ik1",
						Value: "col1_f2_iv1",
					},
					{
						Key:   "col1_f2_ik2",
						Value: "col1_f2_iv2",
					},
				},
			},
			{
				FieldID:     102,
				Name:        "col1_f3",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_BOOL,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     103,
				Name:        "col1_f4",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_INT8,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     104,
				Name:        "col1_f5",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_INT16,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     105,
				Name:        "col1_f6",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_INT32,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     106,
				Name:        "col1_f7",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_INT64,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     107,
				Name:        "col1_f8",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_FLOAT,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     108,
				Name:        "col1_f9",
				Description: "test collection filed 3",
				DataType:    schemapb.DataType_DOUBLE,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
		},
	}

	collection := etcdpb.CollectionMeta{
		ID:            UniqueID(1),
		Schema:        &sch,
		CreateTime:    Timestamp(1),
		SegmentIDs:    make([]UniqueID, 0),
		PartitionTags: make([]string, 0),
	}

	collBytes := proto.MarshalTextString(&collection)
	kvClient.Save("/collection/"+strconv.FormatInt(collection.ID, 10), collBytes)

	segSch := etcdpb.SegmentMeta{
		SegmentID:    UniqueID(1),
		CollectionID: UniqueID(1),
	}
	segBytes := proto.MarshalTextString(&segSch)
	kvClient.Save("/segment/"+strconv.FormatInt(segSch.SegmentID, 10), segBytes)

	return &collection

}
