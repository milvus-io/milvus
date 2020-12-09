package writenode

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

// NOTE: start pulsar before test
func TestDataSyncService_Start(t *testing.T) {
	Params.Init()
	const ctxTimeInMillisecond = 200
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
	pulsarURL, _ := Params.pulsarAddress()
	node := NewWriteNode(ctx, 0)

	// test data generate
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
	const MSGLENGTH = 10
	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < MSGLENGTH; i++ {
		randt := rand.Intn(MSGLENGTH)
		// randt := i
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i), uint32(i),
				},
			},
			InsertRequest: internalPb.InsertRequest{
				MsgType:        internalPb.MsgType_kInsert,
				ReqID:          int64(0),
				CollectionName: "collection0",
				PartitionTag:   "default",
				SegmentID:      int64(0),
				ChannelID:      int64(0),
				ProxyID:        int64(0),
				Timestamps:     []uint64{uint64(randt + 1000), uint64(randt + 1000)},
				RowIDs:         []int64{int64(i), int64(i)},
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

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}

	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalPb.TimeTickMsg{
			MsgType:   internalPb.MsgType_kTimeTick,
			PeerID:    UniqueID(0),
			Timestamp: math.MaxUint64,
		},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	const receiveBufSize = 1024
	producerChannels := Params.insertChannelNames()

	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(pulsarURL)
	insertStream.CreatePulsarProducers(producerChannels)

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	err := insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	node.dataSyncService = newDataSyncService(node.ctx)
	go node.dataSyncService.start()

	node.Close()

	<-ctx.Done()
}
