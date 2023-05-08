package msgstream_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	pkgmsgstream "github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	MsgStream           = pkgmsgstream.MsgStream
	MsgPack             = pkgmsgstream.MsgPack
	BaseMsg             = pkgmsgstream.BaseMsg
	Factory             = pkgmsgstream.Factory
	Timestamp           = pkgmsgstream.Timestamp
	InsertMsg           = pkgmsgstream.InsertMsg
	DeleteMsg           = pkgmsgstream.DeleteMsg
	TimeTickMsg         = pkgmsgstream.TimeTickMsg
	MsgType             = pkgmsgstream.MsgType
	UniqueID            = pkgmsgstream.UniqueID
	TsMsg               = pkgmsgstream.TsMsg
	CreateCollectionMsg = pkgmsgstream.CreateCollectionMsg
	MarshalType         = pkgmsgstream.MarshalType
)

type streamNewer func(ctx context.Context) (MsgStream, error)

// test all stream operation on stream factory
func testMsgStreamOperation(t *testing.T, st msgstream.StreamType, storeDir string) {
	f1 := msgstream.NewFactory(st, storeDir)
	f2 := msgstream.NewFactory(st, storeDir)
	// Clear mq server after testing.

	testFuncs := []func(t *testing.T, f []Factory){
		testInsert,
		testDelete,
		testTimeTick,
		testBroadCast,
		testInsertWithRepack,
		testInsertRepackFuncWithDifferentClient,
		testDeleteRepackFuncWithDifferentClient,
		testDefaultRepack,
		testTimeTickerAndInsert,
		testTimeTickerNoSeek,
		testSeekToLast,
		testTimeTickerSeek,
		testTimeTickUnmarshalHeader,
		testTimeTickerStream1,
		testTimeTickerStream2,
		testMqMsgStreamSeek,
		testMqMsgStreamSeekInvalidMessage,
		testMqMsgStreamSeekLatest,
		testBroadcastMark,
	}

	for _, testFunc := range testFuncs {
		t.Run(
			runtime.FuncForPC(reflect.ValueOf(testFunc).Pointer()).Name(),
			func(t *testing.T) {
				testFunc(t, []Factory{f1, f2})
			},
		)
	}
}

func testInsert(t *testing.T, f []Factory) {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))
	applyProduceAndConsume(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[0].NewMsgStream}, 2)
}

func testDelete(t *testing.T, f []Factory) {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 1))
	applyProduceAndConsume(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[0].NewMsgStream}, 1)
}

func testTimeTick(t *testing.T, f []Factory) {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3))
	applyProduceAndConsume(t, &msgPack, []streamNewer{f[0].NewTtMsgStream, f[0].NewTtMsgStream}, 1)
}

func testBroadCast(t *testing.T, f []Factory) {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3))
	applyBroadCastAndConsume(t, &msgPack, []streamNewer{f[0].NewTtMsgStream, f[0].NewTtMsgStream}, 2)
}

func testInsertWithRepack(t *testing.T, f []Factory) {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))
	applyProduceAndConsumeWithRepack(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[0].NewMsgStream}, 2)
}

func testInsertRepackFuncWithDifferentClient(t *testing.T, f []Factory) {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{1, 3},
	}

	insertRequest := msgpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ShardName:      "1",
		Timestamps:     []Timestamp{1, 1},
		RowIDs:         []int64{1, 3},
		RowData:        []*commonpb.Blob{{}, {}},
	}
	insertMsg := &InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	applyProduceAndConsumeWithRepack(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, 2)
}

func testDeleteRepackFuncWithDifferentClient(t *testing.T, f []Factory) {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{1},
	}

	deleteRequest := msgpb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Delete,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName:   "Collection",
		ShardName:        "chan-1",
		Timestamps:       []Timestamp{1},
		Int64PrimaryKeys: []int64{1},
		NumRows:          1,
	}
	deleteMsg := &DeleteMsg{
		BaseMsg:       baseMsg,
		DeleteRequest: deleteRequest,
	}

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)
	applyProduceAndConsumeWithRepack(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, 2)
}

func testDefaultRepack(t *testing.T, f []Factory) {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 3))

	applyProduceAndConsumeWithRepack(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, 2)
}

func testTimeTickerAndInsert(t *testing.T, f []Factory) {
	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	ctx := context.Background()

	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, getChannel(2))
	defer producer.Close()
	defer consumer.Close()

	var err error
	_, err = producer.Broadcast(&msgPack0)
	assert.Nil(t, err)
	err = producer.Produce(&msgPack1)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack2)
	assert.Nil(t, err)

	receiveAndValidateMsg(ctx, consumer, len(msgPack1.Msgs))
}

func testTimeTickerNoSeek(t *testing.T, f []Factory) {
	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 19))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	msgPack3 := MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 14))
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 9))

	msgPack4 := MsgPack{}
	msgPack4.Msgs = append(msgPack4.Msgs, getTimeTickMsg(11))

	msgPack5 := MsgPack{}
	msgPack5.Msgs = append(msgPack5.Msgs, getTimeTickMsg(15))

	channels := getChannel(1)
	ctx := context.Background()
	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewTtMsgStream}, channels)
	defer producer.Close()

	var err error
	_, err = producer.Broadcast(&msgPack0)
	assert.Nil(t, err)
	err = producer.Produce(&msgPack1)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack2)
	assert.Nil(t, err)
	err = producer.Produce(&msgPack3)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack4)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack5)
	assert.Nil(t, err)

	o1 := consume(ctx, consumer)
	o2 := consume(ctx, consumer)
	o3 := consume(ctx, consumer)
	t.Log(o1.BeginTs)
	t.Log(o2.BeginTs)
	t.Log(o3.BeginTs)
	consumer.Close()

	producer2, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewTtMsgStream}, channels)
	defer producer2.Close()
	defer consumer.Close()
	p1 := consume(ctx, consumer)
	p2 := consume(ctx, consumer)
	p3 := consume(ctx, consumer)
	t.Log(p1.BeginTs)
	t.Log(p2.BeginTs)
	t.Log(p3.BeginTs)

	assert.Equal(t, o1.BeginTs, p1.BeginTs)
	assert.Equal(t, o2.BeginTs, p2.BeginTs)
	assert.Equal(t, o3.BeginTs, p3.BeginTs)
}

func testSeekToLast(t *testing.T, f []Factory) {
	ctx := context.Background()
	channels := getChannel(1)
	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, channels)
	defer producer.Close()

	msgPack := &MsgPack{}
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	// produce test data
	err := producer.Produce(msgPack)
	assert.Nil(t, err)

	// pick a seekPosition
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
		}
	}
	consumer.Close()

	// Create a unknown position consumer and seek.
	consumer = createAndSeekConsumer(ctx, t, f[1].NewMsgStream, channels, []*msgpb.MsgPosition{seekPosition})
	defer consumer.Close()

	// Get latest MsgID.
	lastMsgID, err := consumer.GetLatestMsgID(channels[0])
	assert.Nil(t, err)

	cnt := 0
	var value int64 = 6
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			hasMore = false
		case msgPack, ok := <-consumer.Chan():
			if !ok {
				assert.Fail(t, "Should not reach here")
			}

			assert.Equal(t, 1, len(msgPack.Msgs))
			for _, tsMsg := range msgPack.Msgs {
				assert.Equal(t, value, tsMsg.ID())
				value++
				cnt++

				ret, err := lastMsgID.LessOrEqualThan(tsMsg.Position().MsgID)
				assert.Nil(t, err)
				if ret {
					hasMore = false
					break
				}
			}
		}
	}
	assert.Equal(t, 4, cnt)
}

func testTimeTickerSeek(t *testing.T, f []Factory) {
	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 19))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	msgPack3 := MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 14))
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 9))

	msgPack4 := MsgPack{}
	msgPack4.Msgs = append(msgPack4.Msgs, getTimeTickMsg(11))

	msgPack5 := MsgPack{}
	msgPack5.Msgs = append(msgPack5.Msgs, getTsMsg(commonpb.MsgType_Insert, 12))
	msgPack5.Msgs = append(msgPack5.Msgs, getTsMsg(commonpb.MsgType_Insert, 13))

	msgPack6 := MsgPack{}
	msgPack6.Msgs = append(msgPack6.Msgs, getTimeTickMsg(15))

	msgPack7 := MsgPack{}
	msgPack7.Msgs = append(msgPack7.Msgs, getTimeTickMsg(20))

	ctx := context.Background()
	channels := getChannel(1)
	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewTtMsgStream}, channels)
	defer producer.Close()

	// Send message
	_, err := producer.Broadcast(&msgPack0)
	assert.Nil(t, err)
	err = producer.Produce(&msgPack1)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack2)
	assert.Nil(t, err)
	err = producer.Produce(&msgPack3)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack4)
	assert.Nil(t, err)
	err = producer.Produce(&msgPack5)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack6)
	assert.Nil(t, err)
	_, err = producer.Broadcast(&msgPack7)
	assert.Nil(t, err)

	// Test received message
	receivedMsg := consume(ctx, consumer)
	assert.Equal(t, len(receivedMsg.Msgs), 2)
	assert.Equal(t, receivedMsg.BeginTs, uint64(0))
	assert.Equal(t, receivedMsg.EndTs, uint64(5))
	assert.Equal(t, receivedMsg.StartPositions[0].Timestamp, uint64(0))
	assert.Equal(t, receivedMsg.EndPositions[0].Timestamp, uint64(5))

	receivedMsg2 := consume(ctx, consumer)
	assert.Equal(t, len(receivedMsg2.Msgs), 1)
	assert.Equal(t, receivedMsg2.BeginTs, uint64(5))
	assert.Equal(t, receivedMsg2.EndTs, uint64(11))
	assert.Equal(t, receivedMsg2.StartPositions[0].Timestamp, uint64(5))
	assert.Equal(t, receivedMsg2.EndPositions[0].Timestamp, uint64(11))

	receivedMsg3 := consume(ctx, consumer)
	assert.Equal(t, len(receivedMsg3.Msgs), 3)
	assert.Equal(t, receivedMsg3.BeginTs, uint64(11))
	assert.Equal(t, receivedMsg3.EndTs, uint64(15))
	assert.Equal(t, receivedMsg3.StartPositions[0].Timestamp, uint64(11))
	assert.Equal(t, receivedMsg3.EndPositions[0].Timestamp, uint64(15))

	receivedMsg4 := consume(ctx, consumer)
	assert.Equal(t, len(receivedMsg4.Msgs), 1)
	assert.Equal(t, receivedMsg4.BeginTs, uint64(15))
	assert.Equal(t, receivedMsg4.EndTs, uint64(20))
	assert.Equal(t, receivedMsg4.StartPositions[0].Timestamp, uint64(15))
	assert.Equal(t, receivedMsg4.EndPositions[0].Timestamp, uint64(20))
	consumer.Close()

	consumer = createAndSeekConsumer(ctx, t, f[1].NewTtMsgStream, channels, receivedMsg3.StartPositions)
	seekMsg := consume(ctx, consumer)
	assert.Equal(t, len(seekMsg.Msgs), 3)
	result := []uint64{14, 12, 13}
	for i, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), result[i])
	}

	seekMsg2 := consume(ctx, consumer)
	assert.Equal(t, len(seekMsg2.Msgs), 1)
	for _, msg := range seekMsg2.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(19))
	}
	consumer.Close()

	consumer = createAndSeekConsumer(ctx, t, f[0].NewTtMsgStream, channels, receivedMsg3.EndPositions)
	seekMsg = consume(ctx, consumer)
	assert.Equal(t, len(seekMsg.Msgs), 1)
	for _, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(19))
	}
	consumer.Close()
}

func testTimeTickUnmarshalHeader(t *testing.T, f []Factory) {
	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	channels := getChannel(2)
	ctx := context.Background()
	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewTtMsgStream}, channels)
	defer producer.Close()
	defer consumer.Close()

	_, err := producer.Broadcast(&msgPack0)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	err = producer.Produce(&msgPack1)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	_, err = producer.Broadcast(&msgPack2)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	receiveAndValidateMsg(ctx, consumer, len(msgPack1.Msgs))
}

func testTimeTickerStream1(t *testing.T, f []Factory) {
	consumeChannels := getChannel(2)
	pubChannel1 := []string{consumeChannels[0]}
	pubChannel2 := []string{consumeChannels[1]}
	ctx := context.Background()

	producer1 := createProducer(ctx, t, f[0].NewMsgStream, pubChannel1)
	defer producer1.Close()
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(producer1, msgPacks1))

	producer2 := createProducer(ctx, t, f[0].NewMsgStream, pubChannel2)
	defer producer2.Close()
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(producer2, msgPacks2))

	// consume msg
	consumer := createConsumer(ctx, t, f[1].NewTtMsgStream, consumeChannels)
	defer consumer.Close()
	log.Println("===============receive msg=================")
	checkNMsgPack := func(t *testing.T, outputStream MsgStream, num int) int {
		rcvMsg := 0
		for i := 0; i < num; i++ {
			msgPack := consume(ctx, consumer)
			rcvMsg += len(msgPack.Msgs)
			if len(msgPack.Msgs) > 0 {
				for _, msg := range msgPack.Msgs {
					log.Println("msg type: ", msg.Type(), ", msg value: ", msg)
					assert.Greater(t, msg.BeginTs(), msgPack.BeginTs)
					assert.LessOrEqual(t, msg.BeginTs(), msgPack.EndTs)
				}
				log.Println("================")
			}
		}
		return rcvMsg
	}
	msgCount := checkNMsgPack(t, consumer, len(msgPacks1)/2)
	cnt1 := (len(msgPacks1)/2 - 1) * len(msgPacks1[0].Msgs)
	cnt2 := (len(msgPacks2)/2 - 1) * len(msgPacks2[0].Msgs)
	assert.Equal(t, (cnt1 + cnt2), msgCount)
}

// This testcase will generate MsgPacks as following:
//
//	Insert     Insert     Insert     Insert     Insert     Insert
//
// c1 |----------|----------|----------|----------|----------|----------|
//
//	         ^          ^          ^          ^          ^          ^
//	       TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
//
//	Insert     Insert     Insert     Insert     Insert     Insert
//
// c2 |----------|----------|----------|----------|----------|----------|
//
//	  ^          ^          ^          ^          ^          ^
//	TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
//
// Then check:
//  1. ttMsgStream consumer can seek to the right position and resume
//  2. The count of consumed msg should be equal to the count of produced msg
func testTimeTickerStream2(t *testing.T, f []Factory) {
	consumeChannels := getChannel(2)
	pubChannel1 := []string{consumeChannels[0]}
	pubChannel2 := []string{consumeChannels[1]}
	ctx := context.Background()

	producer1 := createProducer(ctx, t, f[0].NewMsgStream, pubChannel1)
	defer producer1.Close()
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(producer1, msgPacks1))

	producer2 := createProducer(ctx, t, f[0].NewMsgStream, pubChannel2)
	defer producer2.Close()
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(producer2, msgPacks2))

	// consume msg
	log.Println("=============receive msg===================")
	rcvMsgPacks := make([]*MsgPack, 0)

	resumeMsgPack := func(t *testing.T) int {
		var consumer MsgStream
		msgCount := len(rcvMsgPacks)
		if msgCount == 0 {
			consumer = createConsumer(ctx, t, f[1].NewTtMsgStream, consumeChannels)
		} else {
			consumer = createAndSeekConsumer(ctx, t, f[1].NewTtMsgStream, consumeChannels, rcvMsgPacks[msgCount-1].EndPositions)
		}
		msgPack := consume(ctx, consumer)
		rcvMsgPacks = append(rcvMsgPacks, msgPack)
		if len(msgPack.Msgs) > 0 {
			for _, msg := range msgPack.Msgs {
				log.Println("msg type: ", msg.Type(), ", msg value: ", msg)
				assert.Greater(t, msg.BeginTs(), msgPack.BeginTs)
				assert.LessOrEqual(t, msg.BeginTs(), msgPack.EndTs)
			}
			log.Println("================")
		}
		consumer.Close()
		return len(rcvMsgPacks[msgCount].Msgs)
	}

	msgCount := 0
	for i := 0; i < len(msgPacks1)/2; i++ {
		msgCount += resumeMsgPack(t)
	}
	cnt1 := (len(msgPacks1)/2 - 1) * len(msgPacks1[0].Msgs)
	cnt2 := (len(msgPacks2)/2 - 1) * len(msgPacks2[0].Msgs)
	assert.Equal(t, (cnt1 + cnt2), msgCount)
}

func testMqMsgStreamSeek(t *testing.T, f []Factory) {
	channels := getChannel(1)
	ctx := context.Background()

	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, channels)
	defer producer.Close()

	msgPack := &MsgPack{}
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := producer.Produce(msgPack)
	assert.Nil(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
		}
	}
	consumer.Close()

	consumer = createAndSeekConsumer(ctx, t, f[0].NewMsgStream, channels, []*msgpb.MsgPosition{seekPosition})
	for i := 6; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
	}
	consumer.Close()
}

func testMqMsgStreamSeekInvalidMessage(t *testing.T, f []Factory) {
	channels := getChannel(1)
	ctx := context.Background()

	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, channels)
	defer producer.Close()
	defer consumer.Close()

	msgPack := &MsgPack{}
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := producer.Produce(msgPack)
	assert.Nil(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		seekPosition = result.EndPositions[0]
	}

	p := generatePosition(f[0], seekPosition.ChannelName, seekPosition.Timestamp, seekPosition.MsgGroup, []uint64{13})
	consumer2 := createAndSeekConsumer(ctx, t, f[1].NewMsgStream, channels, p)
	defer consumer2.Close()

	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = producer.Produce(msgPack)
	assert.Nil(t, err)
	result := consume(ctx, consumer2)
	assert.Equal(t, result.Msgs[0].ID(), int64(1))
}

func testMqMsgStreamSeekLatest(t *testing.T, f []Factory) {
	channels := getChannel(1)
	ctx := context.Background()

	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, channels)
	defer producer.Close()
	defer consumer.Close()

	msgPack := &MsgPack{}
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := producer.Produce(msgPack)
	assert.Nil(t, err)
	consumer2 := createLatestConsumer(ctx, t, f[1].NewMsgStream, channels)
	defer consumer2.Close()

	msgPack.Msgs = nil
	// produce another 10 tsMs
	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = producer.Produce(msgPack)
	assert.Nil(t, err)

	for i := 10; i < 20; i++ {
		result := consume(ctx, consumer2)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
	}
}

func testBroadcastMark(t *testing.T, f []Factory) {
	channels := getChannel(2)
	ctx := context.Background()

	producer, consumer := createStream(ctx, t, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, channels)
	defer producer.Close()
	defer consumer.Close()

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	ids, err := producer.Broadcast(&msgPack0)
	assert.Nil(t, err)
	assert.NotNil(t, ids)
	assert.Equal(t, len(channels), len(ids))
	for _, c := range channels {
		ids, ok := ids[c]
		assert.True(t, ok)
		assert.Equal(t, len(msgPack0.Msgs), len(ids))
	}

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	ids, err = producer.Broadcast(&msgPack1)
	assert.Nil(t, err)
	assert.NotNil(t, ids)
	assert.Equal(t, len(channels), len(ids))
	for _, c := range channels {
		ids, ok := ids[c]
		assert.True(t, ok)
		assert.Equal(t, len(msgPack1.Msgs), len(ids))
	}

	// edge cases
	_, err = producer.Broadcast(nil)
	assert.NotNil(t, err)

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, &MarshalFailTsMsg{})
	_, err = producer.Broadcast(&msgPack2)
	assert.NotNil(t, err)
}

var _ TsMsg = (*MarshalFailTsMsg)(nil)

type MarshalFailTsMsg struct {
	BaseMsg
}

func (t *MarshalFailTsMsg) ID() UniqueID {
	return 0
}

func (t *MarshalFailTsMsg) Type() MsgType {
	return commonpb.MsgType_Undefined
}

func (t *MarshalFailTsMsg) SourceID() int64 {
	return -1
}

func (t *MarshalFailTsMsg) Marshal(_ TsMsg) (MarshalType, error) {
	return nil, errors.New("mocked error")
}

func (t *MarshalFailTsMsg) Unmarshal(_ MarshalType) (TsMsg, error) {
	return nil, errors.New("mocked error")
}

func createRandMsgPacks(msgsInPack int, numOfMsgPack int, deltaTs int) []*MsgPack {
	msgPacks := make([]*MsgPack, numOfMsgPack)

	// generate MsgPack
	for i := 0; i < numOfMsgPack; i++ {
		if i%2 == 0 {
			msgPacks[i] = getRandInsertMsgPack(msgsInPack, i/2*deltaTs, (i/2+2)*deltaTs+2)
		} else {
			msgPacks[i] = getTimeTickMsgPack(int64((i + 1) / 2 * deltaTs))
		}
	}
	msgPacks = append(msgPacks, nil)
	msgPacks = append(msgPacks, getTimeTickMsgPack(int64(numOfMsgPack*deltaTs)))
	return msgPacks
}

func createMsgPacks(ts [][]int, numOfMsgPack int, deltaTs int) []*MsgPack {
	msgPacks := make([]*MsgPack, numOfMsgPack)

	// generate MsgPack
	for i := 0; i < numOfMsgPack; i++ {
		if i%2 == 0 {
			msgPacks[i] = getInsertMsgPack(ts[i/2])
		} else {
			msgPacks[i] = getTimeTickMsgPack(int64((i + 1) / 2 * deltaTs))
		}
	}
	msgPacks = append(msgPacks, nil)
	msgPacks = append(msgPacks, getTimeTickMsgPack(int64(numOfMsgPack*deltaTs)))
	return msgPacks
}

func sendMsgPacks(ms MsgStream, msgPacks []*MsgPack) error {
	log.Println("==============produce msg==================")
	for i := 0; i < len(msgPacks); i++ {
		printMsgPack(msgPacks[i])
		if i%2 == 0 {
			// insert msg use Produce
			if err := ms.Produce(msgPacks[i]); err != nil {
				return err
			}
		} else {
			// tt msg use Broadcast
			if _, err := ms.Broadcast(msgPacks[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func printMsgPack(msgPack *MsgPack) {
	if msgPack == nil {
		log.Println("msg nil")
	} else {
		for _, v := range msgPack.Msgs {
			log.Println("msg type: ", v.Type(), ", msg value: ", v)
		}
	}
	log.Println("================")
}

func applyBroadCastAndConsume(t *testing.T, msgPack *MsgPack, newer []streamNewer, channelNum int) {
	producer, consumer := createStream(context.Background(), t, newer, getChannel(channelNum))
	defer producer.Close()
	defer consumer.Close()

	_, err := producer.Broadcast(msgPack)
	assert.Nil(t, err)
	receiveAndValidateMsg(context.Background(), consumer, len(msgPack.Msgs)*channelNum)
}

func applyProduceAndConsumeWithRepack(
	t *testing.T,
	msgPack *MsgPack,
	newer []streamNewer,
	channelNum int,
) {
	producer, consumer := createStream(context.Background(), t, newer, getChannel(channelNum))
	producer.SetRepackFunc(repackFunc)
	defer producer.Close()
	defer consumer.Close()

	err := producer.Produce(msgPack)
	assert.Nil(t, err)
	receiveAndValidateMsg(context.Background(), consumer, len(msgPack.Msgs))
}

func applyProduceAndConsume(
	t *testing.T,
	msgPack *MsgPack,
	newer []streamNewer,
	channelNum int,
) {
	producer, consumer := createStream(context.Background(), t, newer, getChannel(channelNum))
	defer producer.Close()
	defer consumer.Close()

	err := producer.Produce(msgPack)
	assert.Nil(t, err)
	receiveAndValidateMsg(context.Background(), consumer, len(msgPack.Msgs))
}

func consume(ctx context.Context, mq MsgStream) *MsgPack {
	for {
		select {
		case msgPack, ok := <-mq.Chan():
			if !ok {
				panic("Should not reach here")
			}
			return msgPack
		case <-ctx.Done():
			return nil
		}
	}
}

func createAndSeekConsumer(ctx context.Context, t *testing.T, newer streamNewer, channels []string, seekPositions []*msgpb.MsgPosition) MsgStream {
	consumer, err := newer(ctx)
	assert.Nil(t, err)
	consumer.AsConsumer(channels, funcutil.RandomString(8), mqwrapper.SubscriptionPositionUnknown)
	err = consumer.Seek(seekPositions)
	assert.Nil(t, err)
	return consumer
}

func createProducer(ctx context.Context, t *testing.T, newer streamNewer, channels []string) MsgStream {
	producer, err := newer(ctx)
	assert.Nil(t, err)
	producer.AsProducer(channels)
	return producer
}

func createConsumer(ctx context.Context, t *testing.T, newer streamNewer, channels []string) MsgStream {
	consumer, err := newer(ctx)
	assert.Nil(t, err)
	consumer.AsConsumer(channels, funcutil.RandomString(8), mqwrapper.SubscriptionPositionEarliest)
	return consumer
}

func createLatestConsumer(ctx context.Context, t *testing.T, newer streamNewer, channels []string) MsgStream {
	consumer, err := newer(ctx)
	assert.Nil(t, err)
	consumer.AsConsumer(channels, funcutil.RandomString(8), mqwrapper.SubscriptionPositionLatest)
	return consumer
}

func createStream(ctx context.Context, t *testing.T, newer []streamNewer, channels []string) (
	MsgStream, MsgStream,
) {
	assert.NotEmpty(t, channels)
	producer, err := newer[0](ctx)
	assert.Nil(t, err)
	producer.AsProducer(channels)

	consumer, err := newer[1](ctx)
	assert.Nil(t, err)
	consumer.AsConsumer(channels, funcutil.RandomString(8), mqwrapper.SubscriptionPositionEarliest)

	return producer, consumer
}

func getChannel(n int) []string {
	channels := make([]string, 0, n)
	for i := 0; i < n; i++ {
		channels = append(channels, funcutil.RandomString(8))
	}
	return channels
}

func getTimeTickMsg(reqID UniqueID) TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	timeTickResult := msgpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     reqID,
			Timestamp: time,
			SourceID:  reqID,
		},
	}
	timeTickMsg := &TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	return timeTickMsg
}

func getTsMsg(msgType MsgType, reqID UniqueID) TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	switch msgType {
	case commonpb.MsgType_Insert:
		insertRequest := msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     reqID,
				Timestamp: time,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ShardName:      "0",
			Timestamps:     []Timestamp{time},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		}
		insertMsg := &InsertMsg{
			BaseMsg:       baseMsg,
			InsertRequest: insertRequest,
		}
		return insertMsg
	case commonpb.MsgType_Delete:
		deleteRequest := msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName:   "Collection",
			ShardName:        "1",
			Timestamps:       []Timestamp{time},
			Int64PrimaryKeys: []int64{1},
			NumRows:          1,
		}
		deleteMsg := &DeleteMsg{
			BaseMsg:       baseMsg,
			DeleteRequest: deleteRequest,
		}
		return deleteMsg
	case commonpb.MsgType_CreateCollection:
		createCollectionRequest := msgpb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			DbName:               "test_db",
			CollectionName:       "test_collection",
			PartitionName:        "test_partition",
			DbID:                 4,
			CollectionID:         5,
			PartitionID:          6,
			Schema:               []byte{},
			VirtualChannelNames:  []string{},
			PhysicalChannelNames: []string{},
		}
		createCollectionMsg := &CreateCollectionMsg{
			BaseMsg:                 baseMsg,
			CreateCollectionRequest: createCollectionRequest,
		}
		return createCollectionMsg
	case commonpb.MsgType_TimeTick:
		timeTickResult := msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
		}
		timeTickMsg := &TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		return timeTickMsg
	}
	return nil
}

func repackFunc(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range msgs {
		keys := hashKeys[i]
		for _, channelID := range keys {
			_, ok := result[channelID]
			if ok == false {
				msgPack := MsgPack{}
				result[channelID] = &msgPack
			}
			result[channelID].Msgs = append(result[channelID].Msgs, request)
		}
	}
	return result, nil
}

func receiveAndValidateMsg(ctx context.Context, outputStream MsgStream, msgCount int) {
	receiveCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-outputStream.Chan():
			if !ok || result == nil || len(result.Msgs) == 0 {
				return
			}
			if len(result.Msgs) > 0 {
				msgs := result.Msgs
				for _, v := range msgs {
					receiveCount++
					log.Println("msg type: ", v.Type(), ", msg value: ", v)
				}
				log.Println("================")
			}
			if receiveCount >= msgCount {
				return
			}
		}
	}
}

// Generate MsgPack contains 'num' msgs, with timestamp in (start, end)
func getRandInsertMsgPack(num int, start int, end int) *MsgPack {
	Rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	set := make(map[int]bool)
	msgPack := MsgPack{}
	for len(set) < num {
		reqID := Rand.Int()%(end-start-1) + start + 1
		_, ok := set[reqID]
		if !ok {
			set[reqID] = true
			msgPack.Msgs = append(msgPack.Msgs, getInsertMsgUniqueID(int64(reqID))) // getTsMsg(commonpb.MsgType_Insert, int64(reqID)))
		}
	}
	return &msgPack
}

func getInsertMsgPack(ts []int) *MsgPack {
	msgPack := MsgPack{}
	for i := 0; i < len(ts); i++ {
		msgPack.Msgs = append(msgPack.Msgs, getInsertMsgUniqueID(int64(ts[i]))) // getTsMsg(commonpb.MsgType_Insert, int64(ts[i])))
	}
	return &msgPack
}

var idCounter atomic.Int64

func getInsertMsgUniqueID(ts UniqueID) TsMsg {
	hashValue := uint32(ts)
	time := uint64(ts)
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	insertRequest := msgpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			MsgID:     idCounter.Inc(),
			Timestamp: time,
			SourceID:  ts,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ShardName:      "0",
		Timestamps:     []Timestamp{time},
		RowIDs:         []int64{1},
		RowData:        []*commonpb.Blob{{}},
	}
	insertMsg := &InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}
	return insertMsg
}

func getTimeTickMsgPack(reqID UniqueID) *MsgPack {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTimeTickMsg(reqID))
	return &msgPack
}

func generatePosition(f Factory, channelName string, timestamp uint64, msgGroup string, targetMsgIDs []uint64) []*msgpb.MsgPosition {
	factoryUnderlying := f.(*msgstream.Factory)
	result := make([]*msgpb.MsgPosition, 0, len(targetMsgIDs))

	for _, targetMsgID := range targetMsgIDs {
		var msgID []byte
		switch factoryUnderlying.StreamType() {
		case msgstream.MsgStreamTypeNmq:
			msgID = nmq.NewNmqID(targetMsgID).Serialize()
		case msgstream.MsgStreamTypeRmq:
			msgID = rmq.NewRmqID(rmq.MessageIDType(targetMsgID)).Serialize()
		default:
			panic("invalid stream type")
		}
		result = append(result, &msgpb.MsgPosition{
			ChannelName: channelName,
			Timestamp:   timestamp,
			MsgGroup:    msgGroup,
			MsgID:       msgID,
		})
	}
	return result
}
