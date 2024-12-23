package msgstream

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

type streamNewer func(ctx context.Context) (MsgStream, error)

// test all stream operation on stream factory
func testMsgStreamOperation(t *testing.T, factories []Factory, pg positionGenerator) {
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
		func(t *testing.T, f []Factory) {
			testMqMsgStreamSeekInvalidMessage(t, f, pg)
		},
		testMqMsgStreamSeekLatest,
		testBroadcastMark,
	}

	for _, testFunc := range testFuncs {
		t.Run(
			runtime.FuncForPC(reflect.ValueOf(testFunc).Pointer()).Name(),
			func(t *testing.T) {
				testFunc(t, factories)
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
	insertRequest := &msgpb.InsertRequest{
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
		BaseMsg: BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{1, 3},
		},
		InsertRequest: insertRequest,
	}
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	applyProduceAndConsumeWithRepack(t, &msgPack, []streamNewer{f[0].NewMsgStream, f[1].NewMsgStream}, 2)
}

func testDeleteRepackFuncWithDifferentClient(t *testing.T, f []Factory) {
	deleteRequest := &msgpb.DeleteRequest{
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
		BaseMsg: BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{1},
		},
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
	_, err = producer.Broadcast(ctx, &msgPack0)
	assert.NoError(t, err)
	err = producer.Produce(ctx, &msgPack1)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack2)
	assert.NoError(t, err)

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
	_, err = producer.Broadcast(ctx, &msgPack0)
	assert.NoError(t, err)
	err = producer.Produce(ctx, &msgPack1)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack2)
	assert.NoError(t, err)
	err = producer.Produce(ctx, &msgPack3)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack4)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack5)
	assert.NoError(t, err)

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
	err := producer.Produce(ctx, msgPack)
	assert.NoError(t, err)

	// pick a seekPosition
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].GetID(), int64(i))
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
	assert.NoError(t, err)

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
				assert.Equal(t, value, tsMsg.GetID())
				value++
				cnt++

				ret, err := lastMsgID.LessOrEqualThan(tsMsg.GetPosition().MsgID)
				assert.NoError(t, err)
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
	_, err := producer.Broadcast(ctx, &msgPack0)
	assert.NoError(t, err)
	err = producer.Produce(ctx, &msgPack1)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack2)
	assert.NoError(t, err)
	err = producer.Produce(ctx, &msgPack3)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack4)
	assert.NoError(t, err)
	err = producer.Produce(ctx, &msgPack5)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack6)
	assert.NoError(t, err)
	_, err = producer.Broadcast(ctx, &msgPack7)
	assert.NoError(t, err)

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
		tsMsg, err := msg.Unmarshal(consumer.GetUnmarshalDispatcher())
		require.NoError(t, err)
		assert.Equal(t, tsMsg.BeginTs(), result[i])
	}

	seekMsg2 := consume(ctx, consumer)
	assert.Equal(t, len(seekMsg2.Msgs), 1)
	for _, msg := range seekMsg2.Msgs {
		tsMsg, err := msg.Unmarshal(consumer.GetUnmarshalDispatcher())
		require.NoError(t, err)
		assert.Equal(t, tsMsg.BeginTs(), uint64(19))
	}
	consumer.Close()

	consumer = createAndSeekConsumer(ctx, t, f[0].NewTtMsgStream, channels, receivedMsg3.EndPositions)
	seekMsg = consume(ctx, consumer)
	assert.Equal(t, len(seekMsg.Msgs), 1)
	for _, msg := range seekMsg.Msgs {
		tsMsg, err := msg.Unmarshal(consumer.GetUnmarshalDispatcher())
		require.NoError(t, err)
		assert.Equal(t, tsMsg.BeginTs(), uint64(19))
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

	_, err := producer.Broadcast(ctx, &msgPack0)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	err = producer.Produce(ctx, &msgPack1)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	_, err = producer.Broadcast(ctx, &msgPack2)
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
					tsMsg, err := msg.Unmarshal(consumer.GetUnmarshalDispatcher())
					require.NoError(t, err)
					log.Println("msg type: ", tsMsg.Type(), ", msg value: ", msg)
					assert.Greater(t, tsMsg.BeginTs(), msgPack.BeginTs)
					assert.LessOrEqual(t, tsMsg.BeginTs(), msgPack.EndTs)
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
	rcvMsgPacks := make([]*ConsumeMsgPack, 0)

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
				tsMsg, err := msg.Unmarshal(consumer.GetUnmarshalDispatcher())
				require.NoError(t, err)
				log.Println("msg type: ", tsMsg.Type(), ", msg value: ", msg)
				assert.Greater(t, tsMsg.BeginTs(), msgPack.BeginTs)
				assert.LessOrEqual(t, tsMsg.BeginTs(), msgPack.EndTs)
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

	err := producer.Produce(ctx, msgPack)
	assert.NoError(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].GetID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
		}
	}
	consumer.Close()

	consumer = createAndSeekConsumer(ctx, t, f[0].NewMsgStream, channels, []*msgpb.MsgPosition{seekPosition})
	for i := 6; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].GetID(), int64(i))
	}
	consumer.Close()
}

func testMqMsgStreamSeekInvalidMessage(t *testing.T, f []Factory, pg positionGenerator) {
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

	err := producer.Produce(ctx, msgPack)
	assert.NoError(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consume(ctx, consumer)
		assert.Equal(t, result.Msgs[0].GetID(), int64(i))
		seekPosition = result.EndPositions[0]
	}

	p := pg(seekPosition.ChannelName, seekPosition.Timestamp, seekPosition.MsgGroup, []uint64{13})
	consumer2 := createAndSeekConsumer(ctx, t, f[1].NewMsgStream, channels, p)
	defer consumer2.Close()

	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = producer.Produce(ctx, msgPack)
	assert.NoError(t, err)
	result := consume(ctx, consumer2)
	assert.Equal(t, result.Msgs[0].GetID(), int64(1))
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

	err := producer.Produce(ctx, msgPack)
	assert.NoError(t, err)
	consumer2 := createLatestConsumer(ctx, t, f[1].NewMsgStream, channels)
	defer consumer2.Close()

	msgPack.Msgs = nil
	// produce another 10 tsMs
	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = producer.Produce(ctx, msgPack)
	assert.NoError(t, err)

	for i := 10; i < 20; i++ {
		result := consume(ctx, consumer2)
		assert.Equal(t, result.Msgs[0].GetID(), int64(i))
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

	ids, err := producer.Broadcast(ctx, &msgPack0)
	assert.NoError(t, err)
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

	ids, err = producer.Broadcast(ctx, &msgPack1)
	assert.NoError(t, err)
	assert.NotNil(t, ids)
	assert.Equal(t, len(channels), len(ids))
	for _, c := range channels {
		ids, ok := ids[c]
		assert.True(t, ok)
		assert.Equal(t, len(msgPack1.Msgs), len(ids))
	}

	// edge cases
	_, err = producer.Broadcast(ctx, nil)
	assert.Error(t, err)

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, &MarshalFailTsMsg{})
	_, err = producer.Broadcast(ctx, &msgPack2)
	assert.Error(t, err)
}

func applyBroadCastAndConsume(t *testing.T, msgPack *MsgPack, newer []streamNewer, channelNum int) {
	producer, consumer := createStream(context.Background(), t, newer, getChannel(channelNum))
	defer producer.Close()
	defer consumer.Close()

	_, err := producer.Broadcast(context.TODO(), msgPack)
	assert.NoError(t, err)
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

	err := producer.Produce(context.TODO(), msgPack)
	assert.NoError(t, err)
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

	err := producer.Produce(context.TODO(), msgPack)
	assert.NoError(t, err)
	receiveAndValidateMsg(context.Background(), consumer, len(msgPack.Msgs))
}

func consume(ctx context.Context, mq MsgStream) *ConsumeMsgPack {
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
	assert.NoError(t, err)
	consumer.AsConsumer(context.Background(), channels, funcutil.RandomString(8), common.SubscriptionPositionUnknown)
	err = consumer.Seek(context.Background(), seekPositions, false)
	assert.NoError(t, err)
	return consumer
}

func createProducer(ctx context.Context, t *testing.T, newer streamNewer, channels []string) MsgStream {
	producer, err := newer(ctx)
	assert.NoError(t, err)
	producer.AsProducer(ctx, channels)
	return producer
}

func createConsumer(ctx context.Context, t *testing.T, newer streamNewer, channels []string) MsgStream {
	consumer, err := newer(ctx)
	assert.NoError(t, err)
	consumer.AsConsumer(context.Background(), channels, funcutil.RandomString(8), common.SubscriptionPositionEarliest)
	return consumer
}

func createLatestConsumer(ctx context.Context, t *testing.T, newer streamNewer, channels []string) MsgStream {
	consumer, err := newer(ctx)
	assert.NoError(t, err)
	consumer.AsConsumer(context.Background(), channels, funcutil.RandomString(8), common.SubscriptionPositionLatest)
	return consumer
}

func createStream(ctx context.Context, t *testing.T, newer []streamNewer, channels []string) (
	MsgStream, MsgStream,
) {
	assert.NotEmpty(t, channels)
	producer, err := newer[0](ctx)
	assert.NoError(t, err)
	producer.AsProducer(ctx, channels)

	consumer, err := newer[1](ctx)
	assert.NoError(t, err)
	consumer.AsConsumer(context.Background(), channels, funcutil.RandomString(8), common.SubscriptionPositionEarliest)

	return producer, consumer
}

func getChannel(n int) []string {
	channels := make([]string, 0, n)
	for i := 0; i < n; i++ {
		channels = append(channels, funcutil.RandomString(8))
	}
	return channels
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
					log.Println("msg type: ", v.GetType(), ", msg value: ", v)
				}
				log.Println("================")
			}
			if receiveCount >= msgCount {
				return
			}
		}
	}
}
