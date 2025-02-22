// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msgstream

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func Test_NewMqMsgStream(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	_, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)
}

// TODO(wxyu): add a mock implement of mqwrapper.Client, then inject errors to improve coverage
func TestMqMsgStream_AsProducer(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// empty channel name
	m.AsProducer(context.TODO(), []string{""})
}

// TODO(wxyu): add a mock implement of mqwrapper.Client, then inject errors to improve coverage
func TestMqMsgStream_AsConsumer(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// repeat calling AsConsumer
	m.AsConsumer(context.Background(), []string{"a"}, "b", mqcommon.SubscriptionPositionUnknown)
	m.AsConsumer(context.Background(), []string{"a"}, "b", mqcommon.SubscriptionPositionUnknown)
}

func TestMqMsgStream_ComputeProduceChannelIndexes(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// empty parameters
	reBucketValues := m.ComputeProduceChannelIndexes([]TsMsg{})
	assert.Nil(t, reBucketValues)

	// not called AsProducer yet
	insertMsg := &InsertMsg{
		BaseMsg: generateBaseMsg(),
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},

			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
			SegmentID:      7,
			ShardName:      "test-channel",
			Timestamps:     []uint64{2, 1, 3},
			RowData:        []*commonpb.Blob{},
		},
	}
	reBucketValues = m.ComputeProduceChannelIndexes([]TsMsg{insertMsg})
	assert.Nil(t, reBucketValues)
}

func TestMqMsgStream_GetProduceChannels(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// empty if not called AsProducer yet
	chs := m.GetProduceChannels()
	assert.Equal(t, 0, len(chs))

	// not empty after AsProducer
	m.AsProducer(context.TODO(), []string{"a"})
	chs = m.GetProduceChannels()
	assert.Equal(t, 1, len(chs))
}

func TestMqMsgStream_Produce(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// Produce before called AsProducer
	insertMsg := &InsertMsg{
		BaseMsg: generateBaseMsg(),
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},

			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
			SegmentID:      7,
			ShardName:      "test-channel",
			Timestamps:     []uint64{2, 1, 3},
			RowData:        []*commonpb.Blob{},
		},
	}
	msgPack := &MsgPack{
		Msgs: []TsMsg{insertMsg},
	}
	err = m.Produce(context.TODO(), msgPack)
	assert.Error(t, err)
}

func TestMqMsgStream_Broadcast(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// Broadcast nil pointer
	_, err = m.Broadcast(context.TODO(), nil)
	assert.Error(t, err)
}

func TestMqMsgStream_Consume(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	// Consume return nil when ctx canceled
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	m, err := NewMqMsgStream(ctx, 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		msgPack := consumer(ctx, m)
		assert.Nil(t, msgPack)
	}()

	cancel()
	wg.Wait()
}

func TestMqMsgStream_Chan(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	ch := m.Chan()
	assert.NotNil(t, ch)
}

func TestMqMsgStream_SeekNotSubscribed(t *testing.T) {
	client, _ := rmq.NewClientWithDefaultOptions(context.TODO())
	defer client.Close()

	factory := &ProtoUDFactory{}
	m, err := NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// seek in not subscribed channel
	p := []*msgpb.MsgPosition{
		{
			ChannelName: "b",
		},
	}
	err = m.Seek(context.Background(), p, false)
	assert.Error(t, err)
}

/****************************************Rmq test******************************************/

func initRmqStream(ctx context.Context,
	producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...RepackFunc,
) (MsgStream, MsgStream) {
	factory := ProtoUDFactory{}

	rmqClient, _ := rmq.NewClientWithDefaultOptions(ctx)
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(ctx, producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	var input MsgStream = inputStream

	rmqClient2, _ := rmq.NewClientWithDefaultOptions(ctx)
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(ctx, consumerChannels, consumerGroupName, mqcommon.SubscriptionPositionEarliest)
	var output MsgStream = outputStream

	return input, output
}

func initRmqTtStream(ctx context.Context,
	producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...RepackFunc,
) (MsgStream, MsgStream) {
	factory := ProtoUDFactory{}

	rmqClient, _ := rmq.NewClientWithDefaultOptions(ctx)
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(ctx, producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	var input MsgStream = inputStream

	rmqClient2, _ := rmq.NewClientWithDefaultOptions(ctx)
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(ctx, consumerChannels, consumerGroupName, mqcommon.SubscriptionPositionEarliest)
	var output MsgStream = outputStream

	return input, output
}

func TestStream_RmqMsgStream_Insert(t *testing.T) {
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerGroupName := "InsertGroup"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	ctx := context.Background()
	inputStream, outputStream := initRmqStream(ctx, producerChannels, consumerChannels, consumerGroupName)
	err := inputStream.Produce(ctx, &msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_RmqTtMsgStream_Insert(t *testing.T) {
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	ctx := context.Background()
	inputStream, outputStream := initRmqTtStream(ctx, producerChannels, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(ctx, &msgPack0)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	err = inputStream.Produce(ctx, &msgPack1)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	_, err = inputStream.Broadcast(ctx, &msgPack2)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack1.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_RmqTtMsgStream_DuplicatedIDs(t *testing.T) {
	c1 := funcutil.RandomString(8)
	producerChannels := []string{c1}
	consumerChannels := []string{c1}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))

	// would not dedup for non-dml messages
	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTsMsg(commonpb.MsgType_CreateCollection, 2))
	msgPack2.Msgs = append(msgPack2.Msgs, getTsMsg(commonpb.MsgType_CreateCollection, 2))

	msgPack3 := MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTimeTickMsg(15))

	ctx := context.Background()
	inputStream, outputStream := initRmqTtStream(ctx, producerChannels, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(ctx, &msgPack0)
	assert.NoError(t, err)
	err = inputStream.Produce(ctx, &msgPack1)
	assert.NoError(t, err)
	err = inputStream.Produce(ctx, &msgPack2)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(ctx, &msgPack3)
	assert.NoError(t, err)

	receivedMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg.Msgs), 3)
	assert.Equal(t, receivedMsg.BeginTs, uint64(0))
	assert.Equal(t, receivedMsg.EndTs, uint64(15))

	outputStream.Close()

	factory := ProtoUDFactory{}

	rmqClient, _ := rmq.NewClientWithDefaultOptions(ctx)
	outputStream, _ = NewMqTtMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	consumerSubName = funcutil.RandomString(8)
	outputStream.AsConsumer(ctx, consumerChannels, consumerSubName, mqcommon.SubscriptionPositionUnknown)
	outputStream.Seek(ctx, receivedMsg.StartPositions, false)
	seekMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(seekMsg.Msgs), 1+2)
	assert.EqualValues(t, seekMsg.Msgs[0].BeginTs(), 1)
	assert.Equal(t, commonpb.MsgType_CreateCollection, seekMsg.Msgs[1].Type())
	assert.Equal(t, commonpb.MsgType_CreateCollection, seekMsg.Msgs[2].Type())

	inputStream.Close()
	outputStream.Close()
}

func TestStream_RmqTtMsgStream_Seek(t *testing.T) {
	c1 := funcutil.RandomString(8)
	producerChannels := []string{c1}
	consumerChannels := []string{c1}
	consumerSubName := funcutil.RandomString(8)

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
	inputStream, outputStream := initRmqTtStream(ctx, producerChannels, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(ctx, &msgPack0)
	assert.NoError(t, err)
	err = inputStream.Produce(ctx, &msgPack1)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(ctx, &msgPack2)
	assert.NoError(t, err)
	err = inputStream.Produce(ctx, &msgPack3)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(ctx, &msgPack4)
	assert.NoError(t, err)
	err = inputStream.Produce(ctx, &msgPack5)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(ctx, &msgPack6)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(ctx, &msgPack7)
	assert.NoError(t, err)

	receivedMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg.Msgs), 2)
	assert.Equal(t, receivedMsg.BeginTs, uint64(0))
	assert.Equal(t, receivedMsg.EndTs, uint64(5))

	assert.Equal(t, receivedMsg.StartPositions[0].Timestamp, uint64(0))
	assert.Equal(t, receivedMsg.EndPositions[0].Timestamp, uint64(5))

	receivedMsg2 := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg2.Msgs), 1)
	assert.Equal(t, receivedMsg2.BeginTs, uint64(5))
	assert.Equal(t, receivedMsg2.EndTs, uint64(11))
	assert.Equal(t, receivedMsg2.StartPositions[0].Timestamp, uint64(5))
	assert.Equal(t, receivedMsg2.EndPositions[0].Timestamp, uint64(11))

	receivedMsg3 := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg3.Msgs), 3)
	assert.Equal(t, receivedMsg3.BeginTs, uint64(11))
	assert.Equal(t, receivedMsg3.EndTs, uint64(15))
	assert.Equal(t, receivedMsg3.StartPositions[0].Timestamp, uint64(11))
	assert.Equal(t, receivedMsg3.EndPositions[0].Timestamp, uint64(15))

	receivedMsg4 := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg4.Msgs), 1)
	assert.Equal(t, receivedMsg4.BeginTs, uint64(15))
	assert.Equal(t, receivedMsg4.EndTs, uint64(20))
	assert.Equal(t, receivedMsg4.StartPositions[0].Timestamp, uint64(15))
	assert.Equal(t, receivedMsg4.EndPositions[0].Timestamp, uint64(20))

	outputStream.Close()

	factory := ProtoUDFactory{}

	rmqClient, _ := rmq.NewClientWithDefaultOptions(ctx)
	outputStream, _ = NewMqTtMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	consumerSubName = funcutil.RandomString(8)
	outputStream.AsConsumer(ctx, consumerChannels, consumerSubName, mqcommon.SubscriptionPositionUnknown)

	outputStream.Seek(ctx, receivedMsg3.StartPositions, false)
	seekMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(seekMsg.Msgs), 3)
	result := []uint64{14, 12, 13}
	for i, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), result[i])
	}

	seekMsg2 := consumer(ctx, outputStream)
	assert.Equal(t, len(seekMsg2.Msgs), 1)
	for _, msg := range seekMsg2.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(19))
	}

	inputStream.Close()
	outputStream.Close()
}

func TestStream_RMqMsgStream_SeekInvalidMessage(t *testing.T) {
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	ctx := context.Background()
	inputStream, outputStream := initRmqStream(ctx, producerChannels, consumerChannels, consumerSubName)

	msgPack := &MsgPack{}
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := inputStream.Produce(ctx, msgPack)
	assert.NoError(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consumer(ctx, outputStream)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		seekPosition = result.EndPositions[0]
	}
	outputStream.Close()

	factory := ProtoUDFactory{}
	rmqClient2, _ := rmq.NewClientWithDefaultOptions(ctx)
	outputStream2, _ := NewMqMsgStream(ctx, 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(ctx, consumerChannels, funcutil.RandomString(8), mqcommon.SubscriptionPositionUnknown)

	id := common.Endian.Uint64(seekPosition.MsgID) + 10
	bs := make([]byte, 8)
	common.Endian.PutUint64(bs, id)
	p := []*msgpb.MsgPosition{
		{
			ChannelName: seekPosition.ChannelName,
			Timestamp:   seekPosition.Timestamp,
			MsgGroup:    seekPosition.MsgGroup,
			MsgID:       bs,
		},
	}

	err = outputStream2.Seek(ctx, p, false)
	assert.NoError(t, err)

	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = inputStream.Produce(ctx, msgPack)
	assert.NoError(t, err)

	result := consumer(ctx, outputStream2)
	assert.Equal(t, result.Msgs[0].ID(), int64(1))

	inputStream.Close()
	outputStream2.Close()
}

func TestStream_RmqTtMsgStream_AsConsumerWithPosition(t *testing.T) {
	producerChannels := []string{"insert1"}
	consumerChannels := []string{"insert1"}
	consumerSubName := "subInsert"
	ctx := context.Background()

	factory := ProtoUDFactory{}

	rmqClient, _ := rmq.NewClientWithDefaultOptions(context.Background())

	otherInputStream, _ := NewMqMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	otherInputStream.AsProducer(context.TODO(), []string{"root_timetick"})
	otherInputStream.Produce(ctx, getTimeTickMsgPack(999))

	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(context.TODO(), producerChannels)

	for i := 0; i < 100; i++ {
		inputStream.Produce(ctx, getTimeTickMsgPack(int64(i)))
	}

	rmqClient2, _ := rmq.NewClientWithDefaultOptions(context.Background())
	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(context.Background(), consumerChannels, consumerSubName, mqcommon.SubscriptionPositionLatest)

	inputStream.Produce(ctx, getTimeTickMsgPack(1000))
	pack := <-outputStream.Chan()
	assert.NotNil(t, pack)
	assert.Equal(t, 1, len(pack.Msgs))
	assert.EqualValues(t, 1000, pack.Msgs[0].BeginTs())

	inputStream.Close()
	outputStream.Close()
}
