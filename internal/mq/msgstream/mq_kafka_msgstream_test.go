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
	"log"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"

	kafkawrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/kafka"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
)

//	 Note: kafka does not support get all data when consuming from the earliest position again.
//func TestStream_KafkaTtMsgStream_NoSeek(t *testing.T) {
//	kafkaAddress := Params.Get("kafka.brokerList")
//	c1 := funcutil.RandomString(8)
//	producerChannels := []string{c1}
//	consumerChannels := []string{c1}
//	consumerSubName := funcutil.RandomString(8)
//
//	msgPack0 := MsgPack{}
//	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))
//
//	msgPack1 := MsgPack{}
//	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
//	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 19))
//
//	msgPack2 := MsgPack{}
//	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))
//
//	msgPack3 := MsgPack{}
//	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 14))
//	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 9))
//
//	msgPack4 := MsgPack{}
//	msgPack4.Msgs = append(msgPack4.Msgs, getTimeTickMsg(11))
//
//	msgPack5 := MsgPack{}
//	msgPack5.Msgs = append(msgPack5.Msgs, getTimeTickMsg(15))
//
//	ctx := context.Background()
//	inputStream := getKafkaInputStream(ctx, kafkaAddress, producerChannels)
//	outputStream := getKafkaTtOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)
//
//	err := inputStream.Broadcast(&msgPack0)
//	assert.Nil(t, err)
//	err = inputStream.Produce(&msgPack1)
//	assert.Nil(t, err)
//	err = inputStream.Broadcast(&msgPack2)
//	assert.Nil(t, err)
//	err = inputStream.Produce(&msgPack3)
//	assert.Nil(t, err)
//	err = inputStream.Broadcast(&msgPack4)
//	assert.Nil(t, err)
//	err = inputStream.Broadcast(&msgPack5)
//	assert.Nil(t, err)
//
//	o1 := consumer(ctx, outputStream)
//	o2 := consumer(ctx, outputStream)
//	o3 := consumer(ctx, outputStream)
//
//	t.Log(o1.BeginTs)
//	t.Log(o2.BeginTs)
//	t.Log(o3.BeginTs)
//	outputStream.Close()
//
//	outputStream2 := getKafkaTtOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)
//	p1 := consumer(ctx, outputStream2)
//	p2 := consumer(ctx, outputStream2)
//	p3 := consumer(ctx, outputStream2)
//	t.Log(p1.BeginTs)
//	t.Log(p2.BeginTs)
//	t.Log(p3.BeginTs)
//	outputStream2.Close()
//
//	assert.Equal(t, o1.BeginTs, p1.BeginTs)
//	assert.Equal(t, o2.BeginTs, p2.BeginTs)
//	assert.Equal(t, o3.BeginTs, p3.BeginTs)
//}

func skipTest(t *testing.T) {
	t.Skip("skip kafka test")
}

func TestStream_KafkaMsgStream_SeekToLast(t *testing.T) {
	skipTest(t)

	kafkaAddress := Params.Get("kafka.brokerList")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := &MsgPack{}
	ctx := context.Background()
	inputStream := getKafkaInputStream(ctx, kafkaAddress, producerChannels)
	defer inputStream.Close()

	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	// produce test data
	err := inputStream.Produce(msgPack)
	assert.Nil(t, err)

	// pick a seekPosition
	var seekPosition *internalpb.MsgPosition
	outputStream := getKafkaOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)
	for i := 0; i < 10; i++ {
		result := consumer(ctx, outputStream)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
			break
		}
	}
	outputStream.Close()

	// create a consumer can consume data from seek position to last msg
	outputStream2 := getKafkaOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)
	lastMsgID, err := outputStream2.GetLatestMsgID(c)
	defer outputStream2.Close()
	assert.Nil(t, err)

	err = outputStream2.Seek([]*internalpb.MsgPosition{seekPosition})
	assert.Nil(t, err)
	outputStream2.Start()

	cnt := 0
	var value int64 = 6
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			hasMore = false
		case msgPack, ok := <-outputStream2.Chan():
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

func TestStream_KafkaTtMsgStream_Seek(t *testing.T) {
	skipTest(t)

	kafkaAddress := Params.Get("kafka.brokerList")
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
	inputStream := getKafkaInputStream(ctx, kafkaAddress, producerChannels)
	outputStream := getKafkaTtOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)

	err := inputStream.Broadcast(&msgPack0)
	assert.Nil(t, err)
	err = inputStream.Produce(&msgPack1)
	assert.Nil(t, err)
	err = inputStream.Broadcast(&msgPack2)
	assert.Nil(t, err)
	err = inputStream.Produce(&msgPack3)
	assert.Nil(t, err)
	err = inputStream.Broadcast(&msgPack4)
	assert.Nil(t, err)
	err = inputStream.Produce(&msgPack5)
	assert.Nil(t, err)
	err = inputStream.Broadcast(&msgPack6)
	assert.Nil(t, err)
	err = inputStream.Broadcast(&msgPack7)
	assert.Nil(t, err)

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

	outputStream = getKafkaTtOutputStreamAndSeek(ctx, kafkaAddress, receivedMsg3.StartPositions)
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

	outputStream2 := getKafkaTtOutputStreamAndSeek(ctx, kafkaAddress, receivedMsg3.EndPositions)
	seekMsg = consumer(ctx, outputStream2)
	assert.Equal(t, len(seekMsg.Msgs), 1)
	for _, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(19))
	}

	inputStream.Close()
	outputStream2.Close()
}

func TestStream_KafkaTtMsgStream_1(t *testing.T) {
	skipTest(t)

	kafkaAddress := Params.Get("kafka.brokerList")
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	p2Channels := []string{c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	ctx := context.Background()
	inputStream1 := getKafkaInputStream(ctx, kafkaAddress, p1Channels)
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))

	inputStream2 := getKafkaInputStream(ctx, kafkaAddress, p2Channels)
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream2, msgPacks2))

	// consume msg
	outputStream := getKafkaTtOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)
	checkNMsgPack := func(t *testing.T, outputStream MsgStream, num int) int {
		rcvMsg := 0
		for i := 0; i < num; i++ {
			msgPack := consumer(ctx, outputStream)
			rcvMsg += len(msgPack.Msgs)
			if len(msgPack.Msgs) > 0 {
				for _, msg := range msgPack.Msgs {
					log.Println("msg type: ", msg.Type(), ", msg value: ", msg)
					assert.Greater(t, msg.BeginTs(), msgPack.BeginTs)
					assert.LessOrEqual(t, msg.BeginTs(), msgPack.EndTs)
				}
			}
		}
		return rcvMsg
	}
	msgCount := checkNMsgPack(t, outputStream, len(msgPacks1)/2)
	cnt1 := (len(msgPacks1)/2 - 1) * len(msgPacks1[0].Msgs)
	cnt2 := (len(msgPacks2)/2 - 1) * len(msgPacks2[0].Msgs)
	assert.Equal(t, (cnt1 + cnt2), msgCount)

	inputStream1.Close()
	inputStream2.Close()
	outputStream.Close()
}

func TestStream_KafkaTtMsgStream_2(t *testing.T) {
	skipTest(t)

	kafkaAddress := Params.Get("kafka.brokerList")
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	p2Channels := []string{c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	ctx := context.Background()
	inputStream1 := getKafkaInputStream(ctx, kafkaAddress, p1Channels)
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))

	inputStream2 := getKafkaInputStream(ctx, kafkaAddress, p2Channels)
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream2, msgPacks2))

	// consume msg
	log.Println("=============receive msg===================")
	rcvMsgPacks := make([]*MsgPack, 0)

	resumeMsgPack := func(t *testing.T) int {
		var outputStream MsgStream
		msgCount := len(rcvMsgPacks)
		if msgCount == 0 {
			outputStream = getKafkaTtOutputStream(ctx, kafkaAddress, consumerChannels, consumerSubName)
		} else {
			outputStream = getKafkaTtOutputStreamAndSeek(ctx, kafkaAddress, rcvMsgPacks[msgCount-1].EndPositions)
		}
		msgPack := consumer(ctx, outputStream)
		rcvMsgPacks = append(rcvMsgPacks, msgPack)
		if len(msgPack.Msgs) > 0 {
			for _, msg := range msgPack.Msgs {
				log.Println("msg type: ", msg.Type(), ", msg value: ", msg)
				assert.Greater(t, msg.BeginTs(), msgPack.BeginTs)
				assert.LessOrEqual(t, msg.BeginTs(), msgPack.EndTs)
			}
			log.Println("================")
		}
		outputStream.Close()
		return len(rcvMsgPacks[msgCount].Msgs)
	}

	msgCount := 0
	for i := 0; i < len(msgPacks1)/2; i++ {
		msgCount += resumeMsgPack(t)
	}
	cnt1 := (len(msgPacks1)/2 - 1) * len(msgPacks1[0].Msgs)
	cnt2 := (len(msgPacks2)/2 - 1) * len(msgPacks2[0].Msgs)
	assert.Equal(t, (cnt1 + cnt2), msgCount)

	inputStream1.Close()
	inputStream2.Close()
}

func TestStream_KafkaTtMsgStream_DataNodeTimetickMsgstream(t *testing.T) {
	skipTest(t)

	kafkaAddress := Params.Get("kafka.brokerList")
	c1 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	consumerChannels := []string{c1}
	consumerSubName := funcutil.RandomString(8)

	ctx := context.Background()

	factory := ProtoUDFactory{}
	kafkaClient := kafkawrapper.NewKafkaClientInstance(kafkaAddress)
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, kafkaClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumerWithPosition(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionLatest)
	outputStream.Start()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case msgPack, ok := <-outputStream.Chan():
				assert.True(t, ok)
				assert.NotNil(t, msgPack)

				if len(msgPack.Msgs) > 0 {
					fmt.Println("msg===:", msgPack.Msgs[0])
					wg.Done()
					return
				}
			}
		}
	}()

	// make producer start to produce messages after invoking Chan
	time.Sleep(5 * time.Second)

	inputStream1 := getKafkaInputStream(ctx, kafkaAddress, p1Channels)
	msgPacks1 := createRandMsgPacks(2, 1, 1)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))
	wg.Wait()

	defer outputStream.Close()
	defer inputStream1.Close()
}

func getKafkaInputStream(ctx context.Context, kafkaAddress string, producerChannels []string, opts ...RepackFunc) MsgStream {
	factory := ProtoUDFactory{}
	kafkaClient := kafkawrapper.NewKafkaClientInstance(kafkaAddress)
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, kafkaClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	return inputStream
}

func getKafkaOutputStream(ctx context.Context, kafkaAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	kafkaClient := kafkawrapper.NewKafkaClientInstance(kafkaAddress)
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, kafkaClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	return outputStream
}

func getKafkaTtOutputStream(ctx context.Context, kafkaAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	kafkaClient := kafkawrapper.NewKafkaClientInstance(kafkaAddress)
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, kafkaClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	return outputStream
}

func getKafkaTtOutputStreamAndSeek(ctx context.Context, kafkaAddress string, positions []*MsgPosition) MsgStream {
	factory := ProtoUDFactory{}
	kafkaClient := kafkawrapper.NewKafkaClientInstance(kafkaAddress)
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, kafkaClient, factory.NewUnmarshalDispatcher())
	consumerName := []string{}
	for _, c := range positions {
		consumerName = append(consumerName, c.ChannelName)
	}
	outputStream.AsConsumer(consumerName, funcutil.RandomString(8))
	outputStream.Seek(positions)
	outputStream.Start()
	return outputStream
}
