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
	"math/rand"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	pulsarwrapper "github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	DefaultPulsarTenant    = "public"
	DefaultPulsarNamespace = "default"
)

var Params paramtable.ComponentParam

func TestMain(m *testing.M) {
	Params.Init()
	mockKafkaCluster, err := kafka.NewMockCluster(1)
	defer mockKafkaCluster.Close()
	if err != nil {
		// nolint
		fmt.Printf("Failed to create MockCluster: %s\n", err)
		os.Exit(1)
	}
	broker := mockKafkaCluster.BootstrapServers()
	Params.Save("kafka.brokerList", broker)

	exitCode := m.Run()
	os.Exit(exitCode)
}

func getPulsarAddress() string {
	pulsarHost := Params.GetWithDefault("pulsar.address", "")
	port := Params.GetWithDefault("pulsar.port", "")
	if len(pulsarHost) != 0 && len(port) != 0 {
		return "pulsar://" + pulsarHost + ":" + port
	}
	panic("invalid pulsar address")
}

func getKafkaBrokerList() string {
	brokerList := Params.Get("kafka.brokerList")
	log.Printf("kafka broker list: %s", brokerList)
	return brokerList
}

func consumer(ctx context.Context, mq MsgStream) *MsgPack {
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

func TestStream_PulsarMsgStream_Insert(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)
	ctx := context.Background()

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_Delete(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 1))

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_TimeTick(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3))

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_BroadCast(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3))

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	receiveMsg(ctx, outputStream, len(consumerChannels)*len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_RepackFunc(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels, repackFunc)
	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)
	err := inputStream.Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_InsertRepackFunc(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)
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

	factory := ProtoUDFactory{}

	ctx := context.Background()
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)

	pulsarClient2, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	var output MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, output, len(msgPack.Msgs)*2)
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_DeleteRepackFunc(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

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

	factory := ProtoUDFactory{}
	ctx := context.Background()
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)

	pulsarClient2, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	var output MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, output, len(msgPack.Msgs)*1)
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_DefaultRepackFunc(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 3))

	factory := ProtoUDFactory{}

	ctx := context.Background()
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)

	pulsarClient2, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	var output MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, output, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarTtMsgStream_Insert(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)
	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	err = inputStream.Produce(&msgPack1)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	_, err = inputStream.Broadcast(&msgPack2)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack1.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarTtMsgStream_NoSeek(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1 := funcutil.RandomString(8)
	producerChannels := []string{c1}
	consumerChannels := []string{c1}
	consumerSubName := funcutil.RandomString(8)

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

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack1)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack2)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack3)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack4)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack5)
	assert.NoError(t, err)

	o1 := consumer(ctx, outputStream)
	o2 := consumer(ctx, outputStream)
	o3 := consumer(ctx, outputStream)

	t.Log(o1.BeginTs)
	t.Log(o2.BeginTs)
	t.Log(o3.BeginTs)
	outputStream.Close()

	outputStream2 := getPulsarTtOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)
	p1 := consumer(ctx, outputStream2)
	p2 := consumer(ctx, outputStream2)
	p3 := consumer(ctx, outputStream2)
	t.Log(p1.BeginTs)
	t.Log(p2.BeginTs)
	t.Log(p3.BeginTs)
	outputStream2.Close()

	assert.Equal(t, o1.BeginTs, p1.BeginTs)
	assert.Equal(t, o2.BeginTs, p2.BeginTs)
	assert.Equal(t, o3.BeginTs, p3.BeginTs)
}

func TestStream_PulsarMsgStream_SeekToLast(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := &MsgPack{}
	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	defer inputStream.Close()

	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	// produce test data
	err := inputStream.Produce(msgPack)
	assert.NoError(t, err)

	// pick a seekPosition
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consumer(ctx, outputStream)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
		}
	}
	outputStream.Close()

	// create a consumer can consume data from seek position to last msg
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream2, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	lastMsgID, err := outputStream2.GetLatestMsgID(c)
	defer outputStream2.Close()
	assert.NoError(t, err)

	err = outputStream2.Seek([]*msgpb.MsgPosition{seekPosition})
	assert.NoError(t, err)

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

func TestStream_PulsarTtMsgStream_Seek(t *testing.T) {
	pulsarAddress := getPulsarAddress()
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
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack1)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack2)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack3)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack4)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack5)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack6)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack7)
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

	outputStream = getPulsarTtOutputStreamAndSeek(ctx, pulsarAddress, receivedMsg3.StartPositions)
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

	outputStream2 := getPulsarTtOutputStreamAndSeek(ctx, pulsarAddress, receivedMsg3.EndPositions)
	seekMsg = consumer(ctx, outputStream2)
	assert.Equal(t, len(seekMsg.Msgs), 1)
	for _, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(19))
	}

	inputStream.Close()
	outputStream2.Close()
}

func TestStream_PulsarTtMsgStream_UnMarshalHeader(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	err = inputStream.Produce(&msgPack1)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	_, err = inputStream.Broadcast(&msgPack2)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack1.Msgs))
	inputStream.Close()
	outputStream.Close()
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

// This testcase will generate MsgPacks as following:
//
//	     Insert     Insert     Insert     Insert     Insert     Insert
//	c1 |----------|----------|----------|----------|----------|----------|
//	              ^          ^          ^          ^          ^          ^
//	            TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
//
//	     Insert     Insert     Insert     Insert     Insert     Insert
//	c2 |----------|----------|----------|----------|----------|----------|
//	              ^          ^          ^          ^          ^          ^
//	            TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
//
// Then check:
//  1. For each msg in MsgPack received by ttMsgStream consumer, there should be
//     msgPack.BeginTs < msg.BeginTs() <= msgPack.EndTs
//  2. The count of consumed msg should be equal to the count of produced msg
func TestStream_PulsarTtMsgStream_1(t *testing.T) {
	pulsarAddr := getPulsarAddress()
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	p2Channels := []string{c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	ctx := context.Background()
	inputStream1 := getPulsarInputStream(ctx, pulsarAddr, p1Channels)
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))

	inputStream2 := getPulsarInputStream(ctx, pulsarAddr, p2Channels)
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream2, msgPacks2))

	// consume msg
	outputStream := getPulsarTtOutputStream(ctx, pulsarAddr, consumerChannels, consumerSubName)
	log.Println("===============receive msg=================")
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
				log.Println("================")
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
func TestStream_PulsarTtMsgStream_2(t *testing.T) {
	pulsarAddr := getPulsarAddress()
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	p2Channels := []string{c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	ctx := context.Background()
	inputStream1 := getPulsarInputStream(ctx, pulsarAddr, p1Channels)
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))

	inputStream2 := getPulsarInputStream(ctx, pulsarAddr, p2Channels)
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream2, msgPacks2))

	// consume msg
	log.Println("=============receive msg===================")
	rcvMsgPacks := make([]*MsgPack, 0)

	resumeMsgPack := func(t *testing.T) int {
		var outputStream MsgStream
		msgCount := len(rcvMsgPacks)
		if msgCount == 0 {
			outputStream = getPulsarTtOutputStream(ctx, pulsarAddr, consumerChannels, consumerSubName)
		} else {
			outputStream = getPulsarTtOutputStreamAndSeek(ctx, pulsarAddr, rcvMsgPacks[msgCount-1].EndPositions)
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

func TestStream_MqMsgStream_Seek(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := &MsgPack{}
	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, consumerSubName)

	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := inputStream.Produce(msgPack)
	assert.NoError(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consumer(ctx, outputStream)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
		}
	}
	outputStream.Close()

	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream2, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	outputStream2.Seek([]*msgpb.MsgPosition{seekPosition})

	for i := 6; i < 10; i++ {
		result := consumer(ctx, outputStream2)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
	}
	outputStream2.Close()

}

func TestStream_MqMsgStream_SeekInvalidMessage(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}

	msgPack := &MsgPack{}
	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)
	defer inputStream.Close()

	outputStream := getPulsarOutputStream(ctx, pulsarAddress, consumerChannels, funcutil.RandomString(8))
	defer outputStream.Close()

	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := inputStream.Produce(msgPack)
	assert.NoError(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consumer(ctx, outputStream)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		seekPosition = result.EndPositions[0]
	}

	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream2, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(consumerChannels, funcutil.RandomString(8), mqwrapper.SubscriptionPositionEarliest)
	defer outputStream2.Close()
	messageID, _ := pulsar.DeserializeMessageID(seekPosition.MsgID)
	// try to seek to not written position
	patchMessageID(&messageID, 13)

	p := []*msgpb.MsgPosition{
		{
			ChannelName: seekPosition.ChannelName,
			Timestamp:   seekPosition.Timestamp,
			MsgGroup:    seekPosition.MsgGroup,
			MsgID:       messageID.Serialize(),
		},
	}

	err = outputStream2.Seek(p)
	assert.NoError(t, err)

	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = inputStream.Produce(msgPack)
	assert.NoError(t, err)
	result := consumer(ctx, outputStream2)
	assert.Equal(t, result.Msgs[0].ID(), int64(1))
}

func TestStream_MqMsgStream_SeekLatest(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := &MsgPack{}
	ctx := context.Background()
	inputStream := getPulsarInputStream(ctx, pulsarAddress, producerChannels)

	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := inputStream.Produce(msgPack)
	assert.NoError(t, err)
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream2, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionLatest)

	msgPack.Msgs = nil
	// produce another 10 tsMs
	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = inputStream.Produce(msgPack)
	assert.NoError(t, err)

	for i := 10; i < 20; i++ {
		result := consumer(ctx, outputStream2)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
	}

	inputStream.Close()
	outputStream2.Close()
}

func TestStream_BroadcastMark(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	producerChannels := []string{c1, c2}

	factory := ProtoUDFactory{}
	pulsarClient, err := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)
	outputStream, err := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	assert.NoError(t, err)

	// add producer channels
	outputStream.AsProducer(producerChannels)

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	ids, err := outputStream.Broadcast(&msgPack0)
	assert.NoError(t, err)
	assert.NotNil(t, ids)
	assert.Equal(t, len(producerChannels), len(ids))
	for _, c := range producerChannels {
		ids, ok := ids[c]
		assert.True(t, ok)
		assert.Equal(t, len(msgPack0.Msgs), len(ids))
	}

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	ids, err = outputStream.Broadcast(&msgPack1)
	assert.NoError(t, err)
	assert.NotNil(t, ids)
	assert.Equal(t, len(producerChannels), len(ids))
	for _, c := range producerChannels {
		ids, ok := ids[c]
		assert.True(t, ok)
		assert.Equal(t, len(msgPack1.Msgs), len(ids))
	}

	// edge cases
	_, err = outputStream.Broadcast(nil)
	assert.Error(t, err)

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, &MarshalFailTsMsg{})
	_, err = outputStream.Broadcast(&msgPack2)
	assert.Error(t, err)

	// mock send fail
	for k, p := range outputStream.producers {
		outputStream.producers[k] = &mockSendFailProducer{Producer: p}
	}
	_, err = outputStream.Broadcast(&msgPack1)
	assert.Error(t, err)

	outputStream.Close()
}

var _ TsMsg = (*MarshalFailTsMsg)(nil)

type MarshalFailTsMsg struct {
	BaseMsg
}

func (t *MarshalFailTsMsg) ID() UniqueID {
	return 0
}

func (t *MarshalFailTsMsg) SetID(id UniqueID) {
	// do nothing
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

func (t *MarshalFailTsMsg) Size() int {
	return 0
}

var _ mqwrapper.Producer = (*mockSendFailProducer)(nil)

type mockSendFailProducer struct {
	mqwrapper.Producer
}

func (p *mockSendFailProducer) Send(_ context.Context, _ *mqwrapper.ProducerMessage) (MessageID, error) {
	return nil, errors.New("mocked error")
}

/* ========================== Utility functions ========================== */
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
			msgPack.Msgs = append(msgPack.Msgs, getInsertMsgUniqueID(int64(reqID))) //getTsMsg(commonpb.MsgType_Insert, int64(reqID)))
		}
	}
	return &msgPack
}

func getInsertMsgPack(ts []int) *MsgPack {
	msgPack := MsgPack{}
	for i := 0; i < len(ts); i++ {
		msgPack.Msgs = append(msgPack.Msgs, getInsertMsgUniqueID(int64(ts[i]))) //getTsMsg(commonpb.MsgType_Insert, int64(ts[i])))
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

func getPulsarInputStream(ctx context.Context, pulsarAddress string, producerChannels []string, opts ...RepackFunc) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	return inputStream
}

func getPulsarOutputStream(ctx context.Context, pulsarAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	return outputStream
}

func getPulsarTtOutputStream(ctx context.Context, pulsarAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionEarliest)
	return outputStream
}

func getPulsarTtOutputStreamAndSeek(ctx context.Context, pulsarAddress string, positions []*MsgPosition) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	consumerName := []string{}
	for _, c := range positions {
		consumerName = append(consumerName, c.ChannelName)
	}
	outputStream.AsConsumer(consumerName, funcutil.RandomString(8), mqwrapper.SubscriptionPositionUnknown)
	outputStream.Seek(positions)
	return outputStream
}

func receiveMsg(ctx context.Context, outputStream MsgStream, msgCount int) {
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

func patchMessageID(mid *pulsar.MessageID, entryID int64) {
	// use direct unsafe conversion
	/* #nosec G103 */
	r := (*iface)(unsafe.Pointer(mid))
	id := (*messageID)(r.Data)
	id.entryID = entryID
}

// unsafe access pointer, same as pulsar.messageID
type messageID struct {
	ledgerID     int64
	entryID      int64
	batchID      int32
	partitionIdx int32
}

// interface struct mapping
type iface struct {
	Type, Data unsafe.Pointer
}
