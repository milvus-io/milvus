// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package msgstream

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/mqclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	client "github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
	"github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestStream_PulsarMsgStream_Insert(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_Delete(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 1))
	//msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 3, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_Search(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Search, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Search, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_SearchResult(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_SearchResult, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_SearchResult, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_TimeTick(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_BroadCast(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Broadcast(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(consumerChannels)*len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_RepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels, repackFunc)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)
	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_InsertRepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{1, 3},
	}

	insertRequest := internalpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ChannelID:      "1",
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

	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	pulsarClient2, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(output, len(msgPack.Msgs)*2)
	(*inputStream).Close()
	(*outputStream).Close()
}

//func TestStream_PulsarMsgStream_DeleteRepackFunc(t *testing.T) {
//	pulsarAddress, _ := Params.Load("_PulsarAddress")
//	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
//	producerChannels := []string{c1, c2}
//	consumerChannels := []string{c1, c2}
//	consumerSubName := funcutil.RandomString(8)
//
//	baseMsg := BaseMsg{
//		BeginTimestamp: 0,
//		EndTimestamp:   0,
//		HashValues:     []uint32{1, 3},
//	}
//
//	deleteRequest := internalpb.DeleteRequest{
//		Base: &commonpb.MsgBase{
//			MsgType:   commonpb.MsgType_Delete,
//			MsgID:     1,
//			Timestamp: 1,
//			SourceID:  1,
//		},
//		CollectionName: "Collection",
//		ChannelID:      "1",
//		Timestamps:     []Timestamp{1, 1},
//		PrimaryKeys:    []int64{1, 3},
//	}
//	deleteMsg := &DeleteMsg{
//		BaseMsg:       baseMsg,
//		DeleteRequest: deleteRequest,
//	}
//
//	msgPack := MsgPack{}
//	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)
//
//	factory := ProtoUDFactory{}
//	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
//	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
//	inputStream.AsProducer(producerChannels)
//	inputStream.Start()
//
//	pulsarClient2, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
//	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
//	outputStream.AsConsumer(consumerChannels, consumerSubName)
//	outputStream.Start()
//	var output MsgStream = outputStream
//
//	err := (*inputStream).Produce(&msgPack)
//	if err != nil {
//		log.Fatalf("produce error = %v", err)
//	}
//	receiveMsg(output, len(msgPack.Msgs)*2)
//	(*inputStream).Close()
//	(*outputStream).Close()
//}

func TestStream_PulsarMsgStream_DefaultRepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Search, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_SearchResult, 3))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_QueryNodeStats, 4))

	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	pulsarClient2, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(output, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarTtMsgStream_Insert(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
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

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Broadcast(&msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = inputStream.Produce(&msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = inputStream.Broadcast(&msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack1.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarTtMsgStream_NoSeek(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
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

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(pulsarAddress, consumerChannels, consumerSubName)

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
	err = inputStream.Broadcast(&msgPack5)
	assert.Nil(t, err)

	o1 := outputStream.Consume()
	o2 := outputStream.Consume()
	o3 := outputStream.Consume()

	t.Log(o1.BeginTs)
	t.Log(o2.BeginTs)
	t.Log(o3.BeginTs)
	outputStream.Close()

	outputStream = getPulsarTtOutputStream(pulsarAddress, consumerChannels, consumerSubName)
	p1 := outputStream.Consume()
	p2 := outputStream.Consume()
	p3 := outputStream.Consume()
	t.Log(p1.BeginTs)
	t.Log(p2.BeginTs)
	t.Log(p3.BeginTs)
	outputStream.Close()

	assert.Equal(t, o1.BeginTs, p1.BeginTs)
	assert.Equal(t, o2.BeginTs, p2.BeginTs)
	assert.Equal(t, o3.BeginTs, p3.BeginTs)

}
func TestStream_PulsarTtMsgStream_Seek(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
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

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(pulsarAddress, consumerChannels, consumerSubName)

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

	outputStream.Consume()
	receivedMsg := outputStream.Consume()
	outputStream.Close()
	outputStream = getPulsarTtOutputStreamAndSeek(pulsarAddress, receivedMsg.EndPositions)

	err = inputStream.Broadcast(&msgPack5)
	assert.Nil(t, err)
	seekMsg := outputStream.Consume()
	for _, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(14))
	}
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarTtMsgStream_UnMarshalHeader(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
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

	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarTtOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	err := inputStream.Broadcast(&msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = inputStream.Produce(&msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = inputStream.Broadcast(&msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack1.Msgs))
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
			if err := ms.Broadcast(msgPacks[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

//
// This testcase will generate MsgPacks as following:
//
//       Insert     Insert     Insert     Insert     Insert     Insert
//  c1 |----------|----------|----------|----------|----------|----------|
//                ^          ^          ^          ^          ^          ^
//              TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
//
//       Insert     Insert     Insert     Insert     Insert     Insert
//  c2 |----------|----------|----------|----------|----------|----------|
//                ^          ^          ^          ^          ^          ^
//              TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
// Then check:
//   1. For each msg in MsgPack received by ttMsgStream consumer, there should be
//        msgPack.BeginTs < msg.BeginTs() <= msgPack.EndTs
//   2. The count of consumed msg should be equal to the count of produced msg
//
func TestStream_PulsarTtMsgStream_1(t *testing.T) {
	pulsarAddr, _ := Params.Load("_PulsarAddress")
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	p2Channels := []string{c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	inputStream1 := getPulsarInputStream(pulsarAddr, p1Channels)
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))

	inputStream2 := getPulsarInputStream(pulsarAddr, p2Channels)
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream2, msgPacks2))

	// consume msg
	outputStream := getPulsarTtOutputStream(pulsarAddr, consumerChannels, consumerSubName)
	log.Println("===============receive msg=================")
	checkNMsgPack := func(t *testing.T, outputStream MsgStream, num int) int {
		rcvMsg := 0
		for i := 0; i < num; i++ {
			msgPack := outputStream.Consume()
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

//
// This testcase will generate MsgPacks as following:
//
//      Insert     Insert     Insert     Insert     Insert     Insert
// c1 |----------|----------|----------|----------|----------|----------|
//               ^          ^          ^          ^          ^          ^
//             TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
//
//      Insert     Insert     Insert     Insert     Insert     Insert
// c2 |----------|----------|----------|----------|----------|----------|
//               ^          ^          ^          ^          ^          ^
//             TT(10)     TT(20)     TT(30)     TT(40)     TT(50)     TT(100)
// Then check:
//   1. ttMsgStream consumer can seek to the right position and resume
//   2. The count of consumed msg should be equal to the count of produced msg
//
func TestStream_PulsarTtMsgStream_2(t *testing.T) {
	pulsarAddr, _ := Params.Load("_PulsarAddress")
	c1 := funcutil.RandomString(8)
	c2 := funcutil.RandomString(8)
	p1Channels := []string{c1}
	p2Channels := []string{c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	inputStream1 := getPulsarInputStream(pulsarAddr, p1Channels)
	msgPacks1 := createRandMsgPacks(3, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream1, msgPacks1))

	inputStream2 := getPulsarInputStream(pulsarAddr, p2Channels)
	msgPacks2 := createRandMsgPacks(5, 10, 10)
	assert.Nil(t, sendMsgPacks(inputStream2, msgPacks2))

	// consume msg
	log.Println("=============receive msg===================")
	rcvMsgPacks := make([]*MsgPack, 0)

	resumeMsgPack := func(t *testing.T) int {
		var outputStream MsgStream
		msgCount := len(rcvMsgPacks)
		if msgCount == 0 {
			outputStream = getPulsarTtOutputStream(pulsarAddr, consumerChannels, consumerSubName)
		} else {
			outputStream = getPulsarTtOutputStreamAndSeek(pulsarAddr, rcvMsgPacks[msgCount-1].EndPositions)
		}
		msgPack := outputStream.Consume()
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
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := &MsgPack{}
	inputStream := getPulsarInputStream(pulsarAddress, producerChannels)
	outputStream := getPulsarOutputStream(pulsarAddress, consumerChannels, consumerSubName)

	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := inputStream.Produce(msgPack)
	assert.Nil(t, err)
	var seekPosition *internalpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := outputStream.Consume()
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		if i == 5 {
			seekPosition = result.EndPositions[0]
		}
	}
	outputStream.Close()

	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream2, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(consumerChannels, consumerSubName)
	outputStream2.Seek([]*internalpb.MsgPosition{seekPosition})
	outputStream2.Start()

	for i := 6; i < 10; i++ {
		result := outputStream2.Consume()
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
	}
	outputStream2.Close()

}

/****************************************Rmq test******************************************/

func initRmq(name string) *etcdkv.EtcdKV {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	etcdKV, err := etcdkv.NewEtcdKV(etcdEndpoints, "/etcd/test/root")
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	err = rocksmq.InitRmq(name, idAllocator)

	if err != nil {
		log.Fatalf("InitRmq error = %v", err)
	}
	return etcdKV
}

func Close(rocksdbName string, intputStream, outputStream MsgStream, etcdKV *etcdkv.EtcdKV) {
	rocksmq.CloseRocksMQ()
	intputStream.Close()
	outputStream.Close()
	etcdKV.Close()
	err := os.RemoveAll(rocksdbName)
	_ = os.RemoveAll(rocksdbName + "_meta_kv")
	log.Println(err)
}

func initRmqStream(producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...RepackFunc) (MsgStream, MsgStream) {
	factory := ProtoUDFactory{}

	rmqClient, _ := mqclient.NewRmqClient(client.ClientOptions{Server: rocksmq.Rmq})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	rmqClient2, _ := mqclient.NewRmqClient(client.ClientOptions{Server: rocksmq.Rmq})
	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerGroupName)
	outputStream.Start()
	var output MsgStream = outputStream

	return input, output
}

func initRmqTtStream(producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...RepackFunc) (MsgStream, MsgStream) {
	factory := ProtoUDFactory{}

	rmqClient, _ := mqclient.NewRmqClient(client.ClientOptions{Server: rocksmq.Rmq})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	rmqClient2, _ := mqclient.NewRmqClient(client.ClientOptions{Server: rocksmq.Rmq})
	outputStream, _ := NewMqTtMsgStream(context.Background(), 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerGroupName)
	outputStream.Start()
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

	rocksdbName := "/tmp/rocksmq_insert"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerGroupName)
	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
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

	rocksdbName := "/tmp/rocksmq_insert_tt"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqTtStream(producerChannels, consumerChannels, consumerSubName)

	err := inputStream.Broadcast(&msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = inputStream.Produce(&msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = inputStream.Broadcast(&msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack1.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
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
		insertRequest := internalpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     reqID,
				Timestamp: time,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ChannelID:      "0",
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
		deleteRequest := internalpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			ChannelID:      "1",
			Timestamp:      Timestamp(1),
		}
		deleteMsg := &DeleteMsg{
			BaseMsg:       baseMsg,
			DeleteRequest: deleteRequest,
		}
		return deleteMsg
	case commonpb.MsgType_Search:
		searchRequest := internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			ResultChannelID: "0",
		}
		searchMsg := &SearchMsg{
			BaseMsg:       baseMsg,
			SearchRequest: searchRequest,
		}
		return searchMsg
	case commonpb.MsgType_SearchResult:
		searchResult := internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SearchResult,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			ResultChannelID: "0",
		}
		searchResultMsg := &SearchResultMsg{
			BaseMsg:       baseMsg,
			SearchResults: searchResult,
		}
		return searchResultMsg
	case commonpb.MsgType_TimeTick:
		timeTickResult := internalpb.TimeTickMsg{
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
	case commonpb.MsgType_QueryNodeStats:
		queryNodeSegStats := internalpb.QueryNodeStats{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_QueryNodeStats,
				SourceID: reqID,
			},
		}
		queryNodeSegStatsMsg := &QueryNodeStatsMsg{
			BaseMsg:        baseMsg,
			QueryNodeStats: queryNodeSegStats,
		}
		return queryNodeSegStatsMsg
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
	timeTickResult := internalpb.TimeTickMsg{
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
			msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, int64(reqID)))
		}
	}
	return &msgPack
}

func getInsertMsgPack(ts []int) *MsgPack {
	msgPack := MsgPack{}
	for i := 0; i < len(ts); i++ {
		msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, int64(ts[i])))
	}
	return &msgPack
}

func getTimeTickMsgPack(reqID UniqueID) *MsgPack {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTimeTickMsg(reqID))
	return &msgPack
}

func getPulsarInputStream(pulsarAddress string, producerChannels []string, opts ...RepackFunc) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	return inputStream
}

func getPulsarOutputStream(pulsarAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	return outputStream
}

func getPulsarTtOutputStream(pulsarAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	return outputStream
}

func getPulsarTtOutputStreamAndSeek(pulsarAddress string, positions []*MsgPosition) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	consumerName := []string{}
	for _, c := range positions {
		consumerName = append(consumerName, c.ChannelName)
	}
	outputStream.AsConsumer(consumerName, positions[0].MsgGroup)
	outputStream.Seek(positions)
	outputStream.Start()
	return outputStream
}

func receiveMsg(outputStream MsgStream, msgCount int) {
	receiveCount := 0
	for {
		result := outputStream.Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				log.Println("msg type: ", v.Type(), ", msg value: ", v)
			}
			log.Println("================")
		}
		if receiveCount >= msgCount {
			break
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
