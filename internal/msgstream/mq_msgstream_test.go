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
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/mqclient"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	client "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
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

func getTsMsg(msgType MsgType, reqID UniqueID, hashValue uint32) TsMsg {
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
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ChannelID:      "0",
			Timestamps:     []Timestamp{uint64(reqID)},
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
			Timestamps:     []Timestamp{1},
			PrimaryKeys:    []IntPrimaryKey{1},
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
			Query:           nil,
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

func getTimeTickMsg(reqID UniqueID, hashValue uint32, time uint64) TsMsg {
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

func initPulsarStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...RepackFunc) (MsgStream, MsgStream) {
	factory := ProtoUDFactory{}

	// set input stream
	pulsarClient, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	// set output stream
	pulsarClient2, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output MsgStream = outputStream

	return input, output
}

func initPulsarTtStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...RepackFunc) (MsgStream, MsgStream) {
	factory := ProtoUDFactory{}

	// set input stream
	pulsarClient, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	// set output stream
	pulsarClient2, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(context.Background(), 100, 100, pulsarClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output MsgStream = outputStream

	return input, output
}

func receiveMsg(outputStream MsgStream, msgCount int) {
	receiveCount := 0
	for {
		result := outputStream.Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				fmt.Println("msg type: ", v.Type(), ", msg value: ", v)
			}
		}
		if receiveCount >= msgCount {
			break
		}
	}
}

func TestStream_PulsarMsgStream_Insert(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 1, 1))
	//msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Delete, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Search, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Search, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_SearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_SearchResult, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, repackFunc)
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

	pulsarClient, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	pulsarClient2, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
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

func TestStream_PulsarMsgStream_DeleteRepackFunc(t *testing.T) {
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

	deleteRequest := internalpb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Delete,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName: "Collection",
		ChannelID:      "1",
		Timestamps:     []Timestamp{1, 1},
		PrimaryKeys:    []int64{1, 3},
	}
	deleteMsg := &DeleteMsg{
		BaseMsg:       baseMsg,
		DeleteRequest: deleteRequest,
	}

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)

	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	pulsarClient2, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
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

func TestStream_PulsarMsgStream_DefaultRepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_TimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Search, 2, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_SearchResult, 3, 3))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_QueryNodeStats, 4, 4))

	factory := ProtoUDFactory{}
	pulsarClient, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(context.Background(), 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	pulsarClient2, _ := mqclient.NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
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
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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

func TestStream_PulsarTtMsgStream_Seek(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 19, 19))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	msgPack3 := MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 14, 14))
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 9, 9))

	msgPack4 := MsgPack{}
	msgPack4.Msgs = append(msgPack4.Msgs, getTimeTickMsg(11, 11, 11))

	msgPack5 := MsgPack{}
	msgPack5.Msgs = append(msgPack5.Msgs, getTimeTickMsg(15, 15, 15))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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
	for _, position := range receivedMsg.StartPositions {
		outputStream.Seek(position)
	}
	err = inputStream.Broadcast(&msgPack5)
	assert.Nil(t, err)
	//seekMsg, _ := outputStream.Consume()
	//for _, msg := range seekMsg.Msgs {
	//	assert.Equal(t, msg.BeginTs(), uint64(14))
	//}
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
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
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

/****************************************Rmq test******************************************/

func initRmq(name string) *etcdkv.EtcdKV {
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	err = rocksmq.InitRmq(name, idAllocator)

	if err != nil {
		log.Fatalf("InitRmq error = %v", err)
	}
	return etcdKV
}

func Close(rocksdbName string, intputStream, outputStream MsgStream, etcdKV *etcdkv.EtcdKV) {
	intputStream.Close()
	outputStream.Close()
	etcdKV.Close()
	err := os.RemoveAll(rocksdbName)
	fmt.Println(err)
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3, 3))

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
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

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
