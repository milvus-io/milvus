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
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
)

func mGetTsMsg(msgType MsgType, reqID UniqueID, hashValue uint32) TsMsg {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	switch msgType {
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
	}
	return nil
}

func createProducer(channels []string) *MemMsgStream {
	InitMmq()
	produceStream, err := NewMemMsgStream(context.Background(), 1024)
	if err != nil {
		log.Fatalf("new msgstream error = %v", err)
	}
	produceStream.AsProducer(channels)
	produceStream.Start()

	return produceStream
}

func createCondumers(channels []string) []*MemMsgStream {
	consumerStreams := make([]*MemMsgStream, 0)
	for _, channel := range channels {
		consumeStream, err := NewMemMsgStream(context.Background(), 1024)
		if err != nil {
			log.Fatalf("new msgstream error = %v", err)
		}

		thisChannel := []string{channel}
		consumeStream.AsConsumer(thisChannel, channel+"_consumer")
		consumerStreams = append(consumerStreams, consumeStream)
	}

	return consumerStreams
}

func TestStream_GlobalMmq_Func(t *testing.T) {
	channels := []string{"red", "blue", "black", "green"}
	produceStream := createProducer(channels)
	defer produceStream.Close()

	consumerStreams := createCondumers(channels)

	// validate channel and consumer count
	assert.Equal(t, len(Mmq.consumers), len(channels), "global mmq channel error")
	for _, consumers := range Mmq.consumers {
		assert.Equal(t, len(consumers), 1, "global mmq consumer error")
	}

	// validate msg produce/consume
	msg := MsgPack{}
	err := Mmq.Produce(channels[0], &msg)
	if err != nil {
		log.Fatalf("global mmq produce error = %v", err)
	}
	cm := consumerStreams[0].Consume()
	assert.Equal(t, cm, &msg, "global mmq consume error")

	err = Mmq.Broadcast(&msg)
	if err != nil {
		log.Fatalf("global mmq broadcast error = %v", err)
	}
	for _, cs := range consumerStreams {
		cm := cs.Consume()
		assert.Equal(t, cm, &msg, "global mmq consume error")
	}

	// validate consumer close
	for _, cs := range consumerStreams {
		cs.Close()
	}
	assert.Equal(t, len(Mmq.consumers), len(channels), "global mmq channel error")
	for _, consumers := range Mmq.consumers {
		assert.Equal(t, len(consumers), 0, "global mmq consumer error")
	}

	// validate channel destroy
	for _, channel := range channels {
		Mmq.DestroyChannel(channel)
	}
	assert.Equal(t, len(Mmq.consumers), 0, "global mmq channel error")
}

// produce msg after consumer created
func TestStream_MemMsgStream_Produce(t *testing.T) {
	channels := []string{"red", "blue", "black", "green"}
	produceStream := createProducer(channels)
	defer produceStream.Close()

	consumerStreams := createCondumers(channels)
	for _, cs := range consumerStreams {
		defer cs.Close()
	}

	msgPack := MsgPack{}
	var hashValue uint32 = 2
	msgPack.Msgs = append(msgPack.Msgs, mGetTsMsg(commonpb.MsgType_Search, 1, hashValue))
	err := produceStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("new msgstream error = %v", err)
	}

	msg := consumerStreams[hashValue].Consume()
	if msg == nil {
		log.Fatalf("msgstream consume error")
	}

	produceStream.Close()
}

// produce msg begore consumer created
func TestStream_MemMsgStream_Consume(t *testing.T) {
	channels := []string{"red", "blue", "black", "green"}
	produceStream := createProducer(channels)
	defer produceStream.Close()

	msgPack := MsgPack{}
	var hashValue uint32 = 3
	msgPack.Msgs = append(msgPack.Msgs, mGetTsMsg(commonpb.MsgType_Search, 1, hashValue))
	err := produceStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("new msgstream error = %v", err)
	}

	consumerStreams := createCondumers(channels)
	for _, cs := range consumerStreams {
		defer cs.Close()
	}

	msg := consumerStreams[hashValue].Consume()
	if msg == nil {
		log.Fatalf("msgstream consume error")
	}

	produceStream.Close()
}

func TestStream_MemMsgStream_BroadCast(t *testing.T) {
	channels := []string{"red", "blue", "black", "green"}
	produceStream := createProducer(channels)
	defer produceStream.Close()

	consumerStreams := createCondumers(channels)
	for _, cs := range consumerStreams {
		defer cs.Close()
	}

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, mGetTsMsg(commonpb.MsgType_Search, 1, 100))
	err := produceStream.Broadcast(&msgPack)
	if err != nil {
		log.Fatalf("new msgstream error = %v", err)
	}

	for _, consumer := range consumerStreams {
		msg := consumer.Consume()
		if msg == nil {
			log.Fatalf("msgstream consume error")
		}
	}
}
