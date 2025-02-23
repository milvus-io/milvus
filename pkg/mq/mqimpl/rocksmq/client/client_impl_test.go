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

package client

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
	server2 "github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var rmqPath = "/tmp/rocksmq_client"

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

func TestClient(t *testing.T) {
	client, err := NewClient(Options{})
	assert.NotNil(t, client)
	assert.NoError(t, err)
}

func TestClient_CreateProducer(t *testing.T) {
	var client0 client
	producer0, err := client0.CreateProducer(ProducerOptions{})
	assert.Nil(t, producer0)
	assert.Error(t, err)

	/////////////////////////////////////////////////
	client, err := NewClient(Options{
		Server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.Error(t, err)
	assert.Nil(t, producer)

	/////////////////////////////////////////////////
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_client1"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client1, err := NewClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client1.Close()
	producer1, err := client1.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.NotNil(t, producer1)
	assert.NoError(t, err)
	defer producer1.Close()

	// /////////////////////////////////////////////////
	// dummyTopic := strings.Repeat(newTopicName(), 100)
	// producer2, err := client1.CreateProducer(ProducerOptions{
	// 	Topic: dummyTopic,
	// })
	// assert.Nil(t, producer2)
	// assert.Error(t, err)
}

func TestClient_Subscribe(t *testing.T) {
	client, err := NewClient(Options{
		Server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)

	/////////////////////////////////////////////////
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_client2"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client1, err := NewClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client1.Close()
	opt := ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	}
	consumer1, err := client1.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer1)
	consumer2, err := client1.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer2)

	opt1 := ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionLatest,
	}
	consumer3, err := client1.Subscribe(opt1)
	assert.NoError(t, err)
	assert.NotNil(t, consumer3)
	consumer4, err := client1.Subscribe(opt1)
	assert.NoError(t, err)
	assert.NotNil(t, consumer4)

	producer1, err := client1.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.NotNil(t, producer1)
	assert.NoError(t, err)
}

func TestClient_SubscribeError(t *testing.T) {
	mockMQ := server2.NewMockRocksMQ(t)
	client, err := NewClient(Options{
		Server: mockMQ,
	})
	testTopic := newTopicName()
	testGroupName := newConsumerName()

	assert.NoError(t, err)
	mockMQ.EXPECT().ExistConsumerGroup(testTopic, testGroupName).Return(false, nil, nil)
	mockMQ.EXPECT().CreateConsumerGroup(testTopic, testGroupName).Return(nil)
	mockMQ.EXPECT().RegisterConsumer(mock.Anything).Return(nil)
	mockMQ.EXPECT().SeekToLatest(testTopic, testGroupName).Return(fmt.Errorf("test error"))

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       testTopic,
		SubscriptionName:            testGroupName,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionLatest,
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)
}

func TestClient_SeekLatest(t *testing.T) {
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/seekLatest"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client, err := NewClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	opt := ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	}
	consumer1, err := client.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer1)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.NotNil(t, producer)
	assert.NoError(t, err)
	msg := &mqcommon.ProducerMessage{
		Payload:    make([]byte, 10),
		Properties: map[string]string{},
	}
	id, err := producer.Send(msg)
	assert.NoError(t, err)

	msgChan := consumer1.Chan()
	msgRead, ok := <-msgChan
	assert.Equal(t, ok, true)
	assert.Equal(t, msgRead.ID(), &server2.RmqID{MessageID: id})

	consumer1.Close()

	opt1 := ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionLatest,
	}
	consumer2, err := client.Subscribe(opt1)
	assert.NoError(t, err)
	assert.NotNil(t, consumer2)

	msgChan = consumer2.Chan()
	loop := true
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for loop {
		select {
		case msg := <-msgChan:
			assert.Equal(t, len(msg.Payload()), 8)
			loop = false
		case <-ticker.C:
			msg := &mqcommon.ProducerMessage{
				Payload: make([]byte, 8),
			}
			_, err = producer.Send(msg)
			assert.NoError(t, err)
		}
	}

	producer1, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.NotNil(t, producer1)
	assert.NoError(t, err)
}

func TestClient_consume(t *testing.T) {
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_client3"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client, err := NewClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client.Close()
	topicName := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.NotNil(t, producer)
	assert.NoError(t, err)

	opt := ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	}
	consumer, err := client.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	msg := &mqcommon.ProducerMessage{
		Payload: make([]byte, 10),
	}
	id, err := producer.Send(msg)
	assert.NoError(t, err)

	msgChan := consumer.Chan()
	msgConsume, ok := <-msgChan
	assert.Equal(t, ok, true)
	assert.Equal(t, &server2.RmqID{MessageID: id}, msgConsume.ID())
}

func TestRocksmq_Properties(t *testing.T) {
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_client4"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client, err := NewClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client.Close()
	topicName := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.NotNil(t, producer)
	assert.NoError(t, err)

	opt := ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	}
	consumer, err := client.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	timeTickMsg := &msgpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     UniqueID(0),
			Timestamp: 100,
			SourceID:  0,
		},
	}
	msgb, errMarshal := proto.Marshal(timeTickMsg)
	assert.NoError(t, errMarshal)
	assert.True(t, len(msgb) > 0)
	header, err := UnmarshalHeader(msgb)
	assert.NoError(t, err)
	assert.NotNil(t, header)
	msg := &mqcommon.ProducerMessage{
		Payload:    msgb,
		Properties: map[string]string{common.TraceIDKey: "a"},
	}

	_, err = producer.Send(msg)
	assert.NoError(t, err)

	msgChan := consumer.Chan()
	msgConsume, ok := <-msgChan
	assert.True(t, ok)
	assert.Equal(t, len(msgConsume.Properties()), 1)
	assert.Equal(t, msgConsume.Properties()[common.TraceIDKey], "a")
	assert.NoError(t, err)

	// rocksmq consumer needs produce to notify to receive msg
	// if produce all in the begin, it will stuck if consume not that fast
	// related with https://github.com/milvus-io/milvus/issues/27801
	msg = &mqcommon.ProducerMessage{
		Payload:    msgb,
		Properties: map[string]string{common.TraceIDKey: "b"},
	}
	_, err = producer.Send(msg)
	assert.NoError(t, err)

	msgConsume, ok = <-msgChan
	assert.True(t, ok)
	assert.Equal(t, len(msgConsume.Properties()), 1)
	assert.Equal(t, msgConsume.Properties()[common.TraceIDKey], "b")
	assert.NoError(t, err)

	timeTickMsg2 := &msgpb.TimeTickMsg{}
	proto.Unmarshal(msgConsume.Payload(), timeTickMsg2)

	assert.Equal(t, timeTickMsg2.Base.MsgType, commonpb.MsgType_TimeTick)
	assert.Equal(t, timeTickMsg2.Base.Timestamp, uint64(100))
}
