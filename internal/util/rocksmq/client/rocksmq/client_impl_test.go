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

package rocksmq

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var rmqPath = "/tmp/rocksmq_client"

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.NotNil(t, client)
	assert.Nil(t, err)
}

func TestClient_CreateProducer(t *testing.T) {
	var client0 client
	producer0, err := client0.CreateProducer(ProducerOptions{})
	assert.Nil(t, producer0)
	assert.Error(t, err)

	/////////////////////////////////////////////////
	client, err := NewClient(ClientOptions{
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
	defer removePath(rmqPath)
	client1, err := NewClient(ClientOptions{
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
	client, err := NewClient(ClientOptions{
		Server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)

	/////////////////////////////////////////////////
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_client2"
	rmq := newRocksMQ(t, rmqPathTest)
	defer removePath(rmqPath)
	client1, err := NewClient(ClientOptions{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client1.Close()
	opt := ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
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
		SubscriptionInitialPosition: SubscriptionPositionLatest,
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

func TestClient_SeekLatest(t *testing.T) {
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/seekLatest"
	rmq := newRocksMQ(t, rmqPathTest)
	defer removePath(rmqPath)
	client, err := NewClient(ClientOptions{
		Server: rmq,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	opt := ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	}
	consumer1, err := client.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer1)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.NotNil(t, producer)
	assert.NoError(t, err)
	msg := &ProducerMessage{
		Payload: make([]byte, 10),
	}
	id, err := producer.Send(msg)
	assert.Nil(t, err)

	msgChan := consumer1.Chan()
	msgRead, ok := <-msgChan
	assert.Equal(t, ok, true)
	assert.Equal(t, msgRead.MsgID, id)

	consumer1.Close()

	opt1 := ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: SubscriptionPositionLatest,
	}
	consumer2, err := client.Subscribe(opt1)
	assert.NoError(t, err)
	assert.NotNil(t, consumer2)

	msgChan = consumer2.Chan()
	loop := true
	for loop {
		select {
		case msg := <-msgChan:
			assert.Equal(t, len(msg.Payload), 8)
			loop = false
		case <-time.After(2 * time.Second):
			msg := &ProducerMessage{
				Payload: make([]byte, 8),
			}
			_, err = producer.Send(msg)
			assert.Nil(t, err)
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
	defer removePath(rmqPath)
	client, err := NewClient(ClientOptions{
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
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	}
	consumer, err := client.Subscribe(opt)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	msg := &ProducerMessage{
		Payload: make([]byte, 10),
	}
	id, err := producer.Send(msg)
	assert.Nil(t, err)

	msgChan := consumer.Chan()
	msgConsume, ok := <-msgChan
	assert.Equal(t, ok, true)
	assert.Equal(t, id, msgConsume.MsgID)
}
