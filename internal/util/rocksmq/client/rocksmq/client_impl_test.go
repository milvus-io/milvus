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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	rmqPath := "/tmp/milvus/test_client1"
	rmq := newRocksMQ(rmqPath)
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
	ctx, cancel := context.WithCancel(context.Background())
	client, err := NewClient(ClientOptions{
		Server: newMockRocksMQ(),
		Ctx:    ctx,
		Cancel: cancel,
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
	rmqPath := "/tmp/milvus/test_client2"
	rmq := newRocksMQ(rmqPath)
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
	assert.Error(t, err)
	assert.Nil(t, consumer3)
	consumer4, err := client1.Subscribe(opt1)
	assert.Error(t, err)
	assert.Nil(t, consumer4)

	producer1, err := client1.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.NotNil(t, producer1)
	assert.NoError(t, err)
}

func TestClient_consume(t *testing.T) {
	rmqPath := "/tmp/milvus/test_client3"
	rmq := newRocksMQ(rmqPath)
	defer removePath(rmqPath)
	ctx, cancel := context.WithCancel(context.Background())
	client, err := NewClient(ClientOptions{
		Server: rmq,
		Ctx:    ctx,
		Cancel: cancel,
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
	producer.Send(msg)

	<-consumer.Chan()

}
