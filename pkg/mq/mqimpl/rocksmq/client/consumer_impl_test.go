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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
)

func TestConsumer_newConsumer(t *testing.T) {
	assert.Equal(t, EarliestMessageID(), int64(-1))

	consumer, err := newConsumer(nil, ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
	})
	assert.Nil(t, consumer)
	assert.Error(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = newConsumer(newMockClient(), ConsumerOptions{})
	assert.Nil(t, consumer)
	assert.Error(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = getExistedConsumer(newMockClient(), ConsumerOptions{}, nil)
	assert.Nil(t, consumer)
	assert.Error(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = newConsumer(newMockClient(), ConsumerOptions{
		Topic: newTopicName(),
	})
	assert.Nil(t, consumer)
	assert.Error(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	/////////////////////////////////////////////////
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_consumer1"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client, err := newClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()
	consumerName := newConsumerName()
	consumer1, err := newConsumer(client, ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            consumerName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer1)
	defer consumer1.Close()
	assert.Equal(t, consumerName, consumer1.Subscription())

	consumer2, err := newConsumer(client, ConsumerOptions{
		Topic: "",
	})
	assert.Error(t, err)
	assert.Nil(t, consumer2)

	consumer3, err := newConsumer(client, ConsumerOptions{
		Topic:            newTopicName(),
		SubscriptionName: "",
	})
	assert.Error(t, err)
	assert.Nil(t, consumer3)

	consumer4, err := getExistedConsumer(client, ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
	}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, consumer4)

	consumer5, err := getExistedConsumer(client, ConsumerOptions{
		Topic: "",
	}, nil)
	assert.Error(t, err)
	assert.Nil(t, consumer5)

	consumer6, err := getExistedConsumer(client, ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            "",
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
	}, nil)
	assert.Error(t, err)
	assert.Nil(t, consumer6)
}

func TestConsumer_Subscription(t *testing.T) {
	topicName := newTopicName()
	consumerName := newConsumerName()
	consumer, err := newConsumer(newMockClient(), ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            consumerName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
	})
	assert.Nil(t, consumer)
	assert.Error(t, err)
	// assert.Equal(t, consumerName, consumer.Subscription())
}

func TestConsumer_Seek(t *testing.T) {
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_consumer2"
	rmq := newRocksMQ(t, rmqPathTest)
	defer rmq.Close()
	defer removePath(rmqPath)
	client, err := newClient(Options{
		Server: rmq,
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	topicName := newTopicName()
	consumerName := newConsumerName()
	consumer, err := newConsumer(client, ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            consumerName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	err = consumer.Seek(0)
	assert.Error(t, err)
}
