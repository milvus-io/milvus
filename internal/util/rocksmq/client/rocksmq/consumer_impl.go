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
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type consumer struct {
	topic        string
	client       *client
	consumerName string
	options      ConsumerOptions

	msgMutex  chan struct{}
	messageCh chan ConsumerMessage
}

func newConsumer(c *client, options ConsumerOptions) (*consumer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	if options.SubscriptionName == "" {
		return nil, newError(InvalidConfiguration, "SubscriptionName is empty")
	}

	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 1)
	}

	return &consumer{
		topic:        options.Topic,
		client:       c,
		consumerName: options.SubscriptionName,
		options:      options,
		msgMutex:     make(chan struct{}, 1),
		messageCh:    messageCh,
	}, nil
}

func getExistedConsumer(c *client, options ConsumerOptions, msgMutex chan struct{}) (*consumer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	if options.SubscriptionName == "" {
		return nil, newError(InvalidConfiguration, "SubscriptionName is empty")
	}

	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 1)
	}

	return &consumer{
		topic:        options.Topic,
		client:       c,
		consumerName: options.SubscriptionName,
		options:      options,
		msgMutex:     msgMutex,
		messageCh:    messageCh,
	}, nil
}

func (c *consumer) Subscription() string {
	return c.consumerName
}

func (c *consumer) Topic() string {
	return c.topic
}

func (c *consumer) MsgMutex() chan struct{} {
	return c.msgMutex
}

func (c *consumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

func (c *consumer) Seek(id UniqueID) error { //nolint:govet
	err := c.client.server.Seek(c.topic, c.consumerName, id)
	if err != nil {
		return err
	}
	c.client.server.Notify(c.topic, c.consumerName)
	return nil
}

func (c *consumer) Close() {
	err := c.client.server.DestroyConsumerGroup(c.topic, c.consumerName)
	if err != nil {
		log.Debug("Consumer close failed", zap.Any("topicName", c.topic), zap.Any("groupName", c.consumerName), zap.Any("error", err))
	}
}
