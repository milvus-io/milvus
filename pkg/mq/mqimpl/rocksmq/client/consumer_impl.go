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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
)

type consumer struct {
	topic        string
	client       *client
	consumerName string
	options      ConsumerOptions

	startOnce sync.Once

	stopCh    chan struct{}
	msgMutex  chan struct{}
	initCh    chan struct{}
	messageCh chan common.Message
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
		messageCh = make(chan common.Message, 1)
	}
	// only used for
	initCh := make(chan struct{}, 1)
	initCh <- struct{}{}
	return &consumer{
		topic:        options.Topic,
		client:       c,
		consumerName: options.SubscriptionName,
		options:      options,
		stopCh:       make(chan struct{}),
		msgMutex:     make(chan struct{}, 1),
		initCh:       initCh,
		messageCh:    messageCh,
	}, nil
}

// getExistedConsumer new a consumer and put the existed mutex channel to the new consumer
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
		messageCh = make(chan common.Message, 1)
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

// Subscription returns the consumer name
func (c *consumer) Subscription() string {
	return c.consumerName
}

// Topic returns the topic of the consumer
func (c *consumer) Topic() string {
	return c.topic
}

// MsgMutex return the message mutex channel of consumer
func (c *consumer) MsgMutex() chan struct{} {
	return c.msgMutex
}

// Chan start consume goroutine and return message channel
func (c *consumer) Chan() <-chan common.Message {
	c.startOnce.Do(func() {
		c.client.wg.Add(1)
		go c.client.consume(c)
	})
	return c.messageCh
}

// Seek seek rocksmq position to id and notify consumer to consume
func (c *consumer) Seek(id UniqueID) error { //nolint:govet
	err := c.client.server.Seek(c.topic, c.consumerName, id)
	if err != nil {
		return err
	}
	c.client.server.Notify(c.topic, c.consumerName)
	return nil
}

// Close destroy current consumer in rocksmq
func (c *consumer) Close() {
	// TODO should panic?
	err := c.client.server.DestroyConsumerGroup(c.topic, c.consumerName)
	if err != nil {
		log.Warn("Consumer close failed", zap.String("topicName", c.topic), zap.String("groupName", c.consumerName), zap.Error(err))
		// TODO: current rocksmq does't promise the msgmutex will be closed in some unittest,
		// make the consuming goroutine leak.
		// Here add a dirty way to close it.
		close(c.msgMutex)
		return
	}
	<-c.stopCh
}

func (c *consumer) GetLatestMsgID() (int64, error) {
	msgID, err := c.client.server.GetLatestMsg(c.topic)
	if err != nil {
		return msgID, err
	}
	return msgID, nil
}

func (c *consumer) CheckTopicValid(topic string) error {
	err := c.client.server.CheckTopicValid(topic)
	return err
}
