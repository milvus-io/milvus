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
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/server"
)

const (
	minimalConsumePendingBufferSize = 16
)

type client struct {
	server    RocksMQ
	wg        *sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newClient(options Options) (*client, error) {
	if options.Server == nil {
		return nil, newError(InvalidConfiguration, "options.Server is nil")
	}

	c := &client{
		server:  options.Server,
		wg:      &sync.WaitGroup{},
		closeCh: make(chan struct{}),
	}
	return c, nil
}

// CreateProducer create a rocksmq producer
func (c *client) CreateProducer(options ProducerOptions) (Producer, error) {
	// Create a producer
	producer, err := newProducer(c, options)
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(c.server).IsNil() {
		return nil, newError(0, "Rmq server is nil")
	}
	// Create a topic in rocksmq, ignore if topic exists
	err = c.server.CreateTopic(options.Topic)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// Subscribe create a rocksmq consumer and start consume in a goroutine
func (c *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	// Create a consumer
	if reflect.ValueOf(c.server).IsNil() {
		return nil, newError(0, "Rmq server is nil")
	}

	exist, con, err := c.server.ExistConsumerGroup(options.Topic, options.SubscriptionName)
	if err != nil {
		return nil, err
	}
	if exist {
		log.Ctx(context.TODO()).Debug("ConsumerGroup already existed", zap.Any("topic", options.Topic), zap.String("SubscriptionName", options.SubscriptionName))
		consumer, err := getExistedConsumer(c, options, con.MsgMutex)
		if err != nil {
			return nil, err
		}
		if options.SubscriptionInitialPosition == common.SubscriptionPositionLatest {
			err = c.server.SeekToLatest(options.Topic, options.SubscriptionName)
			if err != nil {
				return nil, err
			}
		}
		return consumer, nil
	}
	consumer, err := newConsumer(c, options)
	if err != nil {
		return nil, err
	}

	// Create a consumergroup in rocksmq, raise error if consumergroup exists
	err = c.server.CreateConsumerGroup(options.Topic, options.SubscriptionName)
	if err != nil {
		return nil, err
	}

	// Register self in rocksmq server
	cons := &server.Consumer{
		Topic:     consumer.topic,
		GroupName: consumer.consumerName,
		MsgMutex:  consumer.msgMutex,
	}
	if err := c.server.RegisterConsumer(cons); err != nil {
		return nil, err
	}

	if options.SubscriptionInitialPosition == common.SubscriptionPositionLatest {
		err = c.server.SeekToLatest(options.Topic, options.SubscriptionName)
		if err != nil {
			return nil, err
		}
	}

	return consumer, nil
}

func (c *client) consume(consumer *consumer) {
	defer func() {
		close(consumer.stopCh)
		c.wg.Done()
	}()

	if err := c.blockUntilInitDone(consumer); err != nil {
		log.Warn("consumer init failed", zap.Error(err))
		return
	}

	var pendingMsgs []*RmqMessage
	for {
		if len(pendingMsgs) == 0 {
			pendingMsgs = c.tryToConsume(consumer)
		}

		var consumerCh chan<- common.Message
		var waitForSent *RmqMessage
		var newIncomingMsgCh <-chan struct{}
		var timerNotify <-chan time.Time
		if len(pendingMsgs) > 0 {
			// If there's pending sent messages, we can try to deliver them first.
			consumerCh = consumer.messageCh
			waitForSent = pendingMsgs[0]
		} else {
			// If there's no more pending messages, we can wait for new incoming messages.
			// !!! TODO: MsgMutex may lost, not sync up with the consumer,
			// so the tailing message cannot be consumed if no new producing message.
			newIncomingMsgCh = consumer.MsgMutex()
			// It's a bad implementation here, for quickly fixing the previous problem.
			// Every 100ms, wake up and check if the consumer has new incoming data.
			timerNotify = time.After(100 * time.Millisecond)
		}

		select {
		case <-c.closeCh:
			log.Info("Client is closed, consumer goroutine exit")
			return
		case consumerCh <- waitForSent:
			pendingMsgs = pendingMsgs[1:]
		case _, ok := <-newIncomingMsgCh:
			if !ok {
				// consumer MsgMutex closed, goroutine exit
				log.Info("Consumer MsgMutex closed")
				return
			}
		case <-timerNotify:
			continue
		}
	}
}

// blockUntilInitDone block until consumer is initialized
func (c *client) blockUntilInitDone(consumer *consumer) error {
	select {
	case <-c.closeCh:
		return errors.New("client is closed")
	case _, ok := <-consumer.initCh:
		if !ok {
			return errors.New("consumer init failure")
		}
		return nil
	}
}

func (c *client) tryToConsume(consumer *consumer) []*RmqMessage {
	n := cap(consumer.messageCh) - len(consumer.messageCh)
	if n <= minimalConsumePendingBufferSize {
		n = minimalConsumePendingBufferSize
	}
	msgs, err := consumer.client.server.Consume(consumer.topic, consumer.consumerName, n)
	if err != nil {
		log.Warn("Consumer's goroutine cannot consume from (" + consumer.topic + "," + consumer.consumerName + "): " + err.Error())
		return nil
	}
	rmqMsgs := make([]*RmqMessage, 0, len(msgs))
	for _, msg := range msgs {
		rmqMsg, err := unmarshalStreamingMessage(consumer.topic, msg)
		if err == nil {
			rmqMsgs = append(rmqMsgs, rmqMsg)
			continue
		}
		if !errors.Is(err, errNotStreamingServiceMessage) {
			log.Warn("Consumer's goroutine cannot unmarshal streaming message: ", zap.Error(err))
			continue
		}
		// then fallback to the legacy message format.

		// This is the hack, we put property into pl
		properties := make(map[string]string, 0)
		pl, err := UnmarshalHeader(msg.Payload)
		if err == nil && pl != nil && pl.Base != nil {
			properties = pl.Base.Properties
		}
		rmqMsgs = append(rmqMsgs, &RmqMessage{
			msgID:      msg.MsgID,
			payload:    msg.Payload,
			properties: properties,
			topic:      consumer.Topic(),
		})
	}
	return rmqMsgs
}

// Close close the channel to notify rocksmq to stop operation and close rocksmq server
func (c *client) Close() {
	c.closeOnce.Do(func() {
		close(c.closeCh)
		c.wg.Wait()
	})
}
