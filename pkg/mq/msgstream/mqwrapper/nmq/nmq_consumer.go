// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nmq

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// Consumer is a client that used to consume messages from natsmq
type Consumer struct {
	options   mqwrapper.ConsumerOptions
	js        nats.JetStreamContext
	sub       *nats.Subscription
	topic     string
	groupName string
	natsChan  chan *nats.Msg
	msgChan   chan common.Message
	closeChan chan struct{}
	once      sync.Once
	closeOnce sync.Once
	skip      bool
	wg        sync.WaitGroup
}

// Subscription returns the subscription name of this consumer
func (nc *Consumer) Subscription() string {
	return nc.groupName
}

// Chan returns a channel to read messages from natsmq
func (nc *Consumer) Chan() <-chan common.Message {
	if err := nc.closed(); err != nil {
		panic(err)
	}

	if nc.sub == nil {
		log.Error("accessing Chan of an uninitialized subscription.", zap.String("topic", nc.topic), zap.String("groupName", nc.groupName))
		panic("failed to chan a consumer without assign")
	}
	if nc.msgChan == nil {
		nc.once.Do(func() {
			nc.msgChan = make(chan common.Message, 256)
			nc.wg.Add(1)
			go func() {
				defer nc.wg.Done()
				for {
					select {
					case msg := <-nc.natsChan:
						if nc.skip {
							nc.skip = false
							continue
						}
						nc.msgChan <- &nmqMessage{
							raw: msg,
						}
					case <-nc.closeChan:
						log.Info("close nmq consumer ", zap.String("topic", nc.topic), zap.String("groupName", nc.groupName))
						close(nc.msgChan)
						return
					}
				}
			}()
		})
	}
	return nc.msgChan
}

// Seek is used to seek the position in natsmq topic
func (nc *Consumer) Seek(id common.MessageID, inclusive bool) error {
	if err := nc.closed(); err != nil {
		return err
	}
	if nc.sub != nil {
		return errors.New("can not seek() on an initilized consumer")
	}
	if nc.msgChan != nil {
		return errors.New("Seek should be called before Chan")
	}
	log.Info("Seek is called", zap.String("topic", nc.topic), zap.Any("id", id))
	msgID := id.(*nmqID).messageID
	// skip the first message when consume
	nc.skip = !inclusive
	var err error
	nc.sub, err = nc.js.ChanSubscribe(nc.topic, nc.natsChan, nats.StartSequence(msgID))
	if err != nil {
		log.Warn("fail to Seek", zap.Error(err))
	}
	return err
}

// Ack is used to ask a natsmq message
func (nc *Consumer) Ack(message common.Message) {
	if err := message.(*nmqMessage).raw.Ack(); err != nil {
		log.Warn("failed to ack message of nmq", zap.String("topic", message.Topic()), zap.Reflect("msgID", message.ID()))
	}
}

// Close is used to free the resources of this consumer
func (nc *Consumer) Close() {
	nc.closeOnce.Do(func() {
		if nc.sub == nil {
			return
		}
		if err := nc.sub.Unsubscribe(); err != nil {
			log.Warn("failed to unsubscribe subscription of nmq", zap.String("topic", nc.topic), zap.Error(err))
		}
		close(nc.closeChan)
		nc.wg.Wait()
	})
}

// GetLatestMsgID returns the ID of the most recent message processed by the consumer.
func (nc *Consumer) GetLatestMsgID() (common.MessageID, error) {
	if err := nc.closed(); err != nil {
		return nil, err
	}

	info, err := nc.js.StreamInfo(nc.topic)
	if err != nil {
		log.Warn("fail to get stream info of nats", zap.String("topic", nc.topic), zap.Error(err))
		return nil, errors.Wrap(err, "failed to get stream info of nats jetstream")
	}
	msgID := info.State.LastSeq
	return &nmqID{messageID: msgID}, nil
}

// CheckTopicValid verifies if the given topic is valid for this consumer.
// 1. topic should exist.
func (nc *Consumer) CheckTopicValid(topic string) error {
	if err := nc.closed(); err != nil {
		return err
	}

	// A consumer is tied to a topic. In a multi-tenant situation,
	// a consumer is not supposed to check on other topics.
	if topic != nc.topic {
		return fmt.Errorf("consumer of topic %s checking validness of topic %s", nc.topic, topic)
	}

	// check if topic valid or exist.
	_, err := nc.js.StreamInfo(topic)
	if errors.Is(err, nats.ErrStreamNotFound) {
		return merr.WrapErrMqTopicNotFound(topic, err.Error())
	} else if err != nil {
		log.Warn("fail to get stream info of nats", zap.String("topic", nc.topic), zap.Error(err))
		return errors.Wrap(err, "failed to get stream info of nats jetstream")
	}
	return nil
}

// Closed check if Consumer is closed.
func (nc *Consumer) closed() error {
	select {
	case <-nc.closeChan:
		return errors.Newf("Closed Nmq Consumer, topic: %s, subscription name:", nc.topic, nc.groupName)
	default:
		return nil
	}
}
