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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// Consumer is a client that used to consume messages from natsmq
type Consumer struct {
	options   mqwrapper.ConsumerOptions
	js        nats.JetStreamContext
	sub       *nats.Subscription
	topic     string
	groupName string
	natsChan  chan *nats.Msg
	msgChan   chan mqwrapper.Message
	closeChan chan struct{}
	once      sync.Once
	skip      bool
}

// Subscription returns the subscription name of this consumer
func (nc *Consumer) Subscription() string {
	return nc.groupName
}

// Chan returns a channel to read messages from natsmq
func (nc *Consumer) Chan() <-chan mqwrapper.Message {
	if nc.sub == nil {
		log.Error("accessing Chan of an uninitialized subscription.", zap.String("topic", nc.topic), zap.String("groupName", nc.groupName))
		panic("failed to chan a consumer without assign")
	}
	if nc.msgChan == nil {
		nc.once.Do(func() {
			nc.msgChan = make(chan mqwrapper.Message, 256)
			go func() {
				for {
					select {
					case msg := <-nc.natsChan:
						if nc.skip {
							nc.skip = false
							continue
						}
						meta, err := msg.Metadata()
						if err != nil {
							log.Error("Failed to fetch metadata from nats message: ", zap.Error(err))
							continue
						}
						var data NatsMsgData
						err = json.Unmarshal(msg.Data, &data)
						if err != nil {
							log.Error("Failed to parse data from nats message: ", zap.Error(err))
							continue
						}
						nc.msgChan <- &nmqMessage{msg: Message{MsgID: meta.Sequence.Stream, Topic: nc.topic, Payload: data.Payload, Properties: data.Properties}}
					case <-nc.closeChan:
						log.Info("close consumer ", zap.String("topic", nc.topic), zap.String("groupName", nc.groupName))
						close(nc.msgChan)
						close(nc.natsChan)
						return
					}
				}
			}()
		})
	}
	return nc.msgChan
}

// Seek is used to seek the position in natsmq topic
func (nc *Consumer) Seek(id mqwrapper.MessageID, inclusive bool) error {
	if nc.sub != nil {
		return fmt.Errorf("can not seek() on an initilized consumer")
	}
	msgID := id.(*nmqID).messageID
	// skip the first message when consume
	nc.skip = !inclusive
	// TODO: check the subscription config/policy.
	var err error
	nc.sub, err = nc.js.ChanSubscribe(nc.topic, nc.natsChan, nats.StartSequence(msgID))
	return err
}

// Ack is used to ask a natsmq message
func (nc *Consumer) Ack(message mqwrapper.Message) {
	// Do nothing
}

// Close is used to free the resounces of this consumer
func (nc *Consumer) Close() {
	nc.sub.Unsubscribe()
	close(nc.closeChan)
}

func (nc *Consumer) GetLatestMsgID() (mqwrapper.MessageID, error) {
	cinfo, err := nc.sub.ConsumerInfo()
	if err != nil {
		return nil, util.WrapError("failed to get ConsumerInfo", err)
	}
	msgID := cinfo.Delivered.Stream
	return &nmqID{messageID: msgID}, nil
}

func (nc *Consumer) CheckTopicValid(topic string) error {
	// A consumer is tied to a topic. In a multi-tenant situation,
	//a consumer is not supposed to check on other topics.
	if topic != nc.topic {
		return fmt.Errorf("consumer of topic %s checking validness of topic %s", nc.topic, topic)
	}
	_, err := nc.sub.ConsumerInfo()
	if err != nil {
		return util.WrapError("failed to get ConsumerInfo", err)
	}
	return nil
}
