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

package pulsar

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.uber.org/zap"
)

// Consumer consumes from pulsar
type Consumer struct {
	c          pulsar.Consumer
	msgChannel chan mqwrapper.Message
	closeCh    chan struct{}
	once       sync.Once
	skip       bool
	closeOnce  sync.Once
}

// Subscription get a subscription for the consumer
func (pc *Consumer) Subscription() string {
	return pc.c.Subscription()
}

// Chan returns a message channel
func (pc *Consumer) Chan() <-chan mqwrapper.Message {
	if pc.msgChannel == nil {
		pc.once.Do(func() {
			pc.msgChannel = make(chan mqwrapper.Message, 256)
			go func() {
				for { //nolint:gosimple
					select {
					case msg, ok := <-pc.c.Chan():
						if !ok {
							log.Debug("pulsar consumer channel closed")
							return
						}
						if !pc.skip {
							pc.msgChannel <- &pulsarMessage{msg: msg}
						} else {
							pc.skip = false
						}
					case <-pc.closeCh: // workaround for pulsar consumer.receiveCh not closed
						close(pc.msgChannel)
						return
					}
				}
			}()
		})
	}
	return pc.msgChannel
}

// Seek seek consume position to the pointed messageID,
// the pointed messageID will be consumed after the seek in pulsar
func (pc *Consumer) Seek(id mqwrapper.MessageID, inclusive bool) error {
	messageID := id.(*pulsarID).messageID
	err := pc.c.Seek(messageID)
	if err == nil {
		// skip the first message when consume
		pc.skip = !inclusive
	}
	return err
}

// Ack the consumption of a single message
func (pc *Consumer) Ack(message mqwrapper.Message) {
	pm := message.(*pulsarMessage)
	pc.c.Ack(pm.msg)
}

// Close the consumer and stop the broker to push more messages
func (pc *Consumer) Close() {
	pc.closeOnce.Do(func() {
		defer pc.c.Close()
		// Unsubscribe for the consumer
		err := retry.Do(context.Background(), func() error {
			//TODO need to check error retryable
			return pc.c.Unsubscribe()
		}, retry.MaxSleepTime(50*time.Millisecond), retry.Attempts(6))
		if err != nil {
			log.Error("failed to unsubscribe", zap.String("subscription", pc.Subscription()), zap.Error(err))
			panic(err)
		}
		close(pc.closeCh)
	})
}

func (pc *Consumer) GetLatestMsgID() (mqwrapper.MessageID, error) {
	msgID, err := pc.c.GetLastMessageID(pc.c.Name(), mqwrapper.DefaultPartitionIdx)
	return &pulsarID{messageID: msgID}, err
}
