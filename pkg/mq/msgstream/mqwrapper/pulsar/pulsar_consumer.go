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
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// Consumer consumes from pulsar
type Consumer struct {
	c          pulsar.Consumer
	msgChannel chan mqwrapper.Message
	hasSeek    bool
	AtLatest   bool
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
			// this part handles msgstream expectation when the consumer is not seeked
			// pulsar's default behavior is setting postition to the earliest pointer when client of the same subscription pointer is not acked
			// yet, our message stream is to setting to the very start point of the topic
			if !pc.hasSeek && !pc.AtLatest {
				// the concrete value of the MessageID is pulsar.messageID{-1,-1,-1,-1}
				// but Seek function logic does not allow partitionID -1, See line 618-620 of github.com/apache/pulsar-client-go@v0.5.0 pulsar/consumer_impl.go
				mid := pulsar.EarliestMessageID()
				// the patch function use unsafe pointer to set partitionIdx to 0, which is the valid default partition index of current use case
				// NOTE: when pulsar client version check, do check this logic is fixed or offset is changed!!!
				// NOTE: unsafe solution, check implementation asap
				patchEarliestMessageID(&mid)
				pc.c.Seek(mid)
			}

			go func() {
				for { //nolint:gosimple
					select {
					case msg, ok := <-pc.c.Chan():
						if !ok {
							log.Debug("pulsar consumer channel closed")
							return
						}

						if !pc.skip {
							select {
							case pc.msgChannel <- &pulsarMessage{msg: msg}:
							case <-pc.closeCh:
							}
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
		pc.hasSeek = true
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
		// Unsubscribe for the consumer
		fn := func() error {
			err := pc.c.Unsubscribe()
			if err != nil {
				// this is the hack due to pulsar didn't handle error as expected
				if strings.Contains(err.Error(), "Consumer not found") {
					log.Warn("failed to find consumer, skip unsubscribe",
						zap.String("subscription", pc.Subscription()),
						zap.Error(err))
					return nil
				}
				// Pulsar will automatically clean up subscriptions without consumers, so we can ignore this type of error.
				if strings.Contains(err.Error(), "connection closed") {
					log.Warn("connection closed, skip unsubscribe",
						zap.String("subscription", pc.Subscription()),
						zap.Error(err))
					return nil
				}
				return err
			}
			// only close if unsubscribe successfully
			pc.c.Close()
			return nil
		}

		err := retry.Do(context.TODO(), fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
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

func (pc *Consumer) CheckTopicValid(topic string) error {
	latestMsgID, err := pc.GetLatestMsgID()
	// Pulsar creates that topic under the namespace provided in the topic name automatically
	if err != nil {
		return err
	}

	if !latestMsgID.AtEarliestPosition() {
		return merr.WrapErrTopicNotEmpty(topic, "topic is not empty")
	}
	log.Info("created topic is empty", zap.String("topic", topic))
	return nil
}

// patchEarliestMessageID unsafe patch logic to change messageID partitionIdx to 0
// ONLY used in Chan() function
// DON'T use elsewhere
func patchEarliestMessageID(mid *pulsar.MessageID) {
	// cannot use field.SetInt(), since partitionIdx is not exported

	// this reflect+ unsafe solution is disable by go vet

	//ifData := v.InterfaceData() // unwrap interface
	//ifData[1] is the pointer to the exact struct
	// 20 is the offset of paritionIdx of messageID
	//lint:ignore unsafeptr: possible misuse of unsafe.Pointer (govet), hardcoded offset
	//*(*int32)(unsafe.Pointer(v.InterfaceData()[1] + 20)) = 0

	// use direct unsafe conversion
	/* #nosec G103 */
	r := (*iface)(unsafe.Pointer(mid))
	id := (*messageID)(r.Data)
	id.partitionIdx = 0
}

// unsafe access pointer, same as pulsar.messageID
type messageID struct {
	ledgerID     int64
	entryID      int64
	batchID      int32
	partitionIdx int32
}

// interface struct mapping
type iface struct {
	Type, Data unsafe.Pointer
}
