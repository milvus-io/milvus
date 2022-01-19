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

package mqclient

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.uber.org/zap"
)

// PulsarConsumer consumes from pulsar
type PulsarConsumer struct {
	c pulsar.Consumer
	pulsar.Reader
	msgChannel chan Message
	hasSeek    bool
	AtLatest   bool
	closeCh    chan struct{}
	once       sync.Once
	skip       bool
	closeOnce  sync.Once
}

// Subscription get a subscription for the consumer
func (pc *PulsarConsumer) Subscription() string {
	return pc.c.Subscription()
}

// Chan returns a message channel
func (pc *PulsarConsumer) Chan() <-chan Message {
	if pc.msgChannel == nil {
		pc.once.Do(func() {
			pc.msgChannel = make(chan Message, 256)
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
func (pc *PulsarConsumer) Seek(id MessageID, inclusive bool) error {
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
func (pc *PulsarConsumer) Ack(message Message) {
	pm := message.(*pulsarMessage)
	pc.c.Ack(pm.msg)
}

// Close the consumer and stop the broker to push more messages
func (pc *PulsarConsumer) Close() {
	pc.closeOnce.Do(func() {
		defer pc.c.Close()
		// Unsubscribe for the consumer
		err := retry.Do(context.Background(), func() error {
			//TODO need to check error retryable
			return pc.c.Unsubscribe()
		}, retry.MaxSleepTime(50*time.Millisecond), retry.Attempts(3))
		if err != nil {
			log.Error("failed to unsubscribe", zap.String("subscription", pc.Subscription()), zap.Error(err))
			panic(err)
		}
		close(pc.closeCh)
	})
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
