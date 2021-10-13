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

package mqclient

import (
	"sync"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
)

type pulsarConsumer struct {
	c          pulsar.Consumer
	msgChannel chan ConsumerMessage
	hasSeek    bool
	closeCh    chan struct{}
	once       sync.Once
}

func (pc *pulsarConsumer) Subscription() string {
	return pc.c.Subscription()
}

func (pc *pulsarConsumer) Chan() <-chan ConsumerMessage {
	if pc.msgChannel == nil {
		pc.once.Do(func() {
			pc.msgChannel = make(chan ConsumerMessage, 256)
			// this part handles msgstream expectation when the consumer is not seeked
			// pulsar's default behavior is setting postition to the earliest pointer when client of the same subscription pointer is not acked
			// yet, our message stream is to setting to the very start point of the topic
			if !pc.hasSeek {
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
						pc.msgChannel <- &pulsarMessage{msg: msg}
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

func (pc *pulsarConsumer) Seek(id MessageID) error {
	messageID := id.(*pulsarID).messageID
	err := pc.c.Seek(messageID)
	if err == nil {
		pc.hasSeek = true
	}
	return err
}

func (pc *pulsarConsumer) Ack(message ConsumerMessage) {
	pm := message.(*pulsarMessage)
	pc.c.Ack(pm.msg)
}

func (pc *pulsarConsumer) Close() {
	pc.c.Close()
	close(pc.closeCh)
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
