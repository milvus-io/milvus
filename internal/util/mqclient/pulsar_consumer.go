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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
)

type pulsarConsumer struct {
	c          pulsar.Consumer
	msgChannel chan ConsumerMessage
	hasSeek    bool
}

func (pc *pulsarConsumer) Subscription() string {
	return pc.c.Subscription()
}

func (pc *pulsarConsumer) Chan() <-chan ConsumerMessage {
	if pc.msgChannel == nil {
		pc.msgChannel = make(chan ConsumerMessage)
		if !pc.hasSeek {
			pc.c.SeekByTime(time.Unix(0, 0))
		}
		go func() {
			for { //nolint:gosimple
				select {
				case msg, ok := <-pc.c.Chan():
					if !ok {
						close(pc.msgChannel)
						log.Debug("pulsar consumer channel closed")
						return
					}
					pc.msgChannel <- &pulsarMessage{msg: msg}
				}
			}
		}()
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
}
