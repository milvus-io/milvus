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
	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarConsumer struct {
	c          pulsar.Consumer
	msgChannel chan ConsumerMessage
}

func (pc *pulsarConsumer) Subscription() string {
	return pc.c.Subscription()
}

func (pc *pulsarConsumer) Chan() <-chan ConsumerMessage {
	return pc.msgChannel
}

func (pc *pulsarConsumer) Seek(id MessageID) error {
	messageID := id.(*pulsarID).messageID
	return pc.c.Seek(messageID)
}

func (pc *pulsarConsumer) Ack(message ConsumerMessage) {
	pm := message.(*pulsarMessage)
	pc.c.Ack(pm.msg)
}

func (pc *pulsarConsumer) Close() {
	pc.c.Close()
}
