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

// Check pulsarMessage implements ConsumerMessage
var _ Message = (*pulsarMessage)(nil)

type pulsarMessage struct {
	msg pulsar.Message
}

func (pm *pulsarMessage) Topic() string {
	return pm.msg.Topic()
}

func (pm *pulsarMessage) Properties() map[string]string {
	return pm.msg.Properties()
}

func (pm *pulsarMessage) Payload() []byte {
	return pm.msg.Payload()
}

func (pm *pulsarMessage) ID() MessageID {
	id := pm.msg.ID()
	pid := &pulsarID{messageID: id}
	return pid
}

var _ = (*pulsarReaderMessage)(nil)

type pulsarReaderMessage struct {
	msg pulsar.Message
}

// Topic get the topic from which this message originated from
func (m *pulsarReaderMessage) Topic() string {
	return m.msg.Topic()
}

// Properties are application defined key/value pairs that will be attached to the message.
// Return the properties attached to the message.
func (m *pulsarReaderMessage) Properties() map[string]string {
	return m.msg.Properties()
}

// Payload get the payload of the message
func (m *pulsarReaderMessage) Payload() []byte {
	return m.msg.Payload()
}

// ID get the unique message ID associated with this message.
// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
func (m *pulsarReaderMessage) ID() MessageID {
	id := m.msg.ID()
	pid := &pulsarID{messageID: id}
	return pid
}
