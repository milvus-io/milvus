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
	"strings"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"

	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarID struct {
	messageID pulsar.MessageID
}

// Check if pulsarID implements pulsar.MessageID and MessageID interface
var _ pulsar.MessageID = &pulsarID{}
var _ mqwrapper.MessageID = &pulsarID{}

func (pid *pulsarID) Serialize() []byte {
	return pid.messageID.Serialize()
}

func (pid *pulsarID) LedgerID() int64 {
	return pid.messageID.LedgerID()
}

func (pid *pulsarID) EntryID() int64 {
	return pid.messageID.EntryID()
}

func (pid *pulsarID) BatchIdx() int32 {
	return pid.messageID.BatchIdx()
}

func (pid *pulsarID) PartitionIdx() int32 {
	return pid.messageID.PartitionIdx()
}

// SerializePulsarMsgID returns the serialized message ID
func SerializePulsarMsgID(messageID pulsar.MessageID) []byte {
	return messageID.Serialize()
}

// DeserializePulsarMsgID returns the deserialized message ID
func DeserializePulsarMsgID(messageID []byte) (pulsar.MessageID, error) {
	return pulsar.DeserializeMessageID(messageID)
}

// msgIDToString is used to convert a message ID to string
func msgIDToString(messageID pulsar.MessageID) string {
	return strings.ToValidUTF8(string(messageID.Serialize()), "")
}

// StringToMsgID is used to convert a string to message ID
func stringToMsgID(msgString string) (pulsar.MessageID, error) {
	return pulsar.DeserializeMessageID([]byte(msgString))
}
