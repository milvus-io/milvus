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

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

type pulsarID struct {
	messageID pulsar.MessageID
}

// Check if pulsarID implements and MessageID interface
var _ mqwrapper.MessageID = &pulsarID{}

func NewPulsarID(messageID pulsar.MessageID) mqwrapper.MessageID {
	return &pulsarID{
		messageID: messageID,
	}
}

// TODO: remove in future, remove mqwrapper layer.
func (pid *pulsarID) PulsarID() []byte {
	return pid.Serialize()
}

func (pid *pulsarID) Serialize() []byte {
	return pid.messageID.Serialize()
}

func (pid *pulsarID) AtEarliestPosition() bool {
	if pid.messageID.PartitionIdx() <= 0 &&
		pid.messageID.LedgerID() <= 0 &&
		pid.messageID.EntryID() <= 0 &&
		pid.messageID.BatchIdx() <= 0 {
		return true
	}
	return false
}

func (pid *pulsarID) LessOrEqualThan(msgID []byte) (bool, error) {
	pMsgID, err := pulsar.DeserializeMessageID(msgID)
	if err != nil {
		return false, err
	}

	return pid.LTE(&pulsarID{messageID: pMsgID}), nil
}

func (pid *pulsarID) Equal(msgID []byte) (bool, error) {
	pMsgID, err := pulsar.DeserializeMessageID(msgID)
	if err != nil {
		return false, err
	}

	return pid.EQ(&pulsarID{messageID: pMsgID}), nil
}

// LT less than.
func (pid *pulsarID) LT(id2 mqwrapper.MessageID) bool {
	other := id2.(*pulsarID)
	if pid.messageID.LedgerID() != other.messageID.LedgerID() {
		return pid.messageID.LedgerID() < other.messageID.LedgerID()
	}

	if pid.messageID.EntryID() != other.messageID.EntryID() {
		return pid.messageID.EntryID() < other.messageID.EntryID()
	}
	return pid.messageID.BatchIdx() < pid.messageID.BatchIdx()
}

// LTE less than or equal to.
func (pid *pulsarID) LTE(id2 mqwrapper.MessageID) bool {
	other := id2.(*pulsarID).messageID
	if pid.messageID.LedgerID() != other.LedgerID() {
		return pid.messageID.LedgerID() < other.LedgerID()
	}

	if pid.messageID.EntryID() != other.EntryID() {
		return pid.messageID.EntryID() < other.EntryID()
	}
	return pid.messageID.BatchIdx() <= pid.messageID.BatchIdx()
}

// EQ Equal to.
func (pid *pulsarID) EQ(id2 mqwrapper.MessageID) bool {
	other := id2.(*pulsarID).messageID
	return pid.messageID.LedgerID() == other.LedgerID() &&
		pid.messageID.EntryID() == other.EntryID() &&
		pid.messageID.BatchIdx() == other.BatchIdx()
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
