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
	"encoding/binary"

	"github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

type rmqID struct {
	messageID rocksmq.UniqueID
}

// Check if rmqID implements MessageID interface
var _ MessageID = &rmqID{}

func (rid *rmqID) Serialize() []byte {
	return SerializeRmqID(rid.messageID)
}

func (rid *rmqID) LedgerID() int64 {
	// TODO
	return 0
}

func (rid *rmqID) EntryID() int64 {
	// TODO
	return 0
}

func (rid *rmqID) BatchIdx() int32 {
	// TODO
	return 0
}

func (rid *rmqID) PartitionIdx() int32 {
	// TODO
	return 0
}

// SerializeRmqID is used to serialize a message ID to byte array
func SerializeRmqID(messageID int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(messageID))
	return b
}

// DeserializeRmqID is used to deserialize a message ID from byte array
func DeserializeRmqID(messageID []byte) (int64, error) {
	return int64(binary.LittleEndian.Uint64(messageID)), nil
}
