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

package rmq

import (
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

// rmqID wraps message ID for rocksmq
type rmqID struct {
	messageID server.UniqueID
}

// Check if rmqID implements MessageID interface
var _ mqwrapper.MessageID = &rmqID{}

// Serialize convert rmq message id to []byte
func (rid *rmqID) Serialize() []byte {
	return SerializeRmqID(rid.messageID)
}

func (rid *rmqID) AtEarliestPosition() bool {
	return rid.messageID <= 0
}

func (rid *rmqID) LessOrEqualThan(msgID []byte) (bool, error) {
	rMsgID := DeserializeRmqID(msgID)
	return rid.messageID < rMsgID, nil
}

func (rid *rmqID) Equal(msgID []byte) (bool, error) {
	rMsgID := DeserializeRmqID(msgID)
	return rid.messageID == rMsgID, nil
}

// SerializeRmqID is used to serialize a message ID to byte array
func SerializeRmqID(messageID int64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

// DeserializeRmqID is used to deserialize a message ID from byte array
func DeserializeRmqID(messageID []byte) int64 {
	return int64(common.Endian.Uint64(messageID))
}
