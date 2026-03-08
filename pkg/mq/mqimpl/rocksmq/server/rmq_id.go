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

package server

import (
	"github.com/milvus-io/milvus/pkg/v2/common"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
)

// rmqID wraps message ID for rocksmq
type RmqID struct {
	MessageID UniqueID
}

// Check if rmqID implements MessageID interface
var _ mqcommon.MessageID = &RmqID{}

// Serialize convert rmq message id to []byte
func (rid *RmqID) Serialize() []byte {
	return SerializeRmqID(rid.MessageID)
}

func (rid *RmqID) AtEarliestPosition() bool {
	return rid.MessageID <= 0
}

func (rid *RmqID) LessOrEqualThan(msgID []byte) (bool, error) {
	rMsgID := DeserializeRmqID(msgID)
	return rid.MessageID <= rMsgID, nil
}

func (rid *RmqID) Equal(msgID []byte) (bool, error) {
	rMsgID := DeserializeRmqID(msgID)
	return rid.MessageID == rMsgID, nil
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
