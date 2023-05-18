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

package pmq

import (
	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/server"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// pmqID wraps message ID for pebblemq
type pmqID struct {
	messageID server.UniqueID
}

// Check if pmqID implements MessageID interface
var _ mqwrapper.MessageID = &pmqID{}

// Serialize convert pmq message id to []byte
func (rid *pmqID) Serialize() []byte {
	return SerializePmqID(rid.messageID)
}

func (rid *pmqID) AtEarliestPosition() bool {
	return rid.messageID <= 0
}

func (rid *pmqID) LessOrEqualThan(msgID []byte) (bool, error) {
	rMsgID := DeserializePmqID(msgID)
	return rid.messageID <= rMsgID, nil
}

func (rid *pmqID) Equal(msgID []byte) (bool, error) {
	rMsgID := DeserializePmqID(msgID)
	return rid.messageID == rMsgID, nil
}

// SerializePmqID is used to serialize a message ID to byte array
func SerializePmqID(messageID int64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

// DeserializePmqID is used to deserialize a message ID from byte array
func DeserializePmqID(messageID []byte) int64 {
	return int64(common.Endian.Uint64(messageID))
}
