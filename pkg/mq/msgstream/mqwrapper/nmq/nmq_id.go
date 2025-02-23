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

package nmq

import (
	"github.com/milvus-io/milvus/pkg/v2/common"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
)

// MessageIDType is a type alias for server.UniqueID that represents the ID of a Nmq message.
type MessageIDType = uint64

// nmqID wraps message ID for natsmq
type nmqID struct {
	messageID MessageIDType
}

// Check if nmqID implements MessageID interface
var _ mqcommon.MessageID = &nmqID{}

// NewNmqID creates and returns a new instance of the nmqID struct with the given MessageID.
func NewNmqID(id MessageIDType) mqcommon.MessageID {
	return &nmqID{
		messageID: id,
	}
}

// Serialize convert nmq message id to []byte
func (nid *nmqID) Serialize() []byte {
	return SerializeNmqID(nid.messageID)
}

func (nid *nmqID) AtEarliestPosition() bool {
	return nid.messageID <= 1
}

func (nid *nmqID) LessOrEqualThan(msgID []byte) (bool, error) {
	return nid.messageID <= DeserializeNmqID(msgID), nil
}

func (nid *nmqID) Equal(msgID []byte) (bool, error) {
	return nid.messageID == DeserializeNmqID(msgID), nil
}

// SerializeNmqID is used to serialize a message ID to byte array
func SerializeNmqID(messageID MessageIDType) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, messageID)
	return b
}

// DeserializeNmqID is used to deserialize a message ID from byte array
func DeserializeNmqID(messageID []byte) MessageIDType {
	return common.Endian.Uint64(messageID)
}
