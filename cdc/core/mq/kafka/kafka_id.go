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

package kafka

import (
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/util"
)

type kafkaID struct {
	util.CDCMark

	messageID int64
}

var _ api.MessageID = &kafkaID{}

func (kid *kafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.messageID)
}

func (kid *kafkaID) AtEarliestPosition() bool {
	return kid.messageID <= 0
}

func (kid *kafkaID) Equal(msgID []byte) (bool, error) {
	return kid.messageID == DeserializeKafkaID(msgID), nil
}

func (kid *kafkaID) LessOrEqualThan(msgID []byte) (bool, error) {
	return kid.messageID <= DeserializeKafkaID(msgID), nil
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	util.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) int64 {
	return int64(util.Endian.Uint64(messageID))
}
