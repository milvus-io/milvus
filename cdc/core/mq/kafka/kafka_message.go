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
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/util"
)

type kafkaMessage struct {
	util.CDCMark

	msg *kafka.Message
}

func (km *kafkaMessage) Topic() string {
	return *km.msg.TopicPartition.Topic
}

func (km *kafkaMessage) Properties() map[string]string {
	properties := make(map[string]string)
	for _, header := range km.msg.Headers {
		properties[header.Key] = string(header.Value)
	}
	return properties
}

func (km *kafkaMessage) Payload() []byte {
	return km.msg.Value
}

func (km *kafkaMessage) ID() api.MessageID {
	kid := &kafkaID{messageID: int64(km.msg.TopicPartition.Offset)}
	return kid
}
