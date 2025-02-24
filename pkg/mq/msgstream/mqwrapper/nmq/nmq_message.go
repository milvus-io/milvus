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
	"log"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
)

// Check nmqMessage implements ConsumerMessage
var (
	_ common.Message = (*nmqMessage)(nil)
)

// nmqMessage wraps the message for natsmq
type nmqMessage struct {
	raw *nats.Msg

	// lazy initialized field.
	meta *nats.MsgMetadata
}

// Topic returns the topic name of natsmq message
func (nm *nmqMessage) Topic() string {
	// TODO: Dependency: implement of subscription logic of nmq.
	// 1:1 Subject:Topic model is appied on this implementation.
	// M:N model should be a optimize option in future.
	return nm.raw.Subject
}

// Properties returns the properties of natsmq message
func (nm *nmqMessage) Properties() map[string]string {
	properties := make(map[string]string, len(nm.raw.Header))
	for k, vs := range nm.raw.Header {
		if len(vs) > 0 {
			properties[k] = vs[0]
		}
	}
	return properties
}

// Payload returns the payload of natsmq message
func (nm *nmqMessage) Payload() []byte {
	return nm.raw.Data
}

// ID returns the id of natsmq message
func (nm *nmqMessage) ID() common.MessageID {
	if nm.meta == nil {
		var err error
		// raw is always a jetstream message, should never fail.
		nm.meta, err = nm.raw.Metadata()
		if err != nil {
			log.Fatal("raw is always a jetstream message, should never fail", zap.Error(err))
		}
	}
	return &nmqID{messageID: nm.meta.Sequence.Stream}
}
