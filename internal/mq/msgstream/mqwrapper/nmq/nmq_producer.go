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
	"context"
	"encoding/json"

	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/nats-io/nats.go"
)

var _ mqwrapper.Producer = (*nmqProducer)(nil)

// nmqProducer contains a natsmq producer
type nmqProducer struct {
	js    nats.JetStreamContext
	topic string
}

// Topic returns the topic of nmq producer
func (np *nmqProducer) Topic() string {
	return np.topic
}

// Send send the producer messages to natsmq
func (np *nmqProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	msg := NatsMsgData{
		Payload:    message.Payload,
		Properties: message.Properties,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, util.WrapError("failed to create Message payload", err)
	}
	pa, err := np.js.Publish(np.topic, payload)
	return &nmqID{messageID: pa.Sequence}, err
}

// Close does nothing currently
func (np *nmqProducer) Close() {
	// No specific producer to be closed.
	// stream doesn't close here.
}
