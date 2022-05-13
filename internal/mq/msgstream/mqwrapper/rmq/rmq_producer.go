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

package rmq

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/client"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

var _ mqwrapper.Producer = (*rmqProducer)(nil)

// rmqProducer contains a pebblemq producer
type rmqProducer struct {
	p client.Producer
}

// Topic returns the topic of rmq producer
func (rp *rmqProducer) Topic() string {
	return rp.p.Topic()
}

// Send send the producer messages to pebblemq
func (rp *rmqProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	pm := &client.ProducerMessage{Payload: message.Payload}
	id, err := rp.p.Send(pm)
	return &rmqID{messageID: id}, err
}

// Close does nothing currently
func (rp *rmqProducer) Close() {
	//TODO: close producer. Now it has bug
	//rp.p.Close()
}
