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

package mqclient

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

// implementation assertion
var _ Producer = (*pulsarProducer)(nil)

type pulsarProducer struct {
	p pulsar.Producer
}

// Topic returns the topic name of pulsar producer
func (pp *pulsarProducer) Topic() string {
	return pp.p.Topic()
}

func (pp *pulsarProducer) Send(ctx context.Context, message *ProducerMessage) (MessageID, error) {
	ppm := &pulsar.ProducerMessage{Payload: message.Payload, Properties: message.Properties}
	pmID, err := pp.p.Send(ctx, ppm)
	return &pulsarID{messageID: pmID}, err
}

func (pp *pulsarProducer) Close() {
	pp.p.Close()
}
