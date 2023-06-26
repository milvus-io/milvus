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

	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var _ mqwrapper.Producer = (*rmqProducer)(nil)

// rmqProducer contains a rocksmq producer
type rmqProducer struct {
	p client.Producer
}

// Topic returns the topic of rmq producer
func (rp *rmqProducer) Topic() string {
	return rp.p.Topic()
}

// Send send the producer messages to rocksmq
func (rp *rmqProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	start := timerecord.NewTimeRecorder("send msg to stream")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.TotalLabel).Inc()

	pm := &client.ProducerMessage{Payload: message.Payload, Properties: message.Properties}
	id, err := rp.p.Send(pm)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		return &rmqID{messageID: id}, err
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.SendMsgLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.SuccessLabel).Inc()
	return &rmqID{messageID: id}, nil
}

// Close does nothing currently
func (rp *rmqProducer) Close() {
	//TODO: close producer. Now it has bug
	//rp.p.Close()
}
