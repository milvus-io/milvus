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

package pmq

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/client"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var _ mqwrapper.Producer = (*pmqProducer)(nil)

// pmqProducer contains a pebblemq producer
type pmqProducer struct {
	p client.Producer
}

// Topic returns the topic of pmq producer
func (rp *pmqProducer) Topic() string {
	return rp.p.Topic()
}

// Send send the producer messages to pebblemq
func (rp *pmqProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	start := timerecord.NewTimeRecorder("send msg to stream")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.TotalLabel).Inc()

	pm := &client.ProducerMessage{Payload: message.Payload, Properties: message.Properties}
	id, err := rp.p.Send(pm)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		return &pmqID{messageID: id}, err
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.SendMsgLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.SuccessLabel).Inc()
	return &pmqID{messageID: id}, nil
}

// Close does nothing currently
func (rp *pmqProducer) Close() {
	//TODO: close producer. Now it has bug
	//rp.p.Close()
}
