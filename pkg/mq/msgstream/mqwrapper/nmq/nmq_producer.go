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

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
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
	start := timerecord.NewTimeRecorder("send msg to stream")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.TotalLabel).Inc()

	// Encode message
	msg := &nats.Msg{
		Subject: np.topic,
		Header:  make(nats.Header, len(message.Properties)),
		Data:    message.Payload,
	}
	for k, v := range message.Properties {
		msg.Header.Add(k, v)
	}

	// publish to nats-server
	pa, err := np.js.PublishMsg(msg)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		log.Warn("failed to publish message by nmq", zap.String("topic", np.topic), zap.Error(err), zap.Int("payload_size", len(message.Payload)))
		return nil, err
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.SendMsgLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.SuccessLabel).Inc()
	return &nmqID{messageID: pa.Sequence}, err
}

// Close does nothing currently
func (np *nmqProducer) Close() {
	// No specific producer to be closed.
	// stream doesn't close here.
}
