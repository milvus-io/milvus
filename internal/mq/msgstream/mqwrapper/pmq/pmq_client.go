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

package pmq

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/client"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/server"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// pmqClient implements mqwrapper.Client.
var _ mqwrapper.Client = &pmqClient{}

// pmqClient contains a pebblemq client
type pmqClient struct {
	client client.Client
}

func NewClientWithDefaultOptions(ctx context.Context) (mqwrapper.Client, error) {
	option := client.Options{Server: server.Pmq}
	return NewClient(option)
}

// NewClient returns a new pmqClient object
func NewClient(opts client.Options) (*pmqClient, error) {
	c, err := client.NewClient(opts)
	if err != nil {
		log.Error("Failed to set pmq client: ", zap.Error(err))
		return nil, err
	}
	return &pmqClient{client: c}, nil
}

// CreateProducer creates a producer for pebblemq client
func (rc *pmqClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	start := timerecord.NewTimeRecorder("create producer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.TotalLabel).Inc()

	pmqOpts := client.ProducerOptions{Topic: options.Topic}
	pp, err := rc.client.CreateProducer(pmqOpts)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, err
	}
	rp := pmqProducer{p: pp}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateProducerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.SuccessLabel).Inc()
	return &rp, nil
}

// Subscribe subscribes a consumer in pmq client
func (rc *pmqClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	start := timerecord.NewTimeRecorder("create consumer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.TotalLabel).Inc()

	if options.BufSize == 0 {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		err := errors.New("subscription bufSize of rmq should never be zero")
		log.Warn("unexpected subscription consumer options", zap.Error(err))
		return nil, err
	}
	receiveChannel := make(chan client.Message, options.BufSize)

	cli, err := rc.client.Subscribe(client.ConsumerOptions{
		Topic:                       options.Topic,
		SubscriptionName:            options.SubscriptionName,
		MessageChannel:              receiveChannel,
		SubscriptionInitialPosition: options.SubscriptionInitialPosition,
	})
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, err
	}

	rConsumer := &Consumer{c: cli, closeCh: make(chan struct{})}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateConsumerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.SuccessLabel).Inc()
	return rConsumer, nil
}

// EarliestMessageID returns the earliest message ID for pmq client
func (rc *pmqClient) EarliestMessageID() mqwrapper.MessageID {
	rID := client.EarliestMessageID()
	return &pmqID{messageID: rID}
}

// StringToMsgID converts string id to MessageID
func (rc *pmqClient) StringToMsgID(id string) (mqwrapper.MessageID, error) {
	rID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &pmqID{messageID: rID}, nil
}

// BytesToMsgID converts a byte array to messageID
func (rc *pmqClient) BytesToMsgID(id []byte) (mqwrapper.MessageID, error) {
	rID := DeserializePmqID(id)
	return &pmqID{messageID: rID}, nil
}

func (rc *pmqClient) Close() {
	rc.client.Close()
}
