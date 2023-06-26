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
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// nmqClient implements mqwrapper.Client.
var _ mqwrapper.Client = &nmqClient{}

// nmqClient contains a natsmq client
type nmqClient struct {
	conn *nats.Conn
}

// NewClientWithDefaultOptions returns a new NMQ client with default options.
// It retrieves the NMQ client URL from the server configuration.
func NewClientWithDefaultOptions() (mqwrapper.Client, error) {
	url := Nmq.ClientURL()
	log.Info("123123 ", zap.String("url", url))
	return NewClient(url)
}

// NewClient returns a new nmqClient object
func NewClient(url string, options ...nats.Option) (*nmqClient, error) {
	c, err := nats.Connect(url, options...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to set nmq client")
	}
	return &nmqClient{conn: c}, nil
}

// CreateProducer creates a producer for natsmq client
func (nc *nmqClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	start := timerecord.NewTimeRecorder("create producer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.TotalLabel).Inc()

	// TODO: inject jetstream options.
	js, err := nc.conn.JetStream()
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, errors.Wrap(err, "failed to create jetstream context")
	}
	// TODO: (1) investigate on performance of multiple streams vs multiple topics.
	//       (2) investigate if we should have topics under the same stream.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     options.Topic,
		Subjects: []string{options.Topic},
	})
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, errors.Wrap(err, "failed to add/connect to jetstream for producer")
	}
	rp := nmqProducer{js: js, topic: options.Topic}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateProducerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.SuccessLabel).Inc()
	return &rp, nil
}

func (nc *nmqClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	start := timerecord.NewTimeRecorder("create consumer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.TotalLabel).Inc()

	if options.Topic == "" {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, fmt.Errorf("invalid consumer config: empty topic")
	}

	if options.SubscriptionName == "" {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, fmt.Errorf("invalid consumer config: empty subscription name")
	}
	// TODO: inject jetstream options.
	js, err := nc.conn.JetStream()
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, errors.Wrap(err, "failed to create jetstream context")
	}
	// TODO: do we allow passing in an existing natsChan from options?
	// also, revisit the size or make it a user param
	natsChan := make(chan *nats.Msg, options.BufSize)
	// TODO: should we allow subscribe to a topic that doesn't exist yet? Current logic allows it.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     options.Topic,
		Subjects: []string{options.Topic},
	})
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, errors.Wrap(err, "failed to add/connect to jetstream for consumer")
	}
	closeChan := make(chan struct{})

	var sub *nats.Subscription
	position := options.SubscriptionInitialPosition
	// TODO: should we only allow exclusive subscribe? Current logic allows double subscribe.
	switch position {
	case mqwrapper.SubscriptionPositionLatest:
		sub, err = js.ChanSubscribe(options.Topic, natsChan, nats.DeliverNew())
	case mqwrapper.SubscriptionPositionEarliest:
		sub, err = js.ChanSubscribe(options.Topic, natsChan, nats.DeliverAll())
	}
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, errors.Wrap(err, fmt.Sprintf("failed to get consumer info, subscribe position: %d", position))
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateConsumerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.SuccessLabel).Inc()
	return &Consumer{
		js:        js,
		sub:       sub,
		topic:     options.Topic,
		groupName: options.SubscriptionName,
		options:   options,
		natsChan:  natsChan,
		closeChan: closeChan,
	}, nil
}

// EarliestMessageID returns the earliest message ID for nmq client
func (nc *nmqClient) EarliestMessageID() mqwrapper.MessageID {
	return &nmqID{messageID: 1}
}

// StringToMsgID converts string id to MessageID
func (nc *nmqClient) StringToMsgID(id string) (mqwrapper.MessageID, error) {
	rID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse string to MessageID")
	}
	return &nmqID{messageID: rID}, nil
}

// BytesToMsgID converts a byte array to messageID
func (nc *nmqClient) BytesToMsgID(id []byte) (mqwrapper.MessageID, error) {
	rID := DeserializeNmqID(id)
	return &nmqID{messageID: rID}, nil
}

func (nc *nmqClient) Close() {
	nc.conn.Close()
}
