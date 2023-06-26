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

package pulsar

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	pulsarctl "github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type pulsarClient struct {
	tenant    string
	namespace string
	client    pulsar.Client
}

var sc *pulsarClient

var once sync.Once

// NewClient creates a pulsarClient object
// according to the parameter opts of type pulsar.ClientOptions
func NewClient(tenant string, namespace string, opts pulsar.ClientOptions) (*pulsarClient, error) {
	once.Do(func() {
		c, err := pulsar.NewClient(opts)
		if err != nil {
			log.Error("Failed to set pulsar client: ", zap.Error(err))
			return
		}
		cli := &pulsarClient{
			client:    c,
			tenant:    tenant,
			namespace: namespace,
		}
		sc = cli
	})
	return sc, nil
}

// CreateProducer create a pulsar producer from options
func (pc *pulsarClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	start := timerecord.NewTimeRecorder("create producer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.TotalLabel).Inc()

	fullTopicName, err := GetFullTopicName(pc.tenant, pc.namespace, options.Topic)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, err
	}
	opts := pulsar.ProducerOptions{Topic: fullTopicName}
	if options.EnableCompression {
		opts.CompressionType = pulsar.ZSTD
		opts.CompressionLevel = pulsar.Faster
	}
	// disable automatic batching
	opts.DisableBatching = true
	// change the batching max publish delay higher to avoid extra cpu consumption
	opts.BatchingMaxPublishDelay = 1 * time.Minute

	pp, err := pc.client.CreateProducer(opts)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, err
	}
	if pp == nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, errors.New("pulsar is not ready, producer is nil")
	}
	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateProducerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.SuccessLabel).Inc()
	producer := &pulsarProducer{p: pp}
	return producer, nil
}

// Subscribe creates a pulsar consumer instance and subscribe a topic
func (pc *pulsarClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	start := timerecord.NewTimeRecorder("create consumer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.TotalLabel).Inc()

	receiveChannel := make(chan pulsar.ConsumerMessage, options.BufSize)
	fullTopicName, err := GetFullTopicName(pc.tenant, pc.namespace, options.Topic)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, err
	}
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       fullTopicName,
		SubscriptionName:            options.SubscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(options.SubscriptionInitialPosition),
		MessageChannel:              receiveChannel,
	})
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		if strings.Contains(err.Error(), "ConsumerBusy") {
			return nil, retry.Unrecoverable(err)
		}
		return nil, err
	}

	pConsumer := &Consumer{c: consumer, closeCh: make(chan struct{})}
	// prevent seek to earliest patch applied when using latest position options
	if options.SubscriptionInitialPosition == mqwrapper.SubscriptionPositionLatest {
		pConsumer.AtLatest = true
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateConsumerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.SuccessLabel).Inc()
	return pConsumer, nil
}

func GetFullTopicName(tenant string, namespace string, topic string) (string, error) {
	if len(tenant) == 0 || len(namespace) == 0 || len(topic) == 0 {
		log.Error("build full topic name failed",
			zap.String("tenant", tenant),
			zap.String("namesapce", namespace),
			zap.String("topic", topic))
		return "", errors.New("build full topic name failed")
	}

	return fmt.Sprintf("%s/%s/%s", tenant, namespace, topic), nil
}

func NewAdminClient(address, authPlugin, authParams string) (pulsarctl.Client, error) {
	config := common.Config{
		WebServiceURL: address,
		AuthPlugin:    authPlugin,
		AuthParams:    authParams,
	}
	admin, err := pulsarctl.New(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to build pulsar admin client due to %s", err.Error())
	}

	return admin, nil
}

// EarliestMessageID returns the earliest message id
func (pc *pulsarClient) EarliestMessageID() mqwrapper.MessageID {
	msgID := pulsar.EarliestMessageID()
	return &pulsarID{messageID: msgID}
}

// StringToMsgID converts the string id to MessageID type
func (pc *pulsarClient) StringToMsgID(id string) (mqwrapper.MessageID, error) {
	pID, err := stringToMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

// BytesToMsgID converts []byte id to MessageID type
func (pc *pulsarClient) BytesToMsgID(id []byte) (mqwrapper.MessageID, error) {
	pID, err := DeserializePulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

// Close closes the pulsar client
func (pc *pulsarClient) Close() {
	// FIXME(yukun): pulsar.client is a singleton, so can't invoke this close when server run
	// pc.client.Close()
}
