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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	mqcommon "github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
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
	var err error
	once.Do(func() {
		var c pulsar.Client
		c, err = pulsar.NewClient(opts)
		if err != nil {
			mlog.Error(context.TODO(), "Failed to set pulsar client: ", zap.Error(err))
			return
		}
		cli := &pulsarClient{
			client:    c,
			tenant:    tenant,
			namespace: namespace,
		}
		sc = cli
	})
	return sc, err
}

// CreateProducer create a pulsar producer from options
func (pc *pulsarClient) CreateProducer(ctx context.Context, options mqcommon.ProducerOptions) (mqwrapper.Producer, error) {
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
		return nil, merr.WrapErrServiceUnavailable("pulsar is not ready, producer is nil")
	}
	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateProducerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.SuccessLabel).Inc()
	producer := &pulsarProducer{p: pp}
	return producer, nil
}

// Subscribe creates a pulsar consumer instance and subscribe a topic
func (pc *pulsarClient) Subscribe(ctx context.Context, options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
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
		StartMessageIDInclusive:     true,
	})
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, err
	}

	pConsumer := &Consumer{c: consumer, closeCh: make(chan struct{})}
	// prevent seek to earliest patch applied when using latest position options
	if options.SubscriptionInitialPosition == mqcommon.SubscriptionPositionLatest {
		pConsumer.AtLatest = true
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateConsumerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.SuccessLabel).Inc()
	return pConsumer, nil
}

func GetFullTopicName(tenant string, namespace string, topic string) (string, error) {
	if len(tenant) == 0 || len(namespace) == 0 || len(topic) == 0 {
		mlog.Error(context.TODO(), "build full topic name failed",
			zap.String("tenant", tenant),
			zap.String("namesapce", namespace),
			zap.String("topic", topic))
		return "", merr.WrapErrMqInternalMsg("build full topic name failed")
	}

	return fmt.Sprintf("%s/%s/%s", tenant, namespace, topic), nil
}

func NewAdminClient(address, authPlugin, authParams string) (admin.Client, error) {
	cfg := config.Config{
		WebServiceURL: address,
		AuthPlugin:    authPlugin,
		AuthParams:    authParams,
	}
	adminClient, err := admin.New(&cfg)
	if err != nil {
		return nil, merr.WrapErrMqInternal(err, "failed to build pulsar admin client")
	}

	return adminClient, nil
}

// EarliestMessageID returns the earliest message id
func (pc *pulsarClient) EarliestMessageID() mqcommon.MessageID {
	msgID := pulsar.EarliestMessageID()
	return &pulsarID{messageID: msgID}
}

// StringToMsgID converts the string id to MessageID type
func (pc *pulsarClient) StringToMsgID(id string) (mqcommon.MessageID, error) {
	pID, err := stringToMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

// BytesToMsgID converts []byte id to MessageID type
func (pc *pulsarClient) BytesToMsgID(id []byte) (mqcommon.MessageID, error) {
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
