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
	"errors"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.uber.org/zap"
)

type pulsarClient struct {
	client pulsar.Client
}

var sc *pulsarClient
var once sync.Once

func isPulsarError(err error, result ...pulsar.Result) bool {
	if len(result) == 0 {
		return false
	}

	perr, ok := err.(*pulsar.Error)
	if !ok {
		return false
	}
	for _, r := range result {
		if perr.Result() == r {
			return true
		}
	}

	return false
}

// GetPulsarClientInstance creates a pulsarClient object
// according to the parameter opts of type pulsar.ClientOptions
func GetPulsarClientInstance(opts pulsar.ClientOptions) (*pulsarClient, error) {
	once.Do(func() {
		c, err := pulsar.NewClient(opts)
		if err != nil {
			log.Error("Failed to set pulsar client: ", zap.Error(err))
			return
		}
		cli := &pulsarClient{client: c}
		sc = cli
	})
	return sc, nil
}

// CreateProducer create a pulsar producer from options
func (pc *pulsarClient) CreateProducer(options ProducerOptions) (Producer, error) {
	opts := pulsar.ProducerOptions{Topic: options.Topic}
	pp, err := pc.client.CreateProducer(opts)
	if err != nil {
		return nil, err
	}
	if pp == nil {
		return nil, errors.New("pulsar is not ready, producer is nil")
	}
	producer := &pulsarProducer{p: pp}
	return producer, nil
}

// CreateReader creates a pulsar reader instance
func (pc *pulsarClient) CreateReader(options ReaderOptions) (Reader, error) {
	opts := pulsar.ReaderOptions{
		Topic:                   options.Topic,
		StartMessageID:          options.StartMessageID.(*pulsarID).messageID,
		StartMessageIDInclusive: options.StartMessageIDInclusive,
		SubscriptionRolePrefix:  options.SubscriptionRolePrefix,
	}
	pr, err := pc.client.CreateReader(opts)
	if err != nil {
		return nil, err
	}
	if pr == nil {
		return nil, errors.New("pulsar is not ready, producer is nil")
	}
	reader := &pulsarReader{r: pr}
	return reader, nil
}

// Subscribe creates a pulsar consumer instance and subscribe a topic
func (pc *pulsarClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	receiveChannel := make(chan pulsar.ConsumerMessage, options.BufSize)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       options.Topic,
		SubscriptionName:            options.SubscriptionName,
		Type:                        pulsar.SubscriptionType(options.Type),
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(options.SubscriptionInitialPosition),
		MessageChannel:              receiveChannel,
	})
	if err != nil {
		// exclusive consumer already exist
		if isPulsarError(err, pulsar.ConsumerBusy) {
			return nil, retry.Unrecoverable(err)
		}
		return nil, err
	}

	pConsumer := &PulsarConsumer{c: consumer, closeCh: make(chan struct{})}
	// prevent seek to earliest patch applied when using latest position options
	if options.SubscriptionInitialPosition == SubscriptionPositionLatest {
		pConsumer.AtLatest = true
	}

	return pConsumer, nil
}

// EarliestMessageID returns the earliest message id
func (pc *pulsarClient) EarliestMessageID() MessageID {
	msgID := pulsar.EarliestMessageID()
	return &pulsarID{messageID: msgID}
}

// StringToMsgID converts the string id to MessageID type
func (pc *pulsarClient) StringToMsgID(id string) (MessageID, error) {
	pID, err := StringToPulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

// BytesToMsgID converts []byte id to MessageID type
func (pc *pulsarClient) BytesToMsgID(id []byte) (MessageID, error) {
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
