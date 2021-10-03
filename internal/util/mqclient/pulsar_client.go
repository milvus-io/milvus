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

package mqclient

import (
	"errors"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type pulsarClient struct {
	client pulsar.Client
}

var sc *pulsarClient
var once sync.Once

// GetPulsarClientInstance creates a pulsarClient object
// according to the parameter opts of type pulsar.ClientOptions
func GetPulsarClientInstance(opts pulsar.ClientOptions) (*pulsarClient, error) {
	once.Do(func() {
		c, err := pulsar.NewClient(opts)
		if err != nil {
			log.Error("Set pulsar client failed, error", zap.Error(err))
			return
		}
		cli := &pulsarClient{client: c}
		sc = cli
	})
	return sc, nil
}

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
		return nil, err
	}
	//consumer.Seek(pulsar.EarliestMessageID())
	//consumer.SeekByTime(time.Unix(0, 0))
	pConsumer := &pulsarConsumer{c: consumer, closeCh: make(chan struct{})}

	return pConsumer, nil
}

func (pc *pulsarClient) EarliestMessageID() MessageID {
	msgID := pulsar.EarliestMessageID()
	return &pulsarID{messageID: msgID}
}

func (pc *pulsarClient) StringToMsgID(id string) (MessageID, error) {
	pID, err := StringToPulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

func (pc *pulsarClient) BytesToMsgID(id []byte) (MessageID, error) {
	pID, err := DeserializePulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

func (pc *pulsarClient) Close() {
	pc.client.Close()
}
