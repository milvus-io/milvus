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
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestNewPulsarClient(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
}

func TestPulsarCreateProducer(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "test_CreateProducer"
	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
}

func TestPulsarSubscribe(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "test_Subscribe"
	subName := "subName"
	consumer, err := pc.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
}
