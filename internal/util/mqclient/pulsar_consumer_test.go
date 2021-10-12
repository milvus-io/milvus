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
	"fmt"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestPulsarConsumer_Subscription(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)
	defer pc.Close()

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "Topic",
		SubscriptionName:            "SubName",
		Type:                        pulsar.SubscriptionType(Exclusive),
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	str := consumer.Subscription()
	assert.NotNil(t, str)
}

func Test_PatchEarliestMessageID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	// String() -> ledgerID:entryID:partitionIdx
	assert.Equal(t, "-1:-1:-1", fmt.Sprintf("%v", mid))

	patchEarliestMessageID(&mid)

	assert.Equal(t, "-1:-1:0", fmt.Sprintf("%v", mid))
}
