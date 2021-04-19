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

package rocksmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	consumer, err := newConsumer(nil, ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: SubscriptionPositionLatest,
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = newConsumer(newMockClient(), ConsumerOptions{})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = newConsumer(newMockClient(), ConsumerOptions{
		Topic: newTopicName(),
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())
}

func TestSubscription(t *testing.T) {
	topicName := newTopicName()
	consumerName := newConsumerName()
	consumer, err := newConsumer(newMockClient(), ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: consumerName,
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	//assert.Equal(t, consumerName, consumer.Subscription())
}
