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

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	// invalid client
	producer, err := newProducer(nil, ProducerOptions{
		Topic: newTopicName(),
	})
	assert.Nil(t, producer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	// invalid produceroptions
	producer, err = newProducer(newMockClient(), ProducerOptions{})
	assert.Nil(t, producer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())
}

func TestProducerTopic(t *testing.T) {
	topicName := newTopicName()
	producer, err := newProducer(newMockClient(), ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, producer)
	assert.NotNil(t, err)
	//assert.Equal(t, topicName, producer.Topic())
}
