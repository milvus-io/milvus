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
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPulsarReader_Basic(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)
	defer pc.Close()

	topic := "reader-topic-" + funcutil.RandomString(8)

	reader := &PulsarReader{
		topicName:  topic,
		client:     pc.client,
		msgChannel: make(chan Message, 100),
	}

	assert.Equal(t, topic, reader.Subscription())
	assert.False(t, reader.ConsumeAfterSeek())

	assert.NotPanics(t, func() { reader.Ack(nil) })
	assert.NotPanics(t, reader.Close)
	assert.NotPanics(t, reader.Close)
}

func TestPulsarReader(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)
	defer pc.Close()
	topic := "reader-topic-" + funcutil.RandomString(8)

	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	require.NoError(t, err)
	ctx := context.Background()
	// create topic data

	ids := make([]MessageID, 0, 10)
	for i := 0; i < 10; i++ {
		id, err := producer.Send(ctx, &ProducerMessage{
			Payload:    IntToBytes(i),
			Properties: map[string]string{},
		})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	producer.Close()

	reader := &PulsarReader{
		topicName:    topic,
		client:       pc.client,
		msgChannel:   make(chan Message, 100),
		initPosition: SubscriptionPositionEarliest,
	}

	ch := reader.Chan()
	for i := 0; i < 10; i++ {
		msg := <-ch
		assert.Equal(t, i, BytesToInt(msg.Payload()))
	}

	err = reader.Seek(ids[0])
	assert.NoError(t, err)
	for i := 1; i < 10; i++ {
		msg := <-ch

		assert.Equal(t, i, BytesToInt(msg.Payload()))
	}

	reader.Close()
	// test same topic
	reader = &PulsarReader{
		topicName:    topic,
		client:       pc.client,
		msgChannel:   make(chan Message, 100),
		initPosition: SubscriptionPositionEarliest,
	}
	ch = reader.Chan()
	for i := 0; i < 10; i++ {
		msg := <-ch
		assert.Equal(t, i, BytesToInt(msg.Payload()))
	}
	reader.Close()
}
