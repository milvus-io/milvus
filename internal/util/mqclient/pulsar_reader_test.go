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
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestPulsarReader(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)
	defer pc.Close()

	rand.Seed(time.Now().UnixNano())
	topic := fmt.Sprintf("test-%d", rand.Int())

	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	defer producer.Close()

	const N = 10
	var seekID MessageID
	for i := 0; i < N; i++ {
		msg := &ProducerMessage{
			Payload:    []byte(fmt.Sprintf("helloworld-%d", i)),
			Properties: map[string]string{},
		}

		id, err := producer.Send(ctx, msg)
		assert.Nil(t, err)
		if i == 4 {
			seekID = &pulsarID{messageID: id.(*pulsarID).messageID}
		}
	}

	reader, err := pc.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: pc.EarliestMessageID(),
	})
	assert.Nil(t, err)
	assert.NotNil(t, reader)
	defer reader.Close()

	str := reader.Topic()
	assert.NotNil(t, str)

	for i := 0; i < N; i++ {
		revMsg, err := reader.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, revMsg)
	}

	readerOfStartMessageID, err := pc.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          seekID,
		StartMessageIDInclusive: true,
	})
	assert.Nil(t, err)
	defer readerOfStartMessageID.Close()

	for i := 4; i < N; i++ {
		assert.True(t, readerOfStartMessageID.HasNext())
		revMsg, err := readerOfStartMessageID.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, revMsg)
	}

	readerOfSeek, err := pc.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: pc.EarliestMessageID(),
	})
	assert.Nil(t, err)
	defer readerOfSeek.Close()

	err = reader.Seek(seekID)
	assert.Nil(t, err)

	for i := 4; i < N; i++ {
		assert.True(t, readerOfSeek.HasNext())
		revMsg, err := readerOfSeek.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, revMsg)
	}

}
