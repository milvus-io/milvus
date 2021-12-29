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
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewReader(t *testing.T) {
	reader, err := newReader(nil, nil)
	assert.Error(t, err)
	assert.Nil(t, reader)

	reader, err = newReader(newMockClient(), nil)
	assert.Error(t, err)
	assert.Nil(t, reader)

	options := &ReaderOptions{}
	reader, err = newReader(newMockClient(), options)
	assert.Error(t, err)
	assert.Nil(t, reader)

	options.Topic = newTopicName()
	reader, err = newReader(newMockClient(), options)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestReader_Next(t *testing.T) {
	os.MkdirAll(rmqPath, os.ModePerm)
	rmqPathTest := rmqPath + "/test_reader"
	rmq := newRocksMQ(t, rmqPathTest)
	defer removePath(rmqPath)
	client, err := newClient(ClientOptions{
		Server: rmq,
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	topicName := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.NotNil(t, producer)
	assert.NoError(t, err)

	msgNum := 10
	ids := make([]UniqueID, 0)
	for i := 0; i < msgNum; i++ {
		msg := &ProducerMessage{
			Payload: []byte("message_" + strconv.FormatInt(int64(i), 10)),
		}
		id, err := producer.Send(msg)
		assert.NoError(t, err)
		ids = append(ids, id)
	}

	reader1, err := newReader(client, &ReaderOptions{
		Topic:                   topicName,
		StartMessageIDInclusive: true,
		SubscriptionRolePrefix:  "reder1",
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader1)
	assert.Equal(t, reader1.Topic(), topicName)
	defer reader1.Close()

	reader1.Seek(ids[1])
	ctx := context.Background()
	for i := 1; i < msgNum; i++ {
		assert.True(t, reader1.HasNext())
		rMsg, err := reader1.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, rMsg.MsgID, ids[i])
	}
	assert.False(t, reader1.HasNext())

	reader2, err := newReader(client, &ReaderOptions{
		Topic:                   topicName,
		StartMessageIDInclusive: false,
		SubscriptionRolePrefix:  "reader2",
	})
	assert.NoError(t, err)

	reader2.Seek(ids[5])
	for i := 5; i < msgNum-1; i++ {
		assert.True(t, reader2.HasNext())
		rMsg, err := reader2.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, rMsg.MsgID, ids[i+1])
	}
	assert.False(t, reader2.HasNext())
}
