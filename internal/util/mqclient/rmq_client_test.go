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
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
	rocksmq1 "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	path := "/tmp/milvus/rdb_data"
	os.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)
	_ = rocksmq1.InitRocksMQ()
	exitCode := m.Run()
	defer rocksmq1.CloseRocksMQ()
	os.Exit(exitCode)
}

func Test_NewRmqClient(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, err := NewRmqClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)
}

func TestRmqClient_CreateProducer(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, err := NewRmqClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)

	topic := "TestRmqClient_CreateProducer"
	proOpts := ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(proOpts)
	defer producer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	rmqProducer := producer.(*rmqProducer)
	defer rmqProducer.Close()
	assert.Equal(t, rmqProducer.Topic(), topic)

	msg := &ProducerMessage{
		Payload:    []byte{},
		Properties: nil,
	}
	_, err = rmqProducer.Send(context.TODO(), msg)
	assert.Nil(t, err)

	invalidOpts := ProducerOptions{Topic: ""}
	producer, e := client.CreateProducer(invalidOpts)
	assert.Nil(t, producer)
	assert.Error(t, e)
}

func TestRmqClient_Subscribe(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, err := NewRmqClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)

	topic := "TestRmqClient_Subscribe"
	proOpts := ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(proOpts)
	defer producer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	subName := "subName"
	consumerOpts := ConsumerOptions{
		Topic:                       "",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
		BufSize:                     1024,
	}

	consumer, err := client.Subscribe(consumerOpts)
	assert.NotNil(t, err)
	assert.Nil(t, consumer)

	consumerOpts.Topic = topic
	consumer, err = client.Subscribe(consumerOpts)
	defer consumer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, consumer.Subscription(), subName)

	msg := &ProducerMessage{
		Payload:    []byte{1},
		Properties: nil,
	}
	_, err = producer.Send(context.TODO(), msg)
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			rmqmsg := msg.(*rmqMessage)
			msgPayload := rmqmsg.Payload()
			assert.NotEmpty(t, msgPayload)
			msgTopic := rmqmsg.Topic()
			assert.Equal(t, msgTopic, topic)
			msgProp := rmqmsg.Properties()
			assert.Empty(t, msgProp)
			msgID := rmqmsg.ID()
			rID := msgID.(*rmqID)
			assert.NotZero(t, rID)
			err = consumer.Seek(msgID)
			assert.Nil(t, err)
		}
	}
}

func TestRmqClient_EarliestMessageID(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, _ := NewRmqClient(opts)
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
}

func TestRmqClient_StringToMsgID(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, _ := NewRmqClient(opts)
	defer client.Close()

	str := "5"
	res, err := client.StringToMsgID(str)
	assert.Nil(t, err)
	assert.NotNil(t, res)

	str = "X"
	res, err = client.StringToMsgID(str)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestRmqClient_BytesToMsgID(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, _ := NewRmqClient(opts)
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	binary := SerializePulsarMsgID(mid)

	res, err := client.BytesToMsgID(binary)
	assert.Nil(t, err)
	assert.NotNil(t, res)
}
