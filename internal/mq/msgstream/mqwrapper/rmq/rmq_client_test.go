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

package rmq

import (
	"context"
	"os"
	"testing"
	"time"

	rocksmqimplclient "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/client"
	rocksmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	pulsarwrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/stretchr/testify/assert"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	path := "/tmp/milvus/rdb_data"
	defer os.RemoveAll(path)
	_ = rocksmqimplserver.InitRocksMQ(path)
	exitCode := m.Run()
	defer rocksmqimplserver.CloseRocksMQ()
	os.Exit(exitCode)
}

func Test_NewRmqClient(t *testing.T) {
	client, err := createRmqClient()
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)
}

func TestRmqClient_CreateProducer(t *testing.T) {
	opts := rocksmqimplclient.Options{}
	client, err := NewClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)

	topic := "TestRmqClient_CreateProducer"
	proOpts := mqwrapper.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(proOpts)

	defer producer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	rmqProducer := producer.(*rmqProducer)
	defer rmqProducer.Close()
	assert.Equal(t, rmqProducer.Topic(), topic)

	msg := &mqwrapper.ProducerMessage{
		Payload:    []byte{},
		Properties: nil,
	}
	_, err = rmqProducer.Send(context.TODO(), msg)
	assert.Nil(t, err)

	invalidOpts := mqwrapper.ProducerOptions{Topic: ""}
	producer, e := client.CreateProducer(invalidOpts)
	assert.Nil(t, producer)
	assert.Error(t, e)
}

func TestRmqClient_GetLatestMsg(t *testing.T) {
	client, err := createRmqClient()
	assert.Nil(t, err)
	defer client.Close()

	topic := "t2GetLatestMsg"
	proOpts := mqwrapper.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(proOpts)
	assert.Nil(t, err)
	defer producer.Close()

	for i := 0; i < 10; i++ {
		msg := &mqwrapper.ProducerMessage{
			Payload:    []byte{byte(i)},
			Properties: nil,
		}
		_, err = producer.Send(context.TODO(), msg)
		assert.Nil(t, err)
	}

	subName := "subName"
	consumerOpts := mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
		BufSize:                     1024,
	}

	consumer, err := client.Subscribe(consumerOpts)
	assert.Nil(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	expectLastMsg, err := consumer.GetLatestMsgID()
	assert.Nil(t, err)
	var actualLastMsg mqwrapper.Message

	for {
		select {
		case <-ctx.Done():
			ret, err := actualLastMsg.ID().LessOrEqualThan(expectLastMsg.Serialize())
			assert.Nil(t, err)
			assert.False(t, ret)
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			actualLastMsg = msg
		}
	}
}

func TestRmqClient_Subscribe(t *testing.T) {
	client, err := createRmqClient()
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)

	topic := "TestRmqClient_Subscribe"
	proOpts := mqwrapper.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(proOpts)
	defer producer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	subName := "subName"
	consumerOpts := mqwrapper.ConsumerOptions{
		Topic:                       "",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
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

	msg := &mqwrapper.ProducerMessage{
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
			err = consumer.Seek(msgID, true)
			assert.Nil(t, err)
		}
	}
}

func TestRmqClient_EarliestMessageID(t *testing.T) {
	client, _ := createRmqClient()
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
}

func TestRmqClient_StringToMsgID(t *testing.T) {
	client, _ := createRmqClient()
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
	client, _ := createRmqClient()
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	binary := pulsarwrapper.SerializePulsarMsgID(mid)

	res, err := client.BytesToMsgID(binary)
	assert.Nil(t, err)
	assert.NotNil(t, res)
}

func createRmqClient() (*rmqClient, error) {
	opts := rocksmqimplclient.Options{}
	return NewClient(opts)
}
