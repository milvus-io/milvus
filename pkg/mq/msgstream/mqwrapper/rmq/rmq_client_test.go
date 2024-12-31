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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/mq/common"
	client3 "github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/client"
	server2 "github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ServiceParam.MQCfg.EnablePursuitMode.Key, "false")

	rand.Seed(time.Now().UnixNano())
	path := "/tmp/milvus/rdb_data"
	defer os.RemoveAll(path)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	_ = server2.InitRocksMQ(path)
	exitCode := m.Run()
	defer server2.CloseRocksMQ()
	os.Exit(exitCode)
}

func Test_NewRmqClient(t *testing.T) {
	client, err := createRmqClient()
	defer client.Close()
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestRmqClient_CreateProducer(t *testing.T) {
	opts := client3.Options{}
	client, err := NewClient(opts)
	defer client.Close()
	assert.NoError(t, err)
	assert.NotNil(t, client)

	topic := "TestRmqClient_CreateProducer"
	proOpts := common.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(context.TODO(), proOpts)
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	defer producer.Close()

	rmqProducer := producer.(*rmqProducer)
	defer rmqProducer.Close()
	assert.Equal(t, rmqProducer.Topic(), topic)

	msg := &common.ProducerMessage{
		Payload:    []byte{},
		Properties: nil,
	}
	_, err = rmqProducer.Send(context.TODO(), msg)
	assert.NoError(t, err)

	invalidOpts := common.ProducerOptions{Topic: ""}
	producer, e := client.CreateProducer(context.TODO(), invalidOpts)
	assert.Nil(t, producer)
	assert.Error(t, e)
}

func TestRmqClient_GetLatestMsg(t *testing.T) {
	client, err := createRmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := fmt.Sprintf("t2GetLatestMsg-%d", rand.Int())
	proOpts := common.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(context.TODO(), proOpts)
	assert.NoError(t, err)
	defer producer.Close()

	for i := 0; i < 10; i++ {
		msg := &common.ProducerMessage{
			Payload:    []byte{byte(i)},
			Properties: nil,
		}
		_, err = producer.Send(context.TODO(), msg)
		assert.NoError(t, err)
	}

	subName := "subName"
	consumerOpts := mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
		BufSize:                     1024,
	}

	consumer, err := client.Subscribe(context.TODO(), consumerOpts)
	assert.NoError(t, err)

	expectLastMsg, err := consumer.GetLatestMsgID()
	assert.NoError(t, err)

	var actualLastMsg common.Message
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			fmt.Println(i)
			assert.FailNow(t, "consumer failed to yield message in 100 milliseconds")
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			actualLastMsg = msg
		}
	}
	require.NotNil(t, actualLastMsg)
	ret, err := expectLastMsg.LessOrEqualThan(actualLastMsg.ID().Serialize())
	assert.NoError(t, err)
	assert.True(t, ret)
}

func TestRmqClient_Subscribe(t *testing.T) {
	client, err := createRmqClient()
	defer client.Close()
	assert.NoError(t, err)
	assert.NotNil(t, client)

	topic := "TestRmqClient_Subscribe"
	proOpts := common.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(context.TODO(), proOpts)
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	subName := "subName"
	consumerOpts := mqwrapper.ConsumerOptions{
		Topic:                       subName,
		SubscriptionName:            subName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
		BufSize:                     0,
	}
	consumer, err := client.Subscribe(context.TODO(), consumerOpts)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	consumerOpts = mqwrapper.ConsumerOptions{
		Topic:                       "",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
		BufSize:                     1024,
	}

	consumer, err = client.Subscribe(context.TODO(), consumerOpts)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	consumerOpts.Topic = topic
	consumer, err = client.Subscribe(context.TODO(), consumerOpts)
	defer consumer.Close()
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, consumer.Subscription(), subName)

	msg := &common.ProducerMessage{
		Payload:    []byte{1},
		Properties: nil,
	}
	_, err = producer.Send(context.TODO(), msg)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		assert.FailNow(t, "consumer failed to yield message in 100 milliseconds")
	case msg := <-consumer.Chan():
		consumer.Ack(msg)
		rmqmsg := msg.(*client3.RmqMessage)
		msgPayload := rmqmsg.Payload()
		assert.NotEmpty(t, msgPayload)
		msgTopic := rmqmsg.Topic()
		assert.Equal(t, msgTopic, topic)
		msgProp := rmqmsg.Properties()
		assert.Empty(t, msgProp)
		msgID := rmqmsg.ID()
		rID := msgID.(*server2.RmqID)
		assert.NotZero(t, rID)
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
	assert.NoError(t, err)
	assert.NotNil(t, res)

	str = "X"
	res, err = client.StringToMsgID(str)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestRmqClient_BytesToMsgID(t *testing.T) {
	client, _ := createRmqClient()
	defer client.Close()

	binary := server2.SerializeRmqID(0)
	res, err := client.BytesToMsgID(binary)
	assert.NoError(t, err)
	assert.Equal(t, res.(*server2.RmqID).MessageID, int64(0))
}

func createRmqClient() (*rmqClient, error) {
	opts := client3.Options{}
	return NewClient(opts)
}
