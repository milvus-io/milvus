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

package pulsar

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/common"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
)

func TestPulsarConsumer_Subscription(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)
	defer pc.Close()

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "Topic",
		SubscriptionName:            "SubName",
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqcommon.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

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

func TestComsumeCompressedMessage(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)
	defer pc.Close()

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "TestTopics",
		SubscriptionName:            "SubName",
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqcommon.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	producer, err := pc.CreateProducer(context.TODO(), mqcommon.ProducerOptions{Topic: "TestTopics"})
	assert.NoError(t, err)
	compressProducer, err := pc.CreateProducer(context.TODO(), mqcommon.ProducerOptions{Topic: "TestTopics", EnableCompression: true})
	assert.NoError(t, err)

	msg := []byte("test message")
	compressedMsg := []byte("test compressed message")
	traceValue := "test compressed message id"
	_, err = producer.Send(context.Background(), &mqcommon.ProducerMessage{
		Payload:    msg,
		Properties: map[string]string{},
	})
	assert.NoError(t, err)
	recvMsg, err := consumer.Receive(context.Background())
	assert.NoError(t, err)
	consumer.Ack(recvMsg)
	assert.Equal(t, msg, recvMsg.Payload())

	_, err = compressProducer.Send(context.Background(), &mqcommon.ProducerMessage{
		Payload: compressedMsg,
		Properties: map[string]string{
			common.TraceIDKey: traceValue,
		},
	})
	assert.NoError(t, err)
	recvMsg, err = consumer.Receive(context.Background())
	assert.NoError(t, err)
	consumer.Ack(recvMsg)
	assert.Equal(t, compressedMsg, recvMsg.Payload())
	assert.Equal(t, traceValue, recvMsg.Properties()[common.TraceIDKey])

	assert.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestPulsarConsumer_Close(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "Topic-1",
		SubscriptionName:            "SubName-1",
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqcommon.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	str := consumer.Subscription()
	assert.NotNil(t, str)

	pulsarConsumer := &Consumer{c: consumer, closeCh: make(chan struct{})}
	pulsarConsumer.Close()

	// test double close
	pulsarConsumer.Close()
}

func TestPulsarClientCloseUnsubscribeError(t *testing.T) {
	topic := "TestPulsarClientCloseUnsubscribeError"
	subName := "test"
	pulsarAddress := getPulsarAddress()

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()
	assert.NoError(t, err)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.NoError(t, err)

	// subscribe agiain
	_, err = client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ConsumerBusy"))

	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)

	pulsarURL, err := url.ParseRequestURI(pulsarAddress)
	if err != nil {
		panic(err)
	}
	webport := Params.PulsarCfg.WebPort.GetValue()
	webServiceURL := "http://" + pulsarURL.Hostname() + ":" + webport
	admin, err := NewAdminClient(webServiceURL, "", "")
	assert.NoError(t, err)
	err = admin.Subscriptions().Delete(*topicName, subName, true)
	if err != nil {
		webServiceURL = "http://" + pulsarURL.Hostname() + ":" + "8080"
		admin, err := NewAdminClient(webServiceURL, "", "")
		assert.NoError(t, err)
		err = admin.Subscriptions().Delete(*topicName, subName, true)
		assert.NoError(t, err)
	}

	err = consumer.Unsubscribe()
	assert.True(t, strings.Contains(err.Error(), "Consumer not found"))
	t.Log(err)
}

func TestPulsarClientUnsubscribeTwice(t *testing.T) {
	topic := "TestPulsarClientUnsubscribeTwice"
	subName := "test"
	pulsarAddress := getPulsarAddress()

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()
	assert.NoError(t, err)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.NoError(t, err)

	err = consumer.Unsubscribe()
	assert.NoError(t, err)
	err = consumer.Unsubscribe()
	assert.True(t, strings.Contains(err.Error(), "Consumer not found"))
	t.Log(err)
}

func TestCheckPreTopicValid(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "Topic-1",
		SubscriptionName:            "SubName-1",
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqcommon.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	str := consumer.Subscription()
	assert.NotNil(t, str)

	pulsarConsumer := &Consumer{c: consumer, closeCh: make(chan struct{})}
	err = pulsarConsumer.CheckTopicValid("Topic-1")
	assert.NoError(t, err)
}
