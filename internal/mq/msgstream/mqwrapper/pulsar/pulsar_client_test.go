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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"go.uber.org/zap"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/stretchr/testify/assert"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func IntToBytes(n int) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, common.Endian, tmp)
	return bytesBuffer.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, common.Endian, &tmp)
	return int(tmp)
}

func Produce(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, arr []int) {
	producer, err := pc.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")

	for _, v := range arr {
		msg := &mqwrapper.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		_, err = producer.Send(ctx, msg)
		assert.Nil(t, err)
		log.Info("Pub", zap.Any("SND", v))
	}

	log.Info("Produce done")
}

func VerifyMessage(t *testing.T, msg mqwrapper.Message) {
	pload := BytesToInt(msg.Payload())
	log.Info("RECV", zap.Any("v", pload))
	pm := msg.(*pulsarMessage)
	topic := pm.Topic()
	assert.NotEmpty(t, topic)
	log.Info("RECV", zap.Any("t", topic))
	prop := pm.Properties()
	log.Info("RECV", zap.Any("p", len(prop)))
}

// Consume1 will consume random messages and record the last MessageID it received
func Consume1(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan mqwrapper.MessageID, total *int) {
	consumer, err := pc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	var msg mqwrapper.Message
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- msg.ID()

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID mqwrapper.MessageID, total *int) {
	consumer, err := pc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID, true)
	assert.Nil(t, err)

	// skip the last received message
	mm := <-consumer.Chan()
	consumer.Ack(mm)

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func Consume3(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, total *int) {
	consumer, err := pc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume3 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestPulsarClient_Consume1(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan mqwrapper.MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	// launch produce
	Produce(ctx, t, pc, topic, arr)
	time.Sleep(100 * time.Millisecond)

	// launch consume1
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	Consume1(ctx1, t, pc, topic, subName, c, &total1)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	Consume2(ctx2, t, pc, topic, subName, lastMsgID, &total2)

	// launch consume3
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	Consume3(ctx3, t, pc, topic, subName, &total3)

	// stop Consume2
	cancel()
	assert.Equal(t, len(arr), total1+total2)
	assert.Equal(t, len(arr), total3)

	log.Info("main done")
}

func Consume21(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan mqwrapper.MessageID, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	var msg pulsar.ConsumerMessage
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- &pulsarID{messageID: msg.ID()}

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume22(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID mqwrapper.MessageID, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID.(*pulsarID).messageID)
	assert.Nil(t, err)

	// skip the last received message
	mm := <-consumer.Chan()
	consumer.Ack(mm)

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func Consume23(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume3 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestPulsarClient_Consume2(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan mqwrapper.MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	// launch produce
	Produce(ctx, t, pc, topic, arr)
	time.Sleep(100 * time.Millisecond)

	// launch consume1
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	Consume21(ctx1, t, pc, topic, subName, c, &total1)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	Consume22(ctx2, t, pc, topic, subName, lastMsgID, &total2)

	// launch consume3
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	Consume23(ctx3, t, pc, topic, subName, &total3)

	// stop Consume2
	cancel()
	assert.Equal(t, len(arr), total1+total2)
	assert.Equal(t, 0, total3)

	log.Info("main done")
}

func TestPulsarClient_SeekPosition(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer, err := pc.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")
	ids := []mqwrapper.MessageID{}
	arr := []int{1, 2, 3}
	for _, v := range arr {
		msg := &mqwrapper.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		id, err := producer.Send(ctx, msg)
		ids = append(ids, id)
		assert.Nil(t, err)
	}

	log.Info("Produced")

	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	seekID := ids[2].(*pulsarID).messageID
	consumer.Seek(seekID)

	msgChan := consumer.Chan()

	select {
	case msg := <-msgChan:
		assert.Equal(t, seekID.BatchIdx(), msg.ID().BatchIdx())
		assert.Equal(t, seekID.LedgerID(), msg.ID().LedgerID())
		assert.Equal(t, seekID.EntryID(), msg.ID().EntryID())
		assert.Equal(t, seekID.PartitionIdx(), msg.ID().PartitionIdx())
		assert.Equal(t, 3, BytesToInt(msg.Payload()))
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "should not wait")
	}

	seekID = ids[1].(*pulsarID).messageID
	consumer.Seek(seekID)

	msgChan = consumer.Chan()

	select {
	case msg := <-msgChan:
		assert.Equal(t, seekID.BatchIdx(), msg.ID().BatchIdx())
		assert.Equal(t, seekID.LedgerID(), msg.ID().LedgerID())
		assert.Equal(t, seekID.EntryID(), msg.ID().EntryID())
		assert.Equal(t, seekID.PartitionIdx(), msg.ID().PartitionIdx())
		assert.Equal(t, 2, BytesToInt(msg.Payload()))
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "should not wait")
	}
}

func TestPulsarClient_SeekLatest(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer, err := pc.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")

	arr := []int{1, 2, 3}
	for _, v := range arr {
		msg := &mqwrapper.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		_, err = producer.Send(ctx, msg)
		assert.Nil(t, err)
	}

	log.Info("Produced")

	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	msgChan := consumer.Chan()

	loop := true
	for loop {
		select {
		case msg := <-msgChan:
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			assert.Equal(t, v, 4)
			loop = false
		case <-time.After(2 * time.Second):
			log.Info("after 2 seconds")
			msg := &mqwrapper.ProducerMessage{
				Payload:    IntToBytes(4),
				Properties: map[string]string{},
			}
			_, err = producer.Send(ctx, msg)
			assert.Nil(t, err)
		}
	}
}

func TestPulsarClient_EarliestMessageID(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	client, _ := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
}

func TestPulsarClient_StringToMsgID(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	client, _ := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	str := msgIDToString(mid)

	res, err := client.StringToMsgID(str)
	assert.Nil(t, err)
	assert.NotNil(t, res)

	str = "X"
	res, err = client.StringToMsgID(str)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestPulsarClient_BytesToMsgID(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	client, _ := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	binary := SerializePulsarMsgID(mid)

	res, err := client.BytesToMsgID(binary)
	assert.Nil(t, err)
	assert.NotNil(t, res)

	invalidBin := []byte{0}
	res, err = client.BytesToMsgID(invalidBin)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

type mPulsarError struct {
	msg    string
	result pulsar.Result
}

func hackPulsarError(result pulsar.Result) *pulsar.Error {
	pe := &pulsar.Error{}
	// use unsafe to generate test case
	/* #nosec G103 */
	mpe := (*mPulsarError)(unsafe.Pointer(pe))
	mpe.result = result
	return pe
}

func TestIsPulsarError(t *testing.T) {
	type testCase struct {
		err      error
		results  []pulsar.Result
		expected bool
	}
	cases := []testCase{
		{
			err:      errors.New(""),
			results:  []pulsar.Result{},
			expected: false,
		},
		{
			err:      errors.New(""),
			results:  []pulsar.Result{pulsar.ConnectError},
			expected: false,
		},
		{
			err:      hackPulsarError(pulsar.ConsumerBusy),
			results:  []pulsar.Result{pulsar.ConnectError},
			expected: false,
		},
		{
			err:      hackPulsarError(pulsar.ConsumerBusy),
			results:  []pulsar.Result{pulsar.ConnectError, pulsar.ConsumerBusy},
			expected: true,
		},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.expected, isPulsarError(tc.err, tc.results...))
	}
}

type mockPulsarClient struct{}

// CreateProducer Creates the producer instance
// This method will block until the producer is created successfully
func (c *mockPulsarClient) CreateProducer(_ pulsar.ProducerOptions) (pulsar.Producer, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

// Subscribe Creates a `Consumer` by subscribing to a topic.
//
// If the subscription does not exist, a new subscription will be created and all messages published after the
// creation will be retained until acknowledged, even if the consumer is not connected
func (c *mockPulsarClient) Subscribe(_ pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	return nil, hackPulsarError(pulsar.ConsumerBusy)
}

// CreateReader Creates a Reader instance.
// This method will block until the reader is created successfully.
func (c *mockPulsarClient) CreateReader(_ pulsar.ReaderOptions) (pulsar.Reader, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

// TopicPartitions Fetches the list of partitions for a given topic
//
// If the topic is partitioned, this will return a list of partition names.
// If the topic is not partitioned, the returned list will contain the topic
// name itself.
//
// This can be used to discover the partitions and create {@link Reader},
// {@link Consumer} or {@link Producer} instances directly on a particular partition.
func (c *mockPulsarClient) TopicPartitions(topic string) ([]string, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

// Close Closes the Client and free associated resources
func (c *mockPulsarClient) Close() {
}

func TestPulsarClient_SubscribeExclusiveFail(t *testing.T) {
	t.Run("exclusive pulsar consumer failure", func(t *testing.T) {
		pc := &pulsarClient{
			client: &mockPulsarClient{},
		}

		_, err := pc.Subscribe(mqwrapper.ConsumerOptions{})
		assert.Error(t, err)
		assert.True(t, retry.IsUnRecoverable(err))
	})
}
