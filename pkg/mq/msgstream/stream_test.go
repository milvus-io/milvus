package msgstream

import (
	"context"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func testStreamOperation(t *testing.T, mqClient mqwrapper.Client) {
	testFuncs := []func(t *testing.T, c mqwrapper.Client){
		testConcurrentStream,
		testConcurrentStreamAndSubscribeLast,
		testConcurrentStreamAndSeekInclusive,
		testConcurrentStreamAndSeekNoInclusive,
		testConcurrentStreamAndSeekToLast,
	}

	for _, testFunc := range testFuncs {
		t.Run(
			runtime.FuncForPC(reflect.ValueOf(testFunc).Pointer()).Name(),
			func(t *testing.T) {
				testFunc(t, mqClient)
			},
		)
	}
}

func testConcurrentStream(t *testing.T, mqClient mqwrapper.Client) {
	topics := getChannel(2)

	producer, err := mqClient.CreateProducer(context.TODO(), common.ProducerOptions{
		Topic: topics[0],
	})
	defer producer.Close()
	assert.NoError(t, err)

	consumer, err := mqClient.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topics[0],
		SubscriptionName:            funcutil.RandomString(8),
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
		BufSize:                     1024,
	})
	defer consumer.Close()
	assert.NoError(t, err)

	testSendAndRecv(t, producer, consumer)
}

func testConcurrentStreamAndSubscribeLast(t *testing.T, mqClient mqwrapper.Client) {
	topics := getChannel(2)

	producer, err := mqClient.CreateProducer(context.TODO(), common.ProducerOptions{
		Topic: topics[0],
	})
	defer producer.Close()
	assert.NoError(t, err)

	ids := sendMessages(context.Background(), t, producer, generateRandMessage(1024, 1000))

	consumer, err := mqClient.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topics[0],
		SubscriptionName:            funcutil.RandomString(8),
		SubscriptionInitialPosition: common.SubscriptionPositionLatest,
		BufSize:                     1024,
	})
	assert.NoError(t, err)
	latestID, err := consumer.GetLatestMsgID()
	assert.NoError(t, err)

	compare, err := ids[len(ids)-1].Equal(latestID.Serialize())
	assert.NoError(t, err)
	assert.True(t, compare)

	defer consumer.Close()
	testSendAndRecv(t, producer, consumer)
}

func testConcurrentStreamAndSeekInclusive(t *testing.T, mqClient mqwrapper.Client) {
	topics := getChannel(2)

	producer, err := mqClient.CreateProducer(context.TODO(), common.ProducerOptions{
		Topic: topics[0],
	})
	defer producer.Close()
	assert.NoError(t, err)

	cases := generateRandMessage(1024, 1000)
	ids := sendMessages(context.Background(), t, producer, cases)

	consumer, err := mqClient.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topics[0],
		SubscriptionName:            funcutil.RandomString(8),
		SubscriptionInitialPosition: common.SubscriptionPositionUnknown,
		BufSize:                     1024,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	// seek half and inclusive.
	// consume all and compare.
	half := len(ids) / 2
	ids = ids[half:]
	err = consumer.Seek(ids[0], true)
	assert.NoError(t, err)
	consumerIDs := recvMessages(context.Background(), t, consumer, cases[half:], time.Minute)
	compareMultiIDs(t, ids, consumerIDs)
	assert.Empty(t, recvMessages(context.Background(), t, consumer, cases, 5*time.Second))

	testSendAndRecv(t, producer, consumer)
}

func testConcurrentStreamAndSeekNoInclusive(t *testing.T, mqClient mqwrapper.Client) {
	topics := getChannel(2)

	producer, err := mqClient.CreateProducer(context.TODO(), common.ProducerOptions{
		Topic: topics[0],
	})
	defer producer.Close()
	assert.NoError(t, err)

	cases := generateRandMessage(1024, 1000)
	ids := sendMessages(context.Background(), t, producer, cases)

	consumer, err := mqClient.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topics[0],
		SubscriptionName:            funcutil.RandomString(8),
		SubscriptionInitialPosition: common.SubscriptionPositionUnknown,
		BufSize:                     1024,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	// seek half and inclusive.
	// consume all and compare.
	half := len(ids) / 2
	ids = ids[half:]
	err = consumer.Seek(ids[0], false)
	assert.NoError(t, err)
	consumerIDs := recvMessages(context.Background(), t, consumer, cases[half+1:], time.Minute)
	compareMultiIDs(t, ids[1:], consumerIDs)
	assert.Empty(t, recvMessages(context.Background(), t, consumer, cases, 5*time.Second))

	testSendAndRecv(t, producer, consumer)
}

func testConcurrentStreamAndSeekToLast(t *testing.T, mqClient mqwrapper.Client) {
	topics := getChannel(2)

	producer, err := mqClient.CreateProducer(context.TODO(), common.ProducerOptions{
		Topic: topics[0],
	})
	defer producer.Close()
	assert.NoError(t, err)

	cases := generateRandMessage(1024, 1000)
	sendMessages(context.Background(), t, producer, cases)

	consumer, err := mqClient.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topics[0],
		SubscriptionName:            funcutil.RandomString(8),
		SubscriptionInitialPosition: common.SubscriptionPositionUnknown,
		BufSize:                     1024,
	})
	assert.NoError(t, err)
	defer consumer.Close()
	latestID, err := consumer.GetLatestMsgID()
	assert.NoError(t, err)

	// seek half and inclusive.
	// consume all and compare.
	err = consumer.Seek(latestID, false)
	assert.NoError(t, err)
	testSendAndRecv(t, producer, consumer)
}

func testSendAndRecv(t *testing.T, p mqwrapper.Producer, c mqwrapper.Consumer) {
	ctx := context.Background()
	msg := generateRandMessage(1024*5, 10)

	var (
		producerIDs []common.MessageID
		consumerIDs []common.MessageID
	)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		producerIDs = sendMessages(ctx, t, p, msg)
		log.Debug("producing finished", zap.Any("id", producerIDs[0].Serialize()), zap.Any("ids", producerIDs))
	}()

	go func() {
		defer wg.Done()
		consumerIDs = recvMessages(ctx, t, c, msg, 10*time.Second)
	}()
	wg.Wait()

	compareMultiIDs(t, producerIDs, consumerIDs)
	// should be empty.
	assert.Empty(t, recvMessages(context.Background(), t, c, msg, time.Second))
}

func compareMultiIDs(t *testing.T, producerIDs []common.MessageID, consumerIDs []common.MessageID) {
	assert.Equal(t, len(producerIDs), len(consumerIDs))
	for i := range producerIDs {
		compare, err := producerIDs[i].Equal(consumerIDs[i].Serialize())
		assert.NoError(t, err)
		assert.True(t, compare)
	}
}

func generateRandMessage(m int, n int) []string {
	cases := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l := rand.Intn(m)
		cases = append(cases, funcutil.RandomString(l))
	}
	return cases
}

func sendMessages(ctx context.Context, t *testing.T, p mqwrapper.Producer, testCase []string) []common.MessageID {
	ids := make([]common.MessageID, 0, len(testCase))
	for _, s := range testCase {
		id, err := p.Send(ctx, &common.ProducerMessage{
			Payload: []byte(s),
		})
		assert.NoError(t, err)
		ids = append(ids, id)
	}
	return ids
}

func recvMessages(ctx context.Context, t *testing.T, c mqwrapper.Consumer, testCase []string, timeout time.Duration) []common.MessageID {
	ids := make([]common.MessageID, 0, len(testCase))
	timeoutTicker := time.NewTicker(timeout)
	defer timeoutTicker.Stop()
	for {
		select {
		case msg := <-c.Chan():
			ids = append(ids, msg.ID())
			c.Ack(msg)
		case <-timeoutTicker.C:
			return ids
		}
		if len(ids) >= len(testCase) {
			break
		}
	}
	return ids
}
