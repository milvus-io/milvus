package msgstream

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func benchmarkProduceAndConsume(b *testing.B, mqClient mqwrapper.Client, cases [][]byte) {
	topic := fmt.Sprintf("test_produce_and_consume_topic_%d", rand.Int31n(100000))
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		p, err := mqClient.CreateProducer(context.TODO(), common.ProducerOptions{
			Topic: topic,
		})
		assert.NoError(b, err)
		defer p.Close()
		benchmarkMQProduce(b, p, cases)
	}()
	go func() {
		defer wg.Done()
		c, _ := mqClient.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
			Topic:                       topic,
			SubscriptionName:            topic,
			SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
			BufSize:                     1024,
		})
		defer c.Close()
		benchmarkMQConsume(b, c, cases)
	}()
	wg.Wait()
}

func benchmarkMQConsume(b *testing.B, c mqwrapper.Consumer, cases [][]byte) {
	ch := c.Chan()
	for range cases {
		msg := <-ch
		c.Ack(msg)
	}
	c.Close()
}

func benchmarkMQProduce(b *testing.B, p mqwrapper.Producer, cases [][]byte) {
	for _, c := range cases {
		p.Send(context.Background(), &common.ProducerMessage{
			Payload: c,
		})
	}
}

func generateRandBytes(m int, n int) [][]byte {
	letterRunes := funcutil.RandomBytes(2 * m)
	cases := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		startOffset := rand.Intn(m)
		endOffset := startOffset + m

		cases = append(cases, letterRunes[startOffset:endOffset])
	}
	return cases
}
