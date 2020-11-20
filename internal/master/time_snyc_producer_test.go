package master

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type (
	TestTickBarrier struct {
		value int64
		ctx   context.Context
	}
)

func (ttBarrier *TestTickBarrier) GetTimeTick() (Timestamp, error) {
	time.Sleep(1 * time.Second)
	ttBarrier.value++
	return Timestamp(ttBarrier.value), nil
}

func (ttBarrier *TestTickBarrier) Start() error {
	go func(ctx context.Context) {
		<-ctx.Done()
		log.Printf("barrier context done, exit")
	}(ttBarrier.ctx)
	return nil
}
func (ttBarrier *TestTickBarrier) Close() {
	_, cancel := context.WithCancel(context.Background())
	cancel()
}

func initTestPulsarStream(ctx context.Context, pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string, opts ...ms.RepackFunc) (*ms.MsgStream, *ms.MsgStream) {

	// set input stream
	inputStream := ms.NewPulsarMsgStream(ctx, 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	var input ms.MsgStream = inputStream

	// set output stream
	outputStream := ms.NewPulsarMsgStream(ctx, 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := ms.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	var output ms.MsgStream = outputStream

	return &input, &output
}
func receiveMsg(stream *ms.MsgStream) []uint64 {
	receiveCount := 0
	var results []uint64
	for {
		result := (*stream).Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				timetickmsg := v.(*ms.TimeTickMsg)
				results = append(results, timetickmsg.TimeTickMsg.Timestamp)
				receiveCount++
				if receiveCount == 10 {
					return results
				}
			}
		}
	}

}

func TestStream_PulsarMsgStream_TimeTick(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"

	producerChannels := []string{"proxyTtBarrier"}
	consumerChannels := []string{"proxyTtBarrier"}
	consumerSubName := "proxyTtBarrier"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proxyTtInputStream, proxyTtOutputStream := initTestPulsarStream(ctx, pulsarAddress, producerChannels, consumerChannels, consumerSubName)

	producerChannels = []string{"writeNodeBarrier"}
	consumerChannels = []string{"writeNodeBarrier"}
	consumerSubName = "writeNodeBarrier"
	writeNodeInputStream, writeNodeOutputStream := initTestPulsarStream(ctx, pulsarAddress, producerChannels, consumerChannels, consumerSubName)

	timeSyncProducer, _ := NewTimeSyncMsgProducer(ctx)
	timeSyncProducer.SetProxyTtBarrier(&TestTickBarrier{ctx: ctx})
	timeSyncProducer.SetWriteNodeTtBarrier(&TestTickBarrier{ctx: ctx})
	timeSyncProducer.SetDMSyncStream(*proxyTtInputStream)
	timeSyncProducer.SetK2sSyncStream(*writeNodeInputStream)
	(*proxyTtOutputStream).Start()
	(*writeNodeOutputStream).Start()
	timeSyncProducer.Start()
	expected := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	result1 := receiveMsg(proxyTtOutputStream)
	assert.Equal(t, expected, result1)
	result2 := receiveMsg(writeNodeOutputStream)
	assert.Equal(t, expected, result2)

	timeSyncProducer.Close()
}
