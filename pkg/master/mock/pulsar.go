package mock

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/pkg/master/common"
)

func FakePulsarProducer() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               common.PULSAR_URL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: common.PULSAR_TOPIC,
	})
	testSegmentStats, _ := SegmentMarshal(SegmentStats{
		SegementID: uint64(1111),
		MemorySize: uint64(333322),
		MemoryRate: float64(0.13),
	})
	for {
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: testSegmentStats,
		})
		time.Sleep(1 * time.Second)
	}
	defer producer.Close()

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")
}

//TODO add a mock: memory increase
