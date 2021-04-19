package mock

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/conf"
	"log"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func FakePulsarProducer() {
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarAddr,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: conf.Config.Master.PulsarTopic,
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
