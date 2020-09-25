package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/conf"
	"github.com/czs007/suvlim/pkg/master/mock"
)

func main() {
	FakeProduecer()
}
func FakeProduecer() {
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
	testSegmentStats, _ := mock.SegmentMarshal(mock.SegmentStats{
		SegementID: uint64(6875939483227099806),
		MemorySize: uint64(9999),
		MemoryRate: float64(0.13),
	})
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: testSegmentStats,
	})
	time.Sleep(1 * time.Second)
	defer producer.Close()

	if err != nil {
		fmt.Errorf("%v", err)
	}
	fmt.Println("Published message")

}
