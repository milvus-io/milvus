package mock

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/conf"
)

func TestSegmentMarshal(t *testing.T) {
	s := SegmentStats{
		SegementID: uint64(6875873667148255882),
		MemorySize: uint64(9999),
		MemoryRate: float64(0.13),
	}

	data, err := SegmentMarshal(s)
	if err != nil {
		t.Error(err)
	}

	ss, err := SegmentUnMarshal(data)
	if err != nil {
		t.Error(err)
	}
	if ss.MemoryRate != s.MemoryRate {
		fmt.Println(ss.MemoryRate)
		fmt.Println(s.MemoryRate)
		t.Error("Error when marshal")
	}
}

var Ts = Segment{
	SegmentID:      uint64(101111),
	CollectionID:   uint64(12010101),
	CollectionName: "collection-test",
	PartitionTag:   "default",
	ChannelStart:   1,
	ChannelEnd:     100,
	OpenTimeStamp:  uint64(time.Now().Unix()),
	CloseTimeStamp: uint64(time.Now().Add(1 * time.Hour).Unix()),
}

func TestSegment2JSON(t *testing.T) {
	res, err := Segment2JSON(Ts)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}

func TestFakePulsarProducer(t *testing.T) {
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
		SegementID: uint64(6875875531164062448),
		MemorySize: uint64(9999),
		MemoryRate: float64(0.13),
	})
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: testSegmentStats,
	})
	time.Sleep(1 * time.Second)
	defer producer.Close()

	if err != nil {
		t.Error(err)
	}
	fmt.Println("Published message")

}
