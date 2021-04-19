package segment

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/internal/conf"
	masterpb "github.com/czs007/suvlim/internal/proto/master"
	"github.com/czs007/suvlim/internal/master/informer"
)

type SegmentStats struct {
	SegementID uint64
	MemorySize uint64
	MemoryRate float64
	Status     masterpb.SegmentStatus
	Rows       int64
}

func SegmentMarshal(s SegmentStats) ([]byte, error) {
	var nb bytes.Buffer
	enc := gob.NewEncoder(&nb)
	err := enc.Encode(s)
	if err != nil {
		return []byte{}, err
	}
	return nb.Bytes(), nil
}

func SegmentUnMarshal(data []byte) (SegmentStats, error) {
	var ss SegmentStats
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(&ss)
	if err != nil {
		return SegmentStats{}, err
	}
	return ss, nil
}

func Listener(ssChan chan SegmentStats, pc informer.PulsarClient) error {
	consumer, err := pc.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            conf.Config.Master.PulsarTopic,
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		msg, err := consumer.Receive(context.TODO())
		if err != nil {
			log.Fatal(err)
		}
		m, _ := SegmentUnMarshal(msg.Payload())
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), m.SegementID)
		ssChan <- m
		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	return nil
}
