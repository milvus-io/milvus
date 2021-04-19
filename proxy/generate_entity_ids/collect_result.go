package generate_entity_ids

import (
	"context"
	"fmt"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"log"
	"time"
)

func CollectResult(clientNum int, topicName string) [][]byte {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName + "-partition-" + string(clientNum),
		SubscriptionName: "subName",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	var result [][]byte
	ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.Ack(msg)
	if err != nil{
		log.Fatal(err)
	}
	result = append(result, msg.Payload())
	fmt.Println("consumer receive the message successful!")
	canc()

	return result
}