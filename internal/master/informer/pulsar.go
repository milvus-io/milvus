package informer

import (
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	masterParams "github.com/zilliztech/milvus-distributed/internal/master/paramtable"
)

func NewPulsarClient() *PulsarClient {
	pulsarAddress, _ := masterParams.Params.PulsarAddress()
	pulsarAddress = "pulsar://" + pulsarAddress
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarAddress,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	return &PulsarClient{
		Client: client,
	}
}

type PulsarClient struct {
	Client pulsar.Client
}
