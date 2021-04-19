package informer

import (
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"

	"github.com/apache/pulsar-client-go/pulsar"
)

func NewPulsarClient() *PulsarClient {
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

	return &PulsarClient{
		Client: client,
	}
}

type PulsarClient struct {
	Client pulsar.Client
}
