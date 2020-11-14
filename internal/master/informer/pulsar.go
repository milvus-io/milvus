package informer

import (
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
)

func NewPulsarClient() *PulsarClient {
	pulsarAddr, err := gparams.GParams.Load("pulsar.address")
	if err != nil {
		panic(err)
	}
	pulsarPort, err := gparams.GParams.Load("pulsar.port")
	if err != nil {
		panic(err)
	}
	pulsarAddr = "pulsar://" + pulsarAddr + ":" + pulsarPort
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
