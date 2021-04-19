package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/client"
)

type pulsarProducer struct {
	p pulsar.Producer
}

func (pp *pulsarProducer) Topic() string {
	return pp.p.Topic()
}

func (pp *pulsarProducer) Send(ctx context.Context, message *client.ProducerMessage) error {
	ppm := &pulsar.ProducerMessage{Payload: message.Payload}
	_, err := pp.p.Send(ctx, ppm)
	return err
}

func (pp *pulsarProducer) Close() {
	pp.p.Close()
}
