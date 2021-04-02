package mqclient

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarProducer struct {
	p pulsar.Producer
}

func (pp *pulsarProducer) Topic() string {
	return pp.p.Topic()
}

func (pp *pulsarProducer) Send(ctx context.Context, message *ProducerMessage) error {
	ppm := &pulsar.ProducerMessage{Payload: message.Payload, Properties: message.Properties}
	_, err := pp.p.Send(ctx, ppm)
	return err
}

func (pp *pulsarProducer) Close() {
	pp.p.Close()
}
