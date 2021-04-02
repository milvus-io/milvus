package mqclient

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
)

type rmqProducer struct {
	p rocksmq.Producer
}

func (rp *rmqProducer) Topic() string {
	return rp.p.Topic()
}

func (rp *rmqProducer) Send(ctx context.Context, message *ProducerMessage) error {
	pm := &rocksmq.ProducerMessage{Payload: message.Payload}
	return rp.p.Send(pm)
}

func (rp *rmqProducer) Close() {
}
