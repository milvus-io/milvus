package logservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/logservice"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

var _ mqwrapper.Producer = &producer{}

type producer struct {
	producer logservice.Producer
}

func (p *producer) Send(ctx context.Context, producedMsg *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	// TODO: Add message type here.
	msg := message.NewBuilder().
		WithPayload(producedMsg.Payload).
		WithProperties(producedMsg.Properties).
		BuildMutable()

	msgID, err := p.producer.Produce(ctx, msg)
	if err != nil {
		return nil, err
	}
	return msgID, nil
}

func (p *producer) Close() {
	p.producer.Close()
}
