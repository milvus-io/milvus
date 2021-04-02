package mqclient

import (
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
)

type rmqConsumer struct {
	c          rocksmq.Consumer
	msgChannel chan ConsumerMessage
}

func (rc *rmqConsumer) Subscription() string {
	return rc.c.Subscription()
}

func (rc *rmqConsumer) Chan() <-chan ConsumerMessage {
	return rc.msgChannel
}

func (rc *rmqConsumer) Seek(id MessageID) error {
	msgID := id.(*rmqID).messageID
	return rc.c.Seek(msgID)
}

func (rc *rmqConsumer) Ack(message ConsumerMessage) {
}

func (rc *rmqConsumer) Close() {
}
