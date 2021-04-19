package rocksmq

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/client"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
)

type rmqConsumer struct {
	c          rocksmq.Consumer
	msgChannel chan client.ConsumerMessage
}

func (rc *rmqConsumer) Subscription() string {
	return rc.c.Subscription()
}

func (rc *rmqConsumer) Chan() <-chan client.ConsumerMessage {
	return rc.msgChannel
}

func (rc *rmqConsumer) Seek(id client.MessageID) error {
	msgID, err := strconv.ParseInt(string(id.Serialize()), 10, 64)
	if err != nil {
		return err
	}
	return rc.c.Seek(msgID)
}

func (rc *rmqConsumer) Ack(message client.ConsumerMessage) {
}

func (rc *rmqConsumer) Close() {
}
