package mqclient

import (
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
)

type rmqMessage struct {
	msg rocksmq.ConsumerMessage
}

func (rm *rmqMessage) Topic() string {
	return rm.msg.Topic
}

func (rm *rmqMessage) Properties() map[string]string {
	return nil
}

func (rm *rmqMessage) Payload() []byte {
	return rm.msg.Payload
}

func (rm *rmqMessage) ID() MessageID {
	return &rmqID{messageID: rm.msg.MsgID}
}
