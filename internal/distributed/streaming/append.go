package streaming

import (
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// createOrGetProducer creates or get a producer.
// vchannel in same pchannel can share the same producer.
func (w *walAccesserImpl) getProducer(channel string) *producer.ResumableProducer {
	pchannel := funcutil.ToPhysicalChannel(channel)
	w.producerMutex.Lock()
	defer w.producerMutex.Unlock()

	if p, ok := w.producers[pchannel]; ok {
		return p
	}
	p := producer.NewResumableProducer(w.handlerClient.CreateProducer, &producer.ProducerOptions{
		PChannel: pchannel,
	})
	w.producers[pchannel] = p
	return p
}

// assertValidMessage asserts the message is not system message.
func assertValidMessage(msgs ...message.MutableMessage) {
	for _, msg := range msgs {
		if msg.MessageType().IsSystem() {
			panic("system message is not allowed to append from client")
		}
		if msg.MessageType().IsSelfControlled() {
			panic("self controlled message is not allowed to append from client")
		}
		if msg.VChannel() == "" {
			panic("we don't support sent all vchannel message at client now")
		}
	}
}

// assertValidBroadcastMessage asserts the message is not system message.
func assertValidBroadcastMessage(msg message.BroadcastMutableMessage) {
	if msg.MessageType().IsSystem() {
		panic("system message is not allowed to broadcast append from client")
	}
	if msg.MessageType().IsSelfControlled() {
		panic("self controlled message is not allowed to broadcast append from client")
	}
}

// We only support delete and insert message for txn now.
func assertIsDmlMessage(msgs ...message.MutableMessage) {
	for _, msg := range msgs {
		if msg.MessageType() != message.MessageTypeInsert && msg.MessageType() != message.MessageTypeDelete {
			panic("only insert and delete message is allowed in txn")
		}
	}
}
