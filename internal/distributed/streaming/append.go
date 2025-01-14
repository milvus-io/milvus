package streaming

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

// appendToWAL appends the message to the wal.
func (w *walAccesserImpl) appendToWAL(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
	pchannel := funcutil.ToPhysicalChannel(msg.VChannel())
	// get producer of pchannel.
	p := w.getProducer(pchannel)
	return p.Produce(ctx, msg)
}

func (w *walAccesserImpl) broadcastToWAL(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// The broadcast operation will be sent to the coordinator.
	// The coordinator will dispatch the message to all the vchannels with an eventual consistency guarantee.
	return w.streamingCoordClient.Broadcast().Broadcast(ctx, msg)
}

// createOrGetProducer creates or get a producer.
// vchannel in same pchannel can share the same producer.
func (w *walAccesserImpl) getProducer(pchannel string) *producer.ResumableProducer {
	w.producerMutex.Lock()
	defer w.producerMutex.Unlock()

	// TODO: A idle producer should be removed maybe?
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
}

// We only support delete and insert message for txn now.
func assertIsDmlMessage(msgs ...message.MutableMessage) {
	for _, msg := range msgs {
		if msg.MessageType() != message.MessageTypeInsert && msg.MessageType() != message.MessageTypeDelete {
			panic("only insert and delete message is allowed in txn")
		}
	}
}
