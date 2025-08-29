package streaming

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// routePChannel routes the pchannel of the vchannel.
// If the vchannel is control channel, it will return the pchannel of the cchannel.
// Otherwise, it will return the pchannel of the vchannel.
// TODO: support cross-cluster replication, so the remote vchannel should be mapping to the pchannel of the local cluster.
func (w *walAccesserImpl) routePChannel(ctx context.Context, vchannel string) (string, error) {
	assignments, err := w.streamingCoordClient.Assignment().GetLatestAssignments(ctx)
	if err != nil {
		return "", err
	}
	if vchannel == message.ControlChannel {
		return assignments.PChannelOfCChannel(), nil
	}
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	repConfigHelper := assignments.ReplicateConfigHelper
	if repConfigHelper != nil {
		// Route the pchannel to the target cluster if the message is replicated.
		pchannel = repConfigHelper.GetTargetChannel(pchannel, w.clusterID)
	}
	return pchannel, nil
}

// appendToWAL appends the message to the wal.
func (w *walAccesserImpl) appendToWAL(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
	pchannel, err := w.routePChannel(ctx, msg.VChannel())
	if err != nil {
		return nil, err
	}
	// get producer of pchannel.
	p := w.getProducer(pchannel)
	return p.Produce(ctx, msg)
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
