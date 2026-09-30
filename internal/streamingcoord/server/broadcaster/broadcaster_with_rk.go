package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type broadcasterWithRK struct {
	broadcaster    *broadcastTaskManager
	broadcastID    uint64
	controlChannel string
	unreplicable   bool // the message must be unreplicable, see WithUnreplicableResourceKeys.
	guards         *lockGuards
}

func (b *broadcasterWithRK) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	if b.unreplicable && !msg.IsUnreplicable() {
		// The guards are still the caller's here, so its Close() releases them.
		return nil, merr.WrapErrServiceInternalMsg("a broadcast started without the primary check must carry an unreplicable message, got %s", msg.MessageType())
	}

	// Every broadcast goes to the control channel: its ack joins the task into the
	// ack callback scheduler, and its time tick orders the ack callbacks.
	msg = message.WithBroadcastControlChannel(msg, b.controlChannel)

	// consume the guards after the broadcast is called to avoid double unlock.
	guards := b.guards
	b.guards = nil
	return b.broadcaster.broadcast(ctx, msg, b.broadcastID, guards)
}

func (b *broadcasterWithRK) Close() {
	if b.guards != nil {
		b.guards.Unlock()
	}
}
