package broadcaster

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// newBroadcastTaskFromProto creates a new broadcast task from the proto.
func newBroadcastTaskFromProto(proto *streamingpb.BroadcastTask) *broadcastTask {
	msg := message.NewBroadcastMutableMessageBeforeAppend(proto.Message.Payload, proto.Message.Properties)
	bh := msg.BroadcastHeader()
	ackedCount := 0
	for _, acked := range proto.AckedVchannelBitmap {
		ackedCount += int(acked)
	}
	return &broadcastTask{
		mu:               sync.Mutex{},
		header:           bh,
		task:             proto,
		ackedCount:       ackedCount,
		recoverPersisted: true, // the task is recovered from the recovery info, so it's persisted.
	}
}

// newBroadcastTaskFromBroadcastMessage creates a new broadcast task from the broadcast message.
func newBroadcastTaskFromBroadcastMessage(msg message.BroadcastMutableMessage) *broadcastTask {
	header := msg.BroadcastHeader()
	return &broadcastTask{
		Binder: log.Binder{},
		mu:     sync.Mutex{},
		header: header,
		task: &streamingpb.BroadcastTask{
			Message:             &messagespb.Message{Payload: msg.Payload(), Properties: msg.Properties().ToRawMap()},
			State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			AckedVchannelBitmap: make([]byte, len(header.VChannels)),
		},
		ackedCount:       0,
		recoverPersisted: false,
	}
}

// broadcastTask is the state of the broadcast task.
type broadcastTask struct {
	log.Binder
	mu         sync.Mutex
	header     *message.BroadcastHeader
	task       *streamingpb.BroadcastTask
	ackedCount int // the count of the acked vchannels, the idompotenace is promised by task's bitmap.
	// always keep same with the positive counter of task's acked_bitmap.
	recoverPersisted bool // a flag to indicate that the task has been persisted into the recovery info and can be recovered.
}

// Header returns the header of the broadcast task.
func (b *broadcastTask) Header() *message.BroadcastHeader {
	// header is a immutable field, no need to lock.
	return b.header
}

// State returns the State of the broadcast task.
func (b *broadcastTask) State() streamingpb.BroadcastTaskState {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.task.State
}

// PendingBroadcastMessages returns the pending broadcast message of current broad cast.
func (b *broadcastTask) PendingBroadcastMessages() []message.MutableMessage {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg := message.NewBroadcastMutableMessageBeforeAppend(b.task.Message.Payload, b.task.Message.Properties)
	msgs := msg.SplitIntoMutableMessage()
	// If there's no vchannel acked, return all the messages directly.
	if b.ackedCount == 0 {
		return msgs
	}
	// filter out the vchannel that has been acked.
	pendingMessages := make([]message.MutableMessage, 0, len(msgs))
	for i, msg := range msgs {
		if b.task.AckedVchannelBitmap[i] != 0 {
			continue
		}
		pendingMessages = append(pendingMessages, msg)
	}
	return pendingMessages
}

// InitializeRecovery initializes the recovery of the broadcast task.
func (b *broadcastTask) InitializeRecovery(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.recoverPersisted {
		return nil
	}
	if err := b.saveTask(ctx, b.Logger()); err != nil {
		return err
	}
	b.recoverPersisted = true
	return nil
}

// Ack acknowledges the message at the specified vchannel.
func (b *broadcastTask) Ack(ctx context.Context, vchannel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.setVChannelAcked(vchannel)
	if b.isAllDone() {
		// All vchannels are acked, mark the task as done, even if there are still pending messages on working.
		// The pending messages is repeated sent operation, can be ignored.
		b.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
	}
	// We should always save the task after acked.
	// Even if the task mark as done in memory.
	// Because the task is set as done in memory before save the recovery info.
	return b.saveTask(ctx, b.Logger().With(zap.String("ackVChannel", vchannel)))
}

// setVChannelAcked sets the vchannel as acked.
func (b *broadcastTask) setVChannelAcked(vchannel string) {
	idx, err := b.findIdxOfVChannel(vchannel)
	if err != nil {
		panic(err)
	}
	b.task.AckedVchannelBitmap[idx] = 1
	// Check if all vchannels are acked.
	ackedCount := 0
	for _, acked := range b.task.AckedVchannelBitmap {
		ackedCount += int(acked)
	}
	b.ackedCount = ackedCount
}

// findIdxOfVChannel finds the index of the vchannel in the broadcast task.
func (b *broadcastTask) findIdxOfVChannel(vchannel string) (int, error) {
	for i, channelName := range b.header.VChannels {
		if channelName == vchannel {
			return i, nil
		}
	}
	return -1, errors.Errorf("unreachable: vchannel is %s not found in the broadcast task", vchannel)
}

// isAllDone check if all the vchannels are acked.
func (b *broadcastTask) isAllDone() bool {
	return b.ackedCount == len(b.header.VChannels)
}

// BroadcastDone marks the broadcast operation is done.
func (b *broadcastTask) BroadcastDone(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isAllDone() {
		// If all vchannels are acked, mark the task as done.
		b.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
	} else {
		// There's no more pending message, mark the task as wait ack.
		b.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK
	}
	return b.saveTask(ctx, b.Logger())
}

// IsAllAcked returns true if all the vchannels are acked.
func (b *broadcastTask) IsAllAcked() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isAllDone()
}

// IsAcked returns true if any vchannel is acked.
func (b *broadcastTask) IsAcked() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ackedCount > 0
}

// saveTask saves the broadcast task recovery info.
func (b *broadcastTask) saveTask(ctx context.Context, logger *log.MLogger) error {
	logger = logger.With(zap.String("state", b.task.State.String()), zap.Int("ackedVChannelCount", b.ackedCount))
	if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, b.header.BroadcastID, b.task); err != nil {
		logger.Warn("save broadcast task failed", zap.Error(err))
		return err
	}
	logger.Info("save broadcast task done")
	return nil
}
