package broadcaster

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// newBroadcastTaskFromProto creates a new broadcast task from the proto.
func newBroadcastTaskFromProto(proto *streamingpb.BroadcastTask, metrics *broadcasterMetrics) *broadcastTask {
	m := metrics.NewBroadcastTask(proto.GetState())
	msg := message.NewBroadcastMutableMessageBeforeAppend(proto.Message.Payload, proto.Message.Properties)
	bt := &broadcastTask{
		mu:               sync.Mutex{},
		msg:              msg,
		task:             proto,
		recoverPersisted: true, // the task is recovered from the recovery info, so it's persisted.
		metrics:          m,
		allAcked:         make(chan struct{}),
	}
	if isAllDone(proto) {
		close(bt.allAcked)
	}
	return bt
}

// newBroadcastTaskFromBroadcastMessage creates a new broadcast task from the broadcast message.
func newBroadcastTaskFromBroadcastMessage(msg message.BroadcastMutableMessage, metrics *broadcasterMetrics) *broadcastTask {
	m := metrics.NewBroadcastTask(streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING)
	header := msg.BroadcastHeader()
	bt := &broadcastTask{
		Binder: log.Binder{},
		mu:     sync.Mutex{},
		msg:    msg,
		task: &streamingpb.BroadcastTask{
			Message:             msg.IntoMessageProto(),
			State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			AckedVchannelBitmap: make([]byte, len(header.VChannels)),
		},
		recoverPersisted: false,
		metrics:          m,
		allAcked:         make(chan struct{}),
	}
	if isAllDone(bt.task) {
		close(bt.allAcked)
	}
	return bt
}

// broadcastTask is the state of the broadcast task.
type broadcastTask struct {
	log.Binder
	mu               sync.Mutex
	msg              message.BroadcastMutableMessage
	result           map[string]*types.AppendResult
	task             *streamingpb.BroadcastTask
	recoverPersisted bool // a flag to indicate that the task has been persisted into the recovery info and can be recovered.
	metrics          *taskMetricsGuard
	allAcked         chan struct{}
	guards           *lockGuards
}

// SetLogger sets the logger of the broadcast task.
func (b *broadcastTask) SetLogger(logger *log.MLogger) {
	b.Binder.SetLogger(logger.With(log.FieldMessage(b.msg)))
}

// WithResourceKeyLockGuards sets the lock guards for the broadcast task.
func (b *broadcastTask) WithResourceKeyLockGuards(guards *lockGuards) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.guards != nil {
		panic("broadcast task already has lock guards")
	}
	b.guards = guards
}

// Header returns the header of the broadcast task.
func (b *broadcastTask) Header() *message.BroadcastHeader {
	// header is a immutable field, no need to lock.
	return b.msg.BroadcastHeader()
}

// State returns the State of the broadcast task.
func (b *broadcastTask) State() streamingpb.BroadcastTaskState {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.task.State
}

// PendingBroadcastMessages returns the pending broadcast message of current broadcast.
// If the vchannel is already acked, it will be filtered out.
func (b *broadcastTask) PendingBroadcastMessages() []message.MutableMessage {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg := message.NewBroadcastMutableMessageBeforeAppend(b.task.Message.Payload, b.task.Message.Properties)
	msgs := msg.SplitIntoMutableMessage()
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
	if err := b.saveTask(ctx, b.task, b.Logger()); err != nil {
		return err
	}
	b.recoverPersisted = true
	return nil
}

// GetImmutableMessageFromVChannel gets the immutable message from the vchannel.
func (b *broadcastTask) GetImmutableMessageFromVChannel(vchannel string) message.ImmutableMessage {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.getImmutableMessageFromVChannel(vchannel)
}

func (b *broadcastTask) getImmutableMessageFromVChannel(vchannel string) message.ImmutableMessage {
	msg := message.NewBroadcastMutableMessageBeforeAppend(b.task.Message.Payload, b.task.Message.Properties)
	msgs := msg.SplitIntoMutableMessage()
	for _, msg := range msgs {
		if msg.VChannel() == vchannel {
			timetick := uint64(0)
			var messageID message.MessageID
			if result, ok := b.result[vchannel]; ok {
				messageID = result.MessageID
				timetick = result.TimeTick
			}
			// The legacy message don't have timetick, so we need to set it to 0.
			return msg.WithTimeTick(timetick).IntoImmutableMessage(messageID)
		}
	}
	return nil
}

// FastAckAll is used to fast ack all the vchannels after the broadcast message is already write into wal.
func (b *broadcastTask) FastAckAll(ctx context.Context) error {
	msgs := b.getAllBroadcastedImmutableMessages()
	if err := registry.CallMessageAckCallback(ctx, msgs...); err != nil {
		return errors.Wrap(err, "when calling message ack callback")
	}
	if err := b.Ack(ctx, msgs...); err != nil {
		return errors.Wrap(err, "when acking the broadcast message")
	}
	return nil
}

// getAllBroadcastedImmutableMessages gets all the broadcasted immutable messages.
func (b *broadcastTask) getAllBroadcastedImmutableMessages() []message.ImmutableMessage {
	b.mu.Lock()
	defer b.mu.Unlock()

	msgs := make([]message.ImmutableMessage, 0, len(b.result))
	for vchannel := range b.result {
		msgs = append(msgs, b.getImmutableMessageFromVChannel(vchannel))
	}
	if len(msgs) != len(b.msg.BroadcastHeader().VChannels) {
		panic("the number of broadcasted messages should be equal to the number of vchannels")
	}
	return msgs
}

// Ack acknowledges the message at the specified vchannel.
func (b *broadcastTask) Ack(ctx context.Context, msg ...message.ImmutableMessage) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vchannels := make([]string, 0, len(msg))
	for _, msg := range msg {
		vchannels = append(vchannels, msg.VChannel())
	}
	task, ok := b.copyAndSetVChannelAcked(vchannels...)
	if !ok {
		return nil
	}

	// We should always save the task after acked.
	// Even if the task mark as done in memory.
	// Because the task is set as done in memory before save the recovery info.
	if err := b.saveTask(ctx, task, b.Logger().With(zap.Strings("ackVChannels", vchannels))); err != nil {
		return err
	}
	b.task = task
	if isAllDone(task) {
		// all the vchannels are acked, so we can unlock the resource keys.
		b.guards.Unlock()
		b.metrics.ObserveAckAll()
		close(b.allAcked)
	}
	return nil
}

// BlockUntilAllAck blocks until all the vchannels are acked.
func (b *broadcastTask) BlockUntilAllAck(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.allAcked:
		return nil
	}
}

// copyAndSetVChannelAcked copies the task and set the vchannel as acked.
// if the vchannel is already acked, it returns nil and false.
func (b *broadcastTask) copyAndSetVChannelAcked(vchannels ...string) (*streamingpb.BroadcastTask, bool) {
	task := proto.Clone(b.task).(*streamingpb.BroadcastTask)
	for _, vchannel := range vchannels {
		idx, err := findIdxOfVChannel(vchannel, b.Header().VChannels)
		if err != nil {
			panic(err)
		}
		if task.AckedVchannelBitmap[idx] != 0 {
			return nil, false
		}
		task.AckedVchannelBitmap[idx] = 1
		if isAllDone(task) {
			// All vchannels are acked, mark the task as done, even if there are still pending messages on working.
			// The pending messages is repeated sent operation, can be ignored.
			task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
		}
	}
	return task, true
}

// findIdxOfVChannel finds the index of the vchannel in the broadcast task.
func findIdxOfVChannel(vchannel string, vchannels []string) (int, error) {
	for i, channelName := range vchannels {
		if channelName == vchannel {
			return i, nil
		}
	}
	return -1, errors.Errorf("unreachable: vchannel is %s not found in the broadcast task", vchannel)
}

// BroadcastDone marks the broadcast operation is done.
func (b *broadcastTask) BroadcastDone(ctx context.Context, appendResult map[string]*types.AppendResult) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	task := b.copyAndMarkBroadcastDone()
	if err := b.saveTask(ctx, task, b.Logger()); err != nil {
		return err
	}
	b.task = task
	b.result = appendResult
	b.metrics.ObserveBroadcastDone()
	return nil
}

// copyAndMarkBroadcastDone copies the task and mark the broadcast task as done.
// !!! The ack state of the task should not be removed, because the task is a lock-hint of resource key held by a broadcast operation.
// It can be removed only after the broadcast message is acked by all the vchannels.
func (b *broadcastTask) copyAndMarkBroadcastDone() *streamingpb.BroadcastTask {
	task := proto.Clone(b.task).(*streamingpb.BroadcastTask)
	if isAllDone(task) {
		// If all vchannels are acked, mark the task as done.
		task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
	} else {
		// There's no more pending message, mark the task as wait ack.
		task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK
	}
	return task
}

// isAllDone check if all the vchannels are acked.
func isAllDone(task *streamingpb.BroadcastTask) bool {
	for _, acked := range task.AckedVchannelBitmap {
		if acked == 0 {
			return false
		}
	}
	return true
}

// ackedCount returns the count of the acked vchannels.
func ackedCount(task *streamingpb.BroadcastTask) int {
	count := 0
	for _, acked := range task.AckedVchannelBitmap {
		count += int(acked)
	}
	return count
}

// saveTask saves the broadcast task recovery info.
func (b *broadcastTask) saveTask(ctx context.Context, task *streamingpb.BroadcastTask, logger *log.MLogger) error {
	logger = logger.With(zap.String("state", task.State.String()), zap.Int("ackedVChannelCount", ackedCount(task)))
	if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, b.msg.BroadcastHeader().BroadcastID, task); err != nil {
		logger.Warn("save broadcast task failed", zap.Error(err))
		return err
	}
	logger.Info("save broadcast task done")
	b.metrics.ToState(task.State)
	return nil
}
