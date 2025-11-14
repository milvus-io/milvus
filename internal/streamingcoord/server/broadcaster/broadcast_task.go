package broadcaster

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// newBroadcastTaskFromProto creates a new broadcast task from the proto.
func newBroadcastTaskFromProto(proto *streamingpb.BroadcastTask, metrics *broadcasterMetrics, ackCallbackScheduler *ackCallbackScheduler) *broadcastTask {
	msg := message.NewBroadcastMutableMessageBeforeAppend(proto.Message.Payload, proto.Message.Properties)
	m := metrics.NewBroadcastTask(msg.MessageType(), proto.GetState(), msg.BroadcastHeader().ResourceKeys.Collect())
	bt := &broadcastTask{
		mu:                   sync.Mutex{},
		taskMetricsGuard:     m,
		msg:                  msg,
		task:                 proto,
		dirty:                true, // the task is recovered from the recovery info, so it's persisted.
		ackCallbackScheduler: ackCallbackScheduler,
		done:                 make(chan struct{}),
		allAcked:             make(chan struct{}),
	}
	if isAllDone(bt.task) {
		close(bt.allAcked)
	}
	if proto.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
		close(bt.done)
	}
	return bt
}

// newBroadcastTaskFromBroadcastMessage creates a new broadcast task from the broadcast message.
func newBroadcastTaskFromBroadcastMessage(msg message.BroadcastMutableMessage, metrics *broadcasterMetrics, ackCallbackScheduler *ackCallbackScheduler) *broadcastTask {
	m := metrics.NewBroadcastTask(msg.MessageType(), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, msg.BroadcastHeader().ResourceKeys.Collect())
	header := msg.BroadcastHeader()
	bt := &broadcastTask{
		Binder:           log.Binder{},
		taskMetricsGuard: m,
		mu:               sync.Mutex{},
		msg:              msg,
		task: &streamingpb.BroadcastTask{
			Message:             msg.IntoMessageProto(),
			State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			AckedVchannelBitmap: make([]byte, len(header.VChannels)),
			AckedCheckpoints:    make([]*streamingpb.AckedCheckpoint, len(header.VChannels)),
		},
		dirty:                false,
		ackCallbackScheduler: ackCallbackScheduler,
		done:                 make(chan struct{}),
		allAcked:             make(chan struct{}),
	}
	return bt
}

// newBroadcastTaskFromImmutableMessage creates a new broadcast task from the immutable message.
func newBroadcastTaskFromImmutableMessage(msg message.ImmutableMessage, metrics *broadcasterMetrics, ackCallbackScheduler *ackCallbackScheduler) *broadcastTask {
	broadcastMsg := msg.IntoBroadcastMutableMessage()
	task := newBroadcastTaskFromBroadcastMessage(broadcastMsg, metrics, ackCallbackScheduler)
	// if the task is created from the immutable message, it already has been broadcasted, so transfer its state into recovered.
	task.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
	task.ObserveStateChanged(streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED)
	return task
}

// broadcastTask is the state of the broadcast task.
type broadcastTask struct {
	log.Binder
	*taskMetricsGuard

	mu                       sync.Mutex
	msg                      message.BroadcastMutableMessage
	task                     *streamingpb.BroadcastTask
	dirty                    bool // a flag to indicate that the task has been modified and needs to be saved into the recovery info.
	done                     chan struct{}
	allAcked                 chan struct{}
	guards                   *lockGuards
	ackCallbackScheduler     *ackCallbackScheduler
	joinAckCallbackScheduled bool // a flag to indicate that the join ack callback is scheduled.
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

// BroadcastResult returns the broadcast result of the broadcast task.
func (b *broadcastTask) BroadcastResult() (message.BroadcastMutableMessage, map[string]*types.AppendResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	vchannels := b.msg.BroadcastHeader().VChannels
	result := make(map[string]*types.AppendResult, len(vchannels))
	for idx, vchannel := range vchannels {
		if b.task.AckedCheckpoints == nil {
			// forward compatible with the old version.
			result[vchannel] = &types.AppendResult{
				MessageID:              nil,
				LastConfirmedMessageID: nil,
				TimeTick:               0,
			}
			continue
		}
		cp := b.task.AckedCheckpoints[idx]
		if cp == nil || cp.TimeTick == 0 {
			panic("unreachable: BroadcastResult is called before the broadcast task is acked")
		}
		result[vchannel] = &types.AppendResult{
			MessageID:              message.MustUnmarshalMessageID(cp.MessageId),
			LastConfirmedMessageID: message.MustUnmarshalMessageID(cp.LastConfirmedMessageId),
			TimeTick:               cp.TimeTick,
		}
	}
	return b.msg, result
}

// Header returns the header of the broadcast task.
func (b *broadcastTask) Header() *message.BroadcastHeader {
	// header is a immutable field, no need to lock.
	return b.msg.BroadcastHeader()
}

// ControlChannelTimeTick returns the time tick of the control channel.
func (b *broadcastTask) ControlChannelTimeTick() uint64 {
	for idx, vc := range b.Header().VChannels {
		if funcutil.IsControlChannel(vc) {
			return b.task.AckedCheckpoints[idx].TimeTick
		}
	}
	return 0
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
		if b.task.AckedVchannelBitmap[i] != 0 || (b.task.AckedCheckpoints != nil && b.task.AckedCheckpoints[i] != nil) {
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

	if err := b.saveTaskIfDirty(ctx, b.Logger()); err != nil {
		return err
	}
	return nil
}

// GetImmutableMessageFromVChannel gets the immutable message from the vchannel.
func (b *broadcastTask) GetImmutableMessageFromVChannel(vchannel string) message.ImmutableMessage {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.getImmutableMessageFromVChannel(vchannel, nil)
}

func (b *broadcastTask) getImmutableMessageFromVChannel(vchannel string, result *types.AppendResult) message.ImmutableMessage {
	msg := message.NewBroadcastMutableMessageBeforeAppend(b.task.Message.Payload, b.task.Message.Properties)
	msgs := msg.SplitIntoMutableMessage()
	for _, msg := range msgs {
		if msg.VChannel() == vchannel {
			timetick := uint64(0)
			var messageID message.MessageID
			var lastConfirmedMessageID message.MessageID
			if result != nil {
				messageID = result.MessageID
				timetick = result.TimeTick
				lastConfirmedMessageID = result.LastConfirmedMessageID
			}
			// The legacy message don't have last confirmed message id/timetick/message id,
			// so we just mock a unsafely message here.
			if lastConfirmedMessageID == nil {
				return msg.WithTimeTick(timetick).WithLastConfirmedUseMessageID().IntoImmutableMessage(messageID)
			}
			return msg.WithTimeTick(timetick).WithLastConfirmed(lastConfirmedMessageID).IntoImmutableMessage(messageID)
		}
	}
	return nil
}

// Ack acknowledges the message at the specified vchannel.
// return true if all the vchannels are acked at first time, false if not.
func (b *broadcastTask) Ack(ctx context.Context, msgs ...message.ImmutableMessage) (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.ack(ctx, msgs...)
}

// ack acknowledges the message at the specified vchannel.
func (b *broadcastTask) ack(ctx context.Context, msgs ...message.ImmutableMessage) (err error) {
	isControlChannelAcked := b.copyAndSetAckedCheckpoints(msgs...)
	if !b.dirty {
		return nil
	}
	if err := b.saveTaskIfDirty(ctx, b.Logger()); err != nil {
		return err
	}

	allDone := isAllDone(b.task)
	if (isControlChannelAcked || allDone) && !b.joinAckCallbackScheduled {
		// after 2.6.5, the control channel is always broadcasted, it's used to determine the order of the ack callback operations.
		// so if the control channel is acked, it should be added to the ack callback scheduler.
		//
		// allDone is for the compatibility only for the operation before 2.6.5, the control channel is not broadcasted,
		b.ackCallbackScheduler.AddTask(b)
		b.joinAckCallbackScheduled = true
	}
	if allDone {
		close(b.allAcked)
	}
	return nil
}

// hasControlChannel checks if the control channel is broadcasted.
// for the operation since 2.6.5, the control channel is always broadcasted.
// so it's just a dummy function for compatibility.
func (b *broadcastTask) isControlChannelAcked() bool {
	for idx, vc := range b.Header().VChannels {
		if funcutil.IsControlChannel(vc) && b.task.AckedCheckpoints[idx] != nil {
			return true
		}
	}
	return false
}

// BlockUntilDone blocks until the broadcast task is done.
func (b *broadcastTask) BlockUntilDone(ctx context.Context) (*types.BroadcastAppendResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		_, result := b.BroadcastResult()
		return &types.BroadcastAppendResult{
			BroadcastID:   b.Header().BroadcastID,
			AppendResults: result,
		}, nil
	}
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

// copyAndSetAckedCheckpoints copies the task and set the acked checkpoints.
func (b *broadcastTask) copyAndSetAckedCheckpoints(msgs ...message.ImmutableMessage) (isControlChannelAcked bool) {
	task := proto.Clone(b.task).(*streamingpb.BroadcastTask)
	for _, msg := range msgs {
		vchannel := msg.VChannel()
		idx, err := findIdxOfVChannel(vchannel, b.Header().VChannels)
		if err != nil {
			panic(err)
		}
		if len(task.AckedVchannelBitmap) == 0 {
			task.AckedVchannelBitmap = make([]byte, len(b.Header().VChannels))
		}
		if len(task.AckedCheckpoints) == 0 {
			task.AckedCheckpoints = make([]*streamingpb.AckedCheckpoint, len(b.Header().VChannels))
		}
		if cp := task.AckedCheckpoints[idx]; cp != nil && cp.TimeTick != 0 {
			// after proto.Clone, the cp is always not nil, so we also need to check the time tick.
			continue
		}
		// the ack result is dirty, so we need to set the dirty flag to true.
		b.dirty = true
		task.AckedVchannelBitmap[idx] = 1
		task.AckedCheckpoints[idx] = &streamingpb.AckedCheckpoint{
			MessageId:              msg.MessageID().IntoProto(),
			LastConfirmedMessageId: msg.LastConfirmedMessageID().IntoProto(),
			TimeTick:               msg.TimeTick(),
		}
		if funcutil.IsControlChannel(vchannel) {
			isControlChannelAcked = true
		}
	}
	// update current task state.
	b.task = task
	return
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

// FastAck trigger a fast ack operation when the broadcast operation is done.
func (b *broadcastTask) FastAck(ctx context.Context, broadcastResult map[string]*types.AppendResult) error {
	// Broadcast operation is done.
	b.mu.Lock()
	defer b.mu.Unlock()

	b.ObserveBroadcastDone()
	// because we need to wait for the streamingnode to ack the message,
	// however, if the message is already write into wal, the message is determined,
	// so we can make a fast ack operation here to speed up the ack operation.
	msgs := make([]message.ImmutableMessage, 0, len(broadcastResult))
	for vchannel := range broadcastResult {
		msgs = append(msgs, b.getImmutableMessageFromVChannel(vchannel, broadcastResult[vchannel]))
	}
	return b.ack(ctx, msgs...)
}

// DropTombstone drops the tombstone of the broadcast task.
// It will remove the tombstone of the broadcast task in recovery storage.
// After the tombstone is dropped, the idempotency and deduplication can not be guaranteed.
func (b *broadcastTask) DropTombstone(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
	b.dirty = true
	return b.saveTaskIfDirty(ctx, b.Logger())
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

// MarkAckCallbackDone marks the ack callback is done.
func (b *broadcastTask) MarkAckCallbackDone(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.task.State != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
		b.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
		close(b.done)
		b.dirty = true
	}

	if err := b.saveTaskIfDirty(ctx, b.Logger()); err != nil {
		return err
	}

	if b.guards != nil {
		// release the resource key lock if done.
		// if the broadcast task is recovered from the remote cluster by replication,
		// it doesn't hold the resource key lock, so skip it.
		b.guards.Unlock()
	}
	return nil
}

// saveTaskIfDirty saves the broadcast task recovery info if the task is dirty.
func (b *broadcastTask) saveTaskIfDirty(ctx context.Context, logger *log.MLogger) error {
	if !b.dirty {
		return nil
	}
	b.dirty = false
	logger = logger.With(zap.String("state", b.task.State.String()), zap.Int("ackedVChannelCount", ackedCount(b.task)))
	if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, b.Header().BroadcastID, b.task); err != nil {
		logger.Warn("save broadcast task failed", zap.Error(err))
		if ctx.Err() == nil {
			panic("critical error: the save broadcast task is failed before the context is done")
		}
		return err
	}
	b.ObserveStateChanged(b.task.State)
	logger.Info("save broadcast task done")
	return nil
}
