package broadcaster

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// newBroadcastTaskFromProto creates a new broadcast task from the proto.
func newBroadcastTaskFromProto(proto *streamingpb.BroadcastTask, metrics *broadcasterMetrics) *broadcastTask {
	m := metrics.NewBroadcastTask(proto.GetState())
	msg := message.NewBroadcastMutableMessageBeforeAppend(proto.Message.Payload, proto.Message.Properties)
	bh := msg.BroadcastHeader()
	bt := &broadcastTask{
		mu:               sync.Mutex{},
		header:           bh,
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
		header: header,
		task: &streamingpb.BroadcastTask{
			Message:             &messagespb.Message{Payload: msg.Payload(), Properties: msg.Properties().ToRawMap()},
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
	header           *message.BroadcastHeader
	task             *streamingpb.BroadcastTask
	recoverPersisted bool // a flag to indicate that the task has been persisted into the recovery info and can be recovered.
	metrics          *taskMetricsGuard
	allAcked         chan struct{}
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

// Ack acknowledges the message at the specified vchannel.
func (b *broadcastTask) Ack(ctx context.Context, vchannel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.copyAndSetVChannelAcked(vchannel)
	if !ok {
		return nil
	}

	// We should always save the task after acked.
	// Even if the task mark as done in memory.
	// Because the task is set as done in memory before save the recovery info.
	if err := b.saveTask(ctx, task, b.Logger().With(zap.String("ackVChannel", vchannel))); err != nil {
		return err
	}
	b.task = task
	if isAllDone(task) {
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
func (b *broadcastTask) copyAndSetVChannelAcked(vchannel string) (*streamingpb.BroadcastTask, bool) {
	task := proto.Clone(b.task).(*streamingpb.BroadcastTask)
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
func (b *broadcastTask) BroadcastDone(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	task := b.copyAndMarkBroadcastDone()
	if err := b.saveTask(ctx, task, b.Logger()); err != nil {
		return err
	}
	b.task = task
	b.metrics.ObserveBroadcastDone()
	return nil
}

// copyAndMarkBroadcastDone copies the task and mark the broadcast task as done.
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
	if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, b.header.BroadcastID, task); err != nil {
		logger.Warn("save broadcast task failed", zap.Error(err))
		return err
	}
	logger.Info("save broadcast task done")
	b.metrics.ToState(task.State)
	return nil
}
