package broadcaster

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var errBroadcastTaskIsNotDone = errors.New("broadcast task is not done")

// newTask creates a new task
func newTask(task *streamingpb.BroadcastTask, logger *log.MLogger) *broadcastTask {
	bt := message.NewBroadcastMutableMessage(task.Message.Payload, task.Message.Properties)
	msgs := bt.SplitIntoMutableMessage()
	return &broadcastTask{
		logger:          logger.With(zap.Int64("taskID", task.TaskId), zap.Int("broadcastTotal", len(msgs))),
		task:            task,
		pendingMessages: msgs,
		appendResult:    make(map[string]*types.AppendResult, len(msgs)),
		future:          syncutil.NewFuture[*types.BroadcastAppendResult](),
		BackoffWithInstant: typeutil.NewBackoffWithInstant(typeutil.BackoffTimerConfig{
			Default: 10 * time.Second,
			Backoff: typeutil.BackoffConfig{
				InitialInterval: 10 * time.Millisecond,
				Multiplier:      2.0,
				MaxInterval:     10 * time.Second,
			},
		}),
	}
}

// broadcastTask is the task for broadcasting messages.
type broadcastTask struct {
	logger          *log.MLogger
	task            *streamingpb.BroadcastTask
	pendingMessages []message.MutableMessage
	appendResult    map[string]*types.AppendResult
	future          *syncutil.Future[*types.BroadcastAppendResult]
	*typeutil.BackoffWithInstant
}

// Poll polls the task, return nil if the task is done, otherwise not done.
// Poll can be repeated called until the task is done.
func (b *broadcastTask) Poll(ctx context.Context, operator AppendOperator) error {
	if len(b.pendingMessages) > 0 {
		b.logger.Debug("broadcast task is polling to make sent...", zap.Int("pendingMessages", len(b.pendingMessages)))
		resps := operator.AppendMessages(ctx, b.pendingMessages...)
		newPendings := make([]message.MutableMessage, 0)
		for idx, resp := range resps.Responses {
			if resp.Error != nil {
				newPendings = append(newPendings, b.pendingMessages[idx])
				continue
			}
			b.appendResult[b.pendingMessages[idx].VChannel()] = resp.AppendResult
		}
		b.pendingMessages = newPendings
		if len(newPendings) == 0 {
			b.future.Set(&types.BroadcastAppendResult{AppendResults: b.appendResult})
		}
		b.logger.Info("broadcast task make a new broadcast done", zap.Int("pendingMessages", len(b.pendingMessages)))
	}
	if len(b.pendingMessages) == 0 {
		// There's no more pending message, mark the task as done.
		b.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
		if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, b.task); err != nil {
			b.logger.Warn("save broadcast task failed", zap.Error(err))
			b.UpdateInstantWithNextBackOff()
			return err
		}
		return nil
	}
	b.UpdateInstantWithNextBackOff()
	return errBroadcastTaskIsNotDone
}

// BlockUntilTaskDone blocks until the task is done.
func (b *broadcastTask) BlockUntilTaskDone(ctx context.Context) (*types.BroadcastAppendResult, error) {
	return b.future.GetWithContext(ctx)
}

type broadcastTaskArray []*broadcastTask

// Len returns the length of the heap.
func (h broadcastTaskArray) Len() int {
	return len(h)
}

// Less returns true if the element at index i is less than the element at index j.
func (h broadcastTaskArray) Less(i, j int) bool {
	return h[i].NextInstant().Before(h[j].NextInstant())
}

// Swap swaps the elements at indexes i and j.
func (h broadcastTaskArray) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *broadcastTaskArray) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*broadcastTask))
}

// Pop pop the last one at len.
func (h *broadcastTaskArray) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
// Panics if the heap is empty.
func (h *broadcastTaskArray) Peek() interface{} {
	return (*h)[0]
}
