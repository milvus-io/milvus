package broadcaster

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var errBroadcastTaskIsNotDone = errors.New("broadcast task is not done")

// newPendingBroadcastTask creates a new pendingBroadcastTask.
func newPendingBroadcastTask(task *broadcastTask) *pendingBroadcastTask {
	msgs := task.PendingBroadcastMessages()
	return &pendingBroadcastTask{
		broadcastTask:   task,
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

// pendingBroadcastTask is a task that is pending to be broadcasted.
type pendingBroadcastTask struct {
	*broadcastTask
	pendingMessages []message.MutableMessage
	appendResult    map[string]*types.AppendResult
	future          *syncutil.Future[*types.BroadcastAppendResult]
	*typeutil.BackoffWithInstant
}

// Execute reexecute the task, return nil if the task is done, otherwise not done.
// Execute can be repeated called until the task is done.
// Same semantics as the `Poll` operation in eventloop.
func (b *pendingBroadcastTask) Execute(ctx context.Context, operator AppendOperator) error {
	if err := b.broadcastTask.InitializeRecovery(ctx); err != nil {
		b.Logger().Warn("broadcast task initialize recovery failed", zap.Error(err))
		b.UpdateInstantWithNextBackOff()
		return err
	}

	if len(b.pendingMessages) > 0 {
		b.Logger().Debug("broadcast task is polling to make sent...", zap.Int("pendingMessages", len(b.pendingMessages)))
		resps := operator.AppendMessages(ctx, b.pendingMessages...)
		newPendings := make([]message.MutableMessage, 0)
		for idx, resp := range resps.Responses {
			if resp.Error != nil {
				b.Logger().Warn("broadcast task append message failed", zap.Int("idx", idx), zap.Error(resp.Error))
				newPendings = append(newPendings, b.pendingMessages[idx])
				continue
			}
			b.appendResult[b.pendingMessages[idx].VChannel()] = resp.AppendResult
		}
		b.pendingMessages = newPendings
		if len(newPendings) == 0 {
			b.future.Set(&types.BroadcastAppendResult{
				BroadcastID:   b.header.BroadcastID,
				AppendResults: b.appendResult,
			})
		}
		b.Logger().Info("broadcast task make a new broadcast done", zap.Int("backoffRetryMessages", len(b.pendingMessages)))
	}
	if len(b.pendingMessages) == 0 {
		if err := b.broadcastTask.BroadcastDone(ctx); err != nil {
			b.UpdateInstantWithNextBackOff()
			return err
		}
		return nil
	}
	b.UpdateInstantWithNextBackOff()
	return errBroadcastTaskIsNotDone
}

// BlockUntilTaskDone blocks until the task is done.
func (b *pendingBroadcastTask) BlockUntilTaskDone(ctx context.Context) (*types.BroadcastAppendResult, error) {
	return b.future.GetWithContext(ctx)
}

// pendingBroadcastTaskArray is a heap of pendingBroadcastTask.
type pendingBroadcastTaskArray []*pendingBroadcastTask

// Len returns the length of the heap.
func (h pendingBroadcastTaskArray) Len() int {
	return len(h)
}

// Less returns true if the element at index i is less than the element at index j.
func (h pendingBroadcastTaskArray) Less(i, j int) bool {
	return h[i].NextInstant().Before(h[j].NextInstant())
}

// Swap swaps the elements at indexes i and j.
func (h pendingBroadcastTaskArray) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *pendingBroadcastTaskArray) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*pendingBroadcastTask))
}

// Pop pop the last one at len.
func (h *pendingBroadcastTaskArray) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
// Panics if the heap is empty.
func (h *pendingBroadcastTaskArray) Peek() interface{} {
	return (*h)[0]
}
