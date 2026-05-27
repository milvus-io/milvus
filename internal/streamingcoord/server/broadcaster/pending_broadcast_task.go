package broadcaster

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errBroadcastTaskIsNotDone = errors.New("broadcast task is not done")
var errStaleGrowingSourceReleaseFence = errors.New("stale growing-source release fence")

// newPendingBroadcastTask creates a new pendingBroadcastTask.
func newPendingBroadcastTask(task *broadcastTask) *pendingBroadcastTask {
	msgs := task.PendingBroadcastMessages()
	if len(msgs) == 0 {
		return nil
	}
	return &pendingBroadcastTask{
		broadcastTask:   task,
		pendingMessages: msgs,
		appendResult:    make(map[string]*types.AppendResult, len(msgs)),
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
	*typeutil.BackoffWithInstant
}

// Execute reexecute the task, return nil if the task is done, otherwise not done.
// Execute can be repeated called until the task is done.
// Same semantics as the `Poll` operation in eventloop.
func (b *pendingBroadcastTask) Execute(ctx context.Context) error {
	if err := b.InitializeRecovery(ctx); err != nil {
		b.Logger().Warn("broadcast task initialize recovery failed", zap.Error(err))
		return err
	}

	if len(b.pendingMessages) > 0 {
		b.Logger().Debug("broadcast task is polling to make sent...", zap.Int("pendingMessages", len(b.pendingMessages)))
		resps := streaming.WAL().AppendMessages(ctx, b.pendingMessages...)
		newPendings := make([]message.MutableMessage, 0)
		for idx, resp := range resps.Responses {
			pendingMsg := b.pendingMessages[idx]
			if resp.Error != nil {
				if isStaleGrowingSourceReleaseFenceAppendError(pendingMsg, resp.Error) {
					err := errors.Wrapf(errStaleGrowingSourceReleaseFence, "vchannel %s: %s", pendingMsg.VChannel(), resp.Error.Error())
					b.Logger().Warn("skip stale growing-source release fence", zap.Int("idx", idx), zap.Error(resp.Error))
					return b.MarkBroadcastAborted(ctx, err)
				}
				b.Logger().Warn("broadcast task append message failed", zap.Int("idx", idx), zap.Error(resp.Error))
				newPendings = append(newPendings, pendingMsg)
				continue
			}
			b.appendResult[pendingMsg.VChannel()] = resp.AppendResult
		}
		b.pendingMessages = newPendings
		b.Logger().Info("broadcast task make a new broadcast done", zap.Int("backoffRetryMessages", len(b.pendingMessages)))
	}
	if len(b.pendingMessages) == 0 {
		// trigger a fast ack operation when the broadcast operation is done.
		if err := b.FastAck(ctx, b.appendResult); err != nil {
			b.Logger().Warn("broadcast task save task failed", zap.Error(err))
			return err
		}
		return nil
	}
	b.UpdateInstantWithNextBackOff()
	return errBroadcastTaskIsNotDone
}

func isStaleGrowingSourceReleaseFenceAppendError(msg message.MutableMessage, err error) bool {
	if msg.MessageType() != message.MessageTypeManualFlush {
		return false
	}
	if !message.IsGrowingSourceReleaseFence(msg) {
		return false
	}
	streamingErr := status.AsStreamingError(err)
	if streamingErr == nil || !streamingErr.IsUnrecoverable() {
		return false
	}
	cause := strings.ToLower(streamingErr.Cause)
	return strings.Contains(cause, "collection not found") ||
		strings.Contains(cause, "channel not found") ||
		strings.Contains(cause, "channel not available") ||
		strings.Contains(cause, "channel has been dropped") ||
		strings.Contains(cause, "channel dropped") ||
		strings.Contains(cause, "not exist")
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
	old[n-1] = nil // release the memory of underlying array.
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
// Panics if the heap is empty.
func (h *pendingBroadcastTaskArray) Peek() interface{} {
	return (*h)[0]
}
