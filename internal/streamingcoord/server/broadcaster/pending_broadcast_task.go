package broadcaster

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errBroadcastTaskIsNotDone = errors.New("broadcast task is not done")

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
	// barrierApplied is set once the pending messages carry the time tick of
	// the append-first copies as their barrier.
	barrierApplied bool
	*typeutil.BackoffWithInstant
}

// Execute reexecute the task, return nil if the task is done, otherwise not done.
// Execute can be repeated called until the task is done.
// Same semantics as the `Poll` operation in eventloop.
func (b *pendingBroadcastTask) Execute(ctx context.Context) error {
	ctx = message.ExtractTraceContext(ctx, b.msg)

	if err := b.InitializeRecovery(ctx); err != nil {
		b.Logger().Warn(ctx, "broadcast task initialize recovery failed", mlog.FieldMessage(b.BroadcastMessage()), mlog.Err(err))
		return err
	}

	// An append-first copy is appended and persisted before any other copy. A
	// shard split names its source vchannel: the fence must be in the WAL
	// before a target takes the time tick of its genesis.
	if first, rest := b.splitAppendFirst(); len(first) > 0 {
		results, failed := b.appendGroup(ctx, first)
		if len(failed) > 0 {
			// Nothing else may be appended before these copies. Keep every
			// message pending and try again.
			b.UpdateInstantWithNextBackOff()
			return errBroadcastTaskIsNotDone
		}
		// Persist the result now. A coordinator that restarts after this point
		// does not append the copy again, and the task remembers its time tick.
		if err := b.AckAppendFirst(ctx, results); err != nil {
			b.Logger().Warn(ctx, "broadcast task persist the append-first copy failed", mlog.Err(err))
			return err
		}
		// The results are acked already, so they are not kept for the fast ack
		// of the other copies.
		b.pendingMessages = rest
		b.Logger().Info(ctx, "broadcast task appended the append-first copy", mlog.Int("pendingMessages", len(b.pendingMessages)))
	}
	// Every other copy takes a time tick greater than the append-first one,
	// whichever StreamingNode appends it. The barrier comes from the persisted
	// result, so it also holds for a task recovered after a restart.
	b.applyAppendFirstBarrier()

	if len(b.pendingMessages) > 0 {
		b.Logger().Debug(ctx, "broadcast task is polling to make sent...", mlog.FieldMessage(b.BroadcastMessage()), mlog.Int("pendingMessages", len(b.pendingMessages)))
		results, failed := b.appendGroup(ctx, b.pendingMessages)
		for vchannel, result := range results {
			b.appendResult[vchannel] = result
		}
		b.pendingMessages = failed
		b.Logger().Info(ctx, "broadcast task make a new broadcast done", mlog.FieldMessage(b.BroadcastMessage()), mlog.Int("backoffRetryMessages", len(b.pendingMessages)))
	}
	if len(b.pendingMessages) == 0 {
		// trigger a fast ack operation when the broadcast operation is done.
		if err := b.FastAck(ctx, b.appendResult); err != nil {
			b.Logger().Warn(ctx, "broadcast task save task failed", mlog.FieldMessage(b.BroadcastMessage()), mlog.Err(err))
			return err
		}
		return nil
	}
	b.UpdateInstantWithNextBackOff()
	return errBroadcastTaskIsNotDone
}

// splitAppendFirst divides the pending messages into the copies the broadcast
// header names as append-first and the rest. A copy that is already acked is
// not pending, so after it was persisted this returns no first copy.
func (b *pendingBroadcastTask) splitAppendFirst() (first []message.MutableMessage, rest []message.MutableMessage) {
	appendFirst := b.Header().AppendFirstVChannels
	if len(appendFirst) == 0 {
		return nil, b.pendingMessages
	}
	for _, msg := range b.pendingMessages {
		if slices.Contains(appendFirst, msg.VChannel()) {
			first = append(first, msg)
			continue
		}
		rest = append(rest, msg)
	}
	return first, rest
}

// applyAppendFirstBarrier makes every pending message take a time tick greater
// than the time tick of the append-first copies. It does nothing for a
// broadcast without an append-first vchannel.
func (b *pendingBroadcastTask) applyAppendFirstBarrier() {
	if b.barrierApplied {
		return
	}
	barrier := b.AppendFirstTimeTick()
	if barrier == 0 {
		return
	}
	for idx, msg := range b.pendingMessages {
		if msg.BarrierTimeTick() == 0 {
			b.pendingMessages[idx] = msg.WithBarrierTimeTick(barrier)
		}
	}
	b.barrierApplied = true
}

// appendGroup appends a group of messages. It returns the results of the
// messages that were appended and the messages that failed.
func (b *pendingBroadcastTask) appendGroup(ctx context.Context, msgs []message.MutableMessage) (map[string]*types.AppendResult, []message.MutableMessage) {
	resps := streaming.WAL().AppendMessages(ctx, msgs...)
	results := make(map[string]*types.AppendResult, len(msgs))
	failed := make([]message.MutableMessage, 0)
	for idx, resp := range resps.Responses {
		if resp.Error != nil {
			b.Logger().Warn(ctx, "broadcast task append message failed", mlog.FieldMessage(b.BroadcastMessage()), mlog.Int("idx", idx), mlog.Err(resp.Error))
			failed = append(failed, msgs[idx])
			continue
		}
		results[msgs[idx].VChannel()] = resp.AppendResult
	}
	return results, failed
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
