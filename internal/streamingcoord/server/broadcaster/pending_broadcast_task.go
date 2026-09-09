package broadcaster

import (
	"context"
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
	*typeutil.BackoffWithInstant
}

// Execute reexecute the task, return nil if the task is done, otherwise not done.
// Execute can be repeated called until the task is done.
// Same semantics as the `Poll` operation in eventloop.
func (b *pendingBroadcastTask) Execute(ctx context.Context) error {
	ctx = message.ExtractTraceContext(ctx, b.msg)

	if err := b.InitializeRecovery(ctx); err != nil {
		b.Logger().Warn(ctx, "broadcast task initialize recovery failed", mlog.Err(err))
		return err
	}

	// The append-first group lands, and is persisted, before anything else. A
	// shard split puts its source vchannels here: the fence must be in the WAL
	// before any target replica takes its time tick.
	first, rest := b.splitAppendFirst()
	if len(first) > 0 {
		results, pending := b.appendGroup(ctx, first)
		if len(pending) > 0 {
			b.pendingMessages = append(pending, rest...)
			b.UpdateInstantWithNextBackOff()
			return errBroadcastTaskIsNotDone
		}
		if err := b.AckPartial(ctx, results); err != nil {
			b.Logger().Warn(ctx, "broadcast task persist the append-first group failed", mlog.Err(err))
			return err
		}
		b.Logger().Info(ctx, "broadcast task landed the append-first group", mlog.Int("count", len(results)))
		b.pendingMessages = rest
	}

	if len(b.pendingMessages) > 0 {
		b.Logger().Debug(ctx, "broadcast task is polling to make sent...", mlog.Int("pendingMessages", len(b.pendingMessages)))
		results, pending := b.appendGroup(ctx, b.pendingMessages)
		for vchannel, result := range results {
			b.appendResult[vchannel] = result
		}
		b.pendingMessages = pending
		b.Logger().Info(ctx, "broadcast task make a new broadcast done", mlog.Int("backoffRetryMessages", len(b.pendingMessages)))
	}
	if len(b.pendingMessages) == 0 {
		// trigger a fast ack operation when the broadcast operation is done.
		if err := b.FastAck(ctx, b.appendResult); err != nil {
			b.Logger().Warn(ctx, "broadcast task save task failed", mlog.Err(err))
			return err
		}
		return nil
	}
	b.UpdateInstantWithNextBackOff()
	return errBroadcastTaskIsNotDone
}

// splitAppendFirst partitions the still-pending replicas into the append-first
// group named by the broadcast header and the rest. Already-acked replicas are
// not pending, so after a restart that persisted the first group this returns
// an empty first group and the task proceeds to the rest.
func (b *pendingBroadcastTask) splitAppendFirst() (first []message.MutableMessage, rest []message.MutableMessage) {
	appendFirst := typeutil.NewSet(b.header().AppendFirstVChannels...)
	for _, msg := range b.pendingMessages {
		if appendFirst.Contain(msg.VChannel()) {
			first = append(first, msg)
		} else {
			rest = append(rest, msg)
		}
	}
	return first, rest
}

// appendGroup appends one group of replicas and returns the results of the ones
// that landed and the ones to retry.
func (b *pendingBroadcastTask) appendGroup(ctx context.Context, msgs []message.MutableMessage) (map[string]*types.AppendResult, []message.MutableMessage) {
	resps := streaming.WAL().AppendMessages(ctx, msgs...)
	results := make(map[string]*types.AppendResult, len(msgs))
	pending := make([]message.MutableMessage, 0)
	for idx, resp := range resps.Responses {
		if resp.Error != nil {
			b.Logger().Warn(ctx, "broadcast task append message failed", mlog.Int("idx", idx), mlog.FieldVChannel(msgs[idx].VChannel()), mlog.Err(resp.Error))
			pending = append(pending, msgs[idx])
			continue
		}
		results[msgs[idx].VChannel()] = resp.AppendResult
	}
	return results, pending
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
