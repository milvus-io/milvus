package broadcaster

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
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

	// The append-first replica lands, and is persisted, before anything else. A
	// shard split puts its source vchannel here: the fence must be in the WAL
	// before any target replica takes its time tick.
	first, rest := b.splitAppendFirst()
	if first != nil {
		results, pending := b.appendGroup(ctx, []message.MutableMessage{first})
		if len(pending) > 0 {
			// Nothing landed, so there is nothing to persist: retry the same
			// replica ahead of the rest.
			b.UpdateInstantWithNextBackOff()
			return errBroadcastTaskIsNotDone
		}
		if err := b.AckPartial(ctx, results); err != nil {
			b.Logger().Warn(ctx, "broadcast task persist the append-first replica failed", mlog.Err(err))
			return err
		}
		b.Logger().Info(ctx, "broadcast task landed the append-first replica", mlog.FieldVChannel(first.VChannel()))
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
// replica named by the broadcast header and the rest. Already-acked replicas are
// not pending, so after a restart that persisted the first replica this returns
// nil and the task proceeds to the rest.
//
// A header names at most one append-first vchannel (OptBuildBroadcastAppendFirst
// takes exactly one); a header naming more is an invariant violation, not a
// shape to persist partially.
//
// A SECONDARY cluster's liveness depends on how Execute uses this split, not
// merely on the tick ordering it gives this cluster: the append-first replica
// must be appended (and AckPartial-persisted) BEFORE the rest is appended, so
// that its tick is strictly below every other replica's. That is
// premise (a) of the append gate's progress argument in
// internal/distributed/streaming/replicate_service.go
// (waitAppendFirstReplicas). Appending the rest concurrently with this replica
// would keep every test in this package green and wedge a secondary's replicate
// streams.
func (b *pendingBroadcastTask) splitAppendFirst() (first message.MutableMessage, rest []message.MutableMessage) {
	appendFirst := b.header().AppendFirstVChannels
	if len(appendFirst) > 1 {
		panic("broadcast task invariant violated: a broadcast names at most one append-first vchannel")
	}
	if len(appendFirst) == 0 {
		return nil, b.pendingMessages
	}
	rest = make([]message.MutableMessage, 0, len(b.pendingMessages))
	for _, msg := range b.pendingMessages {
		if msg.VChannel() == appendFirst[0] {
			first = msg
			continue
		}
		rest = append(rest, msg)
	}
	return first, rest
}

// appendGroup appends one group of replicas and returns the results of the ones
// that landed and the ones to retry.
func (b *pendingBroadcastTask) appendGroup(ctx context.Context, msgs []message.MutableMessage) (map[string]*types.AppendResult, []message.MutableMessage) {
	resps := streaming.WAL().AppendMessages(ctx, msgs...)
	results := make(map[string]*types.AppendResult, len(msgs))
	pending := make([]message.MutableMessage, 0)
	for idx, msg := range msgs {
		// AppendMessages answers one response per message, in order. A message
		// without one did not land as far as this task can tell, so it is retried
		// rather than trusted.
		if idx >= len(resps.Responses) {
			b.Logger().Warn(ctx, "broadcast task append message got no response", mlog.Int("idx", idx), mlog.FieldVChannel(msg.VChannel()))
			pending = append(pending, msg)
			continue
		}
		resp := resps.Responses[idx]
		if resp.Error != nil {
			b.observeAppendFailure(ctx, idx, msg, resp.Error)
			pending = append(pending, msg)
			continue
		}
		results[msg.VChannel()] = resp.AppendResult
	}
	return results, pending
}

// observeAppendFailure logs a refused replica append and, when the refusal is
// unrecoverable, counts it.
//
// Every append error is retried the same way -- with backoff, forever, the task
// holding its resource keys throughout -- because the broadcaster has no
// terminal state for a persisted task (that is a framework change, recorded as
// a follow-up). An unrecoverable code (status.StreamingError.IsUnrecoverable:
// UNRECOVERABLE, SHARD_FENCED, INVALID_ARGUMENT, ...) says the StreamingNode will
// refuse the same replica again no matter how often it is asked: a vchannel
// fenced by another split, a target conflicting with the collection's shard on
// its pchannel, an unknown split role. Such a task never clears by itself, so
// it is named as such in the log and counted separately, instead of being
// indistinguishable from a replica whose node is merely slow to come back.
func (b *pendingBroadcastTask) observeAppendFailure(ctx context.Context, idx int, msg message.MutableMessage, err error) {
	streamingErr := status.AsStreamingError(err)
	if streamingErr == nil || !streamingErr.IsUnrecoverable() {
		b.Logger().Warn(ctx, "broadcast task append message failed", mlog.Int("idx", idx), mlog.FieldVChannel(msg.VChannel()), mlog.Err(err))
		return
	}
	b.ObserveAppendUnrecoverable(streamingErr.Code)
	b.Logger().Warn(ctx, "broadcast task append message refused as unrecoverable; the task keeps retrying it "+
		"and holds its resource keys until the refusal clears or the task is repaired",
		mlog.Int("idx", idx),
		mlog.FieldVChannel(msg.VChannel()),
		mlog.String("messageType", msg.MessageType().String()),
		mlog.String("streamingCode", streamingErr.Code.String()),
		mlog.Err(err))
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
