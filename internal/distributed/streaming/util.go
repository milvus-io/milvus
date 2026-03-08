package streaming

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type (
	AppendResponses = types.AppendResponses
	AppendResponse  = types.AppendResponse
)

// AppendMessagesToWAL appends messages to the wal.
// It it a helper utility function to append messages to the wal.
// If the messages is belong to one vchannel, it will be sent as a transaction.
// Otherwise, it will be sent as individual messages.
// !!! This function do not promise the atomicity and deliver order of the messages appending.
func (u *walAccesserImpl) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	assertValidMessage(msgs...)

	// dispatch the messages into different vchannel.
	dispatchedMessages, indexes := u.dispatchMessages(msgs...)

	// If only one vchannel, append it directly without other goroutine.
	if len(dispatchedMessages) == 1 {
		return u.appendToVChannel(ctx, msgs[0].VChannel(), msgs...)
	}

	// Otherwise append the messages concurrently.
	mu := &sync.Mutex{}
	resp := types.NewAppendResponseN(len(msgs))

	wg := &sync.WaitGroup{}
	wg.Add(len(dispatchedMessages))
	for vchannel, msgs := range dispatchedMessages {
		vchannel := vchannel
		msgs := msgs
		idxes := indexes[vchannel]
		u.dispatchExecutionPool.Submit(func() (struct{}, error) {
			defer wg.Done()
			singleResp := u.appendToVChannel(ctx, vchannel, msgs...)
			mu.Lock()
			for i, idx := range idxes {
				resp.FillResponseAtIdx(singleResp.Responses[i], idx)
			}
			mu.Unlock()
			return struct{}{}, nil
		})
	}
	wg.Wait()
	return resp
}

// AppendMessagesWithOption appends messages to the wal with the given option.
func (u *walAccesserImpl) AppendMessagesWithOption(ctx context.Context, opts AppendOption, msgs ...message.MutableMessage) AppendResponses {
	for _, msg := range msgs {
		applyOpt(msg, opts)
	}
	return u.AppendMessages(ctx, msgs...)
}

// dispatchMessages dispatches the messages into different vchannel.
func (u *walAccesserImpl) dispatchMessages(msgs ...message.MutableMessage) (map[string][]message.MutableMessage, map[string][]int) {
	dispatchedMessages := make(map[string][]message.MutableMessage, 0)
	indexes := make(map[string][]int, 0)
	for idx, msg := range msgs {
		vchannel := msg.VChannel()
		if _, ok := dispatchedMessages[vchannel]; !ok {
			dispatchedMessages[vchannel] = make([]message.MutableMessage, 0)
			indexes[vchannel] = make([]int, 0)
		}
		dispatchedMessages[vchannel] = append(dispatchedMessages[vchannel], msg)
		indexes[vchannel] = append(indexes[vchannel], idx)
	}
	return dispatchedMessages, indexes
}

// appendToVChannel appends the messages to the specified vchannel.
func (u *walAccesserImpl) appendToVChannel(ctx context.Context, vchannel string, msgs ...message.MutableMessage) AppendResponses {
	if len(msgs) == 0 {
		return types.NewAppendResponseN(0)
	}
	// if only one message here, append it directly, no more goroutine needed.
	// at most time, there's only one message here.
	// TODO: only the partition-key with high partition will generate many message in one time on the same pchannel,
	// we should optimize the message-format, make it into one; but not the goroutine count.
	if len(msgs) == 1 {
		resp := types.NewAppendResponseN(1)
		appendResult, err := u.appendToWAL(ctx, msgs[0])
		resp.FillResponseAtIdx(AppendResponse{
			AppendResult: appendResult,
			Error:        err,
		}, 0)
		return resp
	}

	for {
		if ctx.Err() != nil {
			resp := types.NewAppendResponseN(len(msgs))
			resp.FillAllError(ctx.Err())
			return resp
		}
		resp := u.appendWithTxn(ctx, vchannel, msgs...)
		if err := status.AsStreamingError(resp.UnwrapFirstError()); err != nil && err.IsTxnExpired() {
			// if the transaction is expired,
			// there may be wal is transferred to another streaming node,
			// retry it with new transaction.
			u.Logger().Warn("transaction expired, retrying", zap.String("vchannel", vchannel), zap.Error(err))
			continue
		}
		return resp
	}
}

// appendWithTxn appends the messages to the wal with a transaction.
func (u *walAccesserImpl) appendWithTxn(ctx context.Context, vchannel string, msgs ...message.MutableMessage) AppendResponses {
	resp := types.NewAppendResponseN(len(msgs))

	// Otherwise, we start a transaction to append the messages.
	// The transaction will be committed when all messages are appended.
	txn, err := u.Txn(ctx, TxnOption{
		VChannel: vchannel,
	})
	if err != nil {
		resp.FillAllError(err)
		return resp
	}

	// concurrent produce here.
	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	mu := sync.Mutex{}
	for i, msg := range msgs {
		i := i
		msg := msg
		u.appendExecutionPool.Submit(func() (struct{}, error) {
			defer wg.Done()
			if err := txn.Append(ctx, msg); err != nil {
				mu.Lock()
				resp.FillResponseAtIdx(AppendResponse{
					Error: err,
				}, i)
				mu.Unlock()
			}
			return struct{}{}, nil
		})
	}
	wg.Wait()

	// if there's any error, rollback the transaction.
	// and fill the error with the first error.
	if err := resp.UnwrapFirstError(); err != nil {
		_ = txn.Rollback(ctx) // rollback failure can be ignored.
		resp.FillAllError(err)
		return resp
	}

	// commit the transaction and fill the response.
	appendResult, err := txn.Commit(ctx)
	resp.FillAllResponse(AppendResponse{
		AppendResult: appendResult,
		Error:        err,
	})
	return resp
}

// applyOpt applies the append options to the message.
func applyOpt(msg message.MutableMessage, opts ...AppendOption) message.MutableMessage {
	if len(opts) == 0 {
		return msg
	}
	if opts[0].BarrierTimeTick > 0 {
		msg = msg.WithBarrierTimeTick(opts[0].BarrierTimeTick)
	}
	return msg
}
