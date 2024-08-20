package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// AppendMessagesToWAL appends messages to the wal.
// It it a helper utility function to append messages to the wal.
// If the messages is belong to one vchannel, it will be sent as a transaction.
// Otherwise, it will be sent as individual messages.
// !!! This function do not promise the atomicity and deliver order of the messages appending.
func (u *walAccesserImpl) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	assertNoSystemMessage(msgs...)

	// dispatch the messages into different vchannel.
	dispatchedMessages, indexes := u.dispatchMessages(msgs...)

	// If only one vchannel, append it directly without other goroutine.
	if len(dispatchedMessages) == 1 {
		return u.appendToVChannel(ctx, msgs[0].VChannel(), msgs...)
	}

	// Otherwise append the messages concurrently.
	mu := &sync.Mutex{}
	resp := newAppendResponseN(len(msgs))

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
				resp.fillResponseAtIdx(singleResp.Responses[i], idx)
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
		return newAppendResponseN(0)
	}
	resp := newAppendResponseN(len(msgs))

	// if only one message here, append it directly, no more goroutine needed.
	// at most time, there's only one message here.
	// TODO: only the partition-key with high partition will generate many message in one time on the same pchannel,
	// we should optimize the message-format, make it into one; but not the goroutine count.
	if len(msgs) == 1 {
		appendResult, err := u.appendToWAL(ctx, msgs[0])
		resp.fillResponseAtIdx(AppendResponse{
			AppendResult: appendResult,
			Error:        err,
		}, 0)
		return resp
	}

	// Otherwise, we start a transaction to append the messages.
	// The transaction will be committed when all messages are appended.
	txn, err := u.Txn(ctx, TxnOption{
		VChannel:  vchannel,
		Keepalive: 5 * time.Second,
	})
	if err != nil {
		resp.fillAllError(err)
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
				resp.fillResponseAtIdx(AppendResponse{
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
		resp.fillAllError(err)
		return resp
	}

	// commit the transaction and fill the response.
	appendResult, err := txn.Commit(ctx)
	resp.fillAllResponse(AppendResponse{
		AppendResult: appendResult,
		Error:        err,
	})
	return resp
}

// newAppendResponseN creates a new append response.
func newAppendResponseN(n int) AppendResponses {
	return AppendResponses{
		Responses: make([]AppendResponse, n),
	}
}

// AppendResponse is the response of one append operation.
type AppendResponse struct {
	AppendResult *types.AppendResult
	Error        error
}

// AppendResponses is the response of append operation.
type AppendResponses struct {
	Responses []AppendResponse
}

func (a AppendResponses) MaxTimeTick() uint64 {
	var maxTimeTick uint64
	for _, r := range a.Responses {
		if r.AppendResult != nil && r.AppendResult.TimeTick > maxTimeTick {
			maxTimeTick = r.AppendResult.TimeTick
		}
	}
	return maxTimeTick
}

// UnwrapFirstError returns the first error in the responses.
func (a AppendResponses) UnwrapFirstError() error {
	for _, r := range a.Responses {
		if r.Error != nil {
			return r.Error
		}
	}
	return nil
}

// fillAllError fills all the responses with the same error.
func (a *AppendResponses) fillAllError(err error) {
	for i := range a.Responses {
		a.Responses[i].Error = err
	}
}

// fillResponseAtIdx fill the response at idx
func (a *AppendResponses) fillResponseAtIdx(resp AppendResponse, idx int) {
	a.Responses[idx] = resp
}

func (a *AppendResponses) fillAllResponse(resp AppendResponse) {
	for i := range a.Responses {
		a.Responses[i] = resp
	}
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
