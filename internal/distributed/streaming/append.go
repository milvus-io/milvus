package streaming

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

// newAppendResponseN creates a new append response.
func newAppendResponseN(n int) AppendResponses {
	return AppendResponses{
		Responses: make([]AppendResponse, n),
	}
}

// AppendResponse is the response of one append operation.
type AppendResponse struct {
	MessageID message.MessageID
	Error     error
}

// AppendResponses is the response of append operation.
type AppendResponses struct {
	Responses []AppendResponse
}

// IsAnyError returns the first error in the responses.
func (a AppendResponses) IsAnyError() error {
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

// dispatchByPChannel dispatches the message into different pchannel.
func (w *walAccesserImpl) dispatchByPChannel(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	if len(msgs) == 0 {
		return newAppendResponseN(0)
	}

	// dispatch the messages into different pchannel.
	dispatchedMessages, indexes := w.dispatchMessages(msgs...)

	// only one pchannel, append it directly, no more goroutine needed.
	if len(dispatchedMessages) == 1 {
		for pchannel, msgs := range dispatchedMessages {
			return w.appendToPChannel(ctx, pchannel, msgs...)
		}
	}

	// otherwise, start multiple goroutine to append to different pchannel.
	resp := newAppendResponseN(len(msgs))
	wg := sync.WaitGroup{}
	wg.Add(len(dispatchedMessages))

	mu := sync.Mutex{}
	for pchannel, msgs := range dispatchedMessages {
		pchannel := pchannel
		msgs := msgs
		idxes := indexes[pchannel]
		w.appendExecutionPool.Submit(func() (struct{}, error) {
			defer wg.Done()
			singleResp := w.appendToPChannel(ctx, pchannel, msgs...)
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

// dispatchMessages dispatches the messages into different pchannel.
func (w *walAccesserImpl) dispatchMessages(msgs ...message.MutableMessage) (map[string][]message.MutableMessage, map[string][]int) {
	dispatchedMessages := make(map[string][]message.MutableMessage, 0)
	// record the index of the message in the msgs, used to fill back response.
	indexes := make(map[string][]int, 0)
	for idx, msg := range msgs {
		pchannel := funcutil.ToPhysicalChannel(msg.VChannel())
		if _, ok := dispatchedMessages[pchannel]; !ok {
			dispatchedMessages[pchannel] = make([]message.MutableMessage, 0)
			indexes[pchannel] = make([]int, 0)
		}
		dispatchedMessages[pchannel] = append(dispatchedMessages[pchannel], msg)
		indexes[pchannel] = append(indexes[pchannel], idx)
	}
	return dispatchedMessages, indexes
}

// appendToPChannel appends the messages to the specified pchannel.
func (w *walAccesserImpl) appendToPChannel(ctx context.Context, pchannel string, msgs ...message.MutableMessage) AppendResponses {
	if len(msgs) == 0 {
		return newAppendResponseN(0)
	}
	resp := newAppendResponseN(len(msgs))

	// get producer of pchannel.
	p := w.getProducer(pchannel)

	// if only one message here, append it directly, no more goroutine needed.
	// at most time, there's only one message here.
	// TODO: only the partition-key with high partition will generate many message in one time on the same pchannel,
	// we should optimize the message-format, make it into one; but not the goroutine count.
	if len(msgs) == 1 {
		msgID, err := p.Produce(ctx, msgs[0])
		resp.fillResponseAtIdx(AppendResponse{
			MessageID: msgID,
			Error:     err,
		}, 0)
		return resp
	}

	// concurrent produce here.
	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	mu := sync.Mutex{}
	for i, msg := range msgs {
		i := i
		msg := msg
		w.appendExecutionPool.Submit(func() (struct{}, error) {
			defer wg.Done()
			msgID, err := p.Produce(ctx, msg)

			mu.Lock()
			resp.fillResponseAtIdx(AppendResponse{
				MessageID: msgID,
				Error:     err,
			}, i)
			mu.Unlock()
			return struct{}{}, nil
		})
	}
	wg.Wait()
	return resp
}

// createOrGetProducer creates or get a producer.
// vchannel in same pchannel can share the same producer.
func (w *walAccesserImpl) getProducer(pchannel string) *producer.ResumableProducer {
	w.producerMutex.Lock()
	defer w.producerMutex.Unlock()

	// TODO: A idle producer should be removed maybe?
	if p, ok := w.producers[pchannel]; ok {
		return p
	}
	p := producer.NewResumableProducer(w.handlerClient.CreateProducer, &producer.ProducerOptions{
		PChannel: pchannel,
	})
	w.producers[pchannel] = p
	return p
}
