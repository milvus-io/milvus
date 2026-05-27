package streaming

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
func (w *walAccesserImpl) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	assertValidMessage(msgs...)

	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		err := types.NewAppendResponseN(len(msgs))
		err.FillAllError(ErrWALAccesserClosed)
		return err
	}
	defer w.lifetime.Done()

	// dispatch the messages into different vchannel.
	dispatchedMessages, indexes := w.dispatchMessages(msgs...)

	// Use a slice to maintain the order of vchannels and their corresponding indexes.
	type vchannelTask struct {
		vchannel string
		indexes  []int
	}
	type batchTask struct {
		indexes []int
		respCh  <-chan types.AppendResponse
	}
	type pendingBatchTask struct {
		vchannel string
		indexes  []int
		msgs     []message.MutableMessage
	}
	tasks := make([]vchannelTask, 0, len(dispatchedMessages))
	pendingBatchTasks := make([]pendingBatchTask, 0, len(dispatchedMessages))
	batchTasks := make([]batchTask, 0, len(dispatchedMessages))
	guards := make([]*producer.ProduceGuard, 0, len(dispatchedMessages))
	resp := types.NewAppendResponseN(len(msgs))
	shouldBatch := w.shouldBatchAppendMessages(dispatchedMessages)
	for vchannel, vchannelMsgs := range dispatchedMessages {
		if shouldBatch {
			pendingBatchTasks = append(pendingBatchTasks, pendingBatchTask{
				vchannel: vchannel,
				indexes:  indexes[vchannel],
				msgs:     vchannelMsgs,
			})
			continue
		}
		g, err := w.getProducer(vchannel).BeginProduce(ctx, vchannelMsgs...)
		if err != nil {
			for _, guard := range guards {
				guard.Cancel()
			}
			resp.FillAllError(err)
			return resp
		}
		guards = append(guards, g)
		tasks = append(tasks, vchannelTask{
			vchannel: vchannel,
			indexes:  indexes[vchannel],
		})
	}

	for _, task := range pendingBatchTasks {
		batchTasks = append(batchTasks, batchTask{
			indexes: task.indexes,
			respCh:  w.getAppendBatcher(task.vchannel).submit(ctx, task.msgs...),
		})
	}

	// Batch commit and get responses per vchannel.
	guardResps := producer.BatchCommitProduce(ctx, guards...)

	// Map the responses back to the original order using indexes.
	for i, task := range tasks {
		guardResp := guardResps.Responses[i]
		for _, origIdx := range task.indexes {
			resp.FillResponseAtIdx(guardResp, origIdx)
		}
	}

	for _, task := range batchTasks {
		var batchResp types.AppendResponse
		select {
		case batchResp = <-task.respCh:
		case <-ctx.Done():
			batchResp = types.AppendResponse{Error: ctx.Err()}
		}
		for _, origIdx := range task.indexes {
			resp.FillResponseAtIdx(batchResp, origIdx)
		}
	}

	return resp
}

func (w *walAccesserImpl) shouldBatchAppendMessages(dispatchedMessages map[string][]message.MutableMessage) bool {
	if !w.appendBatchConfig.enabled() {
		return false
	}
	if len(dispatchedMessages) == 0 {
		return false
	}
	for vchannel, msgs := range dispatchedMessages {
		if !w.shouldBatchAppendVChannelMessages(vchannel, msgs...) {
			return false
		}
	}
	return true
}

func (w *walAccesserImpl) shouldBatchAppendVChannelMessages(vchannel string, msgs ...message.MutableMessage) bool {
	if w.getAppendBatcher(vchannel).inCooldown(time.Now()) {
		return false
	}
	for _, msg := range msgs {
		if !msg.MessageType().IsDMLMessageType() {
			return false
		}
	}
	return len(msgs) > 0 && messagesEstimateSize(msgs...) < w.appendBatchConfig.SmallMessageThreshold
}

func (w *walAccesserImpl) appendVChannelMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponse {
	guard, err := w.getProducer(msgs[0].VChannel()).BeginProduce(ctx, msgs...)
	if err != nil {
		return types.AppendResponse{Error: err}
	}
	resp := producer.BatchCommitProduce(ctx, guard)
	return resp.Responses[0]
}

func (w *walAccesserImpl) getAppendBatcher(vchannel string) *appendBatcher {
	w.appendBatcherMutex.Lock()
	defer w.appendBatcherMutex.Unlock()

	if w.appendBatchers == nil {
		w.appendBatchers = make(map[string]*appendBatcher)
	}
	if batcher, ok := w.appendBatchers[vchannel]; ok {
		return batcher
	}
	batcher := newAppendBatcher(w.appendBatchConfig, w.appendVChannelMessages)
	w.appendBatchers[vchannel] = batcher
	return batcher
}

func (w *walAccesserImpl) closeAppendBatchers() {
	w.appendBatcherMutex.Lock()
	batchers := make([]*appendBatcher, 0, len(w.appendBatchers))
	for _, batcher := range w.appendBatchers {
		batchers = append(batchers, batcher)
	}
	w.appendBatchers = make(map[string]*appendBatcher)
	w.appendBatcherMutex.Unlock()

	for _, batcher := range batchers {
		batcher.close()
	}
}

func (w *walAccesserImpl) appendReplicateMessageToWAL(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
	guard, err := w.getProducer(msg.VChannel()).BeginProduce(ctx, msg)
	if err != nil {
		return nil, err
	}
	resp := producer.BatchCommitProduce(ctx, guard)
	return resp.Responses[0].AppendResult, resp.Responses[0].Error
}

// dispatchMessages dispatches the messages into different vchannel.
func (w *walAccesserImpl) dispatchMessages(msgs ...message.MutableMessage) (map[string][]message.MutableMessage, map[string][]int) {
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
