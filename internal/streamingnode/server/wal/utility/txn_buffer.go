package utility

import (
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// NewTxnBuffer creates a new txn buffer.
func NewTxnBuffer(logger *log.MLogger) *TxnBuffer {
	return &TxnBuffer{
		logger:            logger,
		builders:          make(map[message.TxnID]*message.ImmutableTxnMessageBuilder),
		notExpiredTxnHeap: typeutil.NewHeap[*message.TxnContext](&txnContextsOrderByExpired{}),
	}
}

// TxnBuffer is a buffer for txn messages.
type TxnBuffer struct {
	logger            *log.MLogger
	builders          map[message.TxnID]*message.ImmutableTxnMessageBuilder
	notExpiredTxnHeap typeutil.Heap[*message.TxnContext]
}

// HandleImmutableMessages handles immutable messages.
// The timetick of msgs should be in ascending order, and the timetick of all messages is less than or equal to ts.
// Hold the uncommited txn messages until the commit or rollback message comes and pop the commited txn messages.
func (b *TxnBuffer) HandleImmutableMessages(msgs []message.ImmutableMessage, ts uint64) []message.ImmutableMessage {
	if len(msgs) == 0 {
		return msgs
	}
	result := make([]message.ImmutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		// Not a txn message, can be consumed right now.
		if msg.TxnContext() == nil {
			result = append(result, msg)
			continue
		}
		switch msg.MessageType() {
		case message.MessageTypeBeginTxn:
			b.handleBeginTxn(msg)
		case message.MessageTypeCommitTxn:
			if newTxnMsg := b.handleCommitTxn(msg); newTxnMsg != nil {
				result = append(result, newTxnMsg)
			}
		case message.MessageTypeRollbackTxn:
			b.handleRollbackTxn(msg)
		default:
			b.handleTxnBodyMessage(msg)
		}
	}
	b.clearExpiredTxn(ts)
	return result
}

// handleBeginTxn handles begin txn message.
func (b *TxnBuffer) handleBeginTxn(msg message.ImmutableMessage) {
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(msg)
	if err != nil {
		b.logger.DPanic(
			"failed to convert message to begin txn message, it's a critical error",
			zap.Int64("txnID", int64(beginMsg.TxnContext().TxnID)),
			zap.Any("messageID", beginMsg.MessageID()),
			zap.Error(err))
		return
	}
	if _, ok := b.builders[beginMsg.TxnContext().TxnID]; ok {
		b.logger.Warn(
			"txn id already exist, so ignore the repeated begin txn message",
			zap.Int64("txnID", int64(beginMsg.TxnContext().TxnID)),
			zap.Any("messageID", beginMsg.MessageID()),
		)
		return
	}
	b.builders[beginMsg.TxnContext().TxnID] = message.NewImmutableTxnMessageBuilder(beginMsg)
	b.notExpiredTxnHeap.Push(beginMsg.TxnContext())
}

// handleCommitTxn handles commit txn message.
func (b *TxnBuffer) handleCommitTxn(msg message.ImmutableMessage) message.ImmutableMessage {
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(msg)
	if err != nil {
		b.logger.DPanic(
			"failed to convert message to commit txn message, it's a critical error",
			zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
			zap.Any("messageID", commitMsg.MessageID()),
			zap.Error(err))
		return nil
	}
	builder, ok := b.builders[commitMsg.TxnContext().TxnID]
	if !ok {
		b.logger.Warn(
			"txn id not exist, it may be a repeated commited message, so ignore it",
			zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
			zap.Any("messageID", commitMsg.MessageID()),
		)
		return nil
	}

	// build the txn message and remove it from buffer.
	txnMsg, err := builder.Build(commitMsg)
	delete(b.builders, commitMsg.TxnContext().TxnID)
	if err != nil {
		b.logger.Warn(
			"failed to build txn message, it's a critical error, some data is lost",
			zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
			zap.Any("messageID", commitMsg.MessageID()),
			zap.Error(err))
		return nil
	}
	b.logger.Debug(
		"the txn is commited",
		zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
		zap.Any("messageID", commitMsg.MessageID()),
	)
	return txnMsg
}

// handleRollbackTxn handles rollback txn message.
func (b *TxnBuffer) handleRollbackTxn(msg message.ImmutableMessage) {
	rollbackMsg, err := message.AsImmutableRollbackTxnMessageV2(msg)
	if err != nil {
		b.logger.DPanic(
			"failed to convert message to rollback txn message, it's a critical error",
			zap.Int64("txnID", int64(rollbackMsg.TxnContext().TxnID)),
			zap.Any("messageID", rollbackMsg.MessageID()),
			zap.Error(err))
		return
	}
	b.logger.Debug(
		"the txn is rollback, so drop the txn from buffer",
		zap.Int64("txnID", int64(rollbackMsg.TxnContext().TxnID)),
		zap.Any("messageID", rollbackMsg.MessageID()),
	)
	// just drop the txn from buffer.
	delete(b.builders, rollbackMsg.TxnContext().TxnID)
}

// handleTxnBodyMessage handles txn body message.
func (b *TxnBuffer) handleTxnBodyMessage(msg message.ImmutableMessage) {
	builder, ok := b.builders[msg.TxnContext().TxnID]
	if !ok {
		b.logger.Warn(
			"txn id not exist, so ignore the body message",
			zap.Int64("txnID", int64(msg.TxnContext().TxnID)),
			zap.Any("messageID", msg.MessageID()),
		)
		return
	}
	builder.Add(msg)
}

// clearExpiredTxn clears the expired txn.
func (b *TxnBuffer) clearExpiredTxn(ts uint64) {
	for b.notExpiredTxnHeap.Len() > 0 && b.notExpiredTxnHeap.Peek().ExpiredTimeTick() <= ts {
		txnContext := b.notExpiredTxnHeap.Pop()
		if _, ok := b.builders[txnContext.TxnID]; ok {
			delete(b.builders, txnContext.TxnID)
			b.logger.Debug(
				"the txn is expired, so drop the txn from buffer",
				zap.Int64("txnID", int64(txnContext.TxnID)),
				zap.Uint64("expiredTimeTick", txnContext.ExpiredTimeTick()),
				zap.Uint64("currentTimeTick", ts),
			)
		}
	}
}

// txnContextsOrderByExpired is the heap array of the txnSession.
type txnContextsOrderByExpired []*message.TxnContext

func (h txnContextsOrderByExpired) Len() int {
	return len(h)
}

func (h txnContextsOrderByExpired) Less(i, j int) bool {
	return h[i].ExpiredTimeTick() < h[j].ExpiredTimeTick()
}

func (h txnContextsOrderByExpired) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *txnContextsOrderByExpired) Push(x interface{}) {
	*h = append(*h, x.(*message.TxnContext))
}

// Pop pop the last one at len.
func (h *txnContextsOrderByExpired) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *txnContextsOrderByExpired) Peek() interface{} {
	return (*h)[0]
}
