package utility

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// NewTxnBuffer creates a new txn buffer.
func NewTxnBuffer(logger *log.MLogger, metrics *metricsutil.ScannerMetrics) *TxnBuffer {
	return &TxnBuffer{
		logger:   logger,
		builders: make(map[message.TxnID]*message.ImmutableTxnMessageBuilder),
		metrics:  metrics,
	}
}

// TxnBuffer is a buffer for txn messages.
type TxnBuffer struct {
	logger   *log.MLogger
	builders map[message.TxnID]*message.ImmutableTxnMessageBuilder
	metrics  *metricsutil.ScannerMetrics
	bytes    int
}

func (b *TxnBuffer) Bytes() int {
	return b.bytes
}

// GetUncommittedMessageBuilder returns the uncommitted message builders.
func (b *TxnBuffer) GetUncommittedMessageBuilder() map[message.TxnID]*message.ImmutableTxnMessageBuilder {
	return b.builders
}

// HandleImmutableMessages handles immutable messages.
// The timetick of msgs should be in ascending order, and the timetick of all messages is less than or equal to ts.
// Hold the uncommitted txn messages until the commit or rollback message comes and pop the committed txn messages.
func (b *TxnBuffer) HandleImmutableMessages(msgs []message.ImmutableMessage, ts uint64) []message.ImmutableMessage {
	result := make([]message.ImmutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		// Not a txn message, can be consumed right now.
		if msg.TxnContext() == nil {
			b.metrics.ObserveAutoCommitTxn()
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
	b.bytes += beginMsg.EstimateSize()
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
			"txn id not exist, it may be a repeated committed message, so ignore it",
			zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
			zap.Any("messageID", commitMsg.MessageID()),
		)
		return nil
	}

	// build the txn message and remove it from buffer.
	b.bytes -= builder.EstimateSize()
	txnMsg, err := builder.Build(commitMsg)
	delete(b.builders, commitMsg.TxnContext().TxnID)
	if err != nil {
		b.metrics.ObserveErrorTxn()
		b.logger.Warn(
			"failed to build txn message, it's a critical error, some data is lost",
			zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
			zap.Any("messageID", commitMsg.MessageID()),
			zap.Error(err))
		return nil
	}
	b.logger.Debug(
		"the txn is committed",
		zap.Int64("txnID", int64(commitMsg.TxnContext().TxnID)),
		zap.Any("messageID", commitMsg.MessageID()),
	)
	b.metrics.ObserveTxn(message.TxnStateCommitted)
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
	if builder, ok := b.builders[rollbackMsg.TxnContext().TxnID]; ok {
		// just drop the txn from buffer.
		delete(b.builders, rollbackMsg.TxnContext().TxnID)
		b.bytes -= builder.EstimateSize()
		b.metrics.ObserveTxn(message.TxnStateRollbacked)
	}
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
	b.bytes += msg.EstimateSize()
}

// clearExpiredTxn clears the expired txn.
func (b *TxnBuffer) clearExpiredTxn(ts uint64) {
	for txnID, builder := range b.builders {
		if builder.ExpiredTimeTick() <= ts {
			delete(b.builders, txnID)
			b.bytes -= builder.EstimateSize()
			b.metrics.ObserveExpiredTxn()
			if b.logger.Level().Enabled(zap.DebugLevel) {
				b.logger.Debug(
					"the txn is expired, so drop the txn from buffer",
					zap.Int64("txnID", int64(txnID)),
					zap.Uint64("expiredTimeTick", builder.ExpiredTimeTick()),
					zap.Uint64("currentTimeTick", ts),
				)
			}
		}
	}
}
