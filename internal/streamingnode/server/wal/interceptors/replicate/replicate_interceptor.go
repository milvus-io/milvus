package replicate

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const interceptorName = "replicate"

type replicateInterceptor struct {
	replicateManager replicates.ReplicatesManager
	txnManager       *txn.TxnManager
}

func (impl *replicateInterceptor) Name() string {
	return interceptorName
}

func (impl *replicateInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (msgID message.MessageID, err error) {
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		alterReplicateConfig := message.MustAsMutableAlterReplicateConfigMessageV2(msg)
		header := alterReplicateConfig.Header()

		// Check ignore field - if true, skip all processing and just append the message
		// This is used for incomplete switchover messages that should be ignored after force promote
		if header.Ignore {
			mlog.Info(ctx, "AlterReplicateConfig message has ignore flag set, skipping replicate mode switch and txn rollback")
			return appendOp(ctx, msg)
		}

		// A AlterReplicateConfig message is protected by wal level lock, so it's safe to switch replicate mode.
		// switch replicate mode if the message is put replicate config.
		if err := impl.replicateManager.SwitchReplicateMode(ctx, alterReplicateConfig); err != nil {
			return nil, err
		}

		// Append the message first
		msgID, err := appendOp(ctx, msg)
		if err != nil {
			return nil, err
		}

		// After successful append, check if this is a force promote - if so, rollback all in-flight transactions
		// This ensures atomicity: transactions are only rolled back after the config change is persisted in WAL
		if header.ForcePromote && impl.txnManager != nil {
			impl.txnManager.RollbackAllInFlightTransactions()
			mlog.Info(ctx, "Force promote replicate config and roll back all in-flight transactions successfully")
		}

		return msgID, nil
	}

	// Begin to replicate the message.
	acker, err := impl.replicateManager.BeginReplicateMessage(ctx, msg)
	if errors.Is(err, replicates.ErrNotHandledByReplicateManager) {
		// the message is not handled by replicate manager, write it into wal directly.
		return appendOp(ctx, msg)
	}
	if err != nil {
		return nil, err
	}

	var ackErr error
	ackOutcome := replicates.ReplicateAckerOutcomeNone
	defer func() { replicates.AckReplicateMessage(acker, ackErr, ackOutcome) }()
	msgID, err = appendOp(ctx, msg)
	ackErr = err
	if err == nil || msg.TxnContext() == nil ||
		msg.MessageType() == message.MessageTypeBeginTxn ||
		msg.MessageType() == message.MessageTypeCommitTxn ||
		!status.AsStreamingError(err).IsUnrecoverable() {
		return msgID, err
	}

	// Hold the manager lock while appending the rollback; clear transaction state
	// only after it is durable, or recovery would restore the failed transaction.
	// Omit the replicate header to leave the source checkpoint unchanged.
	ackOutcome = replicates.ReplicateAckerOutcomeAbortPending
	if impl.txnManager != nil {
		if session, sessionErr := impl.txnManager.GetSessionOfTxn(msg.TxnContext().TxnID); sessionErr == nil {
			// Block body/commit writes before timestamp allocation, which can fail.
			// Retries must finish the rollback even if the missing metadata is restored.
			if rollbackErr := session.RequestRollback(ctx, msg.TimeTick()); rollbackErr != nil && !status.AsStreamingError(rollbackErr).IsTxnUnavilable() {
				return nil, errors.CombineErrors(err, rollbackErr)
			}
		}
	}
	rollback := message.NewRollbackTxnMessageBuilderV2().
		WithVChannel(msg.VChannel()).
		WithUnreplicable().
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(*msg.TxnContext())
	if _, rollbackErr := appendOp(ctx, rollback); rollbackErr != nil {
		return nil, errors.CombineErrors(err, rollbackErr)
	}
	ackOutcome = replicates.ReplicateAckerOutcomeAborted
	return nil, err
}

func (impl *replicateInterceptor) Close() {
}
