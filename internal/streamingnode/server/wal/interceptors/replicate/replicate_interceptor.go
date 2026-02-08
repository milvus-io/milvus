package replicate

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const interceptorName = "replicate"

type replicateInterceptor struct {
	replicateManager replicates.ReplicateManager
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
			log.Ctx(ctx).Info("AlterReplicateConfig message has ignore flag set, skipping replicate mode switch and txn rollback")
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
			log.Ctx(ctx).Info("Force promote replicate config and roll back all in-flight transactions successfully")
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

	defer func() {
		acker.Ack(err)
	}()
	return appendOp(ctx, msg)
}

func (impl *replicateInterceptor) Close() {
}
