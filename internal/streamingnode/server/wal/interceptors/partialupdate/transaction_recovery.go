package partialupdate

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// recoverTransactions reconstructs the write set of the durable body prefix.
// Runtime body appends extend that prefix before CommitTxn freezes and drains
// the session. No row lock, lease, committed version, or WAL record is restored.
func (i *appendInterceptor) recoverTransactions(param *interceptors.InterceptorBuildParam) {
	if param.InitialRecoverSnapshot == nil || param.InitialRecoverSnapshot.TxnBuffer == nil || param.TxnManager == nil {
		return
	}
	ctx := param.Context
	if ctx == nil {
		ctx = context.Background()
	}
	// KMS may retry an invalid key indefinitely. Bound this optional write-set
	// reconstruction by streaming.partialUpdate.recoveryDecodeTimeout and respect
	// WAL-open cancellation; a failed transaction can use the existing full replay
	// without dropping any durable body. A non-positive timeout expires the
	// context immediately.
	timeout := paramtable.Get().StreamingCfg.PartialUpdateRecoveryDecodeTimeout.GetAsDurationByParse()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for txnID, builder := range param.InitialRecoverSnapshot.TxnBuffer.GetUncommittedMessageBuilder() {
		begin, bodies := builder.Messages()
		i.state.txnMu.Lock()
		i.state.txnLocked(txnID).recovered = true
		i.state.txnMu.Unlock()

		lastBodyTS := begin.TimeTick()
		for _, body := range bodies {
			if body.TimeTick() > lastBodyTS {
				lastBodyTS = body.TimeTick()
			}
		}
		complete := true
		for _, body := range bodies {
			if err := i.recoverTxnBody(ctx, txnID, body); err != nil {
				// A single unsupported or malformed recovered write set must not
				// prevent the WAL from opening. Its CommitTxn requests a full retry.
				complete = false
				break
			}
		}
		if complete {
			i.state.recordTxnBegin(txnID)
		}
		// RegisterCleanup panics on an expired or finished session. The manager
		// restores the lease from the same durable bodies, so this only guards
		// against a lease that ended before its last durable body.
		if session, err := param.TxnManager.GetSessionOfTxn(txnID); err == nil && !session.IsExpiredOrDone(lastBodyTS) {
			// The manager already restored the original keepalive. Registering
			// cleanup at its last durable body TS never extends that lease.
			i.state.registerTxnCleanup(session, lastBodyTS)
		} else {
			i.state.deleteTxn(txnID)
		}
	}
}

func (i *appendInterceptor) recoverTxnBody(ctx context.Context, txnID message.TxnID, body message.ImmutableMessage) error {
	if message.HasPartialUpdateCAS(body) {
		pks, scope, encoded, err := extractPKsFromCASInsertWithContext(ctx, body, i.pkDescriptorGetter)
		if err != nil {
			return err
		}
		meta, err := message.DecodePartialUpdateCASMetadata(encoded)
		if err != nil {
			return err
		}
		if err := i.state.recordTxnCAS(txnID, meta, scope); err != nil {
			return err
		}
		i.state.recordTxnWritesTyped(txnID, pks)
		return nil
	}
	set, err := i.extractOrdinaryWriteSet(ctx, body)
	if err != nil {
		return err
	}
	i.state.recordTxnWritesTyped(txnID, set.pks)
	return nil
}
