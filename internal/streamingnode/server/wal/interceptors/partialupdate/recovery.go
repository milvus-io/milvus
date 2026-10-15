package partialupdate

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const recoveryPKDecodeTimeout = 5 * time.Second

// recoverTransactions restores write sets from the same durable prefix used by
// TxnManager. It never changes the session's lease, re-appends data, or acquires
// row locks. An unsupported body invalidates only that transaction's write set,
// not WAL startup or the recovery of already committed data.
func (i *appendInterceptor) recoverTransactions(param *interceptors.InterceptorBuildParam) {
	if param.InitialRecoverSnapshot == nil || param.InitialRecoverSnapshot.TxnBuffer == nil || param.TxnManager == nil {
		return
	}
	ctx := param.Context
	if ctx == nil {
		ctx = context.Background()
	}
	// Payload decryption retries transient KMS failures. PK reconstruction is
	// optional for startup: bound that work and let only affected transactions
	// replay, retaining their durable bodies and original lease unchanged.
	ctx, cancel := context.WithTimeout(ctx, recoveryPKDecodeTimeout)
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
		for _, body := range bodies {
			if err := i.recoverBody(ctx, txnID, body); err != nil {
				// The commit is rejected either way; stop decoding so that
				// the shared budget is left for the other transactions.
				i.state.txnMu.Lock()
				i.state.txnLocked(txnID).recoveryIncomplete = true
				i.state.txnMu.Unlock()
				break
			}
		}
		i.state.txnMu.Lock()
		txnState := i.state.txnLocked(txnID)
		txnState.observedBegin = !txnState.recoveryIncomplete
		i.state.txnMu.Unlock()
		if session, err := param.TxnManager.GetSessionOfTxn(txnID); err == nil {
			// Registering at the recovered activity timestamp does not renew
			// the lease or reject a session merely because startup took time.
			i.state.registerTxnCleanup(session, lastBodyTS)
		} else {
			i.state.deleteTxn(txnID)
		}
	}
}

func (i *appendInterceptor) recoverBody(ctx context.Context, txnID message.TxnID, msg message.ImmutableMessage) error {
	var pks primarykey.Keys
	var fenceCollectionID int64
	var err error
	if message.HasPartialUpdateCAS(msg) {
		var scope casInsertScope
		var encoded string
		pks, scope, encoded, err = extractPKsFromCASInsertWithContext(ctx, msg, i.pkDescriptorGetter)
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
	} else {
		set, err := i.extractOrdinaryWriteSet(ctx, msg)
		if err != nil {
			return err
		}
		pks = set.pks
		fenceCollectionID = set.fenceCollectionID
	}
	i.state.recordTxnWritesTyped(txnID, pks)
	i.state.recordTxnFence(txnID, fenceCollectionID)
	return nil
}
