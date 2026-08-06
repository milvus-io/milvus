package partialupdate

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const interceptorName = "partialupdate"

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

type interceptorBuilder struct{}

type primaryKeyDescriptorGetter interface {
	GetPrimaryKeyDescriptor(collectionID int64, schemaVersion int32) (shards.PrimaryKeyDescriptor, error)
}

// NewInterceptorBuilder creates one per-WAL partial-update write tracker.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	state := newPartialUpdateState(defaultVersionIndexTTL, defaultVersionIndexBudgetBytes)
	state.channel = param.ChannelInfo
	pkDescriptorGetter, _ := param.ShardManager.(primaryKeyDescriptorGetter)
	return &appendInterceptor{
		state:              state,
		pkDescriptorGetter: pkDescriptorGetter,
	}
}

type appendInterceptor struct {
	state              *partialUpdateState
	pkDescriptorGetter primaryKeyDescriptorGetter
}

func (i *appendInterceptor) Name() string {
	return interceptorName
}

func (i *appendInterceptor) DoAppend(
	ctx context.Context,
	msg message.MutableMessage,
	appendOp interceptors.Append,
) (message.MessageID, error) {
	switch msg.MessageType() {
	case message.MessageTypeCommitTxn:
		return i.appendCommit(ctx, msg, appendOp)
	case message.MessageTypeRollbackTxn:
		return i.appendRollback(ctx, msg, appendOp)
	default:
		return i.appendWrite(ctx, msg, appendOp)
	}
}

func (i *appendInterceptor) appendCommit(
	ctx context.Context,
	msg message.MutableMessage,
	appendOp interceptors.Append,
) (message.MessageID, error) {
	txnContext := msg.TxnContext()
	if txnContext == nil {
		return nil, status.NewUnrecoverableError("partial update commit transaction context is missing")
	}
	defer i.state.deleteTxn(txnContext.TxnID)

	txnState, err := i.state.validateCommit(msg, txnContext.TxnID)
	if err != nil {
		return nil, txn.MarkCommitAdmissionRejected(err)
	}
	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	i.state.publishCommit(msg, txnState)
	return msgID, nil
}

func (i *appendInterceptor) appendRollback(
	ctx context.Context,
	msg message.MutableMessage,
	appendOp interceptors.Append,
) (message.MessageID, error) {
	if txnContext := msg.TxnContext(); txnContext != nil {
		defer i.state.deleteTxn(txnContext.TxnID)
	}
	return appendOp(ctx, msg)
}

func (i *appendInterceptor) appendWrite(
	ctx context.Context,
	msg message.MutableMessage,
	appendOp interceptors.Append,
) (message.MessageID, error) {
	meta, err := message.ExtractPartialUpdateCAS(msg)
	if err != nil {
		return nil, status.NewUnrecoverableError("partial update CAS metadata is malformed: %v", err)
	}
	if meta != nil && msg.TxnContext() == nil {
		return nil, status.NewUnrecoverableError("partial update CAS message must be transactional")
	}

	var pks []any
	var fenceCollectionID int64
	var droppedCollectionID int64
	if meta != nil {
		pks, err = extractPKsFromInsert(msg, meta.GetPrimaryKeyFieldId())
		if err != nil {
			return nil, err
		}
		if err := i.state.recordTxnMeta(msg.TxnContext().TxnID, meta); err != nil {
			return nil, err
		}
	} else {
		extractedPKs, ok, err := extractPKs(msg)
		if err != nil {
			return nil, err
		}
		if ok {
			pks = extractedPKs
		} else if collectionID, ok := extractCollectionFenceID(msg); ok {
			if collectionID == 0 {
				return nil, status.NewUnrecoverableError("partial update collection fence has empty collection id")
			}
			fenceCollectionID = collectionID
		} else if collectionID, ok := extractDropCollectionID(msg); ok {
			if collectionID == 0 {
				return nil, status.NewUnrecoverableError("partial update dropped collection has empty collection id")
			}
			droppedCollectionID = collectionID
		} else if msg.MessageType() == message.MessageTypeInsert {
			pks, fenceCollectionID, err = extractPKsFromOrdinaryInsert(msg, i.pkDescriptorGetter)
			if err != nil {
				return nil, err
			}
		}
	}

	if msg.TxnContext() != nil {
		i.state.registerTxnCleanup(txn.GetTxnSessionFromContext(ctx), msg.TimeTick())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}

	if msg.TxnContext() != nil {
		txnID := msg.TxnContext().TxnID
		if msg.MessageType() == message.MessageTypeBeginTxn {
			i.state.recordTxnBegin(txnID)
		}
		i.state.recordTxnWrites(txnID, pks)
		i.state.recordTxnFence(txnID, fenceCollectionID)
		return msgID, nil
	}

	if len(pks) > 0 {
		i.state.pkVersions.UpdateAll(msg.VChannel(), pks, msg.TimeTick())
	} else {
		i.state.pkVersions.Advance(msg.TimeTick())
	}
	if fenceCollectionID != 0 {
		i.state.fences.Update(msg.VChannel(), fenceCollectionID, msg.TimeTick())
	}
	if droppedCollectionID != 0 {
		i.state.removeDroppedVChannel(msg.VChannel(), droppedCollectionID)
	}
	return msgID, nil
}

func (i *appendInterceptor) Close() {}
