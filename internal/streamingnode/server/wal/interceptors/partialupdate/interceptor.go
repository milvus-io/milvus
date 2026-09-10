package partialupdate

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const interceptorName = "partialupdate"

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

type interceptorBuilder struct {
	versionIndexBudget *versionByteBudget
}

type primaryKeyDescriptorGetter interface {
	GetPrimaryKeyDescriptor(collectionID int64, schemaVersion int32) (shards.PrimaryKeyDescriptor, error)
}

// NewInterceptorBuilder creates one per-WAL partial-update write tracker.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	maxBytes := paramtable.Get().StreamingCfg.PartialUpdateVersionIndexMaxBytes.GetAsSize()
	labels := prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}
	used := metrics.StreamingNodePartialUpdateVersionIndexBytes.With(labels)
	limit := metrics.StreamingNodePartialUpdateVersionIndexMaxBytes.With(labels)
	missed := metrics.StreamingNodePartialUpdateVersionIndexMissedWrites.With(labels)
	limit.Set(float64(maxBytes))
	return &interceptorBuilder{
		versionIndexBudget: newVersionByteBudgetWithMetrics(maxBytes, used, missed),
	}
}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	state := newPartialUpdateStateWithBudget(defaultVersionIndexTTL, b.versionIndexBudget)
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
	markedCAS := message.HasPartialUpdateCAS(msg)
	if markedCAS && msg.TxnContext() == nil {
		return nil, status.NewUnrecoverableError("partial update CAS message must be transactional")
	}

	var pks primaryKeys
	var metaEncoded string
	var casScope casInsertScope
	var fenceCollectionID int64
	var droppedCollectionID int64
	var err error
	if markedCAS {
		pks, casScope, metaEncoded, err = extractPKsFromCASInsertWithContext(ctx, msg, i.pkDescriptorGetter)
		if err != nil {
			return nil, err
		}
		meta, err := message.DecodePartialUpdateCASMetadata(metaEncoded)
		if err != nil {
			return nil, status.NewUnrecoverableError("partial update CAS proof is malformed: %v", err)
		}
		if err := i.state.recordTxnCAS(msg.TxnContext().TxnID, meta, casScope); err != nil {
			return nil, err
		}
	} else {
		extractedPKs, ok, err := extractPKsWithContext(ctx, msg)
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
			pks, fenceCollectionID, err = extractPKsFromOrdinaryInsertWithContext(ctx, msg, i.pkDescriptorGetter)
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
		i.state.recordTxnWritesTyped(txnID, pks)
		i.state.recordTxnFence(txnID, fenceCollectionID)
		return msgID, nil
	}

	if pks.Len() > 0 {
		i.state.pkVersions.UpdateAllTyped(msg.VChannel(), pks, msg.TimeTick())
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

func (i *appendInterceptor) Close() {
	i.state.pkVersions.Close()
}
