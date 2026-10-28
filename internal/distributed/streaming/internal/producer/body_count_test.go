package producer

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestProducerRetriesWholeTxnAfterBodyCountRejection(t *testing.T) {
	p := &ResumableProducer{}
	p.SetLogger(mlog.With())
	g := &ProduceGuard{producer: p}
	var mu sync.Mutex
	var attempt message.TxnID
	var bodyTxns []message.TxnID
	var counts []uint64
	patch := mockey.Mock((*ResumableProducer).produceInternal).To(func(_ *ResumableProducer, _ context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
		mu.Lock()
		defer mu.Unlock()
		switch msg.MessageType() {
		case message.MessageTypeBeginTxn:
			attempt++
			return &types.AppendResult{TxnCtx: &message.TxnContext{TxnID: attempt}}, nil
		case message.MessageTypeCommitTxn:
			counts = append(counts, message.MustAsMutableCommitTxnMessageV2(msg).Header().BodyCnt)
			if attempt == 1 {
				return nil, status.NewTransactionExpired("body count mismatch after an upper-layer duplicate append")
			}
		default:
			bodyTxns = append(bodyTxns, msg.TxnContext().TxnID)
		}
		return &types.AppendResult{}, nil
	}).Build()
	defer patch.UnPatch()
	_, err := g.produceTxn(context.Background(), message.CreateTestEmptyInsertMesage(1, nil), message.CreateTestEmptyInsertMesage(1, nil))
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 2}, counts)
	require.Equal(t, []message.TxnID{1, 1, 2, 2}, bodyTxns)
}
