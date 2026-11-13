package producer

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	handlerproducer "github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type schemaResultProducer struct{ handlerproducer.Producer }

func TestSchemaMismatchInvalidatesBeforeTransactionErrorReduction(t *testing.T) {
	var mu sync.Mutex
	var invalidated []string
	p := &ResumableProducer{lifetime: typeutil.NewLifetime(), opts: &ProducerOptions{OnSchemaVersionMismatch: func(channel string) { mu.Lock(); defer mu.Unlock(); invalidated = append(invalidated, channel) }}}
	raw := &schemaResultProducer{}
	patch := mockey.Mock((*producerWithResumingError).GetProducerAfterAvailable).Return(raw, nil).Build()
	defer patch.UnPatch()
	patch = mockey.Mock((*ResumableProducer).startDistAppendSpanIfRemote).Return(t.Context(), nil).Build()
	defer patch.UnPatch()
	patch = mockey.Mock((*schemaResultProducer).Append).To(func(_ *schemaResultProducer, _ context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
		if string(message.IdempotencyKeyOf(msg)) == "first" {
			return nil, status.NewUnrecoverableError("first error")
		}
		return nil, status.NewSchemaVersionMismatch("schema changed")
	}).Build()
	defer patch.UnPatch()
	first := message.NewInsertMessageBuilderV1().WithIdempotencyKey("first").WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
	second := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
	guard := &ProduceGuard{producer: p}
	err := guard.appendTxnBody(t.Context(), &message.TxnContext{TxnID: 1}, first, second)
	require.ErrorContains(t, err, "first error")
	require.Equal(t, []string{"p_1v0"}, invalidated)
}
