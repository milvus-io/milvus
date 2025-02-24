package utility

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestWithNotPersisted(t *testing.T) {
	ctx := context.Background()
	hint := &NotPersistedHint{MessageID: walimplstest.NewTestMessageID(1)}
	ctx = WithNotPersisted(ctx, hint)

	retrievedHint := GetNotPersisted(ctx)
	assert.NotNil(t, retrievedHint)
	assert.True(t, retrievedHint.MessageID.EQ(hint.MessageID))
}

func TestWithExtraAppendResult(t *testing.T) {
	ctx := context.Background()
	extra := &anypb.Any{}
	txnCtx := &message.TxnContext{
		TxnID: 1,
	}
	result := &ExtraAppendResult{TimeTick: 123, TxnCtx: txnCtx, Extra: extra}
	ctx = WithExtraAppendResult(ctx, result)

	retrievedResult := ctx.Value(extraAppendResultValue).(*ExtraAppendResult)
	assert.NotNil(t, retrievedResult)
	assert.Equal(t, uint64(123), retrievedResult.TimeTick)
	assert.Equal(t, txnCtx.TxnID, retrievedResult.TxnCtx.TxnID)
	assert.Equal(t, extra, retrievedResult.Extra)
}

func TestModifyAppendResultExtra(t *testing.T) {
	ctx := context.Background()
	extra := &anypb.Any{}
	result := &ExtraAppendResult{Extra: extra}
	ctx = WithExtraAppendResult(ctx, result)

	modifier := func(old *anypb.Any) *anypb.Any {
		return &anypb.Any{TypeUrl: "modified"}
	}
	ModifyAppendResultExtra(ctx, modifier)

	retrievedResult := ctx.Value(extraAppendResultValue).(*ExtraAppendResult)
	assert.Equal(t, retrievedResult.Extra.(*anypb.Any).TypeUrl, "modified")

	ModifyAppendResultExtra(ctx, func(old *anypb.Any) *anypb.Any {
		return nil
	})

	retrievedResult = ctx.Value(extraAppendResultValue).(*ExtraAppendResult)
	assert.Nil(t, retrievedResult.Extra)
}

func TestReplaceAppendResultTimeTick(t *testing.T) {
	ctx := context.Background()
	result := &ExtraAppendResult{TimeTick: 1}
	ctx = WithExtraAppendResult(ctx, result)

	ReplaceAppendResultTimeTick(ctx, 2)

	retrievedResult := ctx.Value(extraAppendResultValue).(*ExtraAppendResult)
	assert.Equal(t, retrievedResult.TimeTick, uint64(2))
}

func TestReplaceAppendResultTxnContext(t *testing.T) {
	ctx := context.Background()
	txnCtx := &message.TxnContext{}
	result := &ExtraAppendResult{TxnCtx: txnCtx}
	ctx = WithExtraAppendResult(ctx, result)

	newTxnCtx := &message.TxnContext{TxnID: 2}
	ReplaceAppendResultTxnContext(ctx, newTxnCtx)

	retrievedResult := ctx.Value(extraAppendResultValue).(*ExtraAppendResult)
	assert.Equal(t, retrievedResult.TxnCtx.TxnID, newTxnCtx.TxnID)
}
