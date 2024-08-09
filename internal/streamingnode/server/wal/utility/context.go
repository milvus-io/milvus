package utility

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"google.golang.org/protobuf/types/known/anypb"
)

// extraAppendResultKey is the key type of extra append result.
type extraAppendResultKey int

// extraAppendResultKeyVaule is the key value of extra append result.
var extraAppendResultKeyVaule extraAppendResultKey = 1

// ExtraAppendResult is the extra append result.
type ExtraAppendResult struct {
	TimeTick uint64
	TxnCtx   *message.TxnContext
	Extra    *anypb.Any
}

// WithExtraAppendResult set extra to context
func WithExtraAppendResult(ctx context.Context, r *ExtraAppendResult) context.Context {
	return context.WithValue(ctx, extraAppendResultKeyVaule, r)
}

// AttachAppendResultExtra set extra to context
func AttachAppendResultExtra(ctx context.Context, extra *anypb.Any) {
	result := ctx.Value(extraAppendResultKeyVaule)
	result.(*ExtraAppendResult).Extra = extra
}

// AttachAppendResultTimeTick set time tick to context
func AttachAppendResultTimeTick(ctx context.Context, timeTick uint64) {
	result := ctx.Value(extraAppendResultKeyVaule)
	result.(*ExtraAppendResult).TimeTick = timeTick
}

// AttachAppendResultTxnContext set txn context to context
func AttachAppendResultTxnContext(ctx context.Context, txnCtx *message.TxnContext) {
	result := ctx.Value(extraAppendResultKeyVaule)
	result.(*ExtraAppendResult).TxnCtx = txnCtx
}
