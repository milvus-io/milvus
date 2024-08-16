package utility

import (
	"context"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// walCtxKey is the key type of extra append result.
type walCtxKey int

var (
	extraAppendResultValue walCtxKey = 1
	notPersistedValue      walCtxKey = 2
)

// ExtraAppendResult is the extra append result.
type ExtraAppendResult struct {
	TimeTick uint64
	TxnCtx   *message.TxnContext
	Extra    *anypb.Any
}

// NotPersistedHint is the hint of not persisted message.
type NotPersistedHint struct {
	MessageID message.MessageID // The reused MessageID.
}

// WithNotPersisted set not persisted message to context
func WithNotPersisted(ctx context.Context, hint *NotPersistedHint) context.Context {
	return context.WithValue(ctx, notPersistedValue, hint)
}

// GetNotPersisted get not persisted message from context
func GetNotPersisted(ctx context.Context) *NotPersistedHint {
	val := ctx.Value(notPersistedValue)
	if val == nil {
		return nil
	}
	return val.(*NotPersistedHint)
}

// WithExtraAppendResult set extra to context
func WithExtraAppendResult(ctx context.Context, r *ExtraAppendResult) context.Context {
	return context.WithValue(ctx, extraAppendResultValue, r)
}

// AttachAppendResultExtra set extra to context
func AttachAppendResultExtra(ctx context.Context, extra *anypb.Any) {
	result := ctx.Value(extraAppendResultValue)
	result.(*ExtraAppendResult).Extra = extra
}

// AttachAppendResultTimeTick set time tick to context
func AttachAppendResultTimeTick(ctx context.Context, timeTick uint64) {
	result := ctx.Value(extraAppendResultValue)
	result.(*ExtraAppendResult).TimeTick = timeTick
}

// AttachAppendResultTxnContext set txn context to context
func AttachAppendResultTxnContext(ctx context.Context, txnCtx *message.TxnContext) {
	result := ctx.Value(extraAppendResultValue)
	result.(*ExtraAppendResult).TxnCtx = txnCtx
}
