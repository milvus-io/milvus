package utility

import (
	"context"
	"reflect"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// walCtxKey is the key type of extra append result.
type walCtxKey int

var (
	extraAppendResultValue walCtxKey = 1
	notPersistedValue      walCtxKey = 2
	metricsValue           walCtxKey = 3
)

// ExtraAppendResult is the extra append result.
type ExtraAppendResult struct {
	TimeTick uint64
	TxnCtx   *message.TxnContext
	Extra    protoreflect.ProtoMessage
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

// ModifyAppendResultExtra modify extra in context
func ModifyAppendResultExtra[M protoreflect.ProtoMessage](ctx context.Context, modifier func(old M) (new M)) {
	result := ctx.Value(extraAppendResultValue)
	var old M
	if result.(*ExtraAppendResult).Extra != nil {
		old = result.(*ExtraAppendResult).Extra.(M)
	}
	new := modifier(old)
	if reflect.ValueOf(new).IsNil() {
		result.(*ExtraAppendResult).Extra = nil
		return
	}
	result.(*ExtraAppendResult).Extra = new
}

// ReplaceAppendResultTimeTick set time tick to context
func ReplaceAppendResultTimeTick(ctx context.Context, timeTick uint64) {
	result := ctx.Value(extraAppendResultValue)
	result.(*ExtraAppendResult).TimeTick = timeTick
}

// ReplaceAppendResultTxnContext set txn context to context
func ReplaceAppendResultTxnContext(ctx context.Context, txnCtx *message.TxnContext) {
	result := ctx.Value(extraAppendResultValue)
	result.(*ExtraAppendResult).TxnCtx = txnCtx
}

// WithAppendMetricsContext create a context with metrics recording.
func WithAppendMetricsContext(ctx context.Context, m *metricsutil.AppendMetrics) context.Context {
	return context.WithValue(ctx, metricsValue, m)
}

// MustGetAppendMetrics get append metrics from context
func MustGetAppendMetrics(ctx context.Context) *metricsutil.AppendMetrics {
	return ctx.Value(metricsValue).(*metricsutil.AppendMetrics)
}
