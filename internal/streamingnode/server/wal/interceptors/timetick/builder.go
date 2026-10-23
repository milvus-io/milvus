package timetick

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates a new interceptor builder.
// 1. Add timetick to all message before append to wal.
// 2. Collect timetick info, and generate sync-timetick message to wal.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is a builder to build timeTickAppendInterceptor.
type interceptorBuilder struct{}

// Build implements Builder.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	operator := newTimeTickSyncOperator(param)
	// initialize operation can be async to avoid block the build operation.
	resource.Resource().TimeTickInspector().RegisterSyncOperator(operator)

	tombstones := utility.NewTxnTombstones()
	if param.InitialRecoverSnapshot != nil && param.InitialRecoverSnapshot.TxnTombstones != nil {
		tombstones = param.InitialRecoverSnapshot.TxnTombstones
	}
	tombstones.Advance(param.LastTimeTickMessage.TimeTick())
	return &timeTickAppendInterceptor{
		operator:    operator,
		txnManager:  param.TxnManager,
		commitLocks: lock.NewKeyLock[message.TxnID](),
		tombstones:  tombstones,
	}
}
