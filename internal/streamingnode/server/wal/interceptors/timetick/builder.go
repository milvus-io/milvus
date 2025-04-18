package timetick

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
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
	return &timeTickAppendInterceptor{
		operator:   operator,
		txnManager: txn.NewTxnManager(param.ChannelInfo),
	}
}
