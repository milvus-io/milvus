package lock

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
)

// NewInterceptorBuilder creates a new redo interceptor builder.
// TODO: add it into wal after recovery storage is merged.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is the builder for redo interceptor.
type interceptorBuilder struct{}

// Build creates a new redo interceptor.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	return &lockAppendInterceptor{
		vchannelLocker: lock.NewKeyLock[string](),
		// TODO: txnManager will be intiailized by param 		txnManager:     param.TxnManager,
	}
}
