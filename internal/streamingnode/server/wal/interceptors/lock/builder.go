package lock

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// NewInterceptorBuilder creates a new lock interceptor builder.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is the builder for the lock interceptor.
type interceptorBuilder struct{}

// Build creates a new lock interceptor.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	return &lockAppendInterceptor{
		channel:        param.ChannelInfo,
		vchannelLocker: lock.NewKeyLock[string](),
		txnManager:     param.TxnManager,
		buildParam:     param,
		replicateOrder: newReplicateAppendOrder(param.InitialRecoverSnapshot),
	}
}
