package replicate

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	return &replicateInterceptor{
		replicateManager: param.ReplicateManager,
	}
}
