package redo

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"

// NewInterceptorBuilder creates a new redo interceptor builder.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is the builder for redo interceptor.
type interceptorBuilder struct{}

// Build creates a new redo interceptor.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	return &redoAppendInterceptor{
		shardManager: param.ShardManager,
		gracefulStop: make(chan struct{}),
	}
}
