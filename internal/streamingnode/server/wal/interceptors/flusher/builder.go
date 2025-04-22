package flusher

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
)

// NewInterceptorBuilder creates a new flusher interceptor builder.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is the builder for flusher interceptor.
type interceptorBuilder struct{}

// Build creates a new flusher interceptor.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	flusher := flusherimpl.RecoverWALFlusher(param)
	return &flusherAppendInterceptor{
		flusher: flusher,
	}
}
