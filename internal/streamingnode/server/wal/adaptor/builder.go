package adaptor

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
)

var _ wal.OpenerBuilder = (*builderAdaptorImpl)(nil)

func AdaptImplsToBuilder(builder walimpls.OpenerBuilderImpls, interceptorBuilders ...interceptors.InterceptorBuilder) wal.OpenerBuilder {
	return builderAdaptorImpl{
		builder:             builder,
		interceptorBuilders: interceptorBuilders,
	}
}

type builderAdaptorImpl struct {
	builder             walimpls.OpenerBuilderImpls
	interceptorBuilders []interceptors.InterceptorBuilder
}

func (b builderAdaptorImpl) Name() string {
	return b.builder.Name()
}

func (b builderAdaptorImpl) Build() (wal.Opener, error) {
	o, err := b.builder.Build()
	if err != nil {
		return nil, err
	}
	// Add all interceptor here.
	return adaptImplsToOpener(o, b.interceptorBuilders), nil
}
