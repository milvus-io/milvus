package adaptor

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/ddl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
)

var _ wal.OpenerBuilder = (*builderAdaptorImpl)(nil)

func AdaptImplsToBuilder(builder walimpls.OpenerBuilderImpls) wal.OpenerBuilder {
	return builderAdaptorImpl{
		builder: builder,
	}
}

type builderAdaptorImpl struct {
	builder walimpls.OpenerBuilderImpls
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
	return adaptImplsToOpener(o, []interceptors.InterceptorBuilder{
		redo.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
		segment.NewInterceptorBuilder(),
		ddl.NewInterceptorBuilder(),
	}), nil
}
