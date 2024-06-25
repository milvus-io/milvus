package adaptor

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
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
	_, err := b.builder.Build()
	if err != nil {
		return nil, err
	}
	return nil, nil
	// TODO: wait for implementation.
	// return adaptImplsToOpener(o), nil
}
