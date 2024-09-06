package segment

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/manager"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param interceptors.InterceptorBuildParam) interceptors.Interceptor {
	assignManager := syncutil.NewFuture[*manager.PChannelSegmentAllocManager]()
	ctx, cancel := context.WithCancel(context.Background())
	segmentInterceptor := &segmentInterceptor{
		ctx:           ctx,
		cancel:        cancel,
		logger:        log.With(zap.Any("pchannel", param.WALImpls.Channel())),
		assignManager: assignManager,
	}
	go segmentInterceptor.recoverPChannelManager(param)
	return segmentInterceptor
}
