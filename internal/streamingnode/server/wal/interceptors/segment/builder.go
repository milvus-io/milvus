package segment

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/manager"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param interceptors.InterceptorBuildParam) interceptors.Interceptor {
	assignManager := syncutil.NewFuture[*manager.PChannelSegmentAllocManager]()
	segmentInterceptor := &segmentInterceptor{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		logger: resource.Resource().Logger().With(
			log.FieldComponent("segment-assigner"),
			zap.Any("pchannel", param.WALImpls.Channel()),
		),
		assignManager: assignManager,
	}
	go segmentInterceptor.recoverPChannelManager(param)
	return segmentInterceptor
}
