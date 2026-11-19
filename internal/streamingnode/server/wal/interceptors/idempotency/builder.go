package idempotency

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	params := paramtable.Get()
	retention := idempotencyview.LoadRetentionConfig()
	config := WindowConfig{
		TTL: retention.TTL, MaxSize: retention.MaxSize, MinCount: retention.MinCount,
		MaxKeyLength: params.StreamingCfg.IdempotencyMaxKeyLength.GetAsInt(),
	}
	if param != nil && param.InitialRecoverSnapshot != nil {
		if param.InitialRecoverSnapshot.SummaryRecovery != nil {
			return newIdempotencyInterceptorWithRecovery(config, param.InitialRecoverSnapshot.SummaryRecovery, param)
		}
		return newIdempotencyInterceptorWithSnapshots(config, param.InitialRecoverSnapshot.SummarySnapshots, param)
	}
	return newIdempotencyInterceptorWithParam(config, param)
}
