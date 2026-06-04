package idempotency

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	params := paramtable.Get()
	config := WindowConfig{
		Enabled:      params.StreamingCfg.IdempotencyEnabled.GetAsBool(),
		WindowTTL:    params.StreamingCfg.IdempotencyWindowTTL.GetAsDurationByParse(),
		MinEntries:   params.StreamingCfg.IdempotencyMinEntriesPerWindow.GetAsInt(),
		MaxEntries:   params.StreamingCfg.IdempotencyMaxEntriesPerWindow.GetAsInt(),
		MaxKeyLength: params.StreamingCfg.IdempotencyMaxKeyLength.GetAsInt(),
	}
	if param != nil && param.InitialRecoverSnapshot != nil {
		return newIdempotencyInterceptorWithSnapshots(config, param.InitialRecoverSnapshot.IdempotencyWindows, param)
	}
	return newIdempotencyInterceptorWithParam(config, param)
}
