package idempotency

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	params := paramtable.Get()
	config := sanitizeWindowConfig(WindowConfig{
		Enabled:      params.StreamingCfg.IdempotencyEnabled.GetAsBool(),
		WindowTTL:    params.StreamingCfg.IdempotencyWindowTTL.GetAsDurationByParse(),
		MinEntries:   params.StreamingCfg.IdempotencyMinEntriesPerWindow.GetAsInt(),
		MaxEntries:   params.StreamingCfg.IdempotencyMaxEntriesPerWindow.GetAsInt(),
		MaxKeyLength: params.StreamingCfg.IdempotencyMaxKeyLength.GetAsInt(),
	})
	if param != nil && param.InitialRecoverSnapshot != nil {
		return newIdempotencyInterceptorWithSnapshots(config, param.InitialRecoverSnapshot.IdempotencyWindows, param)
	}
	return newIdempotencyInterceptorWithParam(config, param)
}

// sanitizeWindowConfig repairs invalid combinations of the runtime-tunable
// idempotency parameters by falling back to their defaults with a warning:
// with neither a positive TTL nor a positive max entry cap the window would
// grow without bound per key. Kept in sync with the recovery-side
// config.sanitizeIdempotency, which applies the same fallback.
func sanitizeWindowConfig(config WindowConfig) WindowConfig {
	if !config.Enabled {
		return config
	}
	if config.WindowTTL <= 0 && config.MaxEntries <= 0 {
		fallback, err := time.ParseDuration(paramtable.Get().StreamingCfg.IdempotencyWindowTTL.DefaultValue)
		if err != nil {
			// The default is a compile-time literal; parsing it cannot fail.
			panic(err)
		}
		mlog.Warn(context.TODO(), "idempotency window has neither a positive TTL nor a positive max entry cap; falling back to default TTL",
			mlog.Duration("configuredTTL", config.WindowTTL),
			mlog.Duration("fallbackTTL", fallback))
		config.WindowTTL = fallback
	}
	return config
}
