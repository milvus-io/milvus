package idempotency

import (
	"context"
	"strconv"

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
		MaxBytes:     int(params.StreamingCfg.IdempotencyMaxBytesPerWindow.GetAsSize()),
		MaxKeyLength: params.StreamingCfg.IdempotencyMaxKeyLength.GetAsInt(),
	})
	if param != nil && param.InitialRecoverSnapshot != nil {
		return newIdempotencyInterceptorWithSnapshots(config, param.InitialRecoverSnapshot.SummarySnapshots, param)
	}
	return newIdempotencyInterceptorWithParam(config, param)
}

// sanitizeWindowConfig repairs an unusable byte cap by falling back to the
// default with a warning. maxBytes is the window's only retention bound, so a
// non-positive value would let it grow without limit, one entry per key.
func sanitizeWindowConfig(config WindowConfig) WindowConfig {
	if !config.Enabled || config.MaxBytes > 0 {
		return config
	}
	fallback, err := strconv.Atoi(paramtable.Get().StreamingCfg.IdempotencyMaxBytesPerWindow.DefaultValue)
	if err != nil {
		// The default is a compile-time literal; parsing it cannot fail.
		panic(err)
	}
	mlog.Warn(context.TODO(), "idempotency window has no positive max byte cap; falling back to the default",
		mlog.Int("configuredMaxBytes", config.MaxBytes),
		mlog.Int("fallbackMaxBytes", fallback))
	config.MaxBytes = fallback
	return config
}
