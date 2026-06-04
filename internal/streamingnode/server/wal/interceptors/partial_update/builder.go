package partial_update

import (
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// NewInterceptorBuilder creates a new pk_state_interceptor builder.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	timeout := paramtable.Get().StreamingCfg.PartialUpdatePKVersionCacheTimeout.GetAsDurationByParse()
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ready := make(chan struct{})
	// TODO(partial_update recovery): currently the interceptor is ready
	// immediately, so right after a leader switch the PK version cache is cold
	// and stale CAS appends may be wrongly accepted (assumed-historical match).
	// Phase 2 recovery should rebuild the cache from WAL/storage history and
	// only close(ready) once the cache has caught up to the current checkpoint,
	// gating CAS appends until then. See PartialUpdatePKVersionCacheTimeout for
	// the eviction side; recovery is the cold-start side of the same cache.
	close(ready)
	return &pkStateInterceptor{
		cache: NewPKVersionCache(timeout),
		ready: ready,
	}
}
