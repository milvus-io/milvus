package adaptor

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAdaptorImpl) WatchReplicas() (*replicadiscovery.Watch, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("replica directory WAL is unavailable")
	}
	defer w.lifetime.Done()
	if w.queryViewHandler == nil {
		return nil, status.NewOnShutdownError("replica directory is unavailable")
	}
	return w.queryViewHandler.WatchReplicas(), nil
}
