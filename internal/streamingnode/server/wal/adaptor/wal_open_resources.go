package adaptor

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
)

type walOpenResources struct {
	once            sync.Once
	released        bool
	roWAL           *roWALAdaptorImpl
	param           *interceptors.InterceptorBuildParam
	recoveryStorage recovery.RecoveryStorage
	flusher         *flusherimpl.WALFlusherImpl
}

func (r *walOpenResources) Close() {
	if r.released {
		return
	}
	r.once.Do(func() {
		if r.flusher != nil {
			r.flusher.Close()
		} else if r.recoveryStorage != nil {
			r.recoveryStorage.Close()
		}
		if r.param != nil {
			r.param.Clear()
		}
		if r.roWAL != nil {
			r.roWAL.Close()
		}
	})
}

func (r *walOpenResources) Release() {
	r.released = true
}
