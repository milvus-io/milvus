package adaptor

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
)

type walOpenResources struct {
	once     sync.Once
	released bool // Release and Close are called by the same openRWWAL goroutine.
	roWAL    *roWALAdaptorImpl
	param    *interceptors.InterceptorBuildParam
}

func (r *walOpenResources) Close() {
	if r.released {
		return
	}
	r.once.Do(func() {
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
