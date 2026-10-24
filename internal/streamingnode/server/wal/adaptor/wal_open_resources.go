package adaptor

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
)

type walOpenResources struct {
	once             sync.Once
	released         bool // Release and Close are called by the same openRWWAL goroutine.
	roWAL            *roWALAdaptorImpl
	param            *interceptors.InterceptorBuildParam
	queryViewHandler *snview.SNQueryViewHandler
}

func (r *walOpenResources) Close() {
	if r.released {
		return
	}
	r.once.Do(func() {
		// Release callbacks require the recovery scheduler to remain running.
		if r.queryViewHandler != nil {
			r.queryViewHandler.CloseForHandoff()
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
