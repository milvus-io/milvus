package flusher

import (
	"github.com/milvus-io/milvus-storage/go/common/log"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"sync"
	"time"
)

type Flusher interface {
	// Open ASYNCHRONOUSLY creates and starts flowgraphs belonging to the pchannel.
	// If a flowgraph creation fails, the flusher will keep retrying to create it indefinitely.
	Open(pchannel string)

	// Close SYNCHRONOUSLY stops and removes flowgraphs belonging to the pchannel.
	Close(pchannel string)

	// Start flusher service.
	Start()

	// Stop flusher, will synchronously flush all remaining data.
	Stop()
}

type flusher struct {
	tasks typeutil.ConcurrentMap[string, *datapb.ChannelWatchInfo]

	fgMgr     pipeline.FlowgraphManager
	syncMgr   syncmgr.SyncManager
	wbMgr     writebuffer.BufferManager
	cpUpdater *util.ChannelCheckpointUpdater

	stopOnce sync.Once
	stopChan chan struct{}
}

func (f *flusher) Open() {
	// TODO: 1. query vchannels; 2. for each vchannel:
	// 2.1 get recovery info
	// 2.2 create pipeline
}

func (f *flusher) Start() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-f.stopChan:
				// TODO: trigger flush all
				log.Info("flusher stopped")
				return
			case <-ticker.C:

			}
		}
	}()
}

func (f *flusher) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopChan)
	})
}
