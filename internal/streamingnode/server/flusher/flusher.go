package flusher

import (
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
)

type Flusher interface {
	// Open ASYNCHRONOUSLY creates and starts flowgraphs belonging to the pchannel.
	// If a flowgraph creation fails, the flusher will keep retrying to create it indefinitely.
	Open(pchannel string)

	// Close SYNCHRONOUSLY stops and removes flowgraphs belonging to the pchannel.
	Close(pchannel string)
}

type flusher struct {
	fgMgr     pipeline.FlowgraphManager
	syncMgr   syncmgr.SyncManager
	wbMgr     writebuffer.BufferManager
	cpUpdater *util.ChannelCheckpointUpdater
}

func (f *flusher) Open() {
	// TODO: query vchannels
	//var vchannels []string
	//for _, vchannel := range vchannels {
	//	ds, err := pipeline.NewDataSyncService()
	//}
}
