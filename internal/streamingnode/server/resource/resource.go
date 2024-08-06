package resource

import (
	"reflect"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/internal/types"
)

var r *resourceImpl // singleton resource instance

// optResourceInit is the option to initialize the resource.
type optResourceInit func(r *resourceImpl)

// OptFlusher provides the flusher to the resource.
func OptFlusher(flusher flusher.Flusher) optResourceInit {
	return func(r *resourceImpl) {
		r.flusher = flusher
	}
}

// OptSyncManager provides the sync manager to the resource.
func OptSyncManager(syncMgr syncmgr.SyncManager) optResourceInit {
	return func(r *resourceImpl) {
		r.syncMgr = syncMgr
	}
}

// OptBufferManager provides the write buffer manager to the resource.
func OptBufferManager(wbMgr writebuffer.BufferManager) optResourceInit {
	return func(r *resourceImpl) {
		r.wbMgr = wbMgr
	}
}

// OptETCD provides the etcd client to the resource.
func OptETCD(etcd *clientv3.Client) optResourceInit {
	return func(r *resourceImpl) {
		r.etcdClient = etcd
	}
}

// OptChunkManager provides the chunk manager to the resource.
func OptChunkManager(chunkManager storage.ChunkManager) optResourceInit {
	return func(r *resourceImpl) {
		r.chunkManager = chunkManager
	}
}

// OptRootCoordClient provides the root coordinator client to the resource.
func OptRootCoordClient(rootCoordClient types.RootCoordClient) optResourceInit {
	return func(r *resourceImpl) {
		r.rootCoordClient = rootCoordClient
	}
}

// OptDataCoordClient provides the data coordinator client to the resource.
func OptDataCoordClient(dataCoordClient types.DataCoordClient) optResourceInit {
	return func(r *resourceImpl) {
		r.dataCoordClient = dataCoordClient
	}
}

// OptStreamingNodeCatalog provides the streaming node catalog to the resource.
func OptStreamingNodeCatalog(catalog metastore.StreamingNodeCataLog) optResourceInit {
	return func(r *resourceImpl) {
		r.streamingNodeCatalog = catalog
	}
}

// Init initializes the singleton of resources.
// Should be call when streaming node startup.
func Init(opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
	r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)
	r.segmentAssignStatsManager = stats.NewStatsManager()
	r.segmentSealedInspector = inspector.NewSealedInspector(r.segmentAssignStatsManager.SealNotifier())

	assertNotNil(r.TSOAllocator())
	assertNotNil(r.RootCoordClient())
	assertNotNil(r.DataCoordClient())
	assertNotNil(r.StreamingNodeCatalog())
	assertNotNil(r.SegmentAssignStatsManager())
	assertNotNil(r.SegmentSealedInspector())
}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	flusher flusher.Flusher
	syncMgr syncmgr.SyncManager
	wbMgr   writebuffer.BufferManager

	timestampAllocator        idalloc.Allocator
	idAllocator               idalloc.Allocator
	etcdClient                *clientv3.Client
	chunkManager              storage.ChunkManager
	rootCoordClient           types.RootCoordClient
	dataCoordClient           types.DataCoordClient
	streamingNodeCatalog      metastore.StreamingNodeCataLog
	segmentAssignStatsManager *stats.StatsManager
	segmentSealedInspector    inspector.SealOperationInspector
}

// Flusher returns the flusher.
func (r *resourceImpl) Flusher() flusher.Flusher {
	return r.flusher
}

// SyncManager returns the sync manager.
func (r *resourceImpl) SyncManager() syncmgr.SyncManager {
	return r.syncMgr
}

// BufferManager returns the write buffer manager.
func (r *resourceImpl) BufferManager() writebuffer.BufferManager {
	return r.wbMgr
}

// TSOAllocator returns the timestamp allocator to allocate timestamp.
func (r *resourceImpl) TSOAllocator() idalloc.Allocator {
	return r.timestampAllocator
}

// IDAllocator returns the id allocator to allocate id.
func (r *resourceImpl) IDAllocator() idalloc.Allocator {
	return r.idAllocator
}

// ETCD returns the etcd client.
func (r *resourceImpl) ETCD() *clientv3.Client {
	return r.etcdClient
}

// ChunkManager returns the chunk manager.
func (r *resourceImpl) ChunkManager() storage.ChunkManager {
	return r.chunkManager
}

// RootCoordClient returns the root coordinator client.
func (r *resourceImpl) RootCoordClient() types.RootCoordClient {
	return r.rootCoordClient
}

// DataCoordClient returns the data coordinator client.
func (r *resourceImpl) DataCoordClient() types.DataCoordClient {
	return r.dataCoordClient
}

// StreamingNodeCataLog returns the streaming node catalog.
func (r *resourceImpl) StreamingNodeCatalog() metastore.StreamingNodeCataLog {
	return r.streamingNodeCatalog
}

// SegmentAssignStatManager returns the segment assign stats manager.
func (r *resourceImpl) SegmentAssignStatsManager() *stats.StatsManager {
	return r.segmentAssignStatsManager
}

// SegmentSealedInspector returns the segment sealed inspector.
func (r *resourceImpl) SegmentSealedInspector() inspector.SealOperationInspector {
	return r.segmentSealedInspector
}

// assertNotNil panics if the resource is nil.
func assertNotNil(v interface{}) {
	iv := reflect.ValueOf(v)
	if !iv.IsValid() {
		panic("nil resource")
	}
	switch iv.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Func, reflect.Interface:
		if iv.IsNil() {
			panic("nil resource")
		}
	}
}
