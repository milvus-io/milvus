package resource

import (
	"reflect"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	tinspector "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchantempstore"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var r = &resourceImpl{
	logger: log.With(log.FieldModule(typeutil.StreamingNodeRole)),
} // singleton resource instance

// optResourceInit is the option to initialize the resource.
type optResourceInit func(r *resourceImpl)

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
func OptMixCoordClient(mixCoordClient *syncutil.Future[types.MixCoordClient]) optResourceInit {
	return func(r *resourceImpl) {
		r.mixCoordClient = mixCoordClient
		r.timestampAllocator = idalloc.NewTSOAllocator(r.mixCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.mixCoordClient)
		r.vchannelTempStorage = vchantempstore.NewVChannelTempStorage(r.mixCoordClient)
	}
}

// OptStreamingNodeCatalog provides the streaming node catalog to the resource.
func OptStreamingNodeCatalog(catalog metastore.StreamingNodeCataLog) optResourceInit {
	return func(r *resourceImpl) {
		r.streamingNodeCatalog = catalog
	}
}

// Apply initializes the singleton of resources.
// Should be call when streaming node startup.
func Apply(opts ...optResourceInit) {
	for _, opt := range opts {
		opt(r)
	}
}

// Done finish all initialization of resources.
func Done() {
	r.segmentStatsManager = stats.NewStatsManager()
	r.timeTickInspector = tinspector.NewTimeTickSyncInspector()
	r.syncMgr = syncmgr.NewSyncManager(r.chunkManager)
	r.wbMgr = writebuffer.NewManager(r.syncMgr)
	r.wbMgr.Start()
	assertNotNil(r.ChunkManager())
	assertNotNil(r.TSOAllocator())
	assertNotNil(r.MixCoordClient())
	assertNotNil(r.StreamingNodeCatalog())
	assertNotNil(r.SegmentStatsManager())
	assertNotNil(r.TimeTickInspector())
	assertNotNil(r.SyncManager())
	assertNotNil(r.WriteBufferManager())
}

// Release releases the singleton of resources.
func Release() {
	r.wbMgr.Stop()
	r.syncMgr.Close()
}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	logger               *log.MLogger
	timestampAllocator   idalloc.Allocator
	idAllocator          idalloc.Allocator
	etcdClient           *clientv3.Client
	chunkManager         storage.ChunkManager
	mixCoordClient       *syncutil.Future[types.MixCoordClient]
	streamingNodeCatalog metastore.StreamingNodeCataLog
	segmentStatsManager  *stats.StatsManager
	timeTickInspector    tinspector.TimeTickSyncInspector
	vchannelTempStorage  *vchantempstore.VChannelTempStorage

	// TODO: Global flusher components, should be removed afteer flushering in wal refactoring.
	syncMgr syncmgr.SyncManager
	wbMgr   writebuffer.BufferManager
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

// SyncManager returns the sync manager.
func (r *resourceImpl) SyncManager() syncmgr.SyncManager {
	return r.syncMgr
}

// WriteBufferManager returns the write buffer manager.
func (r *resourceImpl) WriteBufferManager() writebuffer.BufferManager {
	return r.wbMgr
}

// RootCoordClient returns the root coordinator client.
func (r *resourceImpl) MixCoordClient() *syncutil.Future[types.MixCoordClient] {
	return r.mixCoordClient
}

// StreamingNodeCataLog returns the streaming node catalog.
func (r *resourceImpl) StreamingNodeCatalog() metastore.StreamingNodeCataLog {
	return r.streamingNodeCatalog
}

func (r *resourceImpl) SegmentStatsManager() *stats.StatsManager {
	return r.segmentStatsManager
}

func (r *resourceImpl) TimeTickInspector() tinspector.TimeTickSyncInspector {
	return r.timeTickInspector
}

// VChannelTempStorage returns the vchannel temp storage.
func (r *resourceImpl) VChannelTempStorage() *vchantempstore.VChannelTempStorage {
	return r.vchannelTempStorage
}

func (r *resourceImpl) Logger() *log.MLogger {
	return r.logger
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
