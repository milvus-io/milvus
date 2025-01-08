package resource

import (
	"reflect"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	tinspector "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
func OptRootCoordClient(rootCoordClient *syncutil.Future[types.RootCoordClient]) optResourceInit {
	return func(r *resourceImpl) {
		r.rootCoordClient = rootCoordClient
		r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)
	}
}

// OptDataCoordClient provides the data coordinator client to the resource.
func OptDataCoordClient(dataCoordClient *syncutil.Future[types.DataCoordClient]) optResourceInit {
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

// Apply initializes the singleton of resources.
// Should be call when streaming node startup.
func Apply(opts ...optResourceInit) {
	for _, opt := range opts {
		opt(r)
	}
}

// Done finish all initialization of resources.
func Done() {
	r.segmentAssignStatsManager = stats.NewStatsManager()
	r.timeTickInspector = tinspector.NewTimeTickSyncInspector()
	assertNotNil(r.ChunkManager())
	assertNotNil(r.TSOAllocator())
	assertNotNil(r.RootCoordClient())
	assertNotNil(r.DataCoordClient())
	assertNotNil(r.StreamingNodeCatalog())
	assertNotNil(r.SegmentAssignStatsManager())
	assertNotNil(r.TimeTickInspector())
}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	logger                    *log.MLogger
	timestampAllocator        idalloc.Allocator
	idAllocator               idalloc.Allocator
	etcdClient                *clientv3.Client
	chunkManager              storage.ChunkManager
	rootCoordClient           *syncutil.Future[types.RootCoordClient]
	dataCoordClient           *syncutil.Future[types.DataCoordClient]
	streamingNodeCatalog      metastore.StreamingNodeCataLog
	segmentAssignStatsManager *stats.StatsManager
	timeTickInspector         tinspector.TimeTickSyncInspector
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
func (r *resourceImpl) RootCoordClient() *syncutil.Future[types.RootCoordClient] {
	return r.rootCoordClient
}

// DataCoordClient returns the data coordinator client.
func (r *resourceImpl) DataCoordClient() *syncutil.Future[types.DataCoordClient] {
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

func (r *resourceImpl) TimeTickInspector() tinspector.TimeTickSyncInspector {
	return r.timeTickInspector
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
