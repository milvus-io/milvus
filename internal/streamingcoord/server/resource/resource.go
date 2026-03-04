package resource

import (
	"reflect"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var r atomic.Pointer[resourceImpl] // singleton resource instance

// optResourceInit is the option to initialize the resource.
type optResourceInit func(r *resourceImpl)

// OptETCD provides the etcd client to the resource.
func OptETCD(etcd *clientv3.Client) optResourceInit {
	return func(r *resourceImpl) {
		r.etcdClient = etcd
	}
}

// OptRootCoordClient provides the root coordinator client to the resource.
func OptMixCoordClient(mixCoordClient *syncutil.Future[types.MixCoordClient]) optResourceInit {
	return func(r *resourceImpl) {
		r.mixCoordClient = mixCoordClient
		r.idAllocator = idalloc.NewIDAllocator(r.mixCoordClient)
	}
}

// OptStreamingCatalog provides streaming catalog to the resource.
func OptStreamingCatalog(catalog metastore.StreamingCoordCataLog) optResourceInit {
	return func(r *resourceImpl) {
		r.streamingCatalog = catalog
	}
}

// OptSession provides the session to the resource.
func OptSession(session sessionutil.SessionInterface) optResourceInit {
	return func(r *resourceImpl) {
		r.session = session
	}
}

// Init initializes the singleton of resources.
// Should be call when streaming node startup.
func Init(opts ...optResourceInit) {
	newR := &resourceImpl{
		logger: log.With(log.FieldModule(typeutil.StreamingCoordRole)),
	}
	for _, opt := range opts {
		opt(newR)
	}
	assertNotNil(newR.IDAllocator())
	assertNotNil(newR.MixCoordClient())
	assertNotNil(newR.ETCD())
	assertNotNil(newR.StreamingCatalog())
	newR.streamingNodeManagerClient = manager.NewManagerClient(newR.etcdClient)
	assertNotNil(newR.StreamingNodeManagerClient())
	r.Store(newR)
}

// Release release the streamingnode client
func Release() {
	if res := r.Load(); res != nil && res.streamingNodeManagerClient != nil {
		res.streamingNodeManagerClient.Close()
	}
}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r.Load()
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	idAllocator                idalloc.Allocator
	mixCoordClient             *syncutil.Future[types.MixCoordClient]
	session                    sessionutil.SessionInterface
	etcdClient                 *clientv3.Client
	streamingCatalog           metastore.StreamingCoordCataLog
	streamingNodeManagerClient manager.ManagerClient
	logger                     *log.MLogger
}

// RootCoordClient returns the root coordinator client.
func (r *resourceImpl) MixCoordClient() *syncutil.Future[types.MixCoordClient] {
	return r.mixCoordClient
}

// IDAllocator returns the IDAllocator client.
func (r *resourceImpl) IDAllocator() idalloc.Allocator {
	return r.idAllocator
}

// StreamingCatalog returns the StreamingCatalog client.
func (r *resourceImpl) StreamingCatalog() metastore.StreamingCoordCataLog {
	return r.streamingCatalog
}

// ETCD returns the etcd client.
func (r *resourceImpl) ETCD() *clientv3.Client {
	return r.etcdClient
}

func (r *resourceImpl) Session() sessionutil.SessionInterface {
	return r.session
}

// StreamingNodeClient returns the streaming node client.
func (r *resourceImpl) StreamingNodeManagerClient() manager.ManagerClient {
	return r.streamingNodeManagerClient
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
