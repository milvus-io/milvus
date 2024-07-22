package resource

import (
	"reflect"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/internal/types"
)

var r *resourceImpl // singleton resource instance

// optResourceInit is the option to initialize the resource.
type optResourceInit func(r *resourceImpl)

// OptETCD provides the etcd client to the resource.
func OptETCD(etcd *clientv3.Client) optResourceInit {
	return func(r *resourceImpl) {
		r.etcdClient = etcd
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

// Init initializes the singleton of resources.
// Should be call when streaming node startup.
func Init(opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
	r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)

	assertNotNil(r.TSOAllocator())
	assertNotNil(r.ETCD())
	assertNotNil(r.RootCoordClient())
}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	timestampAllocator idalloc.Allocator
	idAllocator        idalloc.Allocator
	etcdClient         *clientv3.Client
	rootCoordClient    types.RootCoordClient
	dataCoordClient    types.DataCoordClient
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

// RootCoordClient returns the root coordinator client.
func (r *resourceImpl) RootCoordClient() types.RootCoordClient {
	return r.rootCoordClient
}

// DataCoordClient returns the data coordinator client.
func (r *resourceImpl) DataCoordClient() types.DataCoordClient {
	return r.dataCoordClient
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
