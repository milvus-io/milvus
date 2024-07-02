package resource

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/timestamp"
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

// Init initializes the singleton of resources.
// Should be call when streaming node startup.
func Init(opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	r.timestampAllocator = timestamp.NewAllocator(r.rootCoordClient)

	assertNotNil(r.TimestampAllocator())
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
	timestampAllocator timestamp.Allocator
	etcdClient         *clientv3.Client
	rootCoordClient    types.RootCoordClient
}

// TimestampAllocator returns the timestamp allocator to allocate timestamp.
func (r *resourceImpl) TimestampAllocator() timestamp.Allocator {
	return r.timestampAllocator
}

// ETCD returns the etcd client.
func (r *resourceImpl) ETCD() *clientv3.Client {
	return r.etcdClient
}

// RootCoordClient returns the root coordinator client.
func (r *resourceImpl) RootCoordClient() types.RootCoordClient {
	return r.rootCoordClient
}

// assertNotNil panics if the resource is nil.
func assertNotNil(v interface{}) {
	if v == nil {
		panic("nil resource")
	}
}
