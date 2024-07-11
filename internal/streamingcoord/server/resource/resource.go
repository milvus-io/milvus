package resource

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/metastore"
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

// OptStreamingCatalog provides streaming catalog to the resource.
func OptStreamingCatalog(catalog metastore.StreamingCoordCataLog) optResourceInit {
	return func(r *resourceImpl) {
		r.streamingCatalog = catalog
	}
}

// Init initializes the singleton of resources.
// Should be call when streaming node startup.
func Init(opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	assertNotNil(r.ETCD())
	assertNotNil(r.StreamingCatalog())
}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	etcdClient       *clientv3.Client
	streamingCatalog metastore.StreamingCoordCataLog
}

// StreamingCatalog returns the StreamingCatalog client.
func (r *resourceImpl) StreamingCatalog() metastore.StreamingCoordCataLog {
	return r.streamingCatalog
}

// ETCD returns the etcd client.
func (r *resourceImpl) ETCD() *clientv3.Client {
	return r.etcdClient
}

// assertNotNil panics if the resource is nil.
func assertNotNil(v interface{}) {
	if v == nil {
		panic("nil resource")
	}
}
