package resource

import (
	"reflect"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/configuration"
	"github.com/milvus-io/milvus/internal/cdc/replicatemanager"
	"github.com/milvus-io/milvus/internal/cdc/replicatestream"
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

// Done finish all initialization of resources.
func Init(opts ...optResourceInit) {
	newR := &resourceImpl{}
	for _, opt := range opts {
		opt(newR)
	}
	newR.configManager = configuration.NewManager()
	// TODO: init
	assertNotNil(newR.ConfigManager())
	assertNotNil(newR.ClusterClient())
	assertNotNil(newR.ReplicateManagerClient())
	assertNotNil(newR.ReplicateStreamClient())
	r = newR
}

// Release releases the singleton of resources.
func Release() {}

// Resource access the underlying singleton of resources.
func Resource() *resourceImpl {
	return r
}

// resourceImpl is a basic resource dependency for streamingnode server.
// All utility on it is concurrent-safe and singleton.
type resourceImpl struct {
	etcdClient             *clientv3.Client
	configManager          configuration.Manager
	clusterClient          cluster.ClusterClient
	replicateManagerClient replicatemanager.ReplicateManagerClient
	replicateStreamClient  replicatestream.ReplicateStreamClient
}

// ETCD returns the etcd client.
func (r *resourceImpl) ETCD() *clientv3.Client {
	return r.etcdClient
}

// ConfigManager returns the configuration manager.
func (r *resourceImpl) ConfigManager() configuration.Manager {
	return r.configManager
}

// ClusterClient returns the cluster client.
func (r *resourceImpl) ClusterClient() cluster.ClusterClient {
	return r.clusterClient
}

// ReplicateManagerClient returns the replicate manager client.
func (r *resourceImpl) ReplicateManagerClient() replicatemanager.ReplicateManagerClient {
	return r.replicateManagerClient
}

// ReplicateStreamClient returns the replicate stream client.
func (r *resourceImpl) ReplicateStreamClient() replicatestream.ReplicateStreamClient {
	return r.replicateStreamClient
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
