// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"reflect"

	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/controller"
	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/kv"
)

var r *resourceImpl // singleton resource instance

// optResourceInit is the option to initialize the resource.
type optResourceInit func(r *resourceImpl)

// OptMetaKV provides the meta kv to the resource.
func OptMetaKV(metaKV kv.MetaKv) optResourceInit {
	return func(r *resourceImpl) {
		r.metaKV = metaKV
	}
}

// OptReplicateManagerClient provides the replicate manager client to the resource.
func OptReplicateManagerClient(replicateManagerClient replication.ReplicateManagerClient) optResourceInit {
	return func(r *resourceImpl) {
		r.replicateManagerClient = replicateManagerClient
	}
}

// OptReplicationCatalog provides the replication catalog to the resource.
func OptReplicationCatalog(catalog metastore.ReplicationCatalog) optResourceInit {
	return func(r *resourceImpl) {
		r.catalog = catalog
	}
}

// OptClusterClient provides the cluster client to the resource.
func OptClusterClient(clusterClient cluster.ClusterClient) optResourceInit {
	return func(r *resourceImpl) {
		r.clusterClient = clusterClient
	}
}

// OptController provides the controller to the resource.
func OptController(controller controller.Controller) optResourceInit {
	return func(r *resourceImpl) {
		r.controller = controller
	}
}

// Done finish all initialization of resources.
func Init(opts ...optResourceInit) {
	newR := &resourceImpl{}
	for _, opt := range opts {
		opt(newR)
	}

	newR.catalog = streamingcoord.NewReplicationCatalog(newR.MetaKV())
	newR.clusterClient = cluster.NewClusterClient()

	assertNotNil(newR.MetaKV())
	assertNotNil(newR.ReplicationCatalog())
	assertNotNil(newR.ClusterClient())
	assertNotNil(newR.ReplicateManagerClient())
	assertNotNil(newR.Controller())
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
	metaKV                 kv.MetaKv
	catalog                metastore.ReplicationCatalog
	clusterClient          cluster.ClusterClient
	replicateManagerClient replication.ReplicateManagerClient
	controller             controller.Controller
}

// MetaKV returns the meta kv.
func (r *resourceImpl) MetaKV() kv.MetaKv {
	return r.metaKV
}

// ReplicationCatalog returns the replication catalog.
func (r *resourceImpl) ReplicationCatalog() metastore.ReplicationCatalog {
	return r.catalog
}

// ClusterClient returns the cluster client.
func (r *resourceImpl) ClusterClient() cluster.ClusterClient {
	return r.clusterClient
}

// ReplicateManagerClient returns the replicate manager client.
func (r *resourceImpl) ReplicateManagerClient() replication.ReplicateManagerClient {
	return r.replicateManagerClient
}

// Controller returns the controller.
func (r *resourceImpl) Controller() controller.Controller {
	return r.controller
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
