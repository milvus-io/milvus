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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/cdc/replication"
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

// OptReplicateManagerClient provides the replicate manager client to the resource.
func OptReplicateManagerClient(replicateManagerClient replication.ReplicateManagerClient) optResourceInit {
	return func(r *resourceImpl) {
		r.replicateManagerClient = replicateManagerClient
	}
}

// Done finish all initialization of resources.
func Init(opts ...optResourceInit) {
	newR := &resourceImpl{}
	for _, opt := range opts {
		opt(newR)
	}

	assertNotNil(newR.ETCD())
	assertNotNil(newR.ReplicateManagerClient())
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
	replicateManagerClient replication.ReplicateManagerClient
}

// ETCD returns the etcd client.
func (r *resourceImpl) ETCD() *clientv3.Client {
	return r.etcdClient
}

// ReplicateManagerClient returns the replicate manager client.
func (r *resourceImpl) ReplicateManagerClient() replication.ReplicateManagerClient {
	return r.replicateManagerClient
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
