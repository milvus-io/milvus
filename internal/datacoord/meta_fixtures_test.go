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

package datacoord

import (
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"

	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"

	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"

	"github.com/milvus-io/milvus/pkg/v3/kv"
)

var _ kv.MetaKv = &metaMemoryKV{}

// newMetaWithEtcd builds a meta over an etcd catalog under rootPath without domain metas.
func newMetaWithEtcd(t *testing.T, rootPath string) (*meta, error) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	catalogKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	return newMetaWithLoader(t, datacoord.NewCatalog(catalogKV, "", rootPath), nil, withoutDomainMetas)
}

var (
	partitionKeyID = UniqueID(800)
	statsTaskID    = UniqueID(900)
)
