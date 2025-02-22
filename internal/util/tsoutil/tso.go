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

package tsoutil

import (
	"path"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/v2/kv"
)

// NewTSOKVBase returns a kv.TxnKV object
func NewTSOKVBase(client *clientv3.Client, tsoRoot, subPath string) kv.TxnKV {
	return etcdkv.NewEtcdKV(client, path.Join(tsoRoot, subPath))
}

// NewTSOTiKVBase returns a kv.TxnKV object
func NewTSOTiKVBase(client *txnkv.Client, tsoRoot, subPath string) kv.TxnKV {
	return tikv.NewTiKV(client, path.Join(tsoRoot, subPath))
}
