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

package shard

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const pkIndexMetricName = "pkindex"

// collectionSchemaLister is the part of the shard manager that the interceptor
// needs to learn the collections that already exist when the WAL opens.
type collectionSchemaLister interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

func newPKIndexWriter(param *interceptors.InterceptorBuildParam) *pkIndexWriter {
	ctx := context.TODO()
	if !paramtable.Get().StreamingCfg.PKIndexEnabled.GetAsBool() {
		return nil
	}

	reg := newRegistry()
	// TODO: GetAllCollectionSchemaInfos skips a collection whose schema the
	// shard manager does not hold, so that collection gets no index and its
	// writes pass through.
	if lister, ok := param.ShardManager.(collectionSchemaLister); ok {
		for collectionID, info := range lister.GetAllCollectionSchemaInfos() {
			// A collection that exists can not have a schema the index rejects, and
			// the WAL can not serve the vchannel without its index.
			if _, err := reg.add(ctx, info.VChannel, collectionID, info.Schema); err != nil {
				panic(fmt.Sprintf("create the primary key index of vchannel %s at wal open: %v", info.VChannel, err))
			}
		}
	}
	// param.TxnManager is a pointer, so it must not be assigned to the interface
	// while it is nil. The interface would then be non-nil and carry a nil pointer.
	var sessions txnSessions
	if param.TxnManager != nil {
		sessions = param.TxnManager
	}
	return &pkIndexWriter{
		registry: reg,
		sessions: sessions,
		metrics:  newMetrics(),
	}
}
