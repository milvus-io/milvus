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

package pkindex

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const interceptorName = "pkindex"

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates the builder of the primary key index interceptor.
// The interceptor must be placed between the replicate and the timetick interceptor.
// It panics when the index is enabled but the binary has no index engine, so a
// misconfigured streaming node fails at start instead of writing without the index.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	enabled := &paramtable.Get().StreamingCfg.PKIndexEnabled
	if enabled.GetAsBool() && !authority.HasEngineFactory() {
		panic(enabled.Key + " is true but this binary has no primary key index engine")
	}
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

// collectionSchemaLister is the part of the shard manager that the interceptor
// needs to learn the collections that already exist when the WAL opens.
type collectionSchemaLister interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	ctx := context.TODO()
	if !paramtable.Get().StreamingCfg.PKIndexEnabled.GetAsBool() {
		return passthroughInterceptor{}
	}

	reg := newRegistry(paramtable.Get().StreamingCfg.PKIndexLockStripes.GetAsInt())
	if lister, ok := param.ShardManager.(collectionSchemaLister); ok {
		for collectionID, info := range lister.GetAllCollectionSchemaInfos() {
			// A collection that exists can not have a schema the index rejects, and
			// the WAL can not serve the vchannel without its index.
			if err := reg.add(ctx, info.VChannel, collectionID, info.Schema); err != nil {
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
	return &appendInterceptor{
		registry: reg,
		sessions: sessions,
		metrics:  newMetrics(),
	}
}

// passthroughInterceptor is used when the index is off.
type passthroughInterceptor struct{}

func (passthroughInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	return appendOp(ctx, msg)
}

func (passthroughInterceptor) Close() {}
