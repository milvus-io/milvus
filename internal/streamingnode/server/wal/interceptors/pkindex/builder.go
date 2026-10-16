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

	"github.com/cockroachdb/errors"

	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const interceptorName = "pkindex"

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates the builder of the primary key index interceptor.
// The interceptor must be placed between the replicate and the timetick interceptor.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

// collectionSchemaLister is the part of the shard manager that the interceptor
// needs to learn the collections that already exist when the WAL opens.
type collectionSchemaLister interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

// Build returns the passthrough interceptor when the streaming node has no
// index manager. The streaming node builds the manager at start exactly when
// streaming.pkindex.enabled is true, and that param can not change at runtime.
// So the manager, not the param, decides.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	ctx := context.TODO()
	mgr := pkindexManager()
	if mgr == nil {
		return passthroughInterceptor{}
	}

	// The WAL recovery owns the index of the pchannel: it creates it before
	// the interceptors are built and closes it with the WAL. The interceptor
	// only borrows it. No index means the recovery did not build one, and the
	// interceptor passes the messages through.
	ch := mgr.Get(param.ChannelInfo.Name)
	if ch == nil {
		return passthroughInterceptor{}
	}
	onFatal := onFatalOf(param)
	reg := newRegistry(ch)
	if lister, ok := param.ShardManager.(collectionSchemaLister); ok {
		for collectionID, info := range lister.GetAllCollectionSchemaInfos() {
			_, err := reg.add(ctx, info.VChannel, collectionID, info.Schema)
			if err == nil {
				continue
			}
			// The WAL can not serve the vchannel without its index. A local disk
			// error makes the WAL reject all appends, and the next open of the WAL
			// tries again. It does not stop the other WALs of the node.
			if errors.Is(err, errOpenIndex) {
				onFatal(err)
				break
			}
			// A collection that exists can not have a schema the index rejects.
			panic(fmt.Sprintf("create the primary key index of vchannel %s at wal open: %v", info.VChannel, err))
		}
	}
	// param.TxnManager is a pointer, so it must not be assigned to the interface
	// while it is nil. The interface would then be non-nil and carry a nil pointer.
	var sessions txnSessions
	if param.TxnManager != nil {
		sessions = param.TxnManager
	}
	return &appendInterceptor{
		ch:       ch,
		registry: reg,
		sessions: sessions,
		metrics:  newMetrics(),
		onFatal:  onFatal,
	}
}

// pkindexManager returns the index manager of the streaming node, nil when
// the resource or the manager is not set.
func pkindexManager() *snpkindex.Manager {
	r := resource.Resource()
	if r == nil {
		return nil
	}
	return r.PKIndexManager()
}

// tailOf returns a function that reads the WAL recovery tail. The function
// returns zeros when rs is nil.
func tailOf(rs recovery.RecoveryStorage) func() snpkindex.WALTail {
	if rs == nil {
		return func() snpkindex.WALTail { return snpkindex.WALTail{} }
	}
	return func() snpkindex.WALTail {
		m := rs.Metrics()
		return snpkindex.WALTail{Bytes: m.RecoveryTailBytes, RecoveryTimeTick: m.RecoveryTimeTick}
	}
}

// onFatalOf returns param.OnFatal. When it is nil, as in tests, the returned
// function only logs the error.
func onFatalOf(param *interceptors.InterceptorBuildParam) func(error) {
	if param.OnFatal != nil {
		return param.OnFatal
	}
	pchannel := param.ChannelInfo.Name
	return func(err error) {
		mlog.Error(context.TODO(), "primary key index failed, no handler marks the WAL unavailable",
			mlog.FieldPChannel(pchannel), mlog.Err(err))
	}
}

// passthroughInterceptor is used when the streaming node has no index manager.
type passthroughInterceptor struct{}

func (passthroughInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	return appendOp(ctx, msg)
}

func (passthroughInterceptor) Close() {}
