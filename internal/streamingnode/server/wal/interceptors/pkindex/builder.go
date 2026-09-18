package pkindex

import (
	"context"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	ctx := context.TODO()
	if !paramtable.Get().StreamingCfg.PKIndexEnabled.GetAsBool() {
		return passthroughInterceptor{}
	}
	if !authority.HasEngineFactory() {
		mlog.Warn(ctx, "the primary key index is enabled but this binary has no index engine, the index stays off",
			mlog.FieldPChannel(param.ChannelInfo.Name))
		return passthroughInterceptor{}
	}

	reg := newRegistry(paramtable.Get().StreamingCfg.PKIndexLockStripes.GetAsInt())
	if lister, ok := param.ShardManager.(collectionSchemaLister); ok {
		for collectionID, info := range lister.GetAllCollectionSchemaInfos() {
			reg.add(ctx, info.VChannel, collectionID, info.Schema)
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
