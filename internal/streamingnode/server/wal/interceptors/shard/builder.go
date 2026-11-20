package shard

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	enabled := &paramtable.Get().StreamingCfg.PKIndexEnabled
	if enabled.GetAsBool() && !HasEngineFactory() {
		panic(enabled.Key + " is true but this binary has no primary key index engine")
	}
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	writer := newPKIndexWriter(param)
	timeTick := timetick.NewInterceptorBuilder().Build(param)
	return &shardWriteInterceptor{
		pkindex:  writer,
		timeTick: timeTick,
		physical: interceptors.NewChainedInterceptor(timeTick, newShardInterceptor(param)),
	}
}

func newShardInterceptor(param *interceptors.InterceptorBuildParam) *shardInterceptor {
	shardInterceptor := &shardInterceptor{
		shardManager: param.ShardManager,
	}
	if schemaProvider, ok := param.ShardManager.(collectionSchemaProvider); ok {
		for collectionID, schemaInfo := range schemaProvider.GetAllCollectionSchemaInfos() {
			shardInterceptor.allocFunctionRunners(collectionID, schemaInfo.VChannel, schemaInfo.Schema)
		}
	}
	shardInterceptor.initOpTable()
	return shardInterceptor
}
