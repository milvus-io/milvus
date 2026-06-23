package shard

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
)

func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
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
