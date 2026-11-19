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
		txnManager:   param.TxnManager,
	}
	if schemaProvider, ok := param.ShardManager.(collectionSchemaProvider); ok {
		for collectionID, schemaInfo := range schemaProvider.GetAllCollectionSchemaInfos() {
			shardInterceptor.allocFunctionRunners(collectionID, schemaInfo.VChannel, schemaInfo.Schema)
		}
	}
	if snapshot := param.InitialRecoverSnapshot; snapshot != nil && snapshot.WritePathRecovery != nil {
		for channel, state := range snapshot.WritePathRecovery.VChannels {
			if state.SplitGenesisTaskID != 0 {
				shardInterceptor.genesis.Store(state.CollectionID, splitGenesis{state.SplitGenesisTaskID, state.SplitSourceVChannel, channel})
				shardInterceptor.addSplitTargetGate(channel, state.CollectionID, state.SplitSourceVChannel)
			}
		}
	}
	shardInterceptor.initOpTable()
	return shardInterceptor
}
