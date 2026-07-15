package vchannel

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"

type runtimeConfig struct {
	metaAndData bool
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}

func emptyObserveResult() moduleapi.ObserveResult {
	return moduleapi.ObserveResult{}
}
