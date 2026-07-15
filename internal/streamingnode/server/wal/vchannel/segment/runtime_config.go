package segment

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
)

type runtimeConfig struct {
	lifecycle       Lifecycle
	packWriter      PackWriter
	runtime         moduleapi.Runtime
	onDataUpdated   func()
	onSegmentSealed func(walview.SegmentSealedEvent)
	flushPolicy     flushPolicy
	metaAndData     bool
	commitL1Limiter *commitL1Limiter
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}
