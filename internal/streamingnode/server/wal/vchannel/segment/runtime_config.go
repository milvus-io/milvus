package segment

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
)

// ViewOwner receives segment-owned asynchronous state changes. One owner is
// shared by all segment views in a vchannel, avoiding per-segment callbacks.
type ViewOwner interface {
	SegmentDataUpdated(segmentID int64, view *SegmentView)
}

// ViewConfig contains dependencies shared by segment views owned by one
// vchannel module.
type ViewConfig struct {
	Lifecycle  Lifecycle
	PackWriter PackWriter
	Runtime    moduleapi.Runtime
	Owner      ViewOwner
}

type runtimeConfig struct {
	lifecycle       Lifecycle
	packWriter      PackWriter
	runtime         moduleapi.Runtime
	flushPolicy     flushPolicy
	metaAndData     bool
	commitL1Limiter *commitL1Limiter
	owner           ViewOwner
}

func runtimeConfigFromViewConfig(config ViewConfig) runtimeConfig {
	return runtimeConfig{
		lifecycle:  config.Lifecycle,
		packWriter: config.PackWriter,
		runtime:    config.Runtime,
		owner:      config.Owner,
	}
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}
