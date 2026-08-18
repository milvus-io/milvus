package recovery

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// newConfig creates a new config for the recovery module.
func newConfig() *config {
	params := paramtable.Get()
	persistInterval := params.StreamingCfg.WALRecoveryPersistInterval.GetAsDurationByParse()
	ackStallTimeout := params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
	maxDirtyMessages := params.StreamingCfg.WALRecoveryMaxDirtyMessage.GetAsInt()
	taskConcurrency := params.StreamingCfg.WALRecoveryTaskConcurrency.GetAsInt()
	cfg := &config{
		persistInterval:  persistInterval,
		ackStallTimeout:  ackStallTimeout,
		maxDirtyMessages: maxDirtyMessages,
		taskConcurrency:  taskConcurrency,
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

// config is the configuration for the recovery module.
type config struct {
	persistInterval  time.Duration // persistInterval is the interval to persist the dirty recovery snapshot.
	ackStallTimeout  time.Duration // ackStallTimeout is the maximum wait before requesting VChannel data persistence.
	maxDirtyMessages int           // maxDirtyMessages is the maximum number of dirty messages to be persisted.
	taskConcurrency  int           // taskConcurrency is the max number of async recovery tasks running concurrently.
}

func (cfg *config) validate() error {
	if cfg.persistInterval <= 0 {
		return status.NewInvalidArgument("persist interval must be greater than 0")
	}
	if cfg.maxDirtyMessages <= 0 {
		return status.NewInvalidArgument("max dirty messages must be greater than 0")
	}
	return nil
}
