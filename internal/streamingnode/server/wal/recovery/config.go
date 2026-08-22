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
	tailLowWatermark := params.StreamingCfg.WALRecoveryTailLowWatermark.GetAsSize()
	tailSoftWatermark := params.StreamingCfg.WALRecoveryTailSoftWatermark.GetAsSize()
	tailHighWatermark := params.StreamingCfg.WALRecoveryTailHighWatermark.GetAsSize()
	cfg := &config{
		persistInterval:        persistInterval,
		ackStallTimeout:        ackStallTimeout,
		maxDirtyMessages:       maxDirtyMessages,
		taskConcurrency:        taskConcurrency,
		tailLowWatermarkBytes:  nonNegativeSize(tailLowWatermark),
		tailSoftWatermarkBytes: nonNegativeSize(tailSoftWatermark),
		tailHighWatermarkBytes: nonNegativeSize(tailHighWatermark),
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

func nonNegativeSize(size int64) uint64 {
	if size <= 0 {
		return 0
	}
	return uint64(size)
}

// config is the configuration for the recovery module.
type config struct {
	persistInterval        time.Duration // persistInterval is the interval to persist the dirty recovery snapshot.
	ackStallTimeout        time.Duration // ackStallTimeout is the maximum wait before requesting VChannel data persistence.
	maxDirtyMessages       int           // maxDirtyMessages is the maximum number of dirty messages to be persisted.
	taskConcurrency        int           // taskConcurrency is the max number of async recovery tasks running concurrently.
	tailLowWatermarkBytes  uint64        // tailLowWatermarkBytes releases append pressure after checkpoint publication catches up.
	tailSoftWatermarkBytes uint64        // tailSoftWatermarkBytes triggers VChannel persistence and append slowdown.
	tailHighWatermarkBytes uint64        // tailHighWatermarkBytes rejects new DML appends.
}

func (cfg *config) validate() error {
	if cfg.persistInterval <= 0 {
		return status.NewInvalidArgument("persist interval must be greater than 0")
	}
	if cfg.maxDirtyMessages <= 0 {
		return status.NewInvalidArgument("max dirty messages must be greater than 0")
	}
	if cfg.tailLowWatermarkBytes == 0 ||
		cfg.tailLowWatermarkBytes >= cfg.tailSoftWatermarkBytes ||
		cfg.tailSoftWatermarkBytes >= cfg.tailHighWatermarkBytes {
		return status.NewInvalidArgument(
			"recovery tail watermarks must satisfy 0 < low < soft < high",
		)
	}
	return nil
}
