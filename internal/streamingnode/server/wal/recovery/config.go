package recovery

import (
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newConfig creates a new config for the recovery module.
func newConfig() *config {
	params := paramtable.Get()
	persistInterval := params.StreamingCfg.WALRecoveryPersistInterval.GetAsDurationByParse()
	maxDirtyMessages := params.StreamingCfg.WALRecoveryMaxDirtyMessage.GetAsInt()
	gracefulTimeout := params.StreamingCfg.WALRecoveryGracefulCloseTimeout.GetAsDurationByParse()
	cfg := &config{
		persistInterval:  persistInterval,
		maxDirtyMessages: maxDirtyMessages,
		gracefulTimeout:  gracefulTimeout,
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

// config is the configuration for the recovery module.
type config struct {
	persistInterval  time.Duration // persistInterval is the interval to persist the dirty recovery snapshot.
	maxDirtyMessages int           // maxDirtyMessages is the maximum number of dirty messages to be persisted.
	gracefulTimeout  time.Duration // gracefulTimeout is the timeout for graceful close of recovery module.
}

func (cfg *config) validate() error {
	if cfg.persistInterval <= 0 {
		return errors.New("persist interval must be greater than 0")
	}
	if cfg.maxDirtyMessages <= 0 {
		return errors.New("max dirty messages must be greater than 0")
	}
	if cfg.gracefulTimeout <= 0 {
		return errors.New("graceful timeout must be greater than 0")
	}
	return nil
}

// newTruncatorConfig creates a new config for the truncator.
func newTruncatorConfig() *truncatorConfig {
	params := paramtable.Get()
	samplerInterval := params.StreamingCfg.WALTruncateSampleInterval.GetAsDurationByParse()
	retentionInterval := params.StreamingCfg.WALTruncateRetentionInterval.GetAsDurationByParse()
	cfg := &truncatorConfig{
		sampleInterval:    samplerInterval,
		retentionInterval: retentionInterval,
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

// truncatorConfig is the configuration for the truncator.
type truncatorConfig struct {
	sampleInterval    time.Duration // the interval to sample the checkpoint
	retentionInterval time.Duration // the retention interval to sample the checkpoint
}

// validate validates the truncator config.
func (cfg *truncatorConfig) validate() error {
	if cfg.sampleInterval <= 0 {
		return errors.New("sampler interval must be greater than 0")
	}
	if cfg.retentionInterval <= 0 {
		return errors.New("retention interval must be greater than 0")
	}
	return nil
}
