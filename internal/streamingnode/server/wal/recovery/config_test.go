package recovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRecoveryConfigValidatesTailWatermarks(t *testing.T) {
	valid := config{
		persistInterval:        time.Second,
		maxDirtyMessages:       1,
		tailLowWatermarkBytes:  1,
		tailSoftWatermarkBytes: 2,
		tailHighWatermarkBytes: 3,
	}
	require.NoError(t, valid.validate())

	for _, watermarks := range [][3]uint64{
		{0, 2, 3},
		{1, 1, 3},
		{1, 3, 3},
		{3, 2, 1},
	} {
		cfg := valid
		cfg.tailLowWatermarkBytes = watermarks[0]
		cfg.tailSoftWatermarkBytes = watermarks[1]
		cfg.tailHighWatermarkBytes = watermarks[2]
		require.Error(t, cfg.validate())
	}
}

func TestRecoveryFlushTriggerConfiguration(t *testing.T) {
	params := paramtable.Get()
	cfg := newConfig()
	require.Equal(t, uint64(6<<30), cfg.flushSizeBytes)
	require.Equal(t, uint64(6144), cfg.flushMessageCount)
	require.Equal(t, uint64(512<<20), cfg.flushIdleMinBytes)
	require.Equal(t, 10*time.Minute, cfg.flushIdleTimeout)
	for _, item := range []*paramtable.ParamItem{
		&params.StreamingCfg.WALRecoveryFlushSizeThreshold,
		&params.StreamingCfg.WALRecoveryFlushMessageThreshold,
		&params.StreamingCfg.WALRecoveryFlushIdleMinSize,
		&params.StreamingCfg.WALRecoveryFlushIdleTimeout,
	} {
		old := item.GetValue()
		t.Cleanup(func() { require.NoError(t, params.Save(item.Key, old)) })
	}
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushSizeThreshold.Key, "32GB"))
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushMessageThreshold.Key, "12"))
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushIdleMinSize.Key, "1MB"))
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushIdleTimeout.Key, "1s"))
	cfg = newConfig()
	require.Equal(t, uint64(32<<30), cfg.flushSizeBytes)
	require.Equal(t, uint64(4<<30), cfg.tailLowWatermarkBytes)
	require.Equal(t, uint64(8<<30), cfg.tailSoftWatermarkBytes)
	require.Equal(t, uint64(16<<30), cfg.tailHighWatermarkBytes)
	require.Equal(t, uint64(12), cfg.flushMessageCount)
	require.Equal(t, uint64(1<<20), cfg.flushIdleMinBytes)
	require.Equal(t, time.Second, cfg.flushIdleTimeout)
	for _, value := range []string{"0", "-1"} {
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushSizeThreshold.Key, value))
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushMessageThreshold.Key, value))
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushIdleMinSize.Key, value))
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryFlushIdleTimeout.Key, value+"s"))
		cfg = newConfig()
		require.Zero(t, cfg.flushSizeBytes)
		require.Zero(t, cfg.flushMessageCount)
		require.Zero(t, cfg.flushIdleMinBytes)
		require.LessOrEqual(t, cfg.flushIdleTimeout, time.Duration(0))
	}
}
