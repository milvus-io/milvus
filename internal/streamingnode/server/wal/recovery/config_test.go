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

func TestRecoveryTailAdditionalTriggerConfiguration(t *testing.T) {
	params := paramtable.Get()
	cfg := newConfig()
	require.Equal(t, uint64(6144), cfg.tailSoftWatermarkMessages)
	require.Equal(t, uint64(512<<20), cfg.tailIdleMinBytes)
	require.Equal(t, 10*time.Minute, cfg.tailIdleTimeout)
	for _, item := range []*paramtable.ParamItem{
		&params.StreamingCfg.WALRecoveryTailSoftWatermarkMessages,
		&params.StreamingCfg.WALRecoveryTailIdleMinSize,
		&params.StreamingCfg.WALRecoveryTailIdleTimeout,
	} {
		old := item.GetValue()
		t.Cleanup(func() { require.NoError(t, params.Save(item.Key, old)) })
	}
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryTailSoftWatermarkMessages.Key, "12"))
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryTailIdleMinSize.Key, "1MB"))
	require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryTailIdleTimeout.Key, "1s"))
	cfg = newConfig()
	require.Equal(t, uint64(12), cfg.tailSoftWatermarkMessages)
	require.Equal(t, uint64(1<<20), cfg.tailIdleMinBytes)
	require.Equal(t, time.Second, cfg.tailIdleTimeout)
	for _, value := range []string{"0", "-1"} {
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryTailSoftWatermarkMessages.Key, value))
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryTailIdleMinSize.Key, value))
		require.NoError(t, params.Save(params.StreamingCfg.WALRecoveryTailIdleTimeout.Key, value+"s"))
		cfg = newConfig()
		require.Zero(t, cfg.tailSoftWatermarkMessages)
		require.Zero(t, cfg.tailIdleMinBytes)
		require.LessOrEqual(t, cfg.tailIdleTimeout, time.Duration(0))
	}
}
