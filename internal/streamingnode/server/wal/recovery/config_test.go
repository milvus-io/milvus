package recovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
