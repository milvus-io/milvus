package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TsetSegmentResourceUsage(t *testing.T) {
	usage := SegmentResourceUsage{}
	assert.Equal(t, uint64(0), usage.GetInuseOrPredictDiskUsage())
	assert.Equal(t, uint64(0), usage.BinLogSizeAtOSS)
	assert.Equal(t, "Predict: (Mem: 0, Disk: 0), InUsed: (Mem: 0, Disk: 0), BinLogSizeAtOSS: (0)", usage.String())

	usage.Predict.DiskSize = 100
	assert.Equal(t, uint64(100), usage.GetInuseOrPredictDiskUsage())

	usage.InUsed.DiskSize = 200
	assert.Equal(t, uint64(200), usage.GetInuseOrPredictDiskUsage())
}
