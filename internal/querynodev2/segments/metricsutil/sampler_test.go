package metricsutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSampler(t *testing.T) {
	sampler := newSampler()
	assert.Equal(t, 0, len(sampler.GetHistory()))

	NewResourceEstimateRecord(testLabel).
		WithDiskUsage(10086).
		WithMemUsage(2048).
		Finish(nil)
	sampler.Scrape()
	assert.NotZero(t, len(sampler.GetHistory()))
	assert.NotNil(t, sampler.GetHistory()[testLabel])
	h := sampler.GetHistory()[testLabel]
	assert.NotNil(t, h.Snapshots[0])
	assert.Equal(t, uint64(10086), h.Snapshots[0].ResourceUsage.DiskUsage)
	assert.Equal(t, uint64(2048), h.Snapshots[0].ResourceUsage.MemUsage)

	// test expire.
	sampler.historyExpiration(time.Now())
	assert.Zero(t, len(sampler.GetHistory()))
}
