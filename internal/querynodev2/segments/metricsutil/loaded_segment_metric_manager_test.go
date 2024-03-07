package metricsutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// deprecated: only for test.
func resetGlobalObserver() {
	globalObserver = newSegmentsObserver()
}

func TestLoadedSegmentMetricManager(t *testing.T) {
	resetGlobalObserver()
	sampler := newSampler()
	NewResourceEstimateRecord(testLabel).
		WithDiskUsage(2048).
		WithMemUsage(1024).Finish(nil)
	sampler.Scrape()

	manager := newLoadedSegmentMetricManager("1")
	manager.Apply(sampler)

	l := databaseLabel{
		database:      testLabel.DatabaseName,
		resourceGroup: testLabel.ResourceGroup,
	}
	assert.NotEmpty(t, manager.lastUsage)
	assert.Equal(t, uint64(1024), manager.lastUsage[l].MemUsage)
	assert.Equal(t, uint64(2048), manager.lastUsage[l].DiskUsage)

	// no new segment incoming, nothing changed.
	sampler.Scrape()
	manager.Apply(sampler)
	assert.NotEmpty(t, manager.lastUsage)
	assert.Equal(t, uint64(1024), manager.lastUsage[l].MemUsage)
	assert.Equal(t, uint64(2048), manager.lastUsage[l].DiskUsage)

	// new segment incoming, update the metrics.
	newLabel := testLabel
	newLabel.SegmentID = 19
	NewResourceEstimateRecord(newLabel).
		WithDiskUsage(4096).
		WithMemUsage(2048).Finish(nil)
	sampler.Scrape()
	manager.Apply(sampler)
	assert.NotEmpty(t, manager.lastUsage)
	assert.Equal(t, uint64(3072), manager.lastUsage[l].MemUsage)
	assert.Equal(t, uint64(6144), manager.lastUsage[l].DiskUsage)

	// new database incoming.
	newLabel = testLabel
	newLabel.DatabaseName = "db13"
	l2 := databaseLabel{
		database:      newLabel.DatabaseName,
		resourceGroup: newLabel.ResourceGroup,
	}
	NewResourceEstimateRecord(newLabel).
		WithDiskUsage(4096).
		WithMemUsage(2048).Finish(nil)
	sampler.Scrape()
	manager.Apply(sampler)

	assert.Equal(t, uint64(2048), manager.lastUsage[l2].MemUsage)
	assert.Equal(t, uint64(4096), manager.lastUsage[l2].DiskUsage)
	// do not change old one.
	assert.Equal(t, uint64(3072), manager.lastUsage[l].MemUsage)
	assert.Equal(t, uint64(6144), manager.lastUsage[l].DiskUsage)

	// test expired
	sampler.historyExpiration(time.Now())
	manager.Apply(sampler)
	assert.Empty(t, manager.lastUsage)
}
