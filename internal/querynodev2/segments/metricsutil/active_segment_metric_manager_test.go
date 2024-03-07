package metricsutil

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestActiveSegmentMetricManagerWithDefaultPredicate(t *testing.T) {
	sampler := newSampler()

	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", "len(Snapshots) >= 4 && (((Snapshots[-1].AccessCount() - Snapshots[-4].AccessCount()) / (Snapshots[-1].Time.Sub(Snapshots[-4].Time).Seconds())) > 0.1)")
	manager := newActiveSegmentMetricManager("1")
	newLabel := testLabel
	newLabel.SegmentID = 10086
	NewResourceEstimateRecord(testLabel).
		WithDiskUsage(2048).
		WithMemUsage(1024).Finish(nil)

	NewResourceEstimateRecord(newLabel).
		WithDiskUsage(4096).
		WithMemUsage(2048).Finish(nil)

	// only one snapshot, no metrics should be updated.
	sampler.Scrape()
	manager.Apply(sampler)
	assert.Empty(t, manager.lastUsage)
	// small qps
	for i := 0; i < 100; i++ {
		NewQuerySegmentAccessRecord(testLabel).Finish(nil)
		NewQuerySegmentAccessRecord(newLabel).Finish(nil)
		if i%25 == 0 {
			sampler.Scrape()
		}
	}

	manager.Apply(sampler)
	assert.NotEmpty(t, manager.lastUsage)
	l := databaseLabel{
		database:      testLabel.DatabaseName,
		resourceGroup: testLabel.ResourceGroup,
	}
	assert.Equal(t, uint64(6144), manager.lastUsage[l].DiskUsage)
	assert.Equal(t, uint64(3072), manager.lastUsage[l].MemUsage)

	// high qps limit
	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", "len(Snapshots) >= 4 && (((Snapshots[-1].AccessCount() - Snapshots[-4].AccessCount()) / (Snapshots[-1].Time.Sub(Snapshots[-4].Time).Seconds())) > 1000000)")
	for i := 0; i < 10; i++ {
		NewQuerySegmentAccessRecord(testLabel).Finish(nil)
		time.Sleep(100 * time.Millisecond)
		sampler.Scrape()
	}

	manager.Apply(sampler)
	assert.Empty(t, manager.lastUsage)
}

func TestPredicateUpdate(t *testing.T) {
	expr := "len(Snapshots) >= 4 && (((Snapshots[-1].AccessCount() - Snapshots[-4].AccessCount()) / (Snapshots[-1].Time.Sub(Snapshots[-4].Time).Seconds())) > 0.1)"
	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", expr)

	m := newActiveSegmentMetricManager("1")
	assert.Empty(t, m.lastExpr)
	assert.Empty(t, m.enabledExpr)
	assert.Nil(t, m.exprProgram)

	m.updateActiveSegmentPredicate()
	assert.Equal(t, expr, m.lastExpr)
	assert.Equal(t, expr, m.enabledExpr)
	assert.NotNil(t, m.exprProgram)

	// illegal expr, using old one.
	expr2 := "....*..."
	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", expr2)
	m.updateActiveSegmentPredicate()
	assert.Equal(t, expr2, m.lastExpr)
	assert.Equal(t, expr, m.enabledExpr)
	assert.NotNil(t, m.exprProgram)

	// cannot pass the testing.
	expr2 = "Snapshots.Get()"
	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", expr2)
	m.updateActiveSegmentPredicate()
	assert.Equal(t, expr2, m.lastExpr)
	assert.Equal(t, expr, m.enabledExpr)
	assert.NotNil(t, m.exprProgram)

	// not bool expression.
	expr2 = "1"
	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", expr2)
	m.updateActiveSegmentPredicate()
	assert.Equal(t, expr2, m.lastExpr)
	assert.Equal(t, expr, m.enabledExpr)
	assert.NotNil(t, m.exprProgram)

	expr = "len(Snapshots) >= 5"
	paramtable.Get().Save("queryNode.activeSegmentPredicateExpr", expr)
	m.updateActiveSegmentPredicate()
	assert.Equal(t, expr, m.lastExpr)
	assert.Equal(t, expr, m.enabledExpr)
	assert.NotNil(t, m.exprProgram)
}
