package metricsutil

import (
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

var testLabel = SegmentLabel{
	SegmentID:     1,
	DatabaseName:  "db",
	ResourceGroup: "rg",
}

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

func TestBaseRecord(t *testing.T) {
	r := newBaseRecord(testLabel)
	assert.Equal(t, testLabel, r.Label())
	err := errors.New("test")
	r.finish(err)
	assert.Equal(t, err, r.getError())
	assert.NotZero(t, r.getDuration())
	assert.NotZero(t, r.getSeconds())
}

func TestSegmentAccessRecorder(t *testing.T) {
	mr := newSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	assert.Equal(t, mr.Label(), SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	assert.False(t, mr.isCacheMiss)
	assert.Zero(t, mr.waitLoadCost)
	assert.Zero(t, mr.getDuration())
	mr.CacheMissing()
	assert.True(t, mr.isCacheMiss)
	assert.NotZero(t, mr.waitLoadCost)
	assert.Zero(t, mr.getDuration())
	mr.finish(nil)
	assert.NotZero(t, mr.getDuration())

	mr = newSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	mr.CacheMissing()
	assert.True(t, mr.isCacheMiss)
	assert.NotZero(t, mr.waitLoadCost)
	assert.Zero(t, mr.getDuration())
	mr.finish(nil)
	assert.NotZero(t, mr.getDuration())

	mr = newSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	mr.finish(nil)
	assert.False(t, mr.isCacheMiss)
	assert.Zero(t, mr.waitLoadCost)
	assert.NotZero(t, mr.getDuration())
}

func TestSearchSegmentAccessMetric(t *testing.T) {
	m := NewSearchSegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.CacheMissing()
	m.Finish(nil)
	assert.NotZero(t, m.getDuration())
}

func TestQuerySegmentAccessMetric(t *testing.T) {
	m := NewQuerySegmentAccessRecord(SegmentLabel{
		SegmentID:     1,
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.CacheMissing()
	m.Finish(nil)
	assert.NotZero(t, m.getDuration())
}

func TestResourceEstimateRecord(t *testing.T) {
	r := NewResourceEstimateRecord(testLabel)
	assert.Equal(t, testLabel, r.Label())
	r.WithDiskUsage(1).WithMemUsage(2).WithRowNum(100)
	assert.Equal(t, uint64(1), r.DiskUsage)
	assert.Equal(t, uint64(2), r.MemUsage)
	assert.Equal(t, uint64(100), r.RowNum)
	r.Finish(nil)
}

func TestCacheRecord(t *testing.T) {
	r1 := NewCacheLoadRecord(testLabel)
	r1.WithBytes(1)
	assert.Equal(t, float64(1), r1.getBytes())
	r1.Finish(nil)
	r2 := NewCacheEvictRecord(testLabel)
	r2.Finish(nil)
}
