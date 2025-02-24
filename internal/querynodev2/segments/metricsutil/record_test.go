package metricsutil

import (
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var testLabel = SegmentLabel{
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
	assert.NotZero(t, r.getMilliseconds())
}

func TestSegmentAccessRecorder(t *testing.T) {
	mr := newSegmentAccessRecord(SegmentLabel{
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	assert.Equal(t, mr.Label(), SegmentLabel{
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
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.CacheMissing()
	m.Finish(nil)
	assert.NotZero(t, m.getDuration())
}

func TestQuerySegmentAccessMetric(t *testing.T) {
	m := NewQuerySegmentAccessRecord(SegmentLabel{
		DatabaseName:  "db1",
		ResourceGroup: "rg1",
	})
	m.CacheMissing()
	m.Finish(nil)
	assert.NotZero(t, m.getDuration())
}

func TestCacheRecord(t *testing.T) {
	r1 := NewCacheLoadRecord(testLabel)
	r1.WithBytes(1)
	assert.Equal(t, float64(1), r1.getBytes())
	r1.Finish(nil)
	r2 := NewCacheEvictRecord(testLabel)
	r2.Finish(nil)
}
