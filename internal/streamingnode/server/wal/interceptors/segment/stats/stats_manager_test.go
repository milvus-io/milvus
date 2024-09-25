package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatsManager(t *testing.T) {
	m := NewStatsManager()

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel", CollectionID: 1, PartitionID: 2, SegmentID: 3}, 3, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 1)
	assert.Len(t, m.vchannelStats, 1)
	assert.Len(t, m.pchannelStats, 1)
	assert.Len(t, m.segmentIndex, 1)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel", CollectionID: 1, PartitionID: 3, SegmentID: 4}, 4, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 2)
	assert.Len(t, m.segmentIndex, 2)
	assert.Len(t, m.vchannelStats, 1)
	assert.Len(t, m.pchannelStats, 1)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel2", CollectionID: 2, PartitionID: 4, SegmentID: 5}, 5, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 3)
	assert.Len(t, m.segmentIndex, 3)
	assert.Len(t, m.vchannelStats, 2)
	assert.Len(t, m.pchannelStats, 1)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel2", VChannel: "vchannel3", CollectionID: 2, PartitionID: 5, SegmentID: 6}, 6, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 4)
	assert.Len(t, m.segmentIndex, 4)
	assert.Len(t, m.vchannelStats, 3)
	assert.Len(t, m.pchannelStats, 2)

	assert.Panics(t, func() {
		m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel", CollectionID: 1, PartitionID: 2, SegmentID: 3}, 3, createSegmentStats(100, 100, 300))
	})

	shouldBlock(t, m.SealNotifier().WaitChan())

	m.AllocRows(3, InsertMetrics{Rows: 50, BinarySize: 50})
	stat := m.GetStatsOfSegment(3)
	assert.Equal(t, uint64(150), stat.Insert.BinarySize)

	shouldBlock(t, m.SealNotifier().WaitChan())
	m.AllocRows(5, InsertMetrics{Rows: 250, BinarySize: 250})
	<-m.SealNotifier().WaitChan()
	infos := m.SealNotifier().Get()
	assert.Len(t, infos, 1)

	m.AllocRows(6, InsertMetrics{Rows: 150, BinarySize: 150})
	shouldBlock(t, m.SealNotifier().WaitChan())

	assert.Equal(t, uint64(250), m.vchannelStats["vchannel3"].BinarySize)
	assert.Equal(t, uint64(100), m.vchannelStats["vchannel2"].BinarySize)
	assert.Equal(t, uint64(250), m.vchannelStats["vchannel"].BinarySize)

	assert.Equal(t, uint64(350), m.pchannelStats["pchannel"].BinarySize)
	assert.Equal(t, uint64(250), m.pchannelStats["pchannel2"].BinarySize)

	m.UpdateOnFlush(3, FlushOperationMetrics{BinLogCounter: 100})
	<-m.SealNotifier().WaitChan()
	infos = m.SealNotifier().Get()
	assert.Len(t, infos, 1)
	m.UpdateOnFlush(1000, FlushOperationMetrics{BinLogCounter: 100})
	shouldBlock(t, m.SealNotifier().WaitChan())

	m.AllocRows(3, InsertMetrics{Rows: 400, BinarySize: 400})
	m.AllocRows(5, InsertMetrics{Rows: 250, BinarySize: 250})
	m.AllocRows(6, InsertMetrics{Rows: 400, BinarySize: 400})
	<-m.SealNotifier().WaitChan()
	infos = m.SealNotifier().Get()
	assert.Len(t, infos, 3)

	m.UnregisterSealedSegment(3)
	m.UnregisterSealedSegment(4)
	m.UnregisterSealedSegment(5)
	m.UnregisterSealedSegment(6)
	assert.Empty(t, m.segmentStats)
	assert.Empty(t, m.vchannelStats)
	assert.Empty(t, m.pchannelStats)
	assert.Empty(t, m.segmentIndex)

	assert.Panics(t, func() {
		m.AllocRows(100, InsertMetrics{Rows: 100, BinarySize: 100})
	})
	assert.Panics(t, func() {
		m.UnregisterSealedSegment(1)
	})
}

func createSegmentStats(row uint64, binarySize uint64, maxBinarSize uint64) *SegmentStats {
	return &SegmentStats{
		Insert: InsertMetrics{
			Rows:       row,
			BinarySize: binarySize,
		},
		MaxBinarySize:    maxBinarSize,
		CreateTime:       time.Now(),
		LastModifiedTime: time.Now(),
		BinLogCounter:    0,
	}
}

func shouldBlock(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		t.Errorf("should block but not")
	case <-time.After(10 * time.Millisecond):
		return
	}
}
