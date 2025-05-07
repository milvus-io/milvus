package stats

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestStatsManager(t *testing.T) {
	paramtable.Init()
	m := NewStatsManager()

	sealOperator1 := mock_utils.NewMockSealOperator(t)
	sealOperator1.EXPECT().Channel().Return(types.PChannelInfo{Name: "pchannel"})
	sealOperator1.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	m.RegisterSealOperator(sealOperator1, nil, nil)
	assert.NotEmpty(t, m.sealOperators)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel", CollectionID: 1, PartitionID: 2, SegmentID: 3}, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 1)
	assert.Len(t, m.vchannelStats, 1)
	assert.Len(t, m.pchannelStats, 1)
	assert.Len(t, m.segmentIndex, 1)
	assert.NotEmpty(t, m.sealOperators)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel", CollectionID: 1, PartitionID: 3, SegmentID: 4}, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 2)
	assert.Len(t, m.segmentIndex, 2)
	assert.Len(t, m.vchannelStats, 1)
	assert.Len(t, m.pchannelStats, 1)
	assert.NotEmpty(t, m.sealOperators)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel2", CollectionID: 2, PartitionID: 4, SegmentID: 5}, createSegmentStats(100, 100, 300))
	assert.Len(t, m.segmentStats, 3)
	assert.Len(t, m.segmentIndex, 3)
	assert.Len(t, m.vchannelStats, 2)
	assert.Len(t, m.pchannelStats, 1)
	assert.NotEmpty(t, m.sealOperators)

	assert.Panics(t, func() {
		// register a growing segment that sealOperator is not registered should panic
		m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel2", VChannel: "vchannel3", CollectionID: 2, PartitionID: 5, SegmentID: 6}, createSegmentStats(100, 100, 300))
	})

	sealOperator2 := mock_utils.NewMockSealOperator(t)
	sealOperator2.EXPECT().Channel().Return(types.PChannelInfo{Name: "pchannel2"})
	sealOperator2.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	m.RegisterSealOperator(
		sealOperator2,
		[]SegmentBelongs{{PChannel: "pchannel2", VChannel: "vchannel3", CollectionID: 2, PartitionID: 5, SegmentID: 6}},
		[]*SegmentStats{createSegmentStats(100, 100, 300)},
	)
	assert.Len(t, m.segmentStats, 4)
	assert.Len(t, m.segmentIndex, 4)
	assert.Len(t, m.vchannelStats, 3)
	assert.Len(t, m.pchannelStats, 2)
	assert.NotEmpty(t, m.sealOperators)

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel2", VChannel: "vchannel3", CollectionID: 2, PartitionID: 5, SegmentID: 7}, createSegmentStats(0, 0, 300))
	assert.Len(t, m.segmentStats, 5)
	assert.Len(t, m.segmentIndex, 5)
	assert.Len(t, m.vchannelStats, 3)
	assert.Len(t, m.pchannelStats, 2)
	assert.NotEmpty(t, m.sealOperators)

	assert.Panics(t, func() {
		// register a growing segment that that already exists should panic.
		m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel", CollectionID: 1, PartitionID: 2, SegmentID: 3}, createSegmentStats(100, 100, 300))
	})

	err := m.AllocRows(3, InsertMetrics{Rows: 50, BinarySize: 50})
	assert.NoError(t, err)
	stat := m.GetStatsOfSegment(3)
	assert.Equal(t, uint64(150), stat.Insert.BinarySize)

	err = m.AllocRows(5, InsertMetrics{Rows: 250, BinarySize: 250})
	assert.ErrorIs(t, err, ErrNotEnoughSpace)

	err = m.AllocRows(6, InsertMetrics{Rows: 150, BinarySize: 150})
	assert.NoError(t, err)

	assert.Equal(t, uint64(250), m.vchannelStats["vchannel3"].BinarySize)
	assert.Equal(t, uint64(100), m.vchannelStats["vchannel2"].BinarySize)
	assert.Equal(t, uint64(250), m.vchannelStats["vchannel"].BinarySize)

	assert.Equal(t, uint64(350), m.pchannelStats["pchannel"].BinarySize)
	assert.Equal(t, uint64(250), m.pchannelStats["pchannel2"].BinarySize)

	m.UpdateOnSync(3, SyncOperationMetrics{BinLogCounterIncr: 100})
	m.UpdateOnSync(1000, SyncOperationMetrics{BinLogCounterIncr: 100})

	err = m.AllocRows(3, InsertMetrics{Rows: 400, BinarySize: 400})
	assert.ErrorIs(t, err, ErrNotEnoughSpace)
	err = m.AllocRows(5, InsertMetrics{Rows: 250, BinarySize: 250})
	assert.ErrorIs(t, err, ErrNotEnoughSpace)
	err = m.AllocRows(6, InsertMetrics{Rows: 400, BinarySize: 400})
	assert.ErrorIs(t, err, ErrNotEnoughSpace)

	err = m.AllocRows(7, InsertMetrics{Rows: 400, BinarySize: 400})
	assert.ErrorIs(t, err, ErrTooLargeInsert)

	m.UnregisterSealedSegment(3)
	m.UnregisterSealedSegment(4)
	m.UnregisterSealedSegment(5)
	m.UnregisterSealedSegment(6)
	m.UnregisterSealedSegment(7)
	assert.Empty(t, m.segmentStats)
	assert.Empty(t, m.vchannelStats)
	assert.Empty(t, m.pchannelStats)
	assert.Empty(t, m.segmentIndex)
	assert.NotEmpty(t, m.sealOperators)

	assert.Panics(t, func() {
		m.AllocRows(100, InsertMetrics{Rows: 100, BinarySize: 100})
	})

	assert.Panics(t, func() {
		m.UnregisterSealedSegment(1)
	})

	m.RegisterNewGrowingSegment(SegmentBelongs{PChannel: "pchannel", VChannel: "vchannel2", CollectionID: 2, PartitionID: 4, SegmentID: 5}, createSegmentStats(100, 100, 300))
	m.UnregisterSealOperator(sealOperator1)
	m.UnregisterSealOperator(sealOperator2)
	assert.Empty(t, m.segmentStats)
	assert.Empty(t, m.vchannelStats)
	assert.Empty(t, m.pchannelStats)
	assert.Empty(t, m.segmentIndex)
	assert.Empty(t, m.sealOperators)
}

func TestConcurrentStasManager(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.SegmentMaxBinlogFileNumber.Key, "5")
	params.Save(params.StreamingCfg.FlushMemoryThreshold.Key, "0.000003")
	params.Save(params.StreamingCfg.FlushGrowingSegmentBytesHwmThreshold.Key, "0.000002")
	params.Save(params.StreamingCfg.FlushGrowingSegmentBytesLwmThreshold.Key, "0.000001")
	params.Save(params.DataCoordCfg.SegmentMaxLifetime.Key, "0.3")
	params.Save(params.DataCoordCfg.SegmentMaxIdleTime.Key, "0.1")
	params.Save(params.DataCoordCfg.SegmentMinSizeFromIdleToSealed.Key, "1024")
	defaultSealWorkerTimerInterval = 10 * time.Millisecond

	m := NewStatsManager()

	pchannelCount := 100
	segments := typeutil.NewConcurrentSet[SegmentBelongs]()
	counter := atomic.NewInt64(0)
	for i := 0; i < pchannelCount; i++ {
		sealOperator := mock_utils.NewMockSealOperator(t)
		sealOperator.EXPECT().Channel().Return(types.PChannelInfo{Name: fmt.Sprintf("pchannel-%d", i)})
		sealOperator.EXPECT().AsyncFlushSegment(mock.Anything).Run(func(info utils.SealSegmentSignal) {
			segments.Remove(info.SegmentBelongs)
			m.UnregisterSealedSegment(info.SegmentBelongs.SegmentID)
			counter.Dec()
		})
		m.RegisterSealOperator(sealOperator, nil, nil)
	}

	allocRows := func(segment SegmentBelongs) bool {
		defer func() {
			// because the alloc rows may panic, so we need to recover it.
			// we don't handle it in the test case, because we don't want to ignore the panic.
			recover()
		}()
		rows := 100 + rand.Int63n(100)
		binarySize := 100 + rand.Int63n(100)
		_ = m.AllocRows(segment.SegmentID, InsertMetrics{Rows: uint64(rows), BinarySize: uint64(binarySize)})

		if rand.Int31n(2) > 0 {
			m.UpdateOnSync(segment.SegmentID, SyncOperationMetrics{BinLogCounterIncr: 100})
		}
		return true
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer wg.Done()
		segmentID := int64(0)
		for i := int64(0); i < int64(pchannelCount); i++ {
			for j := 0; j < 10; j++ {
				segmentID++
				segment := SegmentBelongs{
					PChannel:     fmt.Sprintf("pchannel-%d", i),
					VChannel:     fmt.Sprintf("vchannel-%d", i),
					CollectionID: i,
					PartitionID:  i + 1,
					SegmentID:    segmentID,
				}
				rows := 1000 + rand.Int63n(1000)
				binarySize := 1000 + rand.Int63n(1000)
				maxBinarySize := 4000 + rand.Int63n(10000)
				stats := createSegmentStats(uint64(rows), uint64(binarySize), uint64(maxBinarySize))
				m.RegisterNewGrowingSegment(segment, stats)
				counter.Inc()
				segments.Insert(segment)
			}
		}
	}()
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				time.Sleep(5 * time.Millisecond)
				segments.Range(allocRows)
			}
		}()
	}
	wg.Wait()

	// execute until there's no segment left
	for {
		segments.Range(allocRows)
		if counter.Load() == 0 {
			break
		}
	}
	assert.Equal(t, m.totalStats.Rows, uint64(0))
	assert.Equal(t, m.totalStats.BinarySize, uint64(0))
	assert.Empty(t, m.segmentStats)
	assert.Empty(t, m.vchannelStats)
	assert.Empty(t, m.pchannelStats)
	assert.Empty(t, m.segmentIndex)
	assert.NotEmpty(t, m.sealOperators)
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
