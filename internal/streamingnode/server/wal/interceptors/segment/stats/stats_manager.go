package stats

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type (
	SegmentStats         = utils.SegmentStats
	InsertMetrics        = utils.InsertMetrics
	SegmentBelongs       = utils.SegmentBelongs
	SyncOperationMetrics = utils.SyncOperationMetrics
	SealOperator         = utils.SealOperator
)

var (
	ErrNotEnoughSpace = errors.New("not enough space")
	ErrTooLargeInsert = errors.New("insert too large")
)

// StatsManager is the manager of stats.
// It manages the insert stats of all segments, used to check if a segment has enough space to insert or should be sealed.
// If there will be a lock contention, we can optimize it by apply lock per segment.
type StatsManager struct {
	log.Binder
	worker        *sealWorker
	mu            sync.Mutex
	cfg           statsConfig
	totalStats    InsertMetrics
	pchannelStats map[string]*InsertMetrics
	vchannelStats map[string]*InsertMetrics
	segmentStats  map[int64]*SegmentStats       // map[SegmentID]SegmentStats
	segmentIndex  map[int64]SegmentBelongs      // map[SegmentID]channels
	pchannelIndex map[string]map[int64]struct{} // map[PChannel]SegmentID
	sealOperators map[string]SealOperator
	metricHelper  *metricsHelper
}

// sealSegmentIDWithPolicy is the struct that contains the segment ID and the seal policy.
type sealSegmentIDWithPolicy struct {
	segmentID  int64
	sealPolicy policy.SealPolicy
}

// NewStatsManager creates a new stats manager.
func NewStatsManager() *StatsManager {
	cfg := newStatsConfig()
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	m := &StatsManager{
		mu:            sync.Mutex{},
		cfg:           cfg,
		totalStats:    InsertMetrics{},
		pchannelStats: make(map[string]*InsertMetrics),
		vchannelStats: make(map[string]*InsertMetrics),
		segmentStats:  make(map[int64]*SegmentStats),
		segmentIndex:  make(map[int64]SegmentBelongs),
		pchannelIndex: make(map[string]map[int64]struct{}),
		sealOperators: make(map[string]SealOperator),
		metricHelper:  newMetricsHelper(),
	}
	m.worker = newSealWorker(m)
	go m.worker.loop()
	return m
}

// RegisterSealOperator registers a seal operator and current growing segments related to the seal operator.
// It will perform an atomic operation to register the seal operator and segments into the manager.
func (m *StatsManager) RegisterSealOperator(sealOperator SealOperator, belongs []SegmentBelongs, stats []*SegmentStats) {
	m.registerSealOperator(sealOperator, belongs, stats)
	m.notifyIfTotalGrowingBytesOverHWM()
}

func (m *StatsManager) registerSealOperator(sealOperator SealOperator, belongs []SegmentBelongs, stats []*SegmentStats) {
	if len(belongs) != len(stats) {
		panic("register a seal operator with different length of belongs and stats, critical bug")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.sealOperators[sealOperator.Channel().Name]; ok {
		panic(fmt.Sprintf("register a seal operator %s that already exist, critical bug", sealOperator.Channel().Name))
	}
	m.sealOperators[sealOperator.Channel().Name] = sealOperator
	for i := range belongs {
		m.registerNewGrowingSegment(belongs[i], stats[i])
	}
}

// UnregisterSealOperator unregisters the seal operator and all segments related to it.
func (m *StatsManager) UnregisterSealOperator(sealOperator SealOperator) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove the seal operator from the map
	if _, ok := m.sealOperators[sealOperator.Channel().Name]; !ok {
		panic(fmt.Sprintf("unregister a seal operator %s that not exist, critical bug", sealOperator.Channel().Name))
	}
	delete(m.sealOperators, sealOperator.Channel().Name)

	// Unregister all segments on the seal operator
	m.unregisterAllStatsOnPChannel(sealOperator.Channel().Name)
}

// unregisterAllStatsOnPChannel unregisters all stats on pchannel.
func (m *StatsManager) unregisterAllStatsOnPChannel(pchannel string) int {
	segmentIDs, ok := m.pchannelIndex[pchannel]
	if !ok {
		return 0
	}
	for segmentID := range segmentIDs {
		m.unregisterSealedSegment(segmentID)
	}
	return len(segmentIDs)
}

// RegisterNewGrowingSegment registers a new growing segment.
// delegate the stats management to stats manager.
// It must be called after RegisterSealOperator and before UnregisterSealOperator.
func (m *StatsManager) RegisterNewGrowingSegment(belongs SegmentBelongs, stats *SegmentStats) {
	m.registerNewGrowingSegmentWithMutex(belongs, stats)
	m.notifyIfTotalGrowingBytesOverHWM()
}

func (m *StatsManager) registerNewGrowingSegmentWithMutex(belongs SegmentBelongs, stats *SegmentStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sealOperators[belongs.PChannel]; !ok {
		panic(fmt.Sprintf("register a segment %+v that seal operator is not exist, critical bug", belongs))
	}
	m.registerNewGrowingSegment(belongs, stats)
}

// registerNewGrowingSegment registers a new growing segment.
// delegate the stats management to stats manager.
func (m *StatsManager) registerNewGrowingSegment(belongs SegmentBelongs, stats *SegmentStats) {
	segmentID := belongs.SegmentID
	if _, ok := m.segmentStats[segmentID]; ok {
		panic(fmt.Sprintf("register a segment %d that already exist, critical bug", segmentID))
	}

	m.segmentStats[segmentID] = stats
	m.segmentIndex[segmentID] = belongs
	if _, ok := m.pchannelIndex[belongs.PChannel]; !ok {
		m.pchannelIndex[belongs.PChannel] = make(map[int64]struct{})
	}
	m.pchannelIndex[belongs.PChannel][segmentID] = struct{}{}
	m.totalStats.Collect(stats.Insert)
	if _, ok := m.pchannelStats[belongs.PChannel]; !ok {
		m.pchannelStats[belongs.PChannel] = &InsertMetrics{}
	}
	m.pchannelStats[belongs.PChannel].Collect(stats.Insert)

	if _, ok := m.vchannelStats[belongs.VChannel]; !ok {
		m.vchannelStats[belongs.VChannel] = &InsertMetrics{}
	}
	m.vchannelStats[belongs.VChannel].Collect(stats.Insert)

	m.metricHelper.ObservePChannelBytesUpdate(belongs.PChannel, m.pchannelStats[belongs.PChannel].BinarySize)
}

// AllocRows alloc number of rows on current segment.
// AllocRows will check if the segment has enough space to insert.
// Must be called after RegisterGrowingSegment and before UnregisterGrowingSegment.
func (m *StatsManager) AllocRows(segmentID int64, insert InsertMetrics) error {
	shouldBeSealed, err := m.allocRows(segmentID, insert)
	if err != nil {
		return err
	}
	if shouldBeSealed {
		m.worker.NotifySealSegment(segmentID, policy.PolicyCapacity())
	}
	m.notifyIfTotalGrowingBytesOverHWM()
	return nil
}

// allocRows allocates number of rows on current segment.
func (m *StatsManager) allocRows(segmentID int64, insert InsertMetrics) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Must be exist, otherwise it's a bug.
	info, ok := m.segmentIndex[segmentID]
	if !ok {
		panic(fmt.Sprintf("alloc rows on a segment %d that not exist", segmentID))
	}
	stat := m.segmentStats[segmentID]
	inserted := stat.AllocRows(insert)

	// update the total stats if inserted.
	if inserted {
		m.totalStats.Collect(insert)
		if _, ok := m.pchannelStats[info.PChannel]; !ok {
			m.pchannelStats[info.PChannel] = &InsertMetrics{}
		}
		m.pchannelStats[info.PChannel].Collect(insert)
		if _, ok := m.vchannelStats[info.VChannel]; !ok {
			m.vchannelStats[info.VChannel] = &InsertMetrics{}
		}
		m.vchannelStats[info.VChannel].Collect(insert)

		m.metricHelper.ObservePChannelBytesUpdate(info.PChannel, m.pchannelStats[info.PChannel].BinarySize)
		return stat.ShouldBeSealed(), nil
	}
	if stat.IsEmpty() {
		return false, ErrTooLargeInsert
	}
	return false, ErrNotEnoughSpace
}

// notifyIfTotalGrowingBytesOverHWM notifies if the total bytes is over the high water mark.
func (m *StatsManager) notifyIfTotalGrowingBytesOverHWM() {
	m.mu.Lock()
	size := m.totalStats.BinarySize
	notify := size > uint64(m.cfg.growingBytesHWM)
	m.mu.Unlock()

	if notify {
		m.worker.NotifyGrowingBytes(size)
	}
}

// GetStatsOfSegment gets the stats of segment.
func (m *StatsManager) GetStatsOfSegment(segmentID int64) *SegmentStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.segmentStats[segmentID].Copy()
}

// getSealOperator gets the seal operator of the segment.
func (m *StatsManager) getSealOperator(segmentID int64) (SegmentBelongs, *SegmentStats, SealOperator, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	belongs, ok := m.segmentIndex[segmentID]
	if !ok {
		return belongs, nil, nil, false
	}
	stats, ok := m.segmentStats[segmentID]
	if !ok {
		panic(fmt.Sprintf("stats of segment %d that not exist, critical bug", segmentID))
	}
	sealOperator, ok := m.sealOperators[belongs.PChannel]
	if !ok {
		panic(fmt.Sprintf("seal operator of segment %d that not exist, critical bug", segmentID))
	}
	return belongs, stats.Copy(), sealOperator, true
}

// UpdateOnSync updates the stats of segment on sync.
// It's an async update operation, so it's not necessary to do success.
func (m *StatsManager) UpdateOnSync(segmentID int64, syncMetric SyncOperationMetrics) {
	m.mu.Lock()
	if _, ok := m.segmentIndex[segmentID]; !ok {
		// UpdateOnSync is called asynchronously, so we need to check if the segment is still exist.
		m.mu.Unlock()
		return
	}
	m.segmentStats[segmentID].UpdateOnSync(syncMetric)
	limit := uint64(m.cfg.maxBinlogFileNum)
	notify := m.segmentStats[segmentID].BinLogCounter > limit
	m.mu.Unlock()

	// Trigger seal if the binlog file number reach the limit.
	if notify {
		m.worker.NotifySealSegment(segmentID, policy.PolicyBinlogNumber(limit))
	}
}

// UnregisterSealedSegment unregisters the sealed segment.
func (m *StatsManager) UnregisterSealedSegment(segmentID int64) *SegmentStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.unregisterSealedSegment(segmentID)
}

func (m *StatsManager) unregisterSealedSegment(segmentID int64) *SegmentStats {
	// Must be exist, otherwise it's a bug.
	info, ok := m.segmentIndex[segmentID]
	if !ok {
		panic(fmt.Sprintf("unregister a segment %d that not exist, critical bug", segmentID))
	}

	stats := m.segmentStats[segmentID]

	m.totalStats.Subtract(stats.Insert)
	delete(m.segmentStats, segmentID)
	delete(m.segmentIndex, segmentID)
	if _, ok := m.pchannelIndex[info.PChannel]; ok {
		delete(m.pchannelIndex[info.PChannel], segmentID)
		if len(m.pchannelIndex[info.PChannel]) == 0 {
			delete(m.pchannelIndex, info.PChannel)
		}
	}

	if _, ok := m.pchannelStats[info.PChannel]; ok {
		m.pchannelStats[info.PChannel].Subtract(stats.Insert)
		m.metricHelper.ObservePChannelBytesUpdate(info.PChannel, m.pchannelStats[info.PChannel].BinarySize)
		if m.pchannelStats[info.PChannel].BinarySize == 0 {
			// If the binary size is 0, it means the segment is empty, we can delete it.
			delete(m.pchannelStats, info.PChannel)
		}
	}
	if _, ok := m.vchannelStats[info.VChannel]; ok {
		m.vchannelStats[info.VChannel].Subtract(stats.Insert)
		if m.vchannelStats[info.VChannel].BinarySize == 0 {
			delete(m.vchannelStats, info.VChannel)
		}
	}
	return stats
}

// selectSegmensWithTimePolicy selects segments with time policy.
func (m *StatsManager) selectSegmentsWithTimePolicy() map[int64]policy.SealPolicy {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	sealSegmentIDs := make(map[int64]policy.SealPolicy, 0)
	for segmentID, stat := range m.segmentStats {
		if now.Sub(stat.CreateTime) > m.cfg.maxLifetime {
			sealSegmentIDs[segmentID] = policy.PolicyLifetime(m.cfg.maxLifetime)
			continue
		}
		if stat.Insert.BinarySize > uint64(m.cfg.minSizeFromIdleTime) && now.Sub(stat.LastModifiedTime) > m.cfg.maxIdleTime {
			sealSegmentIDs[segmentID] = policy.PolicyIdle(m.cfg.maxIdleTime, uint64(m.cfg.minSizeFromIdleTime))
			continue
		}
	}
	return sealSegmentIDs
}

// selectSegmentsUntilLessThanLWM selects segments until the total size is less than the threshold.
func (m *StatsManager) selectSegmentsUntilLessThanLWM() []int64 {
	m.mu.Lock()
	restSpace := m.totalStats.BinarySize - uint64(m.cfg.growingBytesLWM)
	m.mu.Unlock()

	if restSpace <= 0 {
		return nil
	}

	segmentIDs := make([]int64, 0)
	stats := m.createStatsSlice()
	statsHeap := typeutil.NewObjectArrayBasedMaximumHeap(stats, func(s segmentWithBinarySize) uint64 {
		return s.binarySize
	})
	for restSpace > 0 && statsHeap.Len() > 0 {
		nextOne := statsHeap.Pop()
		restSpace -= nextOne.binarySize
		segmentIDs = append(segmentIDs, nextOne.segmentID)
	}
	return segmentIDs
}

// createStatsSlice creates a slice of SegmentStats.
func (m *StatsManager) createStatsSlice() []segmentWithBinarySize {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := make([]segmentWithBinarySize, 0, len(m.segmentStats))
	for id, stat := range m.segmentStats {
		if stat.Insert.BinarySize > 0 {
			stats = append(stats, segmentWithBinarySize{
				segmentID:  id,
				binarySize: stat.Insert.BinarySize,
			})
		}
	}
	return stats
}

// updateConfig updates the config of stats manager.
func (m *StatsManager) updateConfig() {
	cfg := newStatsConfig()
	if err := cfg.Validate(); err != nil {
		return
	}
	m.metricHelper.ObserveConfigUpdate(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cfg != cfg {
		m.Logger().Info("update stats manager config", zap.Any("newConfig", cfg), zap.Any("oldConfig", m.cfg))
		m.cfg = cfg
	}
}

// getConfig gets the config of stats manager.
func (m *StatsManager) getConfig() statsConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

type segmentWithBinarySize struct {
	segmentID  int64
	binarySize uint64
}
