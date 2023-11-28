package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/logutil"
)

type CompactionViewManager struct {
	view      *FullViews
	viewGuard sync.RWMutex

	meta         *meta
	eventManager TriggerManager
	allocator    allocator

	closeSig chan struct{}
	closeWg  sync.WaitGroup
}

func NewCompactionViewManager(meta *meta, trigger TriggerManager, allocator allocator) *CompactionViewManager {
	return &CompactionViewManager{
		view: &FullViews{
			collections: make(map[int64][]*SegmentView),
		},
		meta:         meta,
		eventManager: trigger,
		allocator:    allocator,
		closeSig:     make(chan struct{}),
	}
}

func (m *CompactionViewManager) Start() {
	m.closeWg.Add(1)
	go m.checkLoop()
}

func (m *CompactionViewManager) Close() {
	close(m.closeSig)
	m.closeWg.Wait()
}

func (m *CompactionViewManager) checkLoop() {
	defer logutil.LogPanic()
	defer m.closeWg.Done()

	if !Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() {
		return
	}
	interval := Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeSig:
			log.Info("Compaction View checkLoop quit")
			return
		case <-ticker.C:
			m.Check()
		}
	}
}

// Global check could take some time, we need to record the time.
func (m *CompactionViewManager) Check() {
	// Only process L0 compaction now, so just return if its not enabled
	if !Params.DataCoordCfg.EnableLevelZeroSegment.GetAsBool() {
		return
	}

	ctx := context.TODO()
	taskID, err := m.allocator.allocID(ctx)
	if err != nil {
		log.Warn("CompactionViewManager check loop failed, unable to allocate taskID",
			zap.Error(err))
		return
	}

	m.viewGuard.Lock()
	defer m.viewGuard.Unlock()

	events := make(map[CompactionTriggerType][]CompactionView)

	latestCollSegs := m.meta.GetCompactableSegmentGroupByCollection()
	latestCollIDs := lo.Keys(latestCollSegs)
	viewCollIDs := lo.Keys(m.view.collections)

	diffAdd, diffRemove := lo.Difference(latestCollIDs, viewCollIDs)

	for _, collID := range diffRemove {
		delete(m.view.collections, collID)
	}

	for collID, segments := range latestCollSegs {
		levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
			return info.GetLevel() == datapb.SegmentLevel_L0
		})

		// For new collection, TODO: update all segments
		// - for now, just update Level Zero Segments
		if lo.Contains(diffAdd, collID) {
			m.view.collections[collID] = GetSegmentViews(levelZeroSegments...)
			continue
		}

		latestLevelZeroViews, signals := m.GetLatestLevelZeroSegmentWithSignals(collID, levelZeroSegments)
		if len(latestLevelZeroViews) != 0 {
			m.view.collections[collID] = latestLevelZeroViews
		}

		events[TriggerTypeLevelZeroView] = signals
	}

	for eType, views := range events {
		m.eventManager.Notify(taskID, eType, views)
	}
}

func (m *CompactionViewManager) GetLatestLevelZeroSegmentWithSignals(collID UniqueID, LevelZeroSegments []*SegmentInfo) ([]*SegmentView, []CompactionView) {
	partChanView := m.BuildLevelZeroSegmentsView(collID, LevelZeroSegments)

	var signals []CompactionView
	var needUpdate bool = false

	for _, latestView := range partChanView {
		views := m.view.GetSegmentViewBy(collID, func(v *SegmentView) bool {
			return v.label.PartitionID == latestView.label.PartitionID &&
				v.label.Channel == latestView.label.Channel
		})

		if !latestView.Equal(views) {
			needUpdate = true
			signals = append(signals, latestView)
		}
	}

	if needUpdate {
		var allViews []*SegmentView
		for _, latestView := range partChanView {
			allViews = append(allViews, latestView.segments...)
		}
		return allViews, signals
	}

	return nil, signals
}

func (m *CompactionViewManager) BuildLevelZeroSegmentsView(collectionID UniqueID, levelZeroSegments []*SegmentInfo) []*LevelZeroSegmentsView {
	partChanView := make(map[string]*LevelZeroSegmentsView) // "part-chan"  to earliestStartPosition

	for _, seg := range levelZeroSegments {
		key := buildGroupKey(seg.PartitionID, seg.InsertChannel)
		if _, ok := partChanView[key]; !ok {
			label := &CompactionGroupLabel{
				CollectionID: collectionID,
				PartitionID:  seg.PartitionID,
				Channel:      seg.InsertChannel,
			}
			partChanView[key] = &LevelZeroSegmentsView{
				label:                     label,
				segments:                  []*SegmentView{},
				earliestGrowingSegmentPos: m.meta.GetEarliestStartPositionOfGrowingSegments(label),
			}
		}

		partChanView[key].segments = append(partChanView[key].segments, GetSegmentViews(seg)[0])
	}

	return lo.Values(partChanView)
}
