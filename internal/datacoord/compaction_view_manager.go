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

	meta      *meta
	trigger   TriggerManager
	allocator allocator

	closeSig chan struct{}
	closeWg  sync.WaitGroup
}

func NewCompactionViewManager(meta *meta, trigger TriggerManager, allocator allocator) *CompactionViewManager {
	return &CompactionViewManager{
		view: &FullViews{
			collections: make(map[int64][]*SegmentView),
		},
		meta:      meta,
		trigger:   trigger,
		allocator: allocator,
		closeSig:  make(chan struct{}),
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

	log.Info("Compaction view manager start")

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
		log.Warn("CompactionViewManager check failed, unable to allocate taskID",
			zap.Error(err))
		return
	}

	log := log.With(zap.Int64("taskID", taskID))

	m.viewGuard.Lock()
	defer m.viewGuard.Unlock()

	events := make(map[CompactionTriggerType][]CompactionView)

	latestCollSegs := m.meta.GetCompactableSegmentGroupByCollection()
	latestCollIDs := lo.Keys(latestCollSegs)
	viewCollIDs := lo.Keys(m.view.collections)

	_, diffRemove := lo.Difference(latestCollIDs, viewCollIDs)
	for _, collID := range diffRemove {
		delete(m.view.collections, collID)
	}

	// TODO: update all segments views. For now, just update Level Zero Segments
	for collID, segments := range latestCollSegs {
		levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
			return info.GetLevel() == datapb.SegmentLevel_L0
		})

		latestL0Segments := GetViewsByInfo(levelZeroSegments...)
		changedL0Views := m.getChangedLevelZeroViews(collID, latestL0Segments)
		if len(changedL0Views) == 0 {
			continue
		}

		log.Info("Refresh compaction level zero views",
			zap.Int64("collectionID", collID),
			zap.Strings("views", lo.Map(changedL0Views, func(view CompactionView, _ int) string {
				return view.String()
			})))

		m.view.collections[collID] = latestL0Segments
		events[TriggerTypeLevelZeroView] = changedL0Views
	}

	for eType, views := range events {
		m.trigger.Notify(taskID, eType, views)
	}
}

func (m *CompactionViewManager) getChangedLevelZeroViews(collID UniqueID, LevelZeroViews []*SegmentView) []CompactionView {
	latestViews := m.groupL0ViewsByPartChan(collID, LevelZeroViews)
	cachedViews := m.view.GetSegmentViewBy(collID, func(v *SegmentView) bool {
		return v.Level == datapb.SegmentLevel_L0
	})

	var signals []CompactionView
	for _, latestView := range latestViews {
		views := lo.Filter(cachedViews, func(v *SegmentView, _ int) bool {
			return v.label.Equal(latestView.GetGroupLabel())
		})

		if !latestView.Equal(views) {
			signals = append(signals, latestView)
		}
	}
	return signals
}

func (m *CompactionViewManager) groupL0ViewsByPartChan(collectionID UniqueID, levelZeroSegments []*SegmentView) map[string]*LevelZeroSegmentsView {
	partChanView := make(map[string]*LevelZeroSegmentsView) // "part-chan" as key
	for _, view := range levelZeroSegments {
		key := view.label.Key()
		if _, ok := partChanView[key]; !ok {
			partChanView[key] = &LevelZeroSegmentsView{
				label:                     view.label,
				segments:                  []*SegmentView{view},
				earliestGrowingSegmentPos: m.meta.GetEarliestStartPositionOfGrowingSegments(view.label),
			}
		} else {
			partChanView[key].Append(view)
		}
	}

	return partChanView
}
