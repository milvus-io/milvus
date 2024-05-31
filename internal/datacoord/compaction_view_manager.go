package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type CompactionViewManager struct {
	view      *FullViews
	viewGuard lock.RWMutex

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

	// TODO: Only process L0 compaction now, so just return if its not enabled
	if !Params.DataCoordCfg.EnableLevelZeroSegment.GetAsBool() {
		return
	}

	interval := Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second)
	checkTicker := time.NewTicker(interval)
	defer checkTicker.Stop()

	idleTicker := time.NewTicker(interval * 3)
	defer idleTicker.Stop()

	// each time when triggers a compaction, the idleTicker would reset
	refreshViewsAndTrigger := func(ctx context.Context) bool {
		events := m.Check(ctx)
		if len(events) != 0 {
			m.notifyTrigger(ctx, events)
			idleTicker.Reset(interval * 3)
			return true
		}

		return false
	}

	log.Info("Compaction view manager start", zap.Duration("check interval", interval), zap.Duration("idle check interval", interval*3))
	for {
		select {
		case <-m.closeSig:
			log.Info("Compaction View checkLoop quit")
			return
		case <-checkTicker.C:
			refreshViewsAndTrigger(context.Background())

		case <-idleTicker.C:
			// idelTicker will be reset everytime when Check's able to
			// generates compaction events

			// if no views are refreshed, try to get cached views and trigger a
			// TriggerTypeViewIDLE event
			if !refreshViewsAndTrigger(context.Background()) {
				m.triggerEventForIDLEView()
			}
		}
	}
}

func (m *CompactionViewManager) triggerEventForIDLEView() {
	log.Info("Views idle for a long time, try to trigger a TriggerTypeLevelZeroViewIDLE compaction event")
	events := make(map[CompactionTriggerType][]CompactionView)
	for collID := range m.view.collections {
		cachedViews := m.view.GetSegmentViewBy(collID, func(v *SegmentView) bool {
			return v.Level == datapb.SegmentLevel_L0
		})
		if len(cachedViews) > 0 {
			grouped := m.groupL0ViewsByPartChan(collID, cachedViews)
			events[TriggerTypeLevelZeroViewIDLE] = lo.Map(lo.Values(grouped),
				func(l0View *LevelZeroSegmentsView, _ int) CompactionView {
					return l0View
				})
			log.Info("Generate TriggerTypeLevelZeroViewIDLE compaction event", zap.Int64("collectionID", collID))
			break
		}
	}

	if len(events) > 0 {
		m.notifyTrigger(context.Background(), events)
	}
}

func (m *CompactionViewManager) notifyTrigger(ctx context.Context, events map[CompactionTriggerType][]CompactionView) {
	taskID, err := m.allocator.allocID(ctx)
	if err != nil {
		log.Warn("CompactionViewManager notify trigger failed, unable to allocate taskID",
			zap.Error(err))
		return
	}

	for eType, views := range events {
		m.trigger.Notify(taskID, eType, views)
	}
}

// Global check could take some time, we need to record the time.
func (m *CompactionViewManager) Check(ctx context.Context) (events map[CompactionTriggerType][]CompactionView) {
	_, span := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "CompactionView-Check")
	defer span.End()

	m.viewGuard.Lock()
	defer m.viewGuard.Unlock()

	span.AddEvent("CompactionView GetCompactableSegment")
	latestCollSegs := m.meta.GetCompactableSegmentGroupByCollection()
	latestCollIDs := lo.Keys(latestCollSegs)
	viewCollIDs := lo.Keys(m.view.collections)

	_, diffRemove := lo.Difference(latestCollIDs, viewCollIDs)
	for _, collID := range diffRemove {
		delete(m.view.collections, collID)
	}

	// TODO: update all segments views. For now, just update Level Zero Segments
	span.AddEvent("CompactionView Refresh L0 views")
	refreshedL0Views := m.RefreshLevelZeroViews(latestCollSegs)
	if len(refreshedL0Views) > 0 {
		events = make(map[CompactionTriggerType][]CompactionView)
		events[TriggerTypeLevelZeroViewChange] = refreshedL0Views
	}

	return events
}

func (m *CompactionViewManager) RefreshLevelZeroViews(latestCollSegs map[int64][]*SegmentInfo) []CompactionView {
	var allRefreshedL0Veiws []CompactionView
	for collID, segments := range latestCollSegs {
		levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
			return info.GetLevel() == datapb.SegmentLevel_L0
		})

		latestL0Segments := GetViewsByInfo(levelZeroSegments...)
		needRefresh, collRefreshedViews := m.getChangedLevelZeroViews(collID, latestL0Segments)
		if needRefresh {
			log.Info("Refresh compaction level zero views",
				zap.Int64("collectionID", collID),
				zap.Strings("views", lo.Map(collRefreshedViews, func(view CompactionView, _ int) string {
					return view.String()
				})))

			m.view.collections[collID] = latestL0Segments
		}

		if len(collRefreshedViews) > 0 {
			allRefreshedL0Veiws = append(allRefreshedL0Veiws, collRefreshedViews...)
		}
	}

	return allRefreshedL0Veiws
}

func (m *CompactionViewManager) getChangedLevelZeroViews(collID UniqueID, LevelZeroViews []*SegmentView) (needRefresh bool, refreshed []CompactionView) {
	cachedViews := m.view.GetSegmentViewBy(collID, func(v *SegmentView) bool {
		return v.Level == datapb.SegmentLevel_L0
	})

	if len(LevelZeroViews) == 0 && len(cachedViews) != 0 {
		needRefresh = true
		return
	}

	latestViews := m.groupL0ViewsByPartChan(collID, LevelZeroViews)
	for _, latestView := range latestViews {
		views := lo.Filter(cachedViews, func(v *SegmentView, _ int) bool {
			return v.label.Equal(latestView.GetGroupLabel())
		})

		if !latestView.Equal(views) {
			refreshed = append(refreshed, latestView)
			needRefresh = true
		}
	}
	return
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
