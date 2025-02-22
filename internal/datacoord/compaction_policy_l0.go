package datacoord

import (
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type l0CompactionPolicy struct {
	meta *meta

	activeCollections *activeCollections
}

func newL0CompactionPolicy(meta *meta) *l0CompactionPolicy {
	return &l0CompactionPolicy{
		meta:              meta,
		activeCollections: newActiveCollections(),
	}
}

func (policy *l0CompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

// Notify policy to record the active updated(when adding a new L0 segment) collections.
func (policy *l0CompactionPolicy) OnCollectionUpdate(collectionID int64) {
	policy.activeCollections.Record(collectionID)
}

func (policy *l0CompactionPolicy) Trigger() (events map[CompactionTriggerType][]CompactionView, err error) {
	events = make(map[CompactionTriggerType][]CompactionView)
	latestCollSegs := policy.meta.GetCompactableSegmentGroupByCollection()

	// 1. Get active collections
	activeColls := policy.activeCollections.GetActiveCollections()

	// 2. Idle collections  = all collections - active collections
	missCached, idleColls := lo.Difference(activeColls, lo.Keys(latestCollSegs))
	policy.activeCollections.ClearMissCached(missCached...)

	idleCollsSet := typeutil.NewUniqueSet(idleColls...)
	activeL0Views, idleL0Views := []CompactionView{}, []CompactionView{}
	for collID, segments := range latestCollSegs {
		policy.activeCollections.Read(collID)

		levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
			return info.GetLevel() == datapb.SegmentLevel_L0
		})
		if len(levelZeroSegments) == 0 {
			continue
		}

		labelViews := policy.groupL0ViewsByPartChan(collID, GetViewsByInfo(levelZeroSegments...))
		if idleCollsSet.Contain(collID) {
			idleL0Views = append(idleL0Views, labelViews...)
		} else {
			activeL0Views = append(activeL0Views, labelViews...)
		}

	}
	if len(activeL0Views) > 0 {
		events[TriggerTypeLevelZeroViewChange] = activeL0Views
	}

	if len(idleL0Views) > 0 {
		events[TriggerTypeLevelZeroViewIDLE] = idleL0Views
	}
	return
}

func (policy *l0CompactionPolicy) groupL0ViewsByPartChan(collectionID UniqueID, levelZeroSegments []*SegmentView) []CompactionView {
	partChanView := make(map[string]*LevelZeroSegmentsView) // "part-chan" as key
	for _, view := range levelZeroSegments {
		key := view.label.Key()
		if _, ok := partChanView[key]; !ok {
			partChanView[key] = &LevelZeroSegmentsView{
				label:                     view.label,
				segments:                  []*SegmentView{view},
				earliestGrowingSegmentPos: policy.meta.GetEarliestStartPositionOfGrowingSegments(view.label),
			}
		} else {
			partChanView[key].Append(view)
		}
	}

	return lo.Map(lo.Values(partChanView), func(view *LevelZeroSegmentsView, _ int) CompactionView {
		return view
	})
}

type activeCollection struct {
	ID          int64
	lastRefresh time.Time
	readCount   *atomic.Int64
}

func newActiveCollection(ID int64) *activeCollection {
	return &activeCollection{
		ID:          ID,
		lastRefresh: time.Now(),
		readCount:   atomic.NewInt64(0),
	}
}

type activeCollections struct {
	collections map[int64]*activeCollection
	collGuard   sync.RWMutex
}

func newActiveCollections() *activeCollections {
	return &activeCollections{
		collections: make(map[int64]*activeCollection),
	}
}

func (ac *activeCollections) ClearMissCached(collectionIDs ...int64) {
	ac.collGuard.Lock()
	defer ac.collGuard.Unlock()
	lo.ForEach(collectionIDs, func(collID int64, _ int) {
		delete(ac.collections, collID)
	})
}

func (ac *activeCollections) Record(collectionID int64) {
	ac.collGuard.Lock()
	defer ac.collGuard.Unlock()
	if _, ok := ac.collections[collectionID]; !ok {
		ac.collections[collectionID] = newActiveCollection(collectionID)
	} else {
		ac.collections[collectionID].lastRefresh = time.Now()
		ac.collections[collectionID].readCount.Store(0)
	}
}

func (ac *activeCollections) Read(collectionID int64) {
	ac.collGuard.Lock()
	defer ac.collGuard.Unlock()
	if _, ok := ac.collections[collectionID]; ok {
		ac.collections[collectionID].readCount.Inc()
		if ac.collections[collectionID].readCount.Load() >= 3 &&
			time.Since(ac.collections[collectionID].lastRefresh) > 3*paramtable.Get().DataCoordCfg.L0CompactionTriggerInterval.GetAsDuration(time.Second) {
			log.Info("Active(of deletions) collections become idle", zap.Int64("collectionID", collectionID))
			delete(ac.collections, collectionID)
		}
	}
}

func (ac *activeCollections) GetActiveCollections() []int64 {
	ac.collGuard.RLock()
	defer ac.collGuard.RUnlock()

	return lo.Keys(ac.collections)
}
