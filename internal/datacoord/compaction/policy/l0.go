package policy

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Chooses qualified L0 segments to do L0 compaction
type L0CompactionPolicy struct {
	Meta *meta.Meta

	ActiveCollections *activeCollections
	allocator         allocator.Allocator
}

// Ensure l0CompactionPolicy implements CompactionPolicy interface
var _ CompactionPolicy = (*L0CompactionPolicy)(nil)

func NewL0CompactionPolicy(mt *meta.Meta, allocator allocator.Allocator) *L0CompactionPolicy {
	return &L0CompactionPolicy{
		Meta:              mt,
		ActiveCollections: newActiveCollections(),
		allocator:         allocator,
	}
}

func (policy *L0CompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *L0CompactionPolicy) Name() string {
	return "L0Compaction"
}

// Notify policy to record the active updated(when adding a new L0 segment) collections.
func (policy *L0CompactionPolicy) OnCollectionUpdate(collectionID int64) {
	policy.ActiveCollections.Record(collectionID)
}

func (policy *L0CompactionPolicy) Trigger(ctx context.Context) (events map[CompactionTriggerType][]view.CompactionView, err error) {
	latestCollSegs := policy.Meta.GetCompactableSegmentGroupByCollection()

	// 1. Get active collections
	activeColls := policy.ActiveCollections.GetActiveCollections()

	// 2. Idle collections  = all collections - active collections
	missCached, idleColls := lo.Difference(activeColls, lo.Keys(latestCollSegs))
	policy.ActiveCollections.ClearMissCached(missCached...)

	idleCollsSet := typeutil.NewUniqueSet(idleColls...)
	activeL0Views, idleL0Views := []view.CompactionView{}, []view.CompactionView{}
	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		mlog.Warn(ctx, "fail to allocate triggerID to trigger l0 compaction", mlog.Err(err))
		return nil, err
	}
	events = make(map[CompactionTriggerType][]view.CompactionView)
	for collID, segments := range latestCollSegs {
		collection := policy.Meta.GetCollection(collID)
		if collection == nil {
			continue
		}
		if collection.IsExternal() {
			mlog.Info(ctx, "skip l0 compaction for external collection", mlog.FieldCollectionID(collID))
			continue
		}

		policy.ActiveCollections.Read(collID)
		levelZeroSegments := lo.Filter(segments, func(info *meta.SegmentInfo, _ int) bool {
			return info.GetLevel() == datapb.SegmentLevel_L0
		})
		if len(levelZeroSegments) == 0 {
			continue
		}
		labelViews := policy.GroupL0ViewsByPartChan(collID, view.GetViewsByInfo(levelZeroSegments...), newTriggerID)
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
	return events, err
}

func (policy *L0CompactionPolicy) TriggerOneCollection(ctx context.Context, collectionID int64) ([]view.CompactionView, int64, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	log.Info(ctx, "start trigger collection l0 compaction")
	collection := policy.Meta.GetCollection(collectionID)
	if collection == nil {
		log.Warn(ctx, "collection not found in meta")
		return nil, 0, merr.WrapErrCollectionNotLoaded(collectionID, "collection not found")
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip trigger l0 compaction for external collection")
		return nil, 0, nil
	}
	allL0Segments := policy.Meta.SelectSegments(ctx, meta.WithCollection(collectionID), meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		return meta.IsSegmentHealthy(segment) &&
			meta.IsFlushed(segment) &&
			!segment.IsCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L0
	}))

	if len(allL0Segments) == 0 {
		return nil, 0, nil
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to allocate triggerID for l0 compaction", mlog.Err(err))
		return nil, 0, err
	}
	views := policy.GroupL0ViewsByPartChan(collectionID, view.GetViewsByInfo(allL0Segments...), newTriggerID)
	return views, newTriggerID, nil
}

func (policy *L0CompactionPolicy) GroupL0ViewsByPartChan(collectionID UniqueID, levelZeroSegments []*view.SegmentView, triggerID UniqueID) []view.CompactionView {
	partChanView := make(map[string]*view.LevelZeroCompactionView) // "part-chan" as key
	for _, segView := range levelZeroSegments {
		key := segView.Label.Key()
		if _, ok := partChanView[key]; !ok {
			earliestGrowingStartPos := policy.Meta.GetEarliestStartPositionOfGrowingSegments(segView.Label.CollectionID, segView.Label.PartitionID, segView.Label.Channel)
			partChanView[key] = &view.LevelZeroCompactionView{
				Label:           segView.Label,
				L0Segments:      []*view.SegmentView{},
				LatestDeletePos: earliestGrowingStartPos,
				TriggerID:       triggerID,
			}
		}

		l0View := partChanView[key]
		// Only choose segments with position less than or equal to the earliest growing segment position
		if segView.DmlPos.GetTimestamp() <= l0View.LatestDeletePos.GetTimestamp() {
			l0View.Append(segView)
		}
	}

	return lo.Map(lo.Values(partChanView), func(cv *view.LevelZeroCompactionView, _ int) view.CompactionView {
		return cv
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
			mlog.Info(context.TODO(), "Active(of deletions) collections become idle", mlog.FieldCollectionID(collectionID))
			delete(ac.collections, collectionID)
		}
	}
}

func (ac *activeCollections) GetActiveCollections() []int64 {
	ac.collGuard.RLock()
	defer ac.collGuard.RUnlock()

	return lo.Keys(ac.collections)
}
