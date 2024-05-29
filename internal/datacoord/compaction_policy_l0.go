package datacoord

import (
	"context"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ CompactionPolicy = (*l0CompactionPolicy)(nil)

type l0CompactionPolicy struct {
	meta   *meta
	view   *FullViews
	ticker *time.Ticker

	emptyLoopCount int
}

func newL0CompactionPolicy(meta *meta, view *FullViews) *l0CompactionPolicy {
	interval := Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	return &l0CompactionPolicy{
		meta:   meta,
		view:   view,
		ticker: ticker,
	}
}

func (policy *l0CompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() && Params.DataCoordCfg.EnableLevelZeroSegment.GetAsBool()
}

func (policy *l0CompactionPolicy) Stop() {
	policy.ticker.Stop()
}

func (policy *l0CompactionPolicy) Ticker() *time.Ticker {
	return policy.ticker
}

func (policy *l0CompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	// support config hot refresh
	defer policy.ticker.Reset(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second))
	if !policy.Enable() {
		return make(map[CompactionTriggerType][]CompactionView, 0), nil
	}

	events := policy.generateEventForLevelZeroViewChange(ctx)
	if len(events) != 0 {
		// each time when triggers a compaction, the idleTicker would reset
		policy.emptyLoopCount = 0
		return events, nil
	}
	if policy.emptyLoopCount >= 3 {
		idleEvents := policy.generateEventForLevelZeroViewIDLE()
		return idleEvents, nil
	}
	return make(map[CompactionTriggerType][]CompactionView, 0), nil
}

func (policy *l0CompactionPolicy) generateEventForLevelZeroViewChange(ctx context.Context) (events map[CompactionTriggerType][]CompactionView) {
	_, span := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "CompactionView-Check")
	defer span.End()

	span.AddEvent("CompactionView GetCompactableSegment")
	latestCollSegs := policy.meta.GetCompactableSegmentGroupByCollection()
	latestCollIDs := lo.Keys(latestCollSegs)
	viewCollIDs := lo.Keys(policy.view.collections)

	_, diffRemove := lo.Difference(latestCollIDs, viewCollIDs)
	for _, collID := range diffRemove {
		delete(policy.view.collections, collID)
	}

	// TODO: update all segments views. For now, just update Level Zero Segments
	span.AddEvent("CompactionView Refresh L0 views")
	refreshedL0Views := policy.RefreshLevelZeroViews(latestCollSegs)
	if len(refreshedL0Views) > 0 {
		events = make(map[CompactionTriggerType][]CompactionView)
		events[TriggerTypeLevelZeroViewChange] = refreshedL0Views
	}

	return events
}

func (policy *l0CompactionPolicy) RefreshLevelZeroViews(latestCollSegs map[int64][]*SegmentInfo) []CompactionView {
	var allRefreshedL0Veiws []CompactionView
	for collID, segments := range latestCollSegs {
		levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
			return info.GetLevel() == datapb.SegmentLevel_L0
		})

		latestL0Segments := GetViewsByInfo(levelZeroSegments...)
		needRefresh, collRefreshedViews := policy.getChangedLevelZeroViews(collID, latestL0Segments)
		if needRefresh {
			log.Info("Refresh compaction level zero views",
				zap.Int64("collectionID", collID),
				zap.Strings("views", lo.Map(collRefreshedViews, func(view CompactionView, _ int) string {
					return view.String()
				})))

			policy.view.collections[collID].l0 = latestL0Segments
		}

		if len(collRefreshedViews) > 0 {
			allRefreshedL0Veiws = append(allRefreshedL0Veiws, collRefreshedViews...)
		}
	}

	return allRefreshedL0Veiws
}

func (policy *l0CompactionPolicy) getChangedLevelZeroViews(collID UniqueID, LevelZeroViews []*SegmentView) (needRefresh bool, refreshed []CompactionView) {
	cachedViews := policy.view.GetSegmentViewBy(collID, func(v *SegmentView) bool {
		return v.Level == datapb.SegmentLevel_L0
	})

	if len(LevelZeroViews) == 0 && len(cachedViews) != 0 {
		needRefresh = true
		return
	}

	latestViews := policy.groupL0ViewsByPartChan(collID, LevelZeroViews)
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

func (policy *l0CompactionPolicy) groupL0ViewsByPartChan(collectionID UniqueID, levelZeroSegments []*SegmentView) map[string]*LevelZeroSegmentsView {
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

	return partChanView
}

func (policy *l0CompactionPolicy) generateEventForLevelZeroViewIDLE() map[CompactionTriggerType][]CompactionView {
	log.Info("Views idle for a long time, try to trigger a TriggerTypeLevelZeroViewIDLE compaction event")
	events := make(map[CompactionTriggerType][]CompactionView, 0)
	for collID := range policy.view.collections {
		cachedViews := policy.view.GetSegmentViewBy(collID, func(v *SegmentView) bool {
			return v.Level == datapb.SegmentLevel_L0
		})
		if len(cachedViews) > 0 {
			grouped := policy.groupL0ViewsByPartChan(collID, cachedViews)
			events[TriggerTypeLevelZeroViewIDLE] = lo.Map(lo.Values(grouped),
				func(l0View *LevelZeroSegmentsView, _ int) CompactionView {
					return l0View
				})
			log.Info("Generate TriggerTypeLevelZeroViewIDLE compaction event", zap.Int64("collectionID", collID))
			break
		}
	}

	return events
}
