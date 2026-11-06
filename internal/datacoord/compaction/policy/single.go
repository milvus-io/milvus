// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package policy

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// singleCompactionPolicy is to compact one segment with too many delta logs
// support l2 single segment only for now
// todo: move l1 single compaction here
type SingleCompactionPolicy struct {
	Meta      *meta.Meta
	IndexMeta *index.IndexMeta
	allocator allocator.Allocator
	handler   meta.Handler
	// RemainingCapacity reports how many tasks the inspector can still take, so
	// admission does not spend tokens on views that would be dropped. nil means
	// unbounded.
	RemainingCapacity func() int
}

// Ensure singleCompactionPolicy implements CompactionPolicy interface
var _ CompactionPolicy = (*SingleCompactionPolicy)(nil)

func NewSingleCompactionPolicy(mt *meta.Meta, indexMeta *index.IndexMeta, allocator allocator.Allocator, handler meta.Handler) *SingleCompactionPolicy {
	return &SingleCompactionPolicy{Meta: mt, IndexMeta: indexMeta, allocator: allocator, handler: handler}
}

func (policy *SingleCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *SingleCompactionPolicy) Name() string {
	return "SingleCompactionPolicy"
}

func (policy *SingleCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]view.CompactionView, error) {
	collections := policy.Meta.CollectionsForPreparation(ctx)

	events := make(map[CompactionTriggerType][]view.CompactionView, 0)
	// Invisible segments, which a flush or a clustering compaction just wrote,
	// wait for their sort to become visible, so their sorts go first: under
	// dataCoord.compaction.maxTaskNum only the first sorts may be submitted.
	invisibleSortViews := make([]view.CompactionView, 0)
	visibleSortViews := make([]view.CompactionView, 0)
	// Single compaction candidates are gathered from every collection first and
	// admitted in one pass, so the admission budget is shared across
	// collections instead of being drained by the first one in the walk.
	rounds := make(map[int64]SingleCollectionRound)
	candidates := make([]*SingleCandidate, 0)
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		if collection.IsExternal() {
			mlog.Info(ctx, "skip single compaction trigger for external collection", mlog.FieldCollectionID(collection.ID))
			continue
		}
		if policy.Meta.IsCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip single compaction for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}
		round, err := policy.TriggerOneCollection(ctx, collection.ID, false)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			mlog.Warn(ctx, "fail to trigger single compaction", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
		}
		invisibleSortViews = append(invisibleSortViews, round.InvisibleSortViews...)
		visibleSortViews = append(visibleSortViews, round.VisibleSortViews...)
		if len(round.Candidates) > 0 {
			rounds[collection.ID] = round
			candidates = append(candidates, round.Candidates...)
		}
	}

	capacity := -1
	if policy.RemainingCapacity != nil {
		capacity = policy.RemainingCapacity()
	}
	admitted, deferred := GetSingleCompactionAdmitter().Admit(ctx, AdmissionSourcePolicy, candidates, capacity)
	views := make([]view.CompactionView, 0, len(admitted))
	for _, c := range admitted {
		round := rounds[c.collectionID]
		segmentViews := view.GetViewsByInfo(c.segment)
		views = append(views, &MixSegmentView{
			Label:         segmentViews[0].Label,
			Segments:      segmentViews,
			CollectionTTL: round.CollectionTTL,
			TriggerID:     round.TriggerID,
			Admitted:      true,
		})
	}
	if deferred > 0 {
		mlog.RatedInfo(ctx, rate.Limit(10), "deferred single compaction candidates by admission limit",
			mlog.Int("admitted", len(admitted)),
			mlog.Int("deferred", deferred))
	}
	events[TriggerTypeSingle] = views
	events[TriggerTypeSort] = append(invisibleSortViews, visibleSortViews...)
	return events, nil
}

func (policy *SingleCompactionPolicy) TriggerSegmentSortCompaction(
	ctx context.Context,
	segmentID int64,
) view.CompactionView {
	log := mlog.WithLazy(mlog.FieldSegmentID(segmentID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(ctx, rate.Limit(20), "stats task disabled, skip sort compaction")
		return nil
	}
	segment := policy.Meta.GetHealthySegment(ctx, segmentID)
	if segment == nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, segment not healthy")
		return nil
	}

	collection, err := policy.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, unable to get collection from handler",
			mlog.Err(err))
		return nil
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, collection not exist")
		return nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip sort compaction for external collection", mlog.FieldCollectionID(collection.ID))
		return nil
	}
	if !canTriggerSortCompaction(segment) {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction",
			mlog.String("state", segment.GetState().String()),
			mlog.String("level", segment.GetLevel().String()),
			mlog.Bool("isSorted", segment.GetIsSorted()),
			mlog.Bool("isNamespaceSorted", segment.GetIsSortedByNamespace()),
			mlog.Bool("isImporting", segment.GetIsImporting()),
			mlog.Bool("isCompacting", segment.IsCompacting),
			mlog.Bool("isInvisible", segment.GetIsInvisible()))
		return nil
	}
	if policy.Meta.IsSegmentCompactionProtected(segment.GetID()) {
		log.Info(ctx, "skip sort compaction for snapshot-protected segment",
			mlog.FieldSegmentID(segment.GetID()))
		return nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "failed to apply triggerSegmentSortCompaction, get collection ttl failed")
		return nil
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, unable to allocate triggerID", mlog.Err(err))
		return nil
	}

	segmentViews := view.GetViewsByInfo(segment)
	cv := &MixSegmentView{
		Label:         segmentViews[0].Label,
		Segments:      segmentViews,
		CollectionTTL: collectionTTL,
		TriggerID:     newTriggerID,
	}

	log.Info(ctx, "succeeded to apply triggerSegmentSortCompaction",
		mlog.Int64("triggerID", newTriggerID))
	return cv
}

// triggerSortCompaction returns the sort views of the collection's invisible
// and of its visible unsorted segments.
func (policy *SingleCompactionPolicy) triggerSortCompaction(
	ctx context.Context,
	triggerID int64,
	collectionID int64,
	collectionTTL time.Duration,
) ([]view.CompactionView, []view.CompactionView, error) {
	log := mlog.WithLazy(mlog.FieldCollectionID(collectionID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(ctx, rate.Limit(20), "stats task disabled, skip sort compaction")
		return nil, nil, nil
	}
	invisibleViews := make([]view.CompactionView, 0)
	visibleViews := make([]view.CompactionView, 0)

	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, unable to get collection from handler",
			mlog.Err(err))
		return nil, nil, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, collection not exist")
		return nil, nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip triggerSegmentSortCompaction for external collection", mlog.FieldCollectionID(collection.ID))
		return nil, nil, nil
	}
	triggerableSegments := policy.Meta.SelectSegments(ctx, meta.WithCollection(collectionID),
		meta.SegmentFilterFunc(func(seg *meta.SegmentInfo) bool {
			return canTriggerSortCompaction(seg) &&
				!policy.Meta.IsSegmentCompactionProtected(seg.GetID())
		}))
	if len(triggerableSegments) == 0 {
		log.RatedInfo(ctx, rate.Limit(20), "no triggerable segments")
		return invisibleViews, visibleViews, nil
	}

	gbSegments := lo.GroupBy(triggerableSegments, func(seg *meta.SegmentInfo) bool {
		return seg.GetIsInvisible()
	})
	invisibleSegments, ok := gbSegments[true]
	if ok {
		for _, segment := range invisibleSegments {
			segmentViews := view.GetViewsByInfo(segment)
			cv := &MixSegmentView{
				Label:         segmentViews[0].Label,
				Segments:      segmentViews,
				CollectionTTL: collectionTTL,
				TriggerID:     triggerID,
			}
			invisibleViews = append(invisibleViews, cv)
		}
	}

	visibleSegments, ok := gbSegments[false]
	if ok {
		for i, segment := range visibleSegments {
			if i > Params.DataCoordCfg.SortCompactionTriggerCount.GetAsInt() {
				break
			}
			segmentViews := view.GetViewsByInfo(segment)
			cv := &MixSegmentView{
				Label:         segmentViews[0].Label,
				Segments:      segmentViews,
				CollectionTTL: collectionTTL,
				TriggerID:     triggerID,
			}
			visibleViews = append(visibleViews, cv)
		}
	}

	log.Info(ctx, "succeeded to apply triggerSortCompaction",
		mlog.Int64("triggerID", triggerID),
		mlog.Int("triggered view num", len(invisibleViews)+len(visibleViews)))
	return invisibleViews, visibleViews, nil
}

// SingleCollectionRound is what one collection contributes to a round of the
// single compaction policy: its single compaction candidates, which become
// views once admission has let them through, the sort views of its invisible
// and of its visible unsorted segments, and the TTL and trigger id the
// candidates' views are built with.
type SingleCollectionRound struct {
	Candidates         []*SingleCandidate
	InvisibleSortViews []view.CompactionView
	VisibleSortViews   []view.CompactionView
	CollectionTTL      time.Duration
	TriggerID          int64
}

func (policy *SingleCompactionPolicy) TriggerOneCollection(ctx context.Context, collectionID int64, manual bool) (SingleCollectionRound, error) {
	log := mlog.WithLazy(mlog.FieldCollectionID(collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, unable to get collection from handler",
			mlog.Err(err))
		return SingleCollectionRound{}, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, collection not exist")
		return SingleCollectionRound{}, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip single compaction for external collection")
		return SingleCollectionRound{}, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "failed to apply singleCompactionPolicy, get collection ttl failed")
		return SingleCollectionRound{}, err
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, unable to allocate triggerID", mlog.Err(err))
		return SingleCollectionRound{}, err
	}

	invisibleSortViews, visibleSortViews, err := policy.triggerSortCompaction(ctx, newTriggerID, collectionID, collectionTTL)
	if err != nil {
		log.Warn(ctx, "failed to apply singleCompactionPolicy, trigger sort compaction failed", mlog.Err(err))
		return SingleCollectionRound{}, err
	}
	if !IsCollectionAutoCompactionEnabled(collection) {
		log.RatedInfo(ctx, rate.Limit(20), "collection auto compaction disabled")
		return SingleCollectionRound{InvisibleSortViews: invisibleSortViews, VisibleSortViews: visibleSortViews}, nil
	}

	candidates := make([]*SingleCandidate, 0)
	partSegments := meta.GetSegmentsChanPart(policy.Meta, collectionID, meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		return meta.IsSegmentHealthy(segment) &&
			meta.IsFlushed(segment) &&
			!segment.IsCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L2 && // only support L2 for now
			!segment.GetIsInvisible() &&
			!policy.Meta.IsSegmentCompactionProtected(segment.GetID()) // not protected by snapshot
	}))

	for _, group := range partSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.Segments = index.FilterInIndexedSegments(ctx, policy.handler, policy.IndexMeta, false, group.Segments...)
		}

		for _, segment := range group.Segments {
			// L2 candidates are all delete-driven, so they belong to the
			// accumulation class of the admission limiter.
			if HasTooManyDeletions(segment) {
				candidates = append(candidates, NewSingleCandidate(segment, SingleReasonAccumulation))
			}
		}
	}

	if len(candidates) > 0 {
		log.Info(ctx, "succeeded to apply singleCompactionPolicy",
			mlog.Int64("triggerID", newTriggerID),
			mlog.Int("candidate num", len(candidates)))
	}
	return SingleCollectionRound{
		Candidates:         candidates,
		InvisibleSortViews: invisibleSortViews,
		VisibleSortViews:   visibleSortViews,
		CollectionTTL:      collectionTTL,
		TriggerID:          newTriggerID,
	}, nil
}

var _ view.CompactionView = (*MixSegmentView)(nil)

type MixSegmentView struct {
	Label         *view.CompactionGroupLabel
	Segments      []*view.SegmentView
	CollectionTTL time.Duration
	TriggerID     int64
	// Admitted marks a view that took a single compaction admission token,
	// so the token is given back if the view never reaches the queue.
	Admitted bool
}

func (v *MixSegmentView) GetGroupLabel() *view.CompactionGroupLabel {
	if v == nil {
		return &view.CompactionGroupLabel{}
	}
	return v.Label
}

func (v *MixSegmentView) GetSegmentsView() []*view.SegmentView {
	if v == nil {
		return nil
	}

	return v.Segments
}

func (v *MixSegmentView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}
	return view.SumSegmentSize(v.Segments)
}

func (v *MixSegmentView) GetCollectionTTL() time.Duration {
	if v == nil {
		return 0
	}
	return v.CollectionTTL
}

func (v *MixSegmentView) Append(segments ...*view.SegmentView) {
	if v.Segments == nil {
		v.Segments = segments
		return
	}

	v.Segments = append(v.Segments, segments...)
}

func (v *MixSegmentView) String() string {
	strs := lo.Map(v.Segments, func(segView *view.SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("label=<%s>,  segments=%v", v.Label.String(), strs)
}

func (v *MixSegmentView) Trigger() (view.CompactionView, string) {
	return v, ""
}

func (v *MixSegmentView) ForceTrigger() (view.CompactionView, string) {
	panic("implement me")
}

func (v *MixSegmentView) ForceTriggerAll() ([]view.CompactionView, string) {
	panic("implement me")
}

func (v *MixSegmentView) GetTriggerID() int64 {
	return v.TriggerID
}
