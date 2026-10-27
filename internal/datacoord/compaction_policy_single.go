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

package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// singleCompactionPolicy is to compact one segment with too many delta logs
// support l2 single segment only for now
// todo: move l1 single compaction here
type singleCompactionPolicy struct {
	meta      *meta
	allocator allocator.Allocator
	handler   Handler
	// remainingCapacity reports how many tasks the inspector can still take, so
	// admission does not spend tokens on views that would be dropped. nil means
	// unbounded.
	remainingCapacity func() int
}

// Ensure singleCompactionPolicy implements CompactionPolicy interface
var _ CompactionPolicy = (*singleCompactionPolicy)(nil)

func newSingleCompactionPolicy(meta *meta, allocator allocator.Allocator, handler Handler) *singleCompactionPolicy {
	return &singleCompactionPolicy{meta: meta, allocator: allocator, handler: handler}
}

func (policy *singleCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *singleCompactionPolicy) Name() string {
	return "SingleCompactionPolicy"
}

func (policy *singleCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	collections := policy.meta.GetCollections()

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	// Invisible segments, which a flush or a clustering compaction just wrote,
	// wait for their sort to become visible, so their sorts go first: under
	// dataCoord.compaction.maxTaskNum only the first sorts may be submitted.
	invisibleSortViews := make([]CompactionView, 0)
	visibleSortViews := make([]CompactionView, 0)
	// Single compaction candidates are gathered from every collection first and
	// admitted in one pass, so the admission budget is shared across
	// collections instead of being drained by the first one in the walk.
	rounds := make(map[int64]singleCollectionRound)
	candidates := make([]*singleCandidate, 0)
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		if collection.IsExternal() {
			mlog.Info(ctx, "skip single compaction trigger for external collection", mlog.FieldCollectionID(collection.ID))
			continue
		}
		if policy.meta.isCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip single compaction for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}
		round, err := policy.triggerOneCollection(ctx, collection.ID, false)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			mlog.Warn(ctx, "fail to trigger single compaction", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
		}
		invisibleSortViews = append(invisibleSortViews, round.invisibleSortViews...)
		visibleSortViews = append(visibleSortViews, round.visibleSortViews...)
		if len(round.candidates) > 0 {
			rounds[collection.ID] = round
			candidates = append(candidates, round.candidates...)
		}
	}

	capacity := -1
	if policy.remainingCapacity != nil {
		capacity = policy.remainingCapacity()
	}
	admitted, deferred := getSingleCompactionAdmitter().admit(ctx, admissionSourcePolicy, candidates, capacity)
	views := make([]CompactionView, 0, len(admitted))
	for _, c := range admitted {
		round := rounds[c.collectionID]
		segmentViews := GetViewsByInfo(c.segment)
		views = append(views, &MixSegmentView{
			label:         segmentViews[0].label,
			segments:      segmentViews,
			collectionTTL: round.collectionTTL,
			triggerID:     round.triggerID,
			admitted:      true,
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

func (policy *singleCompactionPolicy) triggerSegmentSortCompaction(
	ctx context.Context,
	segmentID int64,
) CompactionView {
	log := mlog.WithLazy(mlog.FieldSegmentID(segmentID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(ctx, rate.Limit(20), "stats task disabled, skip sort compaction")
		return nil
	}
	segment := policy.meta.GetHealthySegment(ctx, segmentID)
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
			mlog.Bool("isCompacting", segment.isCompacting),
			mlog.Bool("isInvisible", segment.GetIsInvisible()))
		return nil
	}
	if policy.meta.isSegmentCompactionProtected(segment.GetID()) {
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

	segmentViews := GetViewsByInfo(segment)
	view := &MixSegmentView{
		label:         segmentViews[0].label,
		segments:      segmentViews,
		collectionTTL: collectionTTL,
		triggerID:     newTriggerID,
	}

	log.Info(ctx, "succeeded to apply triggerSegmentSortCompaction",
		mlog.Int64("triggerID", newTriggerID))
	return view
}

// triggerSortCompaction returns the sort views of the collection's invisible
// and of its visible unsorted segments.
func (policy *singleCompactionPolicy) triggerSortCompaction(
	ctx context.Context,
	triggerID int64,
	collectionID int64,
	collectionTTL time.Duration,
) ([]CompactionView, []CompactionView, error) {
	log := mlog.WithLazy(mlog.FieldCollectionID(collectionID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(ctx, rate.Limit(20), "stats task disabled, skip sort compaction")
		return nil, nil, nil
	}
	invisibleViews := make([]CompactionView, 0)
	visibleViews := make([]CompactionView, 0)

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
	triggerableSegments := policy.meta.SelectSegments(ctx, WithCollection(collectionID),
		SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return canTriggerSortCompaction(seg) &&
				!policy.meta.isSegmentCompactionProtected(seg.GetID())
		}))
	if len(triggerableSegments) == 0 {
		log.RatedInfo(ctx, rate.Limit(20), "no triggerable segments")
		return invisibleViews, visibleViews, nil
	}

	gbSegments := lo.GroupBy(triggerableSegments, func(seg *SegmentInfo) bool {
		return seg.GetIsInvisible()
	})
	invisibleSegments, ok := gbSegments[true]
	if ok {
		for _, segment := range invisibleSegments {
			segmentViews := GetViewsByInfo(segment)
			view := &MixSegmentView{
				label:         segmentViews[0].label,
				segments:      segmentViews,
				collectionTTL: collectionTTL,
				triggerID:     triggerID,
			}
			invisibleViews = append(invisibleViews, view)
		}
	}

	visibleSegments, ok := gbSegments[false]
	if ok {
		for i, segment := range visibleSegments {
			if i > Params.DataCoordCfg.SortCompactionTriggerCount.GetAsInt() {
				break
			}
			segmentViews := GetViewsByInfo(segment)
			view := &MixSegmentView{
				label:         segmentViews[0].label,
				segments:      segmentViews,
				collectionTTL: collectionTTL,
				triggerID:     triggerID,
			}
			visibleViews = append(visibleViews, view)
		}
	}

	log.Info(ctx, "succeeded to apply triggerSortCompaction",
		mlog.Int64("triggerID", triggerID),
		mlog.Int("triggered view num", len(invisibleViews)+len(visibleViews)))
	return invisibleViews, visibleViews, nil
}

// singleCollectionRound is what one collection contributes to a round of the
// single compaction policy: its single compaction candidates, which become
// views once admission has let them through, the sort views of its invisible
// and of its visible unsorted segments, and the TTL and trigger id the
// candidates' views are built with.
type singleCollectionRound struct {
	candidates         []*singleCandidate
	invisibleSortViews []CompactionView
	visibleSortViews   []CompactionView
	collectionTTL      time.Duration
	triggerID          int64
}

func (policy *singleCompactionPolicy) triggerOneCollection(ctx context.Context, collectionID int64, manual bool) (singleCollectionRound, error) {
	log := mlog.WithLazy(mlog.FieldCollectionID(collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, unable to get collection from handler",
			mlog.Err(err))
		return singleCollectionRound{}, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, collection not exist")
		return singleCollectionRound{}, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip single compaction for external collection")
		return singleCollectionRound{}, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "failed to apply singleCompactionPolicy, get collection ttl failed")
		return singleCollectionRound{}, err
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, unable to allocate triggerID", mlog.Err(err))
		return singleCollectionRound{}, err
	}

	invisibleSortViews, visibleSortViews, err := policy.triggerSortCompaction(ctx, newTriggerID, collectionID, collectionTTL)
	if err != nil {
		log.Warn(ctx, "failed to apply singleCompactionPolicy, trigger sort compaction failed", mlog.Err(err))
		return singleCollectionRound{}, err
	}
	if !isCollectionAutoCompactionEnabled(collection) {
		log.RatedInfo(ctx, rate.Limit(20), "collection auto compaction disabled")
		return singleCollectionRound{invisibleSortViews: invisibleSortViews, visibleSortViews: visibleSortViews}, nil
	}

	candidates := make([]*singleCandidate, 0)
	partSegments := GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L2 && // only support L2 for now
			!segment.GetIsInvisible() &&
			!policy.meta.isSegmentCompactionProtected(segment.GetID()) // not protected by snapshot
	}))

	for _, group := range partSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(ctx, policy.handler, policy.meta, false, group.segments...)
		}

		for _, segment := range group.segments {
			// L2 candidates are all delete-driven, so they belong to the
			// accumulation class of the admission limiter.
			if hasTooManyDeletions(segment) {
				candidates = append(candidates, newSingleCandidate(segment, singleReasonAccumulation))
			}
		}
	}

	if len(candidates) > 0 {
		log.Info(ctx, "succeeded to apply singleCompactionPolicy",
			mlog.Int64("triggerID", newTriggerID),
			mlog.Int("candidate num", len(candidates)))
	}
	return singleCollectionRound{
		candidates:         candidates,
		invisibleSortViews: invisibleSortViews,
		visibleSortViews:   visibleSortViews,
		collectionTTL:      collectionTTL,
		triggerID:          newTriggerID,
	}, nil
}

var _ CompactionView = (*MixSegmentView)(nil)

type MixSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	collectionTTL time.Duration
	triggerID     int64
	// admitted marks a view that took a single compaction admission token,
	// so the token is given back if the view never reaches the queue.
	admitted bool
}

func (v *MixSegmentView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *MixSegmentView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *MixSegmentView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}
	return sumSegmentSize(v.segments)
}

func (v *MixSegmentView) GetCollectionTTL() time.Duration {
	if v == nil {
		return 0
	}
	return v.collectionTTL
}

func (v *MixSegmentView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *MixSegmentView) String() string {
	strs := lo.Map(v.segments, func(segView *SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("label=<%s>,  segments=%v", v.label.String(), strs)
}

func (v *MixSegmentView) Trigger() (CompactionView, string) {
	return v, ""
}

func (v *MixSegmentView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}

func (v *MixSegmentView) ForceTriggerAll() ([]CompactionView, string) {
	panic("implement me")
}

func (v *MixSegmentView) GetTriggerID() int64 {
	return v.triggerID
}
