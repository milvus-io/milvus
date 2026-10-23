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
	collections := policy.Meta.GetCollections()

	events := make(map[CompactionTriggerType][]view.CompactionView, 0)
	views := make([]view.CompactionView, 0)
	sortViews := make([]view.CompactionView, 0)
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
		collectionViews, collectionSortViews, _, err := policy.TriggerOneCollection(ctx, collection.ID, false)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			mlog.Warn(ctx, "fail to trigger single compaction", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
		}
		views = append(views, collectionViews...)
		sortViews = append(sortViews, collectionSortViews...)
	}
	events[TriggerTypeSingle] = views
	events[TriggerTypeSort] = sortViews
	return events, nil
}

func (policy *SingleCompactionPolicy) TriggerSegmentSortCompaction(
	ctx context.Context,
	segmentID int64,
) view.CompactionView {
	log := mlog.With(mlog.FieldSegmentID(segmentID))
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

func (policy *SingleCompactionPolicy) triggerSortCompaction(
	ctx context.Context,
	triggerID int64,
	collectionID int64,
	collectionTTL time.Duration,
) ([]view.CompactionView, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(ctx, rate.Limit(20), "stats task disabled, skip sort compaction")
		return nil, nil
	}
	views := make([]view.CompactionView, 0)

	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, unable to get collection from handler",
			mlog.Err(err))
		return nil, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply triggerSegmentSortCompaction, collection not exist")
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip triggerSegmentSortCompaction for external collection", mlog.FieldCollectionID(collection.ID))
		return nil, nil
	}
	triggerableSegments := policy.Meta.SelectSegments(ctx, meta.WithCollection(collectionID),
		meta.SegmentFilterFunc(func(seg *meta.SegmentInfo) bool {
			return canTriggerSortCompaction(seg) &&
				!policy.Meta.IsSegmentCompactionProtected(seg.GetID())
		}))
	if len(triggerableSegments) == 0 {
		log.RatedInfo(ctx, rate.Limit(20), "no triggerable segments")
		return views, nil
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
			views = append(views, cv)
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
			views = append(views, cv)
		}
	}

	log.Info(ctx, "succeeded to apply triggerSortCompaction",
		mlog.Int64("triggerID", triggerID),
		mlog.Int("triggered view num", len(views)))
	return views, nil
}

func (policy *SingleCompactionPolicy) TriggerOneCollection(ctx context.Context, collectionID int64, manual bool) ([]view.CompactionView, []view.CompactionView, int64, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, unable to get collection from handler",
			mlog.Err(err))
		return nil, nil, 0, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, collection not exist")
		return nil, nil, 0, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip single compaction for external collection")
		return nil, nil, 0, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "failed to apply singleCompactionPolicy, get collection ttl failed")
		return nil, nil, 0, err
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to apply singleCompactionPolicy, unable to allocate triggerID", mlog.Err(err))
		return nil, nil, 0, err
	}

	sortViews, err := policy.triggerSortCompaction(ctx, newTriggerID, collectionID, collectionTTL)
	if err != nil {
		log.Warn(ctx, "failed to apply singleCompactionPolicy, trigger sort compaction failed", mlog.Err(err))
		return nil, nil, 0, err
	}
	if !IsCollectionAutoCompactionEnabled(collection) {
		log.RatedInfo(ctx, rate.Limit(20), "collection auto compaction disabled")
		return nil, sortViews, 0, nil
	}

	views := make([]view.CompactionView, 0)
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
			if HasTooManyDeletions(segment) {
				segmentViews := view.GetViewsByInfo(segment)
				cv := &MixSegmentView{
					Label:         segmentViews[0].Label,
					Segments:      segmentViews,
					CollectionTTL: collectionTTL,
					TriggerID:     newTriggerID,
				}
				views = append(views, cv)
			}
		}
	}

	if len(views) > 0 {
		log.Info(ctx, "succeeded to apply singleCompactionPolicy",
			mlog.Int64("triggerID", newTriggerID),
			mlog.Int("triggered view num", len(views)))
	}
	return views, sortViews, newTriggerID, nil
}

var _ view.CompactionView = (*MixSegmentView)(nil)

type MixSegmentView struct {
	Label         *view.CompactionGroupLabel
	Segments      []*view.SegmentView
	CollectionTTL time.Duration
	TriggerID     int64
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
