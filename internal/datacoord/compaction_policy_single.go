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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// singleCompactionPolicy is to compact one segment with too many delta logs
// support l2 single segment only for now
// todo: move l1 single compaction here
type singleCompactionPolicy struct {
	meta      *meta
	allocator allocator.Allocator
	handler   Handler
}

func newSingleCompactionPolicy(meta *meta, allocator allocator.Allocator, handler Handler) *singleCompactionPolicy {
	return &singleCompactionPolicy{meta: meta, allocator: allocator, handler: handler}
}

func (policy *singleCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *singleCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	collections := policy.meta.GetCollections()

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	views := make([]CompactionView, 0)
	sortViews := make([]CompactionView, 0)
	for _, collection := range collections {
		collectionViews, collectionSortViews, _, err := policy.triggerOneCollection(ctx, collection.ID, false)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			log.Warn("fail to trigger single compaction", zap.Int64("collectionID", collection.ID), zap.Error(err))
		}
		views = append(views, collectionViews...)
		sortViews = append(sortViews, collectionSortViews...)
	}
	events[TriggerTypeSingle] = views
	events[TriggerTypeSort] = sortViews
	return events, nil
}

func (policy *singleCompactionPolicy) triggerSegmentSortCompaction(
	ctx context.Context,
	segmentID int64,
) CompactionView {
	log := log.With(zap.Int64("segmentID", segmentID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(20, "stats task disabled, skip sort compaction")
		return nil
	}
	segment := policy.meta.GetHealthySegment(ctx, segmentID)
	if segment == nil {
		log.Warn("fail to apply triggerSegmentSortCompaction, segment not healthy")
		return nil
	}
	if !canTriggerSortCompaction(segment) {
		log.Warn("fail to apply triggerSegmentSortCompaction",
			zap.String("state", segment.GetState().String()),
			zap.String("level", segment.GetLevel().String()),
			zap.Bool("isSorted", segment.GetIsSorted()),
			zap.Bool("isImporting", segment.GetIsImporting()),
			zap.Bool("isCompacting", segment.isCompacting),
			zap.Bool("isInvisible", segment.GetIsInvisible()))
		return nil
	}

	collection, err := policy.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		log.Warn("fail to apply triggerSegmentSortCompaction, unable to get collection from handler",
			zap.Error(err))
		return nil
	}
	if collection == nil {
		log.Warn("fail to apply triggerSegmentSortCompaction, collection not exist")
		return nil
	}
	collectionTTL, err := getCollectionTTL(collection.Properties)
	if err != nil {
		log.Warn("failed to apply triggerSegmentSortCompaction, get collection ttl failed")
		return nil
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("fail to apply triggerSegmentSortCompaction, unable to allocate triggerID", zap.Error(err))
		return nil
	}

	segmentViews := GetViewsByInfo(segment)
	view := &MixSegmentView{
		label:         segmentViews[0].label,
		segments:      segmentViews,
		collectionTTL: collectionTTL,
		triggerID:     newTriggerID,
	}

	log.Info("succeeded to apply triggerSegmentSortCompaction",
		zap.Int64("triggerID", newTriggerID))
	return view
}

func (policy *singleCompactionPolicy) triggerSortCompaction(
	ctx context.Context,
	triggerID int64,
	collectionID int64,
	collectionTTL time.Duration,
) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionID))
	if !Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
		log.RatedInfo(20, "stats task disabled, skip sort compaction")
		return nil, nil
	}
	views := make([]CompactionView, 0)

	triggerableSegments := policy.meta.SelectSegments(ctx, WithCollection(collectionID),
		SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return canTriggerSortCompaction(seg)
		}))
	if len(triggerableSegments) == 0 {
		log.RatedInfo(20, "no triggerable segments")
		return views, nil
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
			views = append(views, view)
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
			views = append(views, view)
		}
	}

	log.Info("succeeded to apply triggerSortCompaction",
		zap.Int64("triggerID", triggerID),
		zap.Int("triggered view num", len(views)))
	return views, nil
}

func (policy *singleCompactionPolicy) triggerOneCollection(ctx context.Context, collectionID int64, manual bool) ([]CompactionView, []CompactionView, int64, error) {
	log := log.With(zap.Int64("collectionID", collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn("fail to apply singleCompactionPolicy, unable to get collection from handler",
			zap.Error(err))
		return nil, nil, 0, err
	}
	if collection == nil {
		log.Warn("fail to apply singleCompactionPolicy, collection not exist")
		return nil, nil, 0, nil
	}

	collectionTTL, err := getCollectionTTL(collection.Properties)
	if err != nil {
		log.Warn("failed to apply singleCompactionPolicy, get collection ttl failed")
		return nil, nil, 0, err
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("fail to apply singleCompactionPolicy, unable to allocate triggerID", zap.Error(err))
		return nil, nil, 0, err
	}

	sortViews, err := policy.triggerSortCompaction(ctx, newTriggerID, collectionID, collectionTTL)
	if err != nil {
		log.Warn("failed to apply singleCompactionPolicy, trigger sort compaction failed", zap.Error(err))
		return nil, nil, 0, err
	}
	if !isCollectionAutoCompactionEnabled(collection) {
		log.RatedInfo(20, "collection auto compaction disabled")
		return nil, sortViews, 0, nil
	}

	views := make([]CompactionView, 0)
	partSegments := GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L2 && // only support L2 for now
			!segment.GetIsInvisible()
	}))

	for _, group := range partSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(ctx, policy.handler, policy.meta, false, group.segments...)
		}

		for _, segment := range group.segments {
			if hasTooManyDeletions(segment) {
				segmentViews := GetViewsByInfo(segment)
				view := &MixSegmentView{
					label:         segmentViews[0].label,
					segments:      segmentViews,
					collectionTTL: collectionTTL,
					triggerID:     newTriggerID,
				}
				views = append(views, view)
			}
		}
	}

	if len(views) > 0 {
		log.Info("succeeded to apply singleCompactionPolicy",
			zap.Int64("triggerID", newTriggerID),
			zap.Int("triggered view num", len(views)))
	}
	return views, sortViews, newTriggerID, nil
}

var _ CompactionView = (*MixSegmentView)(nil)

type MixSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	collectionTTL time.Duration
	triggerID     int64
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
