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

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

// singleCompactionPolicy is to compact one segment with too many delta logs
// support l2 single segment only for now
// todo: move l1 single compaction here
type singleCompactionPolicy struct {
	meta      *meta
	allocator allocator
	handler   Handler
}

func newSingleCompactionPolicy(meta *meta, allocator allocator, handler Handler) *singleCompactionPolicy {
	return &singleCompactionPolicy{meta: meta, allocator: allocator, handler: handler}
}

func (policy *singleCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *singleCompactionPolicy) Trigger() (map[CompactionTriggerType][]CompactionView, error) {
	ctx := context.Background()
	collections := policy.meta.GetCollections()

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	views := make([]CompactionView, 0)
	for _, collection := range collections {
		collectionViews, _, err := policy.triggerOneCollection(ctx, collection.ID, false)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			log.Warn("fail to trigger single compaction", zap.Int64("collectionID", collection.ID), zap.Error(err))
		}
		views = append(views, collectionViews...)
	}
	events[TriggerTypeSingle] = views
	return events, nil
}

func (policy *singleCompactionPolicy) triggerOneCollection(ctx context.Context, collectionID int64, manual bool) ([]CompactionView, int64, error) {
	log := log.With(zap.Int64("collectionID", collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn("fail to apply singleCompactionPolicy, unable to get collection from handler",
			zap.Error(err))
		return nil, 0, err
	}
	if collection == nil {
		log.Warn("fail to apply singleCompactionPolicy, collection not exist")
		return nil, 0, nil
	}
	if !isCollectionAutoCompactionEnabled(collection) {
		log.RatedInfo(20, "collection auto compaction disabled")
		return nil, 0, nil
	}

	newTriggerID, err := policy.allocator.allocID(ctx)
	if err != nil {
		log.Warn("fail to apply singleCompactionPolicy, unable to allocate triggerID", zap.Error(err))
		return nil, 0, err
	}

	partSegments := GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L2 // only support L2 for now
	}))

	views := make([]CompactionView, 0)
	for _, group := range partSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(policy.handler, policy.meta, false, group.segments...)
		}

		collectionTTL, err := getCollectionTTL(collection.Properties)
		if err != nil {
			log.Warn("failed to apply singleCompactionPolicy, get collection ttl failed")
			return make([]CompactionView, 0), 0, err
		}

		for _, segment := range group.segments {
			if isDeltalogTooManySegment(segment) || isDeleteRowsTooManySegment(segment) {
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
	return views, newTriggerID, nil
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
