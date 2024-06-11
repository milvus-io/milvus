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
	"sort"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type clusteringCompactionPolicy struct {
	meta              *meta
	view              *FullViews
	allocator         allocator
	compactionHandler compactionPlanContext
	handler           Handler
}

func newClusteringCompactionPolicy(meta *meta, view *FullViews, allocator allocator, compactionHandler compactionPlanContext, handler Handler) *clusteringCompactionPolicy {
	return &clusteringCompactionPolicy{meta: meta, view: view, allocator: allocator, compactionHandler: compactionHandler, handler: handler}
}

func (policy *clusteringCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() &&
		Params.DataCoordCfg.ClusteringCompactionEnable.GetAsBool() &&
		Params.DataCoordCfg.ClusteringCompactionAutoEnable.GetAsBool()
}

func (policy *clusteringCompactionPolicy) Trigger() (map[CompactionTriggerType][]CompactionView, error) {
	ctx := context.Background()
	collections := policy.meta.GetCollections()
	ts, err := policy.allocator.allocTimestamp(ctx)
	if err != nil {
		log.Warn("allocate ts failed, skip to handle compaction")
		return make(map[CompactionTriggerType][]CompactionView, 0), err
	}

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	views := make([]CompactionView, 0)
	for _, collection := range collections {
		collectionViews, _, err := policy.triggerOneCollection(ctx, collection.ID, ts, false)
		if err != nil {
			log.Warn("fail to trigger collection clustering compaction", zap.Int64("collectionID", collection.ID))
			return make(map[CompactionTriggerType][]CompactionView, 0), err
		}
		views = append(views, collectionViews...)
	}
	events[TriggerTypeClustering] = views
	return events, nil
}

func (policy *clusteringCompactionPolicy) checkAllL2SegmentsContains(ctx context.Context, collectionID, partitionID int64, channel string) bool {
	getCompactingL2Segment := func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionID &&
			segment.PartitionID == partitionID &&
			segment.InsertChannel == channel &&
			isSegmentHealthy(segment) &&
			segment.GetLevel() == datapb.SegmentLevel_L2 &&
			segment.isCompacting
	}
	segments := policy.meta.SelectSegments(SegmentFilterFunc(getCompactingL2Segment))
	if len(segments) > 0 {
		log.Ctx(ctx).Info("there are some segments are compacting",
			zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID),
			zap.String("channel", channel), zap.Int64s("compacting segment", lo.Map(segments, func(segment *SegmentInfo, i int) int64 {
				return segment.GetID()
			})))
		return false
	}
	return true
}

func (policy *clusteringCompactionPolicy) triggerOneCollection(ctx context.Context, collectionID int64, ts Timestamp, manual bool) ([]CompactionView, int64, error) {
	log.Info("trigger collection clustering compaction", zap.Int64("collectionID", collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn("fail to get collection")
		return nil, 0, err
	}
	clusteringKeyField := clustering.GetClusteringKeyField(collection.Schema)
	if clusteringKeyField == nil {
		return nil, 0, nil
	}

	// if not pass, alloc a new one
	if ts == 0 {
		tsNew, err := policy.allocator.allocTimestamp(ctx)
		if err != nil {
			log.Warn("allocate ts failed, skip to handle compaction")
			return nil, 0, err
		}
		ts = tsNew
	}

	compacting, triggerID := policy.collectionIsClusteringCompacting(collection.ID)
	if compacting {
		log.Info("collection is clustering compacting", zap.Int64("collectionID", collection.ID), zap.Int64("triggerID", triggerID))
		return nil, triggerID, nil
	}

	newTriggerID, err := policy.allocator.allocID(ctx)
	if err != nil {
		log.Warn("fail to allocate triggerID", zap.Error(err))
		return nil, 0, err
	}

	partSegments := policy.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionID &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() != datapb.SegmentLevel_L0 // ignore level zero segments
	})

	views := make([]CompactionView, 0)
	// partSegments is list of chanPartSegments, which is channel-partition organized segments
	for _, group := range partSegments {
		log := log.Ctx(ctx).With(zap.Int64("collectionID", group.collectionID),
			zap.Int64("partitionID", group.partitionID),
			zap.String("channel", group.channelName))

		if !policy.checkAllL2SegmentsContains(ctx, group.collectionID, group.partitionID, group.channelName) {
			log.Warn("clustering compaction cannot be done, otherwise the performance will fall back")
			continue
		}

		ct, err := getCompactTime(ts, collection)
		if err != nil {
			log.Warn("get compact time failed, skip to handle compaction")
			return make([]CompactionView, 0), 0, err
		}

		if len(group.segments) == 0 {
			log.Info("the length of SegmentsChanPart is 0, skip to handle compaction")
			continue
		}

		if !manual {
			execute, err := triggerClusteringCompactionPolicy(ctx, policy.meta, group.collectionID, group.partitionID, group.channelName, group.segments)
			if err != nil {
				log.Warn("failed to trigger clustering compaction", zap.Error(err))
				continue
			}
			if !execute {
				continue
			}
		}

		segmentViews := GetViewsByInfo(group.segments...)
		view := &ClusteringSegmentsView{
			label:              segmentViews[0].label,
			segments:           segmentViews,
			clusteringKeyField: clusteringKeyField,
			compactionTime:     ct,
			triggerID:          newTriggerID,
		}
		views = append(views, view)
	}

	log.Info("trigger collection clustering compaction", zap.Int64("collectionID", collectionID), zap.Int("viewNum", len(views)))
	return views, newTriggerID, nil
}

func (policy *clusteringCompactionPolicy) collectionIsClusteringCompacting(collectionID UniqueID) (bool, int64) {
	triggers := policy.meta.compactionTaskMeta.GetCompactionTasksByCollection(collectionID)
	if len(triggers) == 0 {
		return false, 0
	}
	var latestTriggerID int64 = 0
	for triggerID := range triggers {
		if latestTriggerID > triggerID {
			latestTriggerID = triggerID
		}
	}
	tasks := triggers[latestTriggerID]
	if len(tasks) > 0 {
		cTasks := tasks
		summary := summaryCompactionState(cTasks)
		return summary.state == commonpb.CompactionState_Executing, cTasks[0].TriggerID
	}
	return false, 0
}

func calculateClusteringCompactionConfig(view CompactionView) (segmentIDs []int64, totalRows, maxSegmentRows, preferSegmentRows int64) {
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
		segmentIDs = append(segmentIDs, s.ID)
	}
	clusteringMaxSegmentSize := paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSize.GetAsSize()
	clusteringPreferSegmentSize := paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSize.GetAsSize()
	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	maxSegmentRows = view.GetSegmentsView()[0].MaxRowNum * clusteringMaxSegmentSize / segmentMaxSize
	preferSegmentRows = view.GetSegmentsView()[0].MaxRowNum * clusteringPreferSegmentSize / segmentMaxSize
	return
}

func triggerClusteringCompactionPolicy(ctx context.Context, meta *meta, collectionID int64, partitionID int64, channel string, segments []*SegmentInfo) (bool, error) {
	log := log.With(zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
	partitionStatsInfos := meta.partitionStatsMeta.ListPartitionStatsInfos(collectionID, partitionID, channel)
	sort.Slice(partitionStatsInfos, func(i, j int) bool {
		return partitionStatsInfos[i].Version > partitionStatsInfos[j].Version
	})

	if len(partitionStatsInfos) == 0 {
		var newDataSize int64 = 0
		for _, seg := range segments {
			newDataSize += seg.getSegmentSize()
		}
		if newDataSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
			log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", newDataSize))
			return true, nil
		}
		log.Info("No partition stats and no enough new data, skip compaction")
		return false, nil
	}

	partitionStats := partitionStatsInfos[0]
	version := partitionStats.Version
	pTime, _ := tsoutil.ParseTS(uint64(version))
	if time.Since(pTime) < Params.DataCoordCfg.ClusteringCompactionMinInterval.GetAsDuration(time.Second) {
		log.Info("Too short time before last clustering compaction, skip compaction")
		return false, nil
	}
	if time.Since(pTime) > Params.DataCoordCfg.ClusteringCompactionMaxInterval.GetAsDuration(time.Second) {
		log.Info("It is a long time after last clustering compaction, do compaction")
		return true, nil
	}

	var compactedSegmentSize int64 = 0
	var uncompactedSegmentSize int64 = 0
	for _, seg := range segments {
		if lo.Contains(partitionStats.SegmentIDs, seg.ID) {
			compactedSegmentSize += seg.getSegmentSize()
		} else {
			uncompactedSegmentSize += seg.getSegmentSize()
		}
	}

	// size based
	if uncompactedSegmentSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
		log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
		return true, nil
	}
	log.Info("New data is smaller than threshold, skip compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
	return false, nil
}

var _ CompactionView = (*ClusteringSegmentsView)(nil)

type ClusteringSegmentsView struct {
	label              *CompactionGroupLabel
	segments           []*SegmentView
	clusteringKeyField *schemapb.FieldSchema
	compactionTime     *compactTime
	triggerID          int64
}

func (v *ClusteringSegmentsView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *ClusteringSegmentsView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *ClusteringSegmentsView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *ClusteringSegmentsView) String() string {
	strs := lo.Map(v.segments, func(v *SegmentView, _ int) string {
		return v.String()
	})
	return fmt.Sprintf("label=<%s>,  segments=%v", v.label.String(), strs)
}

func (v *ClusteringSegmentsView) Trigger() (CompactionView, string) {
	// todo set reason
	return v, ""
}

func (v *ClusteringSegmentsView) ForceTrigger() (CompactionView, string) {
	// TODO implement me
	panic("implement me")
}
