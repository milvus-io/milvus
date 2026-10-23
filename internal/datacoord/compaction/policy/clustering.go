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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type ClusteringCompactionPolicy struct {
	Meta               *meta.Meta
	CompactionTaskMeta *ctask.CompactionTaskMeta
	partitionStatsMeta *partitionstats.PartitionStatsMeta
	allocator          allocator.Allocator
	handler            meta.Handler
}

// Ensure clusteringCompactionPolicy implements CompactionPolicy interface
var _ CompactionPolicy = (*ClusteringCompactionPolicy)(nil)

func NewClusteringCompactionPolicy(mt *meta.Meta, compactionTaskMeta *ctask.CompactionTaskMeta, partitionStatsMeta *partitionstats.PartitionStatsMeta, allocator allocator.Allocator, handler meta.Handler) *ClusteringCompactionPolicy {
	return &ClusteringCompactionPolicy{Meta: mt, CompactionTaskMeta: compactionTaskMeta, partitionStatsMeta: partitionStatsMeta, allocator: allocator, handler: handler}
}

func (policy *ClusteringCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() &&
		Params.DataCoordCfg.ClusteringCompactionEnable.GetAsBool() &&
		Params.DataCoordCfg.ClusteringCompactionAutoEnable.GetAsBool()
}

func (policy *ClusteringCompactionPolicy) Name() string {
	return "ClusteringCompactionPolicy"
}

func (policy *ClusteringCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]view.CompactionView, error) {
	mlog.Info(ctx, "start trigger clusteringCompactionPolicy...")
	collections := policy.Meta.GetCollections()

	events := make(map[CompactionTriggerType][]view.CompactionView, 0)
	views := make([]view.CompactionView, 0)
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		if collection.IsExternal() {
			mlog.Info(ctx, "skip clustering compaction for external collection", mlog.FieldCollectionID(collection.ID))
			continue
		}
		if policy.Meta.IsCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip clustering compaction for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}
		collectionViews, _, err := policy.TriggerOneCollection(ctx, collection.ID, false)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			mlog.Warn(ctx, "fail to trigger collection clustering compaction", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
		}
		views = append(views, collectionViews...)
	}
	events[TriggerTypeClustering] = views
	return events, nil
}

// todo: remove this check after support partial clustering compaction
func (policy *ClusteringCompactionPolicy) checkAllL2SegmentsContains(ctx context.Context, collectionID, partitionID int64, channel string) bool {
	getCompactingL2Segment := func(segment *meta.SegmentInfo) bool {
		return segment.CollectionID == collectionID &&
			segment.PartitionID == partitionID &&
			segment.InsertChannel == channel &&
			meta.IsSegmentHealthy(segment) &&
			segment.GetLevel() == datapb.SegmentLevel_L2 &&
			segment.IsCompacting
	}
	segments := policy.Meta.SelectSegments(ctx, meta.SegmentFilterFunc(getCompactingL2Segment))
	if len(segments) > 0 {
		mlog.Info(ctx, "there are some segments are compacting",
			mlog.FieldCollectionID(collectionID), mlog.FieldPartitionID(partitionID),
			mlog.String("channel", channel), mlog.Int64s("compacting segment", lo.Map(segments, func(segment *meta.SegmentInfo, i int) int64 {
				return segment.GetID()
			})))
		return false
	}
	return true
}

func (policy *ClusteringCompactionPolicy) TriggerOneCollection(ctx context.Context, collectionID int64, manual bool) ([]view.CompactionView, int64, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	log.Info(ctx, "start trigger collection clustering compaction")
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to get collection from handler")
		return nil, 0, err
	}
	if collection == nil {
		log.Warn(ctx, "collection not exist")
		return nil, 0, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip clustering compaction for external collection")
		return nil, 0, nil
	}
	clusteringKeyField := clustering.GetClusteringKeyField(collection.Schema)
	if clusteringKeyField == nil {
		log.Info(ctx, "the collection has no clustering key, skip tigger clustering compaction")
		return nil, 0, nil
	}

	compacting, triggerID := policy.CollectionIsClusteringCompacting(collection.ID)
	if compacting {
		log.Info(ctx, "collection is clustering compacting", mlog.Int64("triggerID", triggerID))
		return nil, triggerID, nil
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to allocate triggerID", mlog.Err(err))
		return nil, 0, err
	}

	namespaceEnabled := collection.Schema.GetEnableNamespace()
	partSegments := meta.GetSegmentsChanPart(policy.Meta, collectionID, meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		return meta.IsSegmentHealthy(segment) &&
			meta.IsFlushed(segment) &&
			!segment.IsCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() != datapb.SegmentLevel_L0 && // ignore level zero segments
			!segment.GetIsInvisible() &&
			(!namespaceEnabled || segment.GetIsSortedByNamespace()) &&
			!policy.Meta.IsSegmentCompactionProtected(segment.GetID()) // not protected by snapshot
	}))

	views := make([]view.CompactionView, 0)
	// partSegments is list of chanPartSegments, which is channel-partition organized segments
	for _, group := range partSegments {
		log := mlog.With(mlog.FieldPartitionID(group.PartitionID), mlog.String("channel", group.ChannelName))

		if !policy.checkAllL2SegmentsContains(ctx, group.CollectionID, group.PartitionID, group.ChannelName) {
			log.Warn(ctx, "clustering compaction cannot be done, otherwise the performance will fall back")
			continue
		}

		collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
		if err != nil {
			log.Warn(ctx, "get collection ttl failed, skip to handle compaction")
			return make([]view.CompactionView, 0), 0, err
		}

		if len(group.Segments) == 0 {
			log.Info(ctx, "the length of SegmentsChanPart is 0, skip to handle compaction")
			continue
		}

		if !manual {
			execute, err := TriggerClusteringCompactionPolicy(ctx, policy.partitionStatsMeta, group.CollectionID, group.PartitionID, group.ChannelName, group.Segments)
			if err != nil {
				log.Warn(ctx, "failed to trigger clustering compaction", mlog.Err(err))
				continue
			}
			if !execute {
				continue
			}
		}

		segmentViews := view.GetViewsByInfo(group.Segments...)
		cv := &ClusteringSegmentsView{
			Label:              segmentViews[0].Label,
			Segments:           segmentViews,
			ClusteringKeyField: clusteringKeyField,
			CollectionTTL:      collectionTTL,
			TriggerID:          newTriggerID,
		}
		views = append(views, cv)
	}

	log.Info(ctx, "finish trigger collection clustering compaction", mlog.Int("viewNum", len(views)))
	return views, newTriggerID, nil
}

func (policy *ClusteringCompactionPolicy) CollectionIsClusteringCompacting(collectionID UniqueID) (bool, int64) {
	triggers := policy.CompactionTaskMeta.GetCompactionTasksByCollection(collectionID)
	if len(triggers) == 0 {
		return false, 0
	}
	var latestTriggerID int64 = 0
	for triggerID := range triggers {
		if triggerID > latestTriggerID {
			latestTriggerID = triggerID
		}
	}
	tasks := triggers[latestTriggerID]
	if len(tasks) > 0 {
		cTasks := tasks
		summary := SummaryCompactionState(latestTriggerID, cTasks)
		return summary.State == commonpb.CompactionState_Executing, cTasks[0].TriggerID
	}
	return false, 0
}

func CalculateClusteringCompactionConfig(coll *meta.CollectionInfo, cv view.CompactionView, expectedSegmentSize int64) (totalRows, maxSegmentRows, preferSegmentRows int64, err error) {
	segments := cv.GetSegmentsView()
	for _, s := range segments {
		totalRows += s.NumOfRows
	}
	clusteringMaxSegmentSizeRatio := paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.GetAsFloat()
	clusteringPreferSegmentSizeRatio := paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat()

	segmentRows, err := estimateRowsBySegmentSize(segments, expectedSegmentSize)
	if err != nil {
		return 0, 0, 0, err
	}
	maxSegmentRows = int64(float64(segmentRows) * clusteringMaxSegmentSizeRatio)
	preferSegmentRows = int64(float64(segmentRows) * clusteringPreferSegmentSizeRatio)
	return totalRows, maxSegmentRows, preferSegmentRows, err
}

func estimateRowsBySegmentSize(segments []*view.SegmentView, expectedSegmentSize int64) (int64, error) {
	if expectedSegmentSize <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("invalid expected segment size %d", expectedSegmentSize)
	}

	var totalSize float64
	var totalRows int64
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		if segment.NumOfRows <= 0 || segment.Size <= 0 {
			continue
		}
		totalSize += segment.Size
		totalRows += segment.NumOfRows
	}

	if totalRows == 0 || totalSize == 0 {
		return 0, merr.WrapErrServiceInternalMsg("segment view does not contain size info to estimate row count")
	}

	rowSize := totalSize / float64(totalRows)
	if rowSize <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("invalid row size calculated from segment view")
	}

	rows := float64(expectedSegmentSize) / rowSize
	if rows <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("estimated max row count is not positive")
	}

	return int64(rows), nil
}

func TriggerClusteringCompactionPolicy(ctx context.Context, partitionStatsMeta *partitionstats.PartitionStatsMeta, collectionID int64, partitionID int64, channel string, segments []*meta.SegmentInfo) (bool, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID), mlog.FieldPartitionID(partitionID))
	currentVersion := partitionStatsMeta.GetCurrentPartitionStatsVersion(collectionID, partitionID, channel)
	if currentVersion == 0 {
		var newDataSize int64 = 0
		for _, seg := range segments {
			newDataSize += seg.GetSegmentSize()
		}
		if newDataSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
			log.Info(ctx, "New data is larger than threshold, do compaction", mlog.Int64("newDataSize", newDataSize))
			return true, nil
		}
		log.Info(ctx, "No partition stats and no enough new data, skip compaction", mlog.Int64("newDataSize", newDataSize))
		return false, nil
	}

	partitionStats := partitionStatsMeta.GetPartitionStats(collectionID, partitionID, channel, currentVersion)
	if partitionStats == nil {
		log.Info(ctx, "partition stats not found")
		return false, nil
	}
	timestampSeconds := partitionStats.GetCommitTime()
	pTime := time.Unix(timestampSeconds, 0)
	if time.Since(pTime) < Params.DataCoordCfg.ClusteringCompactionMinInterval.GetAsDuration(time.Second) {
		log.Info(ctx, "Too short time before last clustering compaction, skip compaction")
		return false, nil
	}
	if time.Since(pTime) > Params.DataCoordCfg.ClusteringCompactionMaxInterval.GetAsDuration(time.Second) {
		log.Info(ctx, "It is a long time after last clustering compaction, do compaction")
		return true, nil
	}

	var compactedSegmentSize int64 = 0
	var uncompactedSegmentSize int64 = 0
	for _, seg := range segments {
		if lo.Contains(partitionStats.SegmentIDs, seg.ID) {
			compactedSegmentSize += seg.GetSegmentSize()
		} else {
			uncompactedSegmentSize += seg.GetSegmentSize()
		}
	}

	// size based
	if uncompactedSegmentSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
		log.Info(ctx, "New data is larger than threshold, do compaction", mlog.Int64("newDataSize", uncompactedSegmentSize))
		return true, nil
	}
	log.Info(ctx, "New data is smaller than threshold, skip compaction", mlog.Int64("newDataSize", uncompactedSegmentSize))
	return false, nil
}

var _ view.CompactionView = (*ClusteringSegmentsView)(nil)

type ClusteringSegmentsView struct {
	Label              *view.CompactionGroupLabel
	Segments           []*view.SegmentView
	ClusteringKeyField *schemapb.FieldSchema
	CollectionTTL      time.Duration
	TriggerID          int64
}

func (v *ClusteringSegmentsView) GetGroupLabel() *view.CompactionGroupLabel {
	if v == nil {
		return &view.CompactionGroupLabel{}
	}
	return v.Label
}

func (v *ClusteringSegmentsView) GetSegmentsView() []*view.SegmentView {
	if v == nil {
		return nil
	}
	return v.Segments
}

func (v *ClusteringSegmentsView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}
	return view.SumSegmentSize(v.Segments)
}

func (v *ClusteringSegmentsView) GetCollectionTTL() time.Duration {
	if v == nil {
		return 0
	}
	return v.CollectionTTL
}

func (v *ClusteringSegmentsView) Append(segments ...*view.SegmentView) {
	if v.Segments == nil {
		v.Segments = segments
		return
	}

	v.Segments = append(v.Segments, segments...)
}

func (v *ClusteringSegmentsView) String() string {
	strs := lo.Map(v.Segments, func(segView *view.SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("label=<%s>,  segments=%v", v.Label.String(), strs)
}

func (v *ClusteringSegmentsView) Trigger() (view.CompactionView, string) {
	return v, ""
}

func (v *ClusteringSegmentsView) GetTriggerID() int64 {
	return v.TriggerID
}

func (v *ClusteringSegmentsView) ForceTrigger() (view.CompactionView, string) {
	panic("implement me")
}

func (v *ClusteringSegmentsView) ForceTriggerAll() ([]view.CompactionView, string) {
	panic("implement me")
}
