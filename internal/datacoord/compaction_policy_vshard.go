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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

// vshardCompactionPolicy
type vshardCompactionPolicy struct {
	meta          *meta
	allocator     allocator.Allocator
	handler       Handler
	vshardManager VshardManager
}

func newVshardCompactionPolicy(
	meta *meta,
	allocator allocator.Allocator,
	handler Handler,
	vshardManager VshardManager) *vshardCompactionPolicy {
	return &vshardCompactionPolicy{
		meta:          meta,
		allocator:     allocator,
		handler:       handler,
		vshardManager: vshardManager,
	}
}

func (policy *vshardCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() && Params.DataCoordCfg.VShardEnable.GetAsBool()
}

func (policy *vshardCompactionPolicy) Trigger() (map[CompactionTriggerType][]CompactionView, error) {
	log.Info("start trigger vshard Compaction...")
	ctx := context.Background()
	collections := policy.meta.GetCollections()

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	views := make([]CompactionView, 0)
	for _, collection := range collections {
		collectionInfo, err := policy.handler.GetCollection(ctx, collection.ID)
		if err != nil {
			log.Warn("fail to get collection from handler")
			return nil, err
		}
		if collectionInfo == nil {
			log.Warn("collection not exist")
			return nil, nil
		}
		if !isCollectionAutoCompactionEnabled(collectionInfo) {
			log.RatedInfo(20, "collection auto compaction disabled")
			return nil, nil
		}
		expectedSize := getExpectedSegmentSize(policy.meta, collectionInfo)

		for _, partitionID := range collectionInfo.Partitions {
			collectionViews, err := policy.triggerPartition(ctx, collectionInfo, partitionID, expectedSize)
			if err != nil {
				// not throw this error, no need to fail because of one collection
				log.Warn("fail to trigger vshard allocate compaction", zap.Int64("collectionID", collection.ID), zap.Error(err))
			}
			views = append(views, collectionViews...)
		}
	}
	events[TriggerTypeVshard] = views
	return events, nil
}

func (policy *vshardCompactionPolicy) triggerPartition(ctx context.Context, collectionInfo *collectionInfo, partitionID int64, expectedSize int64) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionInfo.ID), zap.Int64("partitionID", partitionID))
	log.Info("start trigger partition vshard compaction")
	allocateVshardViews, err := policy.generateViewForSegmentsWithoutVshard(ctx, collectionInfo, partitionID, expectedSize)
	if err != nil {
		return nil, err
	}
	updateVshardViews, err := policy.generateViewForVshardUpdate(ctx, collectionInfo, partitionID)
	if err != nil {
		return nil, err
	}
	views := append(allocateVshardViews, updateVshardViews...)
	return views, nil
}

func (policy *vshardCompactionPolicy) generateViewForSegmentsWithoutVshard(ctx context.Context, collectionInfo *collectionInfo, partitionID int64, expectedSize int64) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionInfo.ID), zap.Int64("partitionID", partitionID))
	candidatePartChanSegments := policy.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionInfo.ID &&
			segment.PartitionID == partitionID &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L1 && // todo only L1 for now
			segment.GetVshardDesc() == nil
	})
	log.Info("Get segments without vshard", zap.Int("len", len(candidatePartChanSegments)))

	// filter out no vshardInfo partition-channels to avoid useless AllocID
	checkedPartChanSegments := make(map[*chanPartSegments][]*datapb.VShardDesc)
	for _, channelGroup := range candidatePartChanSegments {
		partChanVshardInfos := policy.vshardManager.GetPartChanVShardInfos(partitionID, channelGroup.channelName)
		if len(partChanVshardInfos) == 0 {
			log.Debug("No vshardinfo in the partition-channel, skip trigger channelGroup vshard compaction")
			continue
		}

		vshardDescs := lo.Map(partChanVshardInfos, func(vshard *datapb.VShardInfo, _ int) *datapb.VShardDesc {
			return vshard.GetVshardDesc()
		})
		checkedPartChanSegments[channelGroup] = vshardDescs
	}
	if len(checkedPartChanSegments) == 0 {
		log.Info("No vshardinfo in the partition, skip trigger vshard compactioN")
		return nil, nil
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("fail to allocate triggerID", zap.Error(err))
		return nil, err
	}

	// getCollectionTTL out of the for loop to save a little bit
	collectionTTL, err := getCollectionTTL(collectionInfo.Properties)
	if err != nil {
		log.Warn("get collection ttl failed, skip to handle compaction")
		return make([]CompactionView, 0), err
	}

	views := make([]CompactionView, 0)
	for channelGroup, vshardDescs := range checkedPartChanSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			channelGroup.segments = FilterInIndexedSegments(policy.handler, policy.meta, false, channelGroup.segments...)
		}

		// group segments by size
		groups := make([][]*SegmentInfo, 0)
		var currentGroupSize int64 = 0
		currentGroup := make([]*SegmentInfo, 0)
		for _, segment := range channelGroup.segments {
			if currentGroupSize+segment.getSegmentSize() > expectedSize*int64(len(vshardDescs)) {
				groups = append(groups, currentGroup)
				currentGroup = make([]*SegmentInfo, 0)
				currentGroupSize = 0
			}
			currentGroupSize += segment.getSegmentSize()
			currentGroup = append(currentGroup, segment)
		}
		if len(currentGroup) > 0 {
			groups = append(groups, currentGroup)
		}

		vshardsStr := lo.Map(vshardDescs, func(vshard *datapb.VShardDesc, _ int) string {
			return vshard.String()
		})
		log.Debug("check vshardDescs", zap.Int64("triggerID", newTriggerID), zap.Strings("vshard", vshardsStr))

		for _, segments := range groups {
			view := &VshardSegmentView{
				label: &CompactionGroupLabel{
					CollectionID: collectionInfo.ID,
					PartitionID:  partitionID,
					Channel:      channelGroup.channelName,
				},
				segments:      GetViewsByInfo(segments...),
				collectionTTL: collectionTTL,
				triggerID:     newTriggerID,
				fromVshards:   nil,
				toVshards:     vshardDescs,
			}
			views = append(views, view)
			segmentIDs := lo.Map(segments, func(seg *SegmentInfo, _ int) int64 { return seg.ID })
			log.Info("generate vshard compaction view for segments without vshard", zap.Int64s("segmentIDs", segmentIDs))
		}
	}

	log.Info("finish trigger vshard compaction for segments without vshard", zap.Int64("triggerID", newTriggerID), zap.Int("viewNum", len(views)))
	return views, nil
}

func (policy *vshardCompactionPolicy) generateViewForVshardUpdate(ctx context.Context, collectionInfo *collectionInfo, partitionID int64) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionInfo.ID), zap.Int64("partitionID", partitionID))

	vshardTasks := policy.vshardManager.GetVShardTasks(partitionID)
	vshardTasks = lo.Filter(vshardTasks, func(V *datapb.VShardTask, _ int) bool {
		return V.GetState() == datapb.VShardTaskState_VShardTask_created
	})
	if len(vshardTasks) == 0 {
		log.Info("No vshard task in the partition, skip trigger vshard update compaction")
		return nil, nil
	}

	log.Info("start trigger vshard update compaction")
	containsSegmentVshardFunc := func(set []*datapb.VShardDesc, target *datapb.VShardDesc) bool {
		if target == nil {
			return false
		}
		for _, desc := range set {
			if target.String() == desc.String() {
				return true
			}
		}
		return false
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("fail to allocate triggerID", zap.Error(err))
		return nil, err
	}

	collectionTTL, err := getCollectionTTL(collectionInfo.Properties)
	if err != nil {
		log.Warn("get collection ttl failed, skip to handle compaction")
		return make([]CompactionView, 0), err
	}

	views := make([]CompactionView, 0)
	for _, task := range vshardTasks {
		partSegments := policy.meta.SelectSegments(SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return segment.CollectionID == collectionInfo.ID &&
				segment.PartitionID == partitionID &&
				segment.InsertChannel == task.Vchannel &&
				isSegmentHealthy(segment) &&
				isFlush(segment) &&
				!segment.isCompacting && // not compacting now
				!segment.GetIsImporting() && // not importing now
				segment.GetLevel() != datapb.SegmentLevel_L0 &&
				containsSegmentVshardFunc(task.GetFrom(), segment.GetVshardDesc())
		}))

		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			partSegments = FilterInIndexedSegments(policy.handler, policy.meta, false, partSegments...)
		}

		log.Info("generate vshard update compaction view", zap.Int("segmentNum", len(partSegments)))
		if len(partSegments) == 0 {
			continue
		}

		view := &VshardSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: collectionInfo.ID,
				PartitionID:  partitionID,
				Channel:      task.GetVchannel(),
			},
			segments:      GetViewsByInfo(partSegments...),
			collectionTTL: collectionTTL,
			triggerID:     newTriggerID,
			fromVshards:   task.GetFrom(),
			toVshards:     task.GetTo(),
			vshardTaskId:  task.GetId(),
		}
		views = append(views, view)
	}

	log.Info("finish generate vshard update compaction view", zap.Int64("triggerID", newTriggerID), zap.Int("viewNum", len(views)))
	return views, nil
}

var _ CompactionView = (*VshardSegmentView)(nil)

type VshardSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	collectionTTL time.Duration
	triggerID     int64
	fromVshards   []*datapb.VShardDesc
	toVshards     []*datapb.VShardDesc
	vshardTaskId  int64
}

func (v *VshardSegmentView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *VshardSegmentView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *VshardSegmentView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *VshardSegmentView) String() string {
	strs := lo.Map(v.segments, func(segView *SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("label=<%s>, segmentNum=%d segments=%v", v.label.String(), len(v.segments), strs)
}

func (v *VshardSegmentView) Trigger() (CompactionView, string) {
	return v, ""
}

func (v *VshardSegmentView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}
