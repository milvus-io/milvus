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
	log.Info("start trigger vshard allocate Compaction...")
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
	log.Info("start trigger vshard compaction")
	allocateVshardViews, err := policy.allocateVshard(ctx, collectionInfo, partitionID, expectedSize)
	if err != nil {
		return nil, err
	}
	updateVshardViews, err := policy.updateVshard(ctx, collectionInfo, partitionID)
	if err != nil {
		return nil, err
	}
	views := append(allocateVshardViews, updateVshardViews...)
	return views, nil
}

func (policy *vshardCompactionPolicy) allocateVshard(ctx context.Context, collectionInfo *collectionInfo, partitionID int64, expectedSize int64) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionInfo.ID), zap.Int64("partitionID", partitionID))
	noVshardSegments := policy.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionInfo.ID &&
			segment.PartitionID == partitionID &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() && // not importing now
			segment.GetLevel() == datapb.SegmentLevel_L1 && // todo L1 for now
			segment.GetVshardDesc() == nil
	})
	log.Info("Get segments without vshard", zap.Int("len", len(noVshardSegments)))

	// filter out no vshardInfo partition-channels to avoid useless AllocID
	noVshardSegments = lo.Filter(noVshardSegments, func(chanPartSegments *chanPartSegments, _ int) bool {
		partitionChVshardInfos := policy.vshardManager.GetNormalVShardInfos(partitionID, chanPartSegments.channelName)
		if len(partitionChVshardInfos) == 0 {
			log.Info("No vshardinfo in the partition-channel, skip trigger channelGroup vshard allocate compaction")
			return false
		}
		return true
	})

	newTriggerID, _, err := policy.allocator.AllocN(int64(len(noVshardSegments)))
	if err != nil {
		log.Warn("fail to allocate triggerID", zap.Error(err))
		return nil, err
	}

	views := make([]CompactionView, 0)
	for _, channelGroup := range noVshardSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			channelGroup.segments = FilterInIndexedSegments(policy.handler, policy.meta, false, channelGroup.segments...)
		}

		collectionTTL, err := getCollectionTTL(collectionInfo.Properties)
		if err != nil {
			log.Warn("get collection ttl failed, skip to handle compaction")
			return make([]CompactionView, 0), err
		}

		partitionChVshardInfos := policy.vshardManager.GetNormalVShardInfos(partitionID, channelGroup.channelName)
		if len(partitionChVshardInfos) == 0 {
			log.Info("No vshardInfo in the partition-channel, skip trigger channelGroup vshard allocate compaction")
			continue
		}
		vshardDescs := lo.Map(partitionChVshardInfos, func(vshard *datapb.VShardInfo, _ int) *datapb.VShardDesc {
			return vshard.GetVshardDesc()
		})

		// group segments by size
		groups := make([][]*SegmentInfo, 0)
		var currentGroupSize int64 = 0
		currentGroup := make([]*SegmentInfo, 0)
		for _, segment := range channelGroup.segments {
			if currentGroupSize+segment.getSegmentSize() > expectedSize {
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
			log.Info("generate vshard allocate compaction view", zap.Int64s("segmentIDs", segmentIDs))
		}
		newTriggerID++
	}

	log.Info("finish trigger vshard allocate compaction", zap.Int("viewNum", len(views)))
	return views, nil
}

func (policy *vshardCompactionPolicy) updateVshard(ctx context.Context, collectionInfo *collectionInfo, partitionID int64) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionInfo.ID), zap.Int64("partitionID", partitionID))

	vshardTasks := policy.vshardManager.GetVShardTasks(partitionID)
	vshardTasks = lo.Filter(vshardTasks, func(V *datapb.VShardTask, _ int) bool {
		return V.GetState() == datapb.VShardTaskState_VShardTask_created
	})
	if len(vshardTasks) == 0 {
		log.Info("No vshard task in the partition, skip trigger vshard move compaction")
		return nil, nil
	}
	log.Info("start trigger vshard move compaction")

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

		collectionTTL, err := getCollectionTTL(collectionInfo.Properties)
		if err != nil {
			log.Warn("get collection ttl failed, skip to handle compaction")
			return make([]CompactionView, 0), err
		}

		log.Info("generate view", zap.Int("segmentNum", len(partSegments)))
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

	log.Info("finish trigger vshard move compaction", zap.Int("viewNum", len(views)))
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
