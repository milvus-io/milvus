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
	"golang.org/x/exp/slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/logutil"
)

type VshardManager interface {
	Start()
	Stop()

	GetNormalVShardInfos(partitionID int64, channel string) []*datapb.VShardInfo
	GetVShardTasks(partitionID int64) []*datapb.VShardTask
	ReportVShardCompaction(partitionID int64, taskID int64) error
}

var _ VshardManager = (*VshardManagerImpl)(nil)

type VshardManagerImpl struct {
	meta      CompactionMeta
	allocator allocator.Allocator
	mu        sync.Mutex
	stopOnce  sync.Once
	stopCh    chan struct{}
	stopWg    sync.WaitGroup
}

func NewVshardManagerImpl(meta CompactionMeta, allocator allocator.Allocator) *VshardManagerImpl {
	return &VshardManagerImpl{
		meta:      meta,
		allocator: allocator,
		stopCh:    make(chan struct{}),
	}
}

func (v *VshardManagerImpl) Start() {
	v.stopWg.Add(2)
	go v.loopCheck()
	go v.loopClean()
}

func (v *VshardManagerImpl) Stop() {
	v.stopOnce.Do(func() {
		close(v.stopCh)
	})
	v.stopWg.Wait()
}

func (v *VshardManagerImpl) loopCheck() {
	defer logutil.LogPanic()
	defer v.stopWg.Done()
	interval := Params.DataCoordCfg.VshardCheckInterval.GetAsDuration(time.Second)
	log.Info("VshardManager start loop check", zap.Any("check result interval", interval))
	checkTicker := time.NewTicker(interval)
	defer checkTicker.Stop()
	for {
		select {
		case <-v.stopCh:
			log.Info("VshardManager quit loop check")
			return
		case <-checkTicker.C:
			err := v.check()
			if err != nil {
				log.Warn("fail to update vshard", zap.Error(err))
			}
		}
	}
}

func (v *VshardManagerImpl) loopClean() {
	interval := Params.DataCoordCfg.VshardCleanInterval.GetAsDuration(time.Second)
	log.Info("VshardManagerImpl start clean check loop", zap.Any("gc interval", interval))
	defer v.stopWg.Done()
	cleanTicker := time.NewTicker(interval)
	defer cleanTicker.Stop()
	for {
		select {
		case <-v.stopCh:
			log.Info("VshardManager quit loop check")
			return
		case <-cleanTicker.C:
			err := v.clean()
			if err != nil {
				log.Warn("fail to clean vshards", zap.Error(err))
			}
		}
	}
}

// clean all dropped vshardInfos and finish/failed vshardTasks
func (v *VshardManagerImpl) clean() error {
	vshardInfos := v.meta.GetVshardMeta().ListVShardInfos()
	for _, vshardInfo := range vshardInfos {
		if vshardInfo.GetState() == datapb.VShardInfoState_VShard_dropped {
			err := v.meta.GetVshardMeta().DropVShardInfo(vshardInfo)
			if err != nil {
				return err
			}
		}
	}
	vshardTasks := v.meta.GetVshardMeta().ListVShardTasks()
	for _, vshardTask := range vshardTasks {
		if vshardTask.GetState() == datapb.VShardTaskState_VShardTask_finished || vshardTask.GetState() == datapb.VShardTaskState_VShardTask_failed {
			err := v.meta.GetVshardMeta().DropVShardTask(vshardTask)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// check all collection, if they need to update vshardInfo
func (v *VshardManagerImpl) check() error {
	if !Params.DataCoordCfg.VShardEnable.GetAsBool() {
		log.Info("vshard is not enabled")
		return nil
	}
	collections := v.meta.GetCollections()
	for _, collection := range collections {
		pShardNum := len(collection.VChannelNames) // shardNum
		for _, partitionID := range collection.Partitions {
			err := v.checkPartition(collection, partitionID, pShardNum)
			if err != nil {
				// not throw this error because no need to fail because of one collection-partition
				log.Warn("fail to check partition vshard update", zap.Int64("collectionID", collection.ID), zap.Int64("partitionID", partitionID), zap.Error(err))
			}
		}
	}
	return nil
}

func (v *VshardManagerImpl) checkPartition(collection *collectionInfo, partitionID int64, pShardNum int) error {
	log := log.With(zap.Int64("collection", collection.ID), zap.Int64("partition", partitionID))
	partChSegments := v.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return segment.GetCollectionID() == collection.ID &&
			segment.GetPartitionID() == partitionID &&
			!segment.GetIsImporting() &&
			isSegmentHealthy(segment) &&
			segment.GetLevel() != datapb.SegmentLevel_L0
	})

	vchannels := collection.VChannelNames
	slices.Sort(vchannels)
	channelOrder := make(map[string]int, 0)
	for i, vchannel := range vchannels {
		channelOrder[vchannel] = i
	}
	for _, group := range partChSegments {
		log := log.With(zap.String("channel", group.channelName))
		totalSize := CalSegmentsSize(group.segments)
		log.Info("VshardManager start check channel-partition", zap.Int("segmentNum", len(group.segments)), zap.Int64("size", totalSize))

		noVshard := &datapb.VShardDesc{
			VshardModulus: 1,
			VshardResidue: 0,
		}
		// group segments by vshard
		vshardSegments := make(map[*datapb.VShardDesc][]*SegmentInfo, 0)
		for _, seg := range group.segments {
			vshardDesc := seg.VshardDesc
			if vshardDesc == nil {
				vshardDesc = noVshard
			}
			_, exist := vshardSegments[vshardDesc]
			if !exist {
				vshardSegments[vshardDesc] = make([]*SegmentInfo, 0)
			}
			vshardSegments[vshardDesc] = append(vshardSegments[vshardDesc], seg)
		}

		if _, exist := vshardSegments[noVshard]; exist && len(vshardSegments) == 1 {
			// only contains segments without vshard
			err := v.generateVshardAllocate(vshardSegments[noVshard], totalSize, pShardNum, channelOrder[group.channelName])
			if err != nil {
				log.Error("fail to generateVshardAllocate", zap.Error(err))
				return err
			}
		} else {
			// todo currently, we only support vshard split, support vshard merge
			for vshard, segments := range vshardSegments {
				if vshard.GetVshardModulus() == int32(1) {
					// segments without vshard
					continue
				}
				log.Info("try generate vshard split", zap.String("vshard", vshard.String()))
				err := v.generateVshardMove(vshard, segments, totalSize, pShardNum)
				if err != nil {
					log.Error("fail to generateVshardMove", zap.String("vshard", vshard.String()), zap.Error(err))
					return err
				}
			}
		}
	}

	return nil
}

// generateVshardAllocate check for partition without vshard
func (v *VshardManagerImpl) generateVshardAllocate(segments []*SegmentInfo, partitionSize int64, pShardNum int, pshardOrder int) error {
	collectionID := segments[0].CollectionID
	partitionID := segments[0].PartitionID
	channel := segments[0].InsertChannel
	log := log.With(zap.Int64("collection", collectionID), zap.Int64("partition", partitionID), zap.String("channel", channel), zap.Int("pshardOrder", pshardOrder))
	log.Info("start generate first vshard info")

	vshardSize := CalSegmentsSize(segments)
	vshardInfos := v.GetNormalVShardInfos(partitionID, channel)
	if len(vshardInfos) > 0 {
		log.Info("partition channel vshard is created, skip generate new vshard")
		return nil
	}

	// size policy
	var newVshardNum int32 = 1
	if vshardSize > int64(float64(Params.DataCoordCfg.VshardSplitThreshold.GetAsSize())*1.5) {
		newVshardNum = nextPowerOfTwo(int32(vshardSize/Params.DataCoordCfg.VshardSplitThreshold.GetAsSize()) + 1)
	}

	if newVshardNum > 1 {
		vshardInfos := make([]*datapb.VShardInfo, 0)
		for i := int32(0); i < newVshardNum; i++ {
			vshardInfos = append(vshardInfos, &datapb.VShardInfo{
				CollectionId: collectionID,
				PartitionId:  partitionID,
				Vchannel:     channel,
				VshardDesc: &datapb.VShardDesc{
					VshardModulus: newVshardNum * int32(pShardNum),
					VshardResidue: i*int32(pShardNum) + int32(pshardOrder),
				},
				State: datapb.VShardInfoState_VShard_normal,
			})
		}

		err := v.meta.GetVshardMeta().SaveVShardInfos(vshardInfos)
		if err != nil {
			log.Error("fail to save vshard info", zap.Error(err))
			return err
		}
		log.Info("save vshardInfo", zap.Any("vshards", vshardInfos))
		return nil
	}
	log.Info("no vshard info generated as datasize has not reach threshold")
	return nil
}

func (v *VshardManagerImpl) generateVshardMove(vshard *datapb.VShardDesc, segments []*SegmentInfo, partitionSize int64, pShardNum int) error {
	collectionID := segments[0].CollectionID
	partitionID := segments[0].PartitionID
	channel := segments[0].InsertChannel
	log := log.With(zap.Int64("collection", collectionID), zap.Int64("partition", partitionID), zap.String("channel", channel), zap.String("vshard", vshard.String()))
	vshardSize := CalSegmentsSize(segments)

	existTasks := v.GetVShardTasks(partitionID)
	existTasks = lo.Filter(existTasks, func(task *datapb.VShardTask, _ int) bool {
		return task.Vchannel == channel && len(task.From) == 1 && task.From[0] == vshard && task.State == datapb.VShardTaskState_VShardTask_created
	})
	if len(existTasks) > 0 {
		log.Info("vshard is resharding, skip generate new task")
		return nil
	}

	// todo size policy
	log.Info("try generate vshard resplit", zap.Int64("vshardSize", vshardSize))
	if vshardSize > Params.DataCoordCfg.VshardSplitThreshold.GetAsSize() {
		// do split
		newVShard1 := &datapb.VShardDesc{
			VshardModulus: vshard.VshardModulus * 2,
			VshardResidue: vshard.VshardResidue,
		}
		newVShard2 := &datapb.VShardDesc{
			VshardModulus: vshard.VshardModulus * 2,
			VshardResidue: vshard.VshardResidue + vshard.VshardModulus,
		}

		vshardInfos := make([]*datapb.VShardInfo, 0)
		vshardInfos = append(vshardInfos, &datapb.VShardInfo{
			CollectionId: collectionID,
			PartitionId:  partitionID,
			Vchannel:     channel,
			VshardDesc:   newVShard1,
			State:        datapb.VShardInfoState_VShard_normal,
		})
		vshardInfos = append(vshardInfos, &datapb.VShardInfo{
			CollectionId: collectionID,
			PartitionId:  partitionID,
			Vchannel:     channel,
			VshardDesc:   newVShard2,
			State:        datapb.VShardInfoState_VShard_normal,
		})
		// old vshard
		vshardInfos = append(vshardInfos, &datapb.VShardInfo{
			CollectionId: collectionID,
			PartitionId:  partitionID,
			Vchannel:     channel,
			VshardDesc:   vshard,
			State:        datapb.VShardInfoState_VShard_resharding,
		})

		startID, _, err := v.allocator.AllocN(1)
		if err != nil {
			log.Error("fail to allocate vshard task id", zap.Error(err))
			return err
		}

		reVshardTask := &datapb.VShardTask{
			Id:           startID,
			CollectionId: collectionID,
			PartitionId:  partitionID,
			Vchannel:     segments[0].InsertChannel,
			From:         []*datapb.VShardDesc{vshard},
			To:           []*datapb.VShardDesc{newVShard1, newVShard2},
		}

		err = v.meta.GetVshardMeta().SaveVShardInfosAndVshardTask(vshardInfos, reVshardTask)
		if err != nil {
			log.Error("fail to save vshard infos and vshard task", zap.Error(err))
			return err
		}
		log.Info("VShardManager saved vshard infos and vshard task", zap.String("task", reVshardTask.String()))
		return nil
	}

	return nil
}

func (v *VshardManagerImpl) ReportVShardCompaction(partitionID, taskID int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	task := v.meta.GetVshardMeta().GetVShardTaskByID(partitionID, taskID)
	if task == nil {
		log.Warn("no vshard task found", zap.Int64("partitionID", partitionID), zap.Int64("taskID", taskID))
		return errors.New("no vshard task found")
	}

	// 1. check if the toVShards are active, fail the compaction if not
	activeVshards := v.meta.GetVshardMeta().GetVShardInfo(task.GetPartitionId(), task.GetVchannel())
	activeVshards = lo.Filter(activeVshards, func(vshardInfo *datapb.VShardInfo, _ int) bool {
		return vshardInfo.State == datapb.VShardInfoState_VShard_normal
	})
	log.Info("vshard active", zap.Int("vshards", len(activeVshards)))
	for _, activeShard := range activeVshards {
		log.Info("vshard active", zap.String("vshard", activeShard.String()))
	}
	containsSegmentVshardFunc := func(input *datapb.VShardDesc) bool {
		for _, activeShard := range activeVshards {
			log.Info("vshard active", zap.String("vshard", activeShard.String()))
			if activeShard.VshardDesc.String() == input.String() {
				return true
			}
		}
		return false
	}
	for _, vshard := range task.GetTo() {
		if !containsSegmentVshardFunc(vshard) {
			// todo wayblink wrap error
			log.Warn("vshard is not active", zap.String("vshard", vshard.String()))
			return errors.New("vshard is not active")
		}
	}

	// 2. check if all segments of fromShard is dropped
	//    if dropped, mark the task as complete
	if task.GetFrom() != nil {
		remainSegments := v.meta.SelectSegments(SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return segment.CollectionID == task.GetCollectionId() &&
				segment.PartitionID == task.GetPartitionId() &&
				segment.InsertChannel == task.GetVchannel() &&
				segment.VshardDesc == task.GetFrom()[0] &&
				isSegmentHealthy(segment)
		}))
		if len(remainSegments) == 0 {
			task.State = datapb.VShardTaskState_VShardTask_finished

			vshardInfo := &datapb.VShardInfo{
				CollectionId: task.GetCollectionId(),
				PartitionId:  task.GetPartitionId(),
				Vchannel:     task.GetVchannel(),
				VshardDesc:   task.GetFrom()[0],
				State:        datapb.VShardInfoState_VShard_dropped,
			}

			err := v.meta.GetVshardMeta().SaveVShardInfosAndVshardTask([]*datapb.VShardInfo{vshardInfo}, task)
			if err != nil {
				log.Warn("fail to update vshard Infos and vshard task", zap.String("task", task.String()), zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (v *VshardManagerImpl) GetNormalVShardInfos(partition int64, channel string) []*datapb.VShardInfo {
	v.mu.Lock()
	defer v.mu.Unlock()
	res := v.meta.GetVshardMeta().GetVShardInfo(partition, channel)
	res = lo.Filter(res, func(vshardInfo *datapb.VShardInfo, _ int) bool {
		return vshardInfo.State == datapb.VShardInfoState_VShard_normal
	})
	return res
}

func (v *VshardManagerImpl) GetVShardTasks(partition int64) []*datapb.VShardTask {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.meta.GetVshardMeta().GetVShardTasksByPartition(partition)
}

func CalSegmentsSize(segments []*SegmentInfo) int64 {
	segmentSizes := lo.Map(segments, func(seg *SegmentInfo, id int) int64 {
		return seg.getSegmentSize()
	})
	totalSize := lo.Reduce(segmentSizes, func(left, right int64, _ int) int64 {
		return left + right
	}, 0)
	return totalSize
}

// 1 -> 1
// 2 -> 2
// 3 -> 4
// 4 -> 4
// 5 -> 8
// 6 -> 8
// 7 -> 8
// 8 -> 8
// 255 -> 256
// 256 -> 256
// 257 -> 512
func nextPowerOfTwo(n int32) int32 {
	if n <= 0 {
		return 1
	}
	n--         // To handle exact powers of 2, subtract 1 first
	n |= n >> 1 // Set all bits below the highest set bit
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
