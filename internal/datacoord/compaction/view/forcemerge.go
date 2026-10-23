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

package view

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	forceMergeSizeTolerance       = 0.05
	forceMergeKnapsackLossDivisor = int64(20)
)

// static segment view, only algothrims here, no IO
type ForceMergeSegmentView struct {
	Label         *CompactionGroupLabel
	Segments      []*meta.SegmentInfo
	TriggerID     int64
	CollectionTTL time.Duration

	ConfigMaxSize      float64
	ExpectedTargetSize float64

	Topology *CollectionTopology

	TargetSegmentSize int64
	// targetSegmentCount records the ForceTriggerAll planning result for logging
	// and callers of GetTargetSegmentCount. Scheduler ID allocation is derived
	// from targetSegmentSize so stale counts cannot over-reserve IDs.
	TargetSegmentCount int64
}

func (v *ForceMergeSegmentView) GetTargetSegmentSize() int64 {
	return v.TargetSegmentSize
}

func (v *ForceMergeSegmentView) GetTargetSegmentCount() int64 {
	return v.TargetSegmentCount
}

func (v *ForceMergeSegmentView) GetGroupLabel() *CompactionGroupLabel {
	return v.Label
}

func (v *ForceMergeSegmentView) GetSegmentsView() []*SegmentView {
	return GetViewsByInfo(v.Segments...)
}

func (v *ForceMergeSegmentView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}

	var total float64
	for _, segment := range v.Segments {
		total += float64(segment.EnsureStats().GetInsertBinlogSize())
	}
	return total
}

func (v *ForceMergeSegmentView) GetCollectionTTL() time.Duration {
	if v == nil {
		return 0
	}
	return v.CollectionTTL
}

func (v *ForceMergeSegmentView) Append(_ ...*SegmentView) {
	panic("force merge view cannot append SegmentView")
}

func (v *ForceMergeSegmentView) String() string {
	return fmt.Sprintf("ForceMerge: %s, segments=%d, triggerID=%d",
		v.Label, len(v.Segments), v.TriggerID)
}

func (v *ForceMergeSegmentView) Trigger() (CompactionView, string) {
	panic("implement me")
}

func (v *ForceMergeSegmentView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}

func (v *ForceMergeSegmentView) GetTriggerID() int64 {
	return v.TriggerID
}

func (v *ForceMergeSegmentView) calculateTargetSizeCount() (targetSize int64, targetCount int64) {
	log := mlog.With(mlog.Int64("triggerID", v.TriggerID), mlog.String("label", v.Label.String()))
	machineSafeSize := v.calculateMaxSafeSize()
	if machineSafeSize < v.ConfigMaxSize {
		log.Info(context.TODO(), "maxSafeSize is less than configMaxSize, set to configMaxSize",
			mlog.Float64("maxSafeSize", machineSafeSize),
			mlog.Float64("configMaxSize", v.ConfigMaxSize))
		machineSafeSize = v.ConfigMaxSize
	}

	selectedTargetSize := machineSafeSize
	if v.ExpectedTargetSize > 0 {
		if v.ExpectedTargetSize <= machineSafeSize {
			log.Info(context.TODO(), "using user-provided target size",
				mlog.Float64("expectedTargetSize", v.ExpectedTargetSize),
				mlog.Float64("maxSafeSize", machineSafeSize))
			selectedTargetSize = v.ExpectedTargetSize
		} else {
			log.Warn(context.TODO(), "user-provided target size exceeds maxSafeSize, using maxSafeSize",
				mlog.Float64("expectedTargetSize", v.ExpectedTargetSize),
				mlog.Float64("maxSafeSize", machineSafeSize))
		}
	}

	selectedTargetSize = min(selectedTargetSize*(1+forceMergeSizeTolerance), machineSafeSize)
	targetSize = forceMergeEffectiveSize(selectedTargetSize)
	totalSize := v.GetTotalSize()
	targetCount = estimatedForceMergeOutputCount(totalSize, targetSize)

	queryNodeCount := int64(len(v.Topology.QueryNodeMemory))
	numReplicas := int64(v.Topology.NumReplicas)
	if numReplicas == 0 {
		numReplicas = 1
	}
	numShards := int64(v.Topology.NumShards)
	if numShards == 0 {
		numShards = 1
	}

	perShardParallelism := queryNodeCount / (numReplicas * numShards)
	if perShardParallelism < 1 {
		perShardParallelism = 1
	}

	if perShardParallelism > 1 && targetCount < perShardParallelism {
		desiredCount := perShardParallelism
		adjustedTargetSize := totalSize / float64(desiredCount)
		if adjustedTargetSize >= v.ConfigMaxSize {
			targetSize = min(targetSize, forceMergeEffectiveSize(math.Floor(adjustedTargetSize)))
			targetCount = estimatedForceMergeOutputCount(totalSize, targetSize)
			log.Info(context.TODO(), "adjusted target count for parallel loading per shard",
				mlog.Int64("queryNodeCount", queryNodeCount),
				mlog.Int64("numReplicas", numReplicas),
				mlog.Int64("numShards", numShards),
				mlog.Int64("perShardParallelism", perShardParallelism),
				mlog.Int64("adjustedTargetCount", targetCount),
				mlog.Int64("adjustedTargetSize", targetSize))
		}
	}

	log.Info(context.TODO(), "topology-aware force merge calculation",
		mlog.Int64("targetSegmentCount", targetCount),
		mlog.Int64("targetSegmentSize", targetSize),
		mlog.Int64("queryNodeCount", queryNodeCount),
		mlog.Int64("numReplicas", numReplicas),
		mlog.Int64("numShards", numShards),
		mlog.Int64("perShardParallelism", perShardParallelism))
	return targetSize, targetCount
}

func (v *ForceMergeSegmentView) ForceTriggerAll() ([]CompactionView, string) {
	if len(v.Segments) == 0 {
		return nil, "force merge trigger"
	}

	targetSize, targetCount := v.calculateTargetSizeCount()
	groups := groupForceMergeSegments(v.Segments, targetSize)

	mlog.Info(context.TODO(), "planned force merge groups",
		mlog.Int64("triggerID", v.TriggerID),
		mlog.String("label", v.Label.String()),
		mlog.String("strategy", "v1 multi-round knapsack"),
		mlog.Int64("targetSegmentSize", targetSize),
		mlog.Int64("wholePoolTargetCount", targetCount),
		mlog.Int("taskCount", len(groups)),
		mlog.Int64("plannedOutputCount", totalForceMergeGroupOutputs(groups, targetSize)),
		mlog.Int64("peakResidualInput", peakForceMergeGroupInput(groups, targetSize)))

	results := make([]CompactionView, 0, len(groups))
	for _, group := range groups {
		results = append(results, &ForceMergeSegmentView{
			Label:              v.Label,
			Segments:           group,
			TriggerID:          v.TriggerID,
			CollectionTTL:      v.CollectionTTL,
			ConfigMaxSize:      v.ConfigMaxSize,
			ExpectedTargetSize: v.ExpectedTargetSize,
			TargetSegmentSize:  targetSize,
			TargetSegmentCount: plannedForceMergeOutputCount(forceMergeResidualSize(group), targetSize),
			Topology:           v.Topology,
		})
	}
	return results, "force merge trigger"
}

func groupForceMergeSegments(segments []*meta.SegmentInfo, targetSize int64) [][]*meta.SegmentInfo {
	threeTargetCapacity := forceMergeRoundCapacity(targetSize, 3)
	groups := make([][]*meta.SegmentInfo, 0, len(segments))
	packable := make([]*meta.SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		residualSize := segment.GetResidualSegmentSize()
		if residualSize > threeTargetCapacity {
			groups = append(groups, []*meta.SegmentInfo{segment})
			continue
		}
		packable = append(packable, segment)
	}

	packer := NewSegmentPacker("force-merge-v1-multi-round", packable, nil)
	lossAllowance := targetSize / forceMergeKnapsackLossDivisor
	rounds := []struct {
		capacity    int64
		maxLeftSize int64
	}{
		{capacity: targetSize, maxLeftSize: lossAllowance},
		{capacity: forceMergeRoundCapacity(targetSize, 2), maxLeftSize: lossAllowance},
		{capacity: threeTargetCapacity, maxLeftSize: math.MaxInt64},
	}

	for _, packingRound := range rounds {
		for {
			packed, _ := packer.Pack(
				packingRound.capacity,
				packingRound.maxLeftSize,
				0,
				math.MaxInt64,
			)
			if len(packed) == 0 {
				break
			}

			groups = append(groups, packed)
		}
	}

	if len(packer.Candidates) != 0 {
		panic("3T force-merge packing round did not drain candidates")
	}
	return groups
}

func forceMergeResidualSize(segments []*meta.SegmentInfo) int64 {
	var total int64
	for _, segment := range segments {
		total += segment.GetResidualSegmentSize()
	}
	return total
}

func plannedForceMergeOutputCount(residualSize, targetSize int64) int64 {
	if residualSize <= 0 || targetSize <= 0 {
		return 1
	}
	return 1 + (residualSize-1)/targetSize
}

func estimatedForceMergeOutputCount(inputSize float64, targetSize int64) int64 {
	if inputSize <= 0 || math.IsNaN(inputSize) || targetSize <= 0 {
		return 1
	}
	return max(int64(math.Ceil(inputSize/float64(targetSize))), 1)
}

func forceMergeRoundCapacity(targetSize, multiplier int64) int64 {
	if targetSize <= 0 || multiplier <= 0 {
		return 0
	}
	if targetSize > math.MaxInt64/multiplier {
		return math.MaxInt64
	}
	return targetSize * multiplier
}

func forceMergeEffectiveSize(size float64) int64 {
	if size < 1 || math.IsNaN(size) {
		return 1
	}
	if size >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(size)
}

func totalForceMergeGroupOutputs(groups [][]*meta.SegmentInfo, targetSize int64) int64 {
	total := int64(0)
	for _, group := range groups {
		total += plannedForceMergeOutputCount(forceMergeResidualSize(group), targetSize)
	}
	return total
}

func peakForceMergeGroupInput(groups [][]*meta.SegmentInfo, targetSize int64) int64 {
	peak := int64(0)
	inputCeiling := forceMergeRoundCapacity(targetSize, 3)
	for _, group := range groups {
		residualSize := forceMergeResidualSize(group)
		if len(group) != 1 || residualSize <= inputCeiling {
			peak = max(peak, residualSize)
		}
	}
	return peak
}

func (v *ForceMergeSegmentView) calculateMaxSafeSize() float64 {
	log := mlog.With(mlog.Int64("triggerID", v.TriggerID), mlog.String("label", v.Label.String()))
	if len(v.Topology.QueryNodeMemory) == 0 || len(v.Topology.DataNodeMemory) == 0 {
		log.Warn(context.TODO(), "No querynodes or datanodes in topology, using config size")
		return v.ConfigMaxSize
	}

	// QueryNode constraint: use global minimum memory
	querynodeMemoryFactor := paramtable.Get().DataCoordCfg.CompactionForceMergeQueryNodeMemoryFactor.GetAsFloat()
	qnMaxSafeSize := float64(lo.Min(lo.Values(v.Topology.QueryNodeMemory))) / querynodeMemoryFactor

	// DataNode constraint: segments must fit in smallest DataNode
	datanodeMemoryFactor := paramtable.Get().DataCoordCfg.CompactionForceMergeDataNodeMemoryFactor.GetAsFloat()
	dnMaxSafeSize := float64(lo.Min(lo.Values(v.Topology.DataNodeMemory))) / datanodeMemoryFactor

	maxSafeSize := min(qnMaxSafeSize, dnMaxSafeSize)
	if v.Topology.IsStandaloneMode && !v.Topology.IsPooling {
		log.Info(context.TODO(), "force merge on standalone not pooling mode, half the max size",
			mlog.Float64("qnMaxSafeSize", qnMaxSafeSize),
			mlog.Float64("dnMaxSafeSize", dnMaxSafeSize),
			mlog.Float64("maxSafeSize/2", maxSafeSize/2),
			mlog.Float64("configMaxSize", v.ConfigMaxSize))
		// dn and qn are co-located, half the min
		return maxSafeSize * 0.5
	}

	log.Info(context.TODO(), "force merge on cluster/pooling mode",
		mlog.Float64("qnMaxSafeSize", qnMaxSafeSize),
		mlog.Float64("dnMaxSafeSize", dnMaxSafeSize),
		mlog.Float64("maxSafeSize", maxSafeSize),
		mlog.Float64("configMaxSize", v.ConfigMaxSize))
	return maxSafeSize
}
