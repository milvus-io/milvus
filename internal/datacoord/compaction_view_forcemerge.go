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
	"fmt"
	"math"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	forceMergeSizeTolerance       = 0.05
	forceMergeKnapsackLossDivisor = int64(20)
)

// static segment view, only algothrims here, no IO
type ForceMergeSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentInfo
	triggerID     int64
	collectionTTL time.Duration

	configMaxSize      float64
	expectedTargetSize float64

	topology *CollectionTopology

	targetSegmentSize int64
	// targetSegmentCount records the ForceTriggerAll planning result for logging
	// and callers of GetTargetSegmentCount. Scheduler ID allocation is derived
	// from targetSegmentSize so stale counts cannot over-reserve IDs.
	targetSegmentCount int64
}

func (v *ForceMergeSegmentView) GetTargetSegmentSize() int64 {
	return v.targetSegmentSize
}

func (v *ForceMergeSegmentView) GetTargetSegmentCount() int64 {
	return v.targetSegmentCount
}

func (v *ForceMergeSegmentView) GetGroupLabel() *CompactionGroupLabel {
	return v.label
}

func (v *ForceMergeSegmentView) GetSegmentsView() []*SegmentView {
	return GetViewsByInfo(v.segments...)
}

func (v *ForceMergeSegmentView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}

	var total float64
	for _, segment := range v.segments {
		total += GetBinlogSizeAsBytes(segment.GetBinlogs())
	}
	return total
}

func (v *ForceMergeSegmentView) GetCollectionTTL() time.Duration {
	if v == nil {
		return 0
	}
	return v.collectionTTL
}

func (v *ForceMergeSegmentView) Append(_ ...*SegmentView) {
	panic("force merge view cannot append SegmentView")
}

func (v *ForceMergeSegmentView) String() string {
	return fmt.Sprintf("ForceMerge: %s, segments=%d, triggerID=%d",
		v.label, len(v.segments), v.triggerID)
}

func (v *ForceMergeSegmentView) Trigger() (CompactionView, string) {
	panic("implement me")
}

func (v *ForceMergeSegmentView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}

func (v *ForceMergeSegmentView) GetTriggerID() int64 {
	return v.triggerID
}

func (v *ForceMergeSegmentView) calculateTargetSizeCount() (targetSize int64, targetCount int64) {
	log := log.With(zap.Int64("triggerID", v.triggerID), zap.String("label", v.label.String()))
	machineSafeSize := v.calculateMaxSafeSize()
	if machineSafeSize < v.configMaxSize {
		log.Info("maxSafeSize is less than configMaxSize, set to configMaxSize",
			zap.Float64("maxSafeSize", machineSafeSize),
			zap.Float64("configMaxSize", v.configMaxSize))
		machineSafeSize = v.configMaxSize
	}

	selectedTargetSize := machineSafeSize
	if v.expectedTargetSize > 0 {
		if v.expectedTargetSize <= machineSafeSize {
			log.Info("using user-provided target size",
				zap.Float64("expectedTargetSize", v.expectedTargetSize),
				zap.Float64("maxSafeSize", machineSafeSize))
			selectedTargetSize = v.expectedTargetSize
		} else {
			log.Warn("user-provided target size exceeds maxSafeSize, using maxSafeSize",
				zap.Float64("expectedTargetSize", v.expectedTargetSize),
				zap.Float64("maxSafeSize", machineSafeSize))
		}
	}

	selectedTargetSize = min(selectedTargetSize*(1+forceMergeSizeTolerance), machineSafeSize)
	targetSize = forceMergeEffectiveSize(selectedTargetSize)
	totalSize := v.GetTotalSize()
	targetCount = estimatedForceMergeOutputCount(totalSize, targetSize)

	queryNodeCount := int64(len(v.topology.QueryNodeMemory))
	numReplicas := int64(v.topology.NumReplicas)
	if numReplicas == 0 {
		numReplicas = 1
	}
	numShards := int64(v.topology.NumShards)
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
		if adjustedTargetSize >= v.configMaxSize {
			targetSize = min(targetSize, forceMergeEffectiveSize(math.Floor(adjustedTargetSize)))
			targetCount = estimatedForceMergeOutputCount(totalSize, targetSize)
			log.Info("adjusted target count for parallel loading per shard",
				zap.Int64("queryNodeCount", queryNodeCount),
				zap.Int64("numReplicas", numReplicas),
				zap.Int64("numShards", numShards),
				zap.Int64("perShardParallelism", perShardParallelism),
				zap.Int64("adjustedTargetCount", targetCount),
				zap.Int64("adjustedTargetSize", targetSize))
		}
	}

	log.Info("topology-aware force merge calculation",
		zap.Int64("targetSegmentCount", targetCount),
		zap.Int64("targetSegmentSize", targetSize),
		zap.Int64("queryNodeCount", queryNodeCount),
		zap.Int64("numReplicas", numReplicas),
		zap.Int64("numShards", numShards),
		zap.Int64("perShardParallelism", perShardParallelism))
	return targetSize, targetCount
}

func (v *ForceMergeSegmentView) ForceTriggerAll() ([]CompactionView, string) {
	if len(v.segments) == 0 {
		return nil, "force merge trigger"
	}

	targetSize, targetCount := v.calculateTargetSizeCount()
	groups := groupForceMergeSegments(v.segments, targetSize)

	log.Info("planned force merge groups",
		zap.Int64("triggerID", v.triggerID),
		zap.String("label", v.label.String()),
		zap.String("strategy", "v1 multi-round knapsack"),
		zap.Int64("targetSegmentSize", targetSize),
		zap.Int64("wholePoolTargetCount", targetCount),
		zap.Int("taskCount", len(groups)),
		zap.Int64("plannedOutputCount", totalForceMergeGroupOutputs(groups, targetSize)),
		zap.Int64("peakResidualInput", peakForceMergeGroupInput(groups, targetSize)))

	results := make([]CompactionView, 0, len(groups))
	for _, group := range groups {
		results = append(results, &ForceMergeSegmentView{
			label:              v.label,
			segments:           group,
			triggerID:          v.triggerID,
			collectionTTL:      v.collectionTTL,
			configMaxSize:      v.configMaxSize,
			expectedTargetSize: v.expectedTargetSize,
			targetSegmentSize:  targetSize,
			targetSegmentCount: plannedForceMergeOutputCount(forceMergeResidualSize(group), targetSize),
			topology:           v.topology,
		})
	}
	return results, "force merge trigger"
}

func groupForceMergeSegments(segments []*SegmentInfo, targetSize int64) [][]*SegmentInfo {
	threeTargetCapacity := forceMergeRoundCapacity(targetSize, 3)
	groups := make([][]*SegmentInfo, 0, len(segments))
	packable := make([]*SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		residualSize := segment.GetResidualSegmentSize()
		if residualSize > threeTargetCapacity {
			groups = append(groups, []*SegmentInfo{segment})
			continue
		}
		packable = append(packable, segment)
	}

	packer := newSegmentPacker("force-merge-v1-multi-round", packable, nil)
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
			packed, _ := packer.pack(
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

	if len(packer.candidates) != 0 {
		panic("3T force-merge packing round did not drain candidates")
	}
	return groups
}

func forceMergeResidualSize(segments []*SegmentInfo) int64 {
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

func totalForceMergeGroupOutputs(groups [][]*SegmentInfo, targetSize int64) int64 {
	total := int64(0)
	for _, group := range groups {
		total += plannedForceMergeOutputCount(forceMergeResidualSize(group), targetSize)
	}
	return total
}

func peakForceMergeGroupInput(groups [][]*SegmentInfo, targetSize int64) int64 {
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
	log := log.With(zap.Int64("triggerID", v.triggerID), zap.String("label", v.label.String()))
	if len(v.topology.QueryNodeMemory) == 0 || len(v.topology.DataNodeMemory) == 0 {
		log.Warn("No querynodes or datanodes in topology, using config size")
		return v.configMaxSize
	}

	// QueryNode constraint: use global minimum memory
	querynodeMemoryFactor := paramtable.Get().DataCoordCfg.CompactionForceMergeQueryNodeMemoryFactor.GetAsFloat()
	qnMaxSafeSize := float64(lo.Min(lo.Values(v.topology.QueryNodeMemory))) / querynodeMemoryFactor

	// DataNode constraint: segments must fit in smallest DataNode
	datanodeMemoryFactor := paramtable.Get().DataCoordCfg.CompactionForceMergeDataNodeMemoryFactor.GetAsFloat()
	dnMaxSafeSize := float64(lo.Min(lo.Values(v.topology.DataNodeMemory))) / datanodeMemoryFactor

	maxSafeSize := min(qnMaxSafeSize, dnMaxSafeSize)
	if v.topology.IsStandaloneMode && !v.topology.IsPooling {
		log.Info("force merge on standalone not pooling mode, half the max size",
			zap.Float64("qnMaxSafeSize", qnMaxSafeSize),
			zap.Float64("dnMaxSafeSize", dnMaxSafeSize),
			zap.Float64("maxSafeSize/2", maxSafeSize/2),
			zap.Float64("configMaxSize", v.configMaxSize))
		// dn and qn are co-located, half the min
		return maxSafeSize * 0.5
	}

	log.Info("force merge on cluster/pooling mode",
		zap.Float64("qnMaxSafeSize", qnMaxSafeSize),
		zap.Float64("dnMaxSafeSize", dnMaxSafeSize),
		zap.Float64("maxSafeSize", maxSafeSize),
		zap.Float64("configMaxSize", v.configMaxSize))
	return maxSafeSize
}

func sumSegmentSize(views []*SegmentView) float64 {
	return lo.SumBy(views, func(v *SegmentView) float64 { return v.Size })
}
