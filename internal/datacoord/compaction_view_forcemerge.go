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
	"math"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	forceMergeSizeTolerance       = 0.05
	forceMergeKnapsackLossDivisor = int64(20)
	forceMergeKnapsackMaxSegments = int64(4096)

	forceMergePackingRoundOversized    forceMergePackingRound = "oversized-singleton"
	forceMergePackingRoundOneTarget    forceMergePackingRound = "1T"
	forceMergePackingRoundTwoTargets   forceMergePackingRound = "2T"
	forceMergePackingRoundThreeTargets forceMergePackingRound = "3T"
)

type forceMergePackingRound string

type forceMergePackingGroup struct {
	segments     []*SegmentView
	residualSize int64
	round        forceMergePackingRound
}

// static segment view, only algothrims here, no IO
type ForceMergeSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	triggerID     int64
	collectionTTL time.Duration

	configMaxSize      float64
	expectedTargetSize float64

	topology *CollectionTopology

	targetSegmentSize float64
	// targetSegmentCount records the ForceTriggerAll planning result for logging
	// and callers of GetTargetSegmentCount. Scheduler ID allocation is derived
	// from targetSegmentSize so stale counts cannot over-reserve IDs.
	targetSegmentCount int64
}

func (v *ForceMergeSegmentView) GetTargetSegmentSize() float64 {
	return v.targetSegmentSize
}

func (v *ForceMergeSegmentView) GetTargetSegmentCount() int64 {
	return v.targetSegmentCount
}

func (v *ForceMergeSegmentView) GetGroupLabel() *CompactionGroupLabel {
	return v.label
}

func (v *ForceMergeSegmentView) GetSegmentsView() []*SegmentView {
	return v.segments
}

func (v *ForceMergeSegmentView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}
	return sumSegmentSize(v.segments)
}

func (v *ForceMergeSegmentView) GetCollectionTTL() time.Duration {
	if v == nil {
		return 0
	}
	return v.collectionTTL
}

func (v *ForceMergeSegmentView) Append(segments ...*SegmentView) {
	v.segments = append(v.segments, segments...)
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
	log := mlog.With(mlog.Int64("triggerID", v.triggerID), mlog.String("label", v.label.String()))
	machineSafeSize := v.calculateMaxSafeSize()
	if machineSafeSize < v.configMaxSize {
		log.Info(context.TODO(), "maxSafeSize is less than configMaxSize, set to configMaxSize",
			mlog.Float64("maxSafeSize", machineSafeSize),
			mlog.Float64("configMaxSize", v.configMaxSize))
		machineSafeSize = v.configMaxSize
	}

	selectedTargetSize := machineSafeSize
	if v.expectedTargetSize > 0 {
		if v.expectedTargetSize <= machineSafeSize {
			log.Info(context.TODO(), "using user-provided target size",
				mlog.Float64("expectedTargetSize", v.expectedTargetSize),
				mlog.Float64("maxSafeSize", machineSafeSize))
			selectedTargetSize = v.expectedTargetSize
		} else {
			log.Warn(context.TODO(), "user-provided target size exceeds maxSafeSize, using maxSafeSize",
				mlog.Float64("expectedTargetSize", v.expectedTargetSize),
				mlog.Float64("maxSafeSize", machineSafeSize))
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
	if len(v.segments) == 0 {
		return nil, "force merge trigger"
	}

	targetSize, targetCount := v.calculateTargetSizeCount()
	groups := groupForceMergeSegments(v.segments, targetSize)

	mlog.Info(context.TODO(), "planned force merge groups",
		mlog.Int64("triggerID", v.triggerID),
		mlog.String("label", v.label.String()),
		mlog.String("strategy", "v1 multi-round knapsack"),
		mlog.Int64("targetSegmentSize", targetSize),
		mlog.Int64("wholePoolTargetCount", targetCount),
		mlog.Int("taskCount", len(groups)),
		mlog.Int64("plannedOutputCount", totalForceMergeGroupOutputs(groups, targetSize)),
		mlog.Int64("peakResidualInput", peakForceMergeGroupInput(groups)))

	results := make([]CompactionView, 0, len(groups))
	for _, group := range groups {
		results = append(results, &ForceMergeSegmentView{
			label:              v.label,
			segments:           group.segments,
			triggerID:          v.triggerID,
			collectionTTL:      v.collectionTTL,
			configMaxSize:      v.configMaxSize,
			expectedTargetSize: v.expectedTargetSize,
			targetSegmentSize:  float64(targetSize),
			targetSegmentCount: plannedForceMergeOutputCount(group.residualSize, targetSize),
			topology:           v.topology,
		})
	}
	return results, "force merge trigger"
}

func groupForceMergeSegments(segments []*SegmentView, targetSize int64) []forceMergePackingGroup {
	threeTargetCapacity := forceMergeRoundCapacity(targetSize, 3)
	groups := make([]forceMergePackingGroup, 0, len(segments))
	packable := make([]*SegmentInfo, 0, len(segments))
	segmentByCandidate := make(map[*SegmentInfo]*SegmentView, len(segments))
	for _, segment := range segments {
		candidate := forceMergeKnapsackCandidate(segment)
		residualSize := candidate.GetResidualSegmentSize()
		if residualSize > threeTargetCapacity {
			groups = append(groups, forceMergePackingGroup{
				segments:     []*SegmentView{segment},
				residualSize: residualSize,
				round:        forceMergePackingRoundOversized,
			})
			continue
		}
		packable = append(packable, candidate)
		segmentByCandidate[candidate] = segment
	}

	packer := newSegmentPacker("force-merge-v1-multi-round", packable, nil)
	lossAllowance := targetSize / forceMergeKnapsackLossDivisor
	rounds := []struct {
		capacity    int64
		maxLeftSize int64
		round       forceMergePackingRound
	}{
		{capacity: targetSize, maxLeftSize: lossAllowance, round: forceMergePackingRoundOneTarget},
		{capacity: forceMergeRoundCapacity(targetSize, 2), maxLeftSize: lossAllowance, round: forceMergePackingRoundTwoTargets},
		{capacity: threeTargetCapacity, maxLeftSize: math.MaxInt64, round: forceMergePackingRoundThreeTargets},
	}

	for _, packingRound := range rounds {
		for {
			packed, left := packer.pack(
				packingRound.capacity,
				packingRound.maxLeftSize,
				0,
				forceMergeKnapsackMaxSegments,
			)
			if len(packed) == 0 {
				break
			}

			groupSegments := make([]*SegmentView, 0, len(packed))
			for _, candidate := range packed {
				groupSegments = append(groupSegments, segmentByCandidate[candidate])
			}
			groups = append(groups, forceMergePackingGroup{
				segments:     groupSegments,
				residualSize: packingRound.capacity - left,
				round:        packingRound.round,
			})
		}
	}

	if len(packer.candidates) != 0 {
		panic("3T force-merge packing round did not drain candidates")
	}
	return groups
}

func forceMergeKnapsackCandidate(segment *SegmentView) *SegmentInfo {
	return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:        segment.ID,
		NumOfRows: segment.NumOfRows,
		Stats: &datapb.Statistics{
			InsertBinlogSize: forceMergeWholeBytes(segment.Size),
			DeltaBinlogSize:  forceMergeWholeBytes(segment.DeltaSize),
			DeleteNumRows:    int64(segment.DeltaRowCount),
		},
	}}
}

func forceMergeWholeBytes(size float64) int64 {
	if size <= 0 || math.IsNaN(size) {
		return 0
	}
	if size >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(size)
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

func totalForceMergeGroupOutputs(groups []forceMergePackingGroup, targetSize int64) int64 {
	total := int64(0)
	for _, group := range groups {
		total += plannedForceMergeOutputCount(group.residualSize, targetSize)
	}
	return total
}

func peakForceMergeGroupInput(groups []forceMergePackingGroup) int64 {
	peak := int64(0)
	for _, group := range groups {
		if group.round != forceMergePackingRoundOversized {
			peak = max(peak, group.residualSize)
		}
	}
	return peak
}

func (v *ForceMergeSegmentView) calculateMaxSafeSize() float64 {
	log := mlog.With(mlog.Int64("triggerID", v.triggerID), mlog.String("label", v.label.String()))
	if len(v.topology.QueryNodeMemory) == 0 || len(v.topology.DataNodeMemory) == 0 {
		log.Warn(context.TODO(), "No querynodes or datanodes in topology, using config size")
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
		log.Info(context.TODO(), "force merge on standalone not pooling mode, half the max size",
			mlog.Float64("qnMaxSafeSize", qnMaxSafeSize),
			mlog.Float64("dnMaxSafeSize", dnMaxSafeSize),
			mlog.Float64("maxSafeSize/2", maxSafeSize/2),
			mlog.Float64("configMaxSize", v.configMaxSize))
		// dn and qn are co-located, half the min
		return maxSafeSize * 0.5
	}

	log.Info(context.TODO(), "force merge on cluster/pooling mode",
		mlog.Float64("qnMaxSafeSize", qnMaxSafeSize),
		mlog.Float64("dnMaxSafeSize", dnMaxSafeSize),
		mlog.Float64("maxSafeSize", maxSafeSize),
		mlog.Float64("configMaxSize", v.configMaxSize))
	return maxSafeSize
}
