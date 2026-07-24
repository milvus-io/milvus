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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const forceMergeSizeTolerance = 0.05

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
	targetCount = plannedForceMergeOutputCount(totalSize, targetSize)

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
			targetCount = plannedForceMergeOutputCount(totalSize, targetSize)
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
	threshold := paramtable.Get().DataCoordCfg.CompactionMaxFullSegmentThreshold.GetAsInt()

	var plan []forceMergeTaskPlan
	strategy := "exact DP"
	if len(v.segments) <= threshold {
		plan = exactForceMergePlan(v.segments, targetSize)
	} else {
		strategy = "bounded sequential"
		plan = boundedSequentialForceMergePlan(v.segments, targetSize)
	}

	mlog.Info(context.TODO(), "planned force merge groups",
		mlog.Int64("triggerID", v.triggerID),
		mlog.String("label", v.label.String()),
		mlog.String("strategy", strategy),
		mlog.Int64("targetSegmentSize", targetSize),
		mlog.Int64("wholePoolTargetCount", targetCount),
		mlog.Int("taskCount", len(plan)),
		mlog.Int64("plannedOutputCount", totalForceMergePlanOutputs(plan)),
		mlog.Float64("peakTaskInput", peakForceMergeTaskInput(plan)))

	results := make([]CompactionView, 0, len(plan))
	for _, task := range plan {
		results = append(results, &ForceMergeSegmentView{
			label:              v.label,
			segments:           v.segments[task.start:task.end],
			triggerID:          v.triggerID,
			collectionTTL:      v.collectionTTL,
			configMaxSize:      v.configMaxSize,
			expectedTargetSize: v.expectedTargetSize,
			targetSegmentSize:  float64(targetSize),
			targetSegmentCount: task.outputCount,
			topology:           v.topology,
		})
	}
	return results, "force merge trigger"
}

type forceMergeTaskPlan struct {
	start              int
	end                int
	inputSize          float64
	outputCount        int64
	oversizedSingleton bool
}

type forceMergeExactState struct {
	valid              bool
	outputCount        int64
	peakInput          float64
	previous           int
	taskInputSize      float64
	taskOutputCount    int64
	oversizedSingleton bool
}

func exactForceMergePlan(
	segments []*SegmentView,
	targetSize int64,
) []forceMergeTaskPlan {
	n := len(segments)
	if n == 0 {
		return nil
	}

	inputCeiling := forceMergeMultiSegmentInputCeiling(targetSize)
	// Keep one lexicographically best state per received-order prefix.
	// Equal scores retain the first boundary visited by these ordered loops.
	states := make([]forceMergeExactState, n+1)
	states[0] = forceMergeExactState{valid: true, previous: -1}
	for end := 1; end <= n; end++ {
		groupSize := 0.0
		for start := end - 1; start >= 0; start-- {
			groupSize += segments[start].Size
			segmentCount := end - start
			oversizedSingleton := segmentCount == 1 && groupSize > inputCeiling
			if segmentCount > 1 && groupSize > inputCeiling {
				break
			}

			previous := states[start]
			if !previous.valid {
				continue
			}

			taskOutputCount := plannedForceMergeOutputCount(groupSize, targetSize)
			outputCount := previous.outputCount + taskOutputCount
			peakInput := previous.peakInput
			if !oversizedSingleton {
				peakInput = max(peakInput, groupSize)
			}

			current := states[end]
			if current.valid && (current.outputCount < outputCount ||
				(current.outputCount == outputCount && current.peakInput <= peakInput)) {
				continue
			}
			states[end] = forceMergeExactState{
				valid:              true,
				outputCount:        outputCount,
				peakInput:          peakInput,
				previous:           start,
				taskInputSize:      groupSize,
				taskOutputCount:    taskOutputCount,
				oversizedSingleton: oversizedSingleton,
			}
		}
	}

	if !states[n].valid {
		return nil
	}

	plan := make([]forceMergeTaskPlan, 0, n)
	for end := n; end > 0; {
		state := states[end]
		plan = append(plan, forceMergeTaskPlan{
			start:              state.previous,
			end:                end,
			inputSize:          state.taskInputSize,
			outputCount:        state.taskOutputCount,
			oversizedSingleton: state.oversizedSingleton,
		})
		end = state.previous
	}
	reverseForceMergePlan(plan)
	return plan
}

func boundedSequentialForceMergePlan(
	segments []*SegmentView,
	targetSize int64,
) []forceMergeTaskPlan {
	if len(segments) == 0 {
		return nil
	}

	inputCeiling := forceMergeMultiSegmentInputCeiling(targetSize)
	plan := make([]forceMergeTaskPlan, 0, len(segments))
	for start := 0; start < len(segments); {
		end := start + 1
		groupSize := segments[start].Size
		if groupSize > inputCeiling {
			plan = append(plan, forceMergeTaskPlan{
				start:              start,
				end:                end,
				inputSize:          groupSize,
				outputCount:        plannedForceMergeOutputCount(groupSize, targetSize),
				oversizedSingleton: true,
			})
			start = end
			continue
		}

		for end < len(segments) {
			nextSize := segments[end].Size
			if groupSize+nextSize > inputCeiling {
				break
			}

			currentFullOutputs := math.Floor(groupSize / float64(targetSize))
			nextFullOutputs := math.Floor((groupSize + nextSize) / float64(targetSize))
			if currentFullOutputs > 0 &&
				math.Mod(groupSize, float64(targetSize)) < forceMergeSizeTolerance*float64(targetSize) &&
				nextFullOutputs == currentFullOutputs {
				break
			}

			groupSize += nextSize
			end++
		}

		plan = append(plan, forceMergeTaskPlan{
			start:       start,
			end:         end,
			inputSize:   groupSize,
			outputCount: plannedForceMergeOutputCount(groupSize, targetSize),
		})
		start = end
	}
	return plan
}

func plannedForceMergeOutputCount(inputSize float64, targetSize int64) int64 {
	if inputSize <= 0 || math.IsNaN(inputSize) || targetSize <= 0 {
		return 1
	}
	return max(int64(math.Ceil(inputSize/float64(targetSize))), 1)
}

func forceMergeMultiSegmentInputCeiling(targetSize int64) float64 {
	return 3 * float64(targetSize)
}

func forceMergeEffectiveSize(size float64) int64 {
	if size < 1 || math.IsNaN(size) {
		return 1
	}
	return int64(size)
}

func totalForceMergePlanOutputs(plan []forceMergeTaskPlan) int64 {
	total := int64(0)
	for _, task := range plan {
		total += task.outputCount
	}
	return total
}

func peakForceMergeTaskInput(plan []forceMergeTaskPlan) float64 {
	peak := 0.0
	for _, task := range plan {
		if !task.oversizedSingleton {
			peak = max(peak, task.inputSize)
		}
	}
	return peak
}

func reverseForceMergePlan(plan []forceMergeTaskPlan) {
	for left, right := 0, len(plan)-1; left < right; left, right = left+1, right-1 {
		plan[left], plan[right] = plan[right], plan[left]
	}
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
