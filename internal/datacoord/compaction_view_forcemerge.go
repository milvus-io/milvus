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

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

const (
	defaultToleranceMB = 0.05
)

// static segment view, only algothrims here, no IO
type ForceMergeSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	triggerID     int64
	collectionTTL time.Duration

	configMaxSize float64
	topology      *CollectionTopology

	targetSize  float64
	targetCount int64
}

func (v *ForceMergeSegmentView) GetGroupLabel() *CompactionGroupLabel {
	return v.label
}

func (v *ForceMergeSegmentView) GetSegmentsView() []*SegmentView {
	return v.segments
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

func (v *ForceMergeSegmentView) calculateTargetSizeCount() (maxSafeSize float64, targetCount int64) {
	log := log.With(zap.Int64("triggerID", v.triggerID), zap.String("label", v.label.String()))
	maxSafeSize = v.calculateMaxSafeSize()
	if maxSafeSize < v.configMaxSize {
		log.Info("maxSafeSize is less than configMaxSize, set to configMaxSize",
			zap.Float64("targetSize", maxSafeSize),
			zap.Float64("configMaxSize", v.configMaxSize))
		maxSafeSize = v.configMaxSize
	}

	targetCount = max(1, int64(sumSegmentSize(v.segments)/maxSafeSize))
	log.Info("topology-aware force merge calculation",
		zap.Int64("targetSegmentCount", targetCount),
		zap.Float64("maxSafeSize", maxSafeSize))
	return maxSafeSize, targetCount
}

func (v *ForceMergeSegmentView) ForceTriggerAll() ([]CompactionView, string) {
	targetSizePerSegment, targetCount := v.calculateTargetSizeCount()
	groups := adaptiveGroupSegments(v.segments, float64(targetSizePerSegment))

	results := make([]CompactionView, 0, len(groups))
	for _, group := range groups {
		results = append(results, &ForceMergeSegmentView{
			label:         v.label,
			segments:      group,
			triggerID:     v.triggerID,
			collectionTTL: v.collectionTTL,
			configMaxSize: v.configMaxSize,
			targetSize:    targetSizePerSegment,
			targetCount:   targetCount,
			topology:      v.topology,
		})
	}
	return results, "force merge trigger"
}

// adaptiveGroupSegments automatically selects the best grouping algorithm based on segment count
// For small segment counts (≤ threshold), use maxFull for optimal full segment count
// For large segment counts, use larger for better performance
func adaptiveGroupSegments(segments []*SegmentView, targetSize float64) [][]*SegmentView {
	if len(segments) == 0 {
		return nil
	}

	n := len(segments)

	// Get threshold from config, fallback to default if not available
	threshold := paramtable.Get().DataCoordCfg.CompactionMaxFullSegmentThreshold.GetAsInt()

	// Use maxFull for small segment counts to maximize full segments
	// Use larger for large segment counts for O(n) performance
	if n <= threshold {
		return maxFullSegmentsGrouping(segments, targetSize)
	}

	return largerGroupingSegments(segments, targetSize)
}

// largerGroupingSegments groups segments to minimize number of tasks
// Strategy: Create larger groups that produce multiple full target-sized segments
// This approach favors fewer compaction tasks with larger batches
func largerGroupingSegments(segments []*SegmentView, targetSize float64) [][]*SegmentView {
	if len(segments) == 0 {
		return nil
	}

	n := len(segments)
	// Pre-allocate with estimated capacity to reduce allocations
	estimatedGroups := max(1, n/10)
	groups := make([][]*SegmentView, 0, estimatedGroups)

	i := 0
	for i < n {
		groupStart := i
		groupSize := 0.0

		// Accumulate segments to form multiple target-sized outputs
		for i < n {
			groupSize += segments[i].Size
			i++

			// Check if we should stop
			if i < n {
				nextSize := groupSize + segments[i].Size
				currentFull := int(groupSize / targetSize)
				nextFull := int(nextSize / targetSize)

				// Stop if we have full segments and next addition won't give another full segment
				if currentFull > 0 && nextFull == currentFull {
					currentRemainder := math.Mod(groupSize, targetSize)
					if currentRemainder < targetSize*defaultToleranceMB {
						break
					}
				}
			}
		}

		groups = append(groups, segments[groupStart:i])
	}

	return groups
}

// maxFullSegmentsGrouping groups segments to maximize number of full target-sized outputs
// Strategy: Use dynamic programming to find partitioning that produces most full segments
// This approach minimizes tail segments and achieves best space utilization
func maxFullSegmentsGrouping(segments []*SegmentView, targetSize float64) [][]*SegmentView {
	if len(segments) == 0 {
		return nil
	}

	n := len(segments)

	// Pre-compute prefix sums to avoid repeated summation
	prefixSum := make([]float64, n+1)
	for i := 0; i < n; i++ {
		prefixSum[i+1] = prefixSum[i] + segments[i].Size
	}

	// dp[i] = best result for segments[0:i]
	type dpState struct {
		fullSegments int
		tailSegments int
		numGroups    int
		groupIndices []int
	}

	dp := make([]dpState, n+1)
	dp[0] = dpState{fullSegments: 0, tailSegments: 0, numGroups: 0, groupIndices: make([]int, 0, n/10)}

	for i := 1; i <= n; i++ {
		dp[i] = dpState{fullSegments: -1, tailSegments: math.MaxInt32, numGroups: 0, groupIndices: nil}

		// Try different starting positions for the last group
		for j := 0; j < i; j++ {
			// Calculate group size from j to i-1 using prefix sums (O(1) instead of O(n))
			groupSize := prefixSum[i] - prefixSum[j]

			numFull := int(groupSize / targetSize)
			remainder := math.Mod(groupSize, targetSize)
			hasTail := 0
			if remainder > 0.01 {
				hasTail = 1
			}

			newFull := dp[j].fullSegments + numFull
			newTails := dp[j].tailSegments + hasTail
			newGroups := dp[j].numGroups + 1

			// Prioritize: more full segments, then fewer tails, then more groups (for parallelism)
			isBetter := false
			if newFull > dp[i].fullSegments {
				isBetter = true
			} else if newFull == dp[i].fullSegments && newTails < dp[i].tailSegments {
				isBetter = true
			} else if newFull == dp[i].fullSegments && newTails == dp[i].tailSegments && newGroups > dp[i].numGroups {
				isBetter = true
			}

			if isBetter {
				// Pre-allocate with exact capacity to avoid reallocation
				newIndices := make([]int, 0, len(dp[j].groupIndices)+1)
				newIndices = append(newIndices, dp[j].groupIndices...)
				newIndices = append(newIndices, j)
				dp[i] = dpState{
					fullSegments: newFull,
					tailSegments: newTails,
					numGroups:    newGroups,
					groupIndices: newIndices,
				}
			}
		}
	}

	// Reconstruct groups from indices
	if dp[n].groupIndices == nil {
		return [][]*SegmentView{segments}
	}

	groupStarts := append(dp[n].groupIndices, n)
	groups := make([][]*SegmentView, 0, len(groupStarts))
	for i := 0; i < len(groupStarts); i++ {
		start := groupStarts[i]
		end := n
		if i+1 < len(groupStarts) {
			end = groupStarts[i+1]
		}
		if start < end {
			groups = append(groups, segments[start:end])
		}
	}

	return groups
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
			zap.Float64("maxSafeSize", maxSafeSize),
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
