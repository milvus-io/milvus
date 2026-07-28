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
	"encoding/json"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

const (
	forceMergeV1KnapsackMaxSegments = int64(4096)
	forceMergeV1KnapsackReportEnv   = "FORCE_MERGE_V1_KNAPSACK_REPORT"
)

// forceTriggerAllV1KnapsackComparison reproduces the non-TTL V1
// force-compaction pack path for the recovered size-only fixtures, without
// wiring it into production. All segments are prioritized, the merge knapsack
// is empty, and oversized leftovers become singleton tasks. It deliberately
// shares the current force-merge target calculation so this experiment
// compares grouping policy rather than target-size policy.
func forceTriggerAllV1KnapsackComparison(view *ForceMergeSegmentView) ([]CompactionView, string) {
	if len(view.segments) == 0 {
		return nil, "force merge trigger"
	}

	targetSize, _ := view.calculateTargetSizeCount()
	candidates := make([]*SegmentInfo, 0, len(view.segments))
	segmentViews := make(map[*SegmentInfo]*SegmentView, len(view.segments))
	for _, segment := range view.segments {
		candidate := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:        segment.ID,
			NumOfRows: 1,
			Stats: &datapb.Statistics{
				InsertBinlogSize: int64(segment.Size),
			},
		}}
		candidates = append(candidates, candidate)
		segmentViews[candidate] = segment
	}

	toUpdate := newSegmentPacker("force-merge-v1-update-comparison", candidates, nil)
	toMerge := newSegmentPacker("force-merge-v1-merge-comparison", nil, nil)

	groups := make([][]*SegmentView, 0, len(view.segments))
	for {
		packed, _ := toUpdate.packWith(
			targetSize,
			math.MaxInt64,
			0,
			forceMergeV1KnapsackMaxSegments,
			toMerge,
		)
		if len(packed) == 0 {
			break
		}

		group := make([]*SegmentView, 0, len(packed))
		for _, candidate := range packed {
			group = append(group, segmentViews[candidate])
		}
		groups = append(groups, group)
	}

	for _, candidate := range toUpdate.candidates {
		groups = append(groups, []*SegmentView{segmentViews[candidate]})
	}

	results := make([]CompactionView, 0, len(groups))
	for _, group := range groups {
		results = append(results, &ForceMergeSegmentView{
			label:              view.label,
			segments:           group,
			triggerID:          view.triggerID,
			collectionTTL:      view.collectionTTL,
			configMaxSize:      view.configMaxSize,
			expectedTargetSize: view.expectedTargetSize,
			topology:           view.topology,
			targetSegmentSize:  float64(targetSize),
			targetSegmentCount: plannedForceMergeOutputCount(sumSegmentSize(group), targetSize),
		})
	}
	return results, "force merge trigger"
}

type forceMergeKnapsackComparisonFixture struct {
	name            string
	sizes           []float64
	ids             []int64
	requestedTarget float64
	threshold       int
	queryNodeCount  int
}

type forceMergeKnapsackComparisonOutputPlan struct {
	InputSegmentIDs          []int64   `json:"inputSegmentIds"`
	InputSegmentSizes        []float64 `json:"inputSegmentSizes"`
	InputSize                float64   `json:"inputSize"`
	TargetSize               int64     `json:"targetSize"`
	DerivedFinalSegmentCount int64     `json:"derivedFinalSegmentCount"`
}

type forceMergeKnapsackComparisonPlannerResult struct {
	Planner                  string                                   `json:"planner"`
	OutputPlans              []forceMergeKnapsackComparisonOutputPlan `json:"outputPlans"`
	OutputPlanCount          int                                      `json:"outputPlanCount"`
	DerivedFinalSegmentCount int64                                    `json:"derivedFinalSegmentCount"`
	LargestOutputPlanInput   float64                                  `json:"largestOutputPlanInput"`
	PreservesReceivedOrder   bool                                     `json:"preservesReceivedOrder"`
}

type forceMergeKnapsackComparisonCaseResult struct {
	Name                 string                                    `json:"name"`
	RequestedTargetSize  float64                                   `json:"requestedTargetSize"`
	FinalTargetSize      int64                                     `json:"finalTargetSize"`
	Threshold            int                                       `json:"threshold"`
	CurrentPlanningPath  string                                    `json:"currentPlanningPath"`
	OriginalSegmentIDs   []int64                                   `json:"originalSegmentIds"`
	OriginalSegmentSizes []float64                                 `json:"originalSegmentSizes"`
	Current              forceMergeKnapsackComparisonPlannerResult `json:"current"`
	V1Knapsack           forceMergeKnapsackComparisonPlannerResult `json:"v1Knapsack"`
}

type forceMergeKnapsackComparisonReport struct {
	TargetPolicy   string                                   `json:"targetPolicy"`
	KnapsackSource string                                   `json:"knapsackSource"`
	Cases          []forceMergeKnapsackComparisonCaseResult `json:"cases"`
}

func forceMergeKnapsackComparisonFixtures() []forceMergeKnapsackComparisonFixture {
	// Keep this table comparison-local so the experiment does not modify or
	// couple to the production regression test. Values intentionally mirror the
	// recovered 22-case matrix in compaction_view_forcemerge_test.go.
	const gib = float64(1 << 30)
	productionSizes := roundedForceMergeGiBSizes(
		2.40, 2.51, 2.62, 2.73, 2.84, 2.95, 2.36,
		2.47, 2.58, 2.69, 2.80, 2.91, 2.22, 2.33,
		2.44, 2.55, 2.66, 2.77, 2.88, 2.99, 2.91,
	)
	tinyTailSizes := append(repeatForceMergeSizes(40, 10), 5, 12, 18, 20)

	return []forceMergeKnapsackComparisonFixture{
		{name: "01_production_at_threshold", sizes: productionSizes, requestedTarget: 4 * gib, threshold: 100},
		{name: "02_production_above_threshold", sizes: productionSizes, requestedTarget: 4 * gib, threshold: 20},
		{name: "03_six_equal_at_threshold", sizes: repeatForceMergeSizes(6, 70), requestedTarget: 100, threshold: 6},
		{name: "04_six_equal_above_threshold", sizes: repeatForceMergeSizes(6, 70), requestedTarget: 100, threshold: 5},
		{name: "05_uniform_1gib_at_threshold", sizes: repeatForceMergeSizes(12, gib), requestedTarget: 3 * gib, threshold: 12},
		{name: "06_uniform_1_02gib_at_threshold", sizes: repeatForceMergeSizes(12, 1095216660), requestedTarget: 3 * gib, threshold: 12},
		{name: "07_oversized_pair_at_threshold", sizes: []float64{315, 315}, requestedTarget: 100, threshold: 2},
		{name: "08_uniform_130_at_threshold", sizes: repeatForceMergeSizes(10, 130), requestedTarget: 100, threshold: 10},
		{name: "09_mixed_at_threshold", sizes: []float64{37, 162, 23, 276, 31, 249, 162}, requestedTarget: 100, threshold: 7},
		{name: "10_mixed_above_threshold", sizes: []float64{37, 162, 23, 276, 31, 249, 162}, requestedTarget: 100, threshold: 6},
		{name: "11_three_target_pair_at_threshold", sizes: []float64{150, 150}, requestedTarget: 100, threshold: 2},
		{name: "12_three_target_pair_plus_one_at_threshold", sizes: []float64{150, 151}, requestedTarget: 100, threshold: 2},
		{name: "13_equal_total_boundary_at_threshold", sizes: []float64{210, 190}, requestedTarget: 100, threshold: 2},
		{name: "14_unequal_total_boundary_at_threshold", sizes: []float64{211, 189}, requestedTarget: 100, threshold: 2},
		{name: "15_near_full_below_5_percent_above_threshold", sizes: []float64{95, 9, 10}, requestedTarget: 100, threshold: 2},
		{name: "16_near_full_at_5_percent_above_threshold", sizes: []float64{95, 10, 10}, requestedTarget: 100, threshold: 2},
		{name: "17_topology_floor_at_threshold", sizes: repeatForceMergeSizes(10, 100), requestedTarget: 1000, threshold: 10, queryNodeCount: 10},
		{name: "18_topology_floor_above_threshold", sizes: repeatForceMergeSizes(10, 100), requestedTarget: 1000, threshold: 9, queryNodeCount: 10},
		{name: "19_one_hundred_at_threshold", sizes: repeatForceMergeSizes(100, 51), requestedTarget: 100, threshold: 100},
		{name: "20_one_hundred_one_above_threshold", sizes: repeatForceMergeSizes(101, 51), requestedTarget: 100, threshold: 100},
		{name: "21_tiny_tail_at_threshold", sizes: tinyTailSizes, requestedTarget: 100, threshold: 100},
		{name: "22_received_order_at_threshold", sizes: repeatForceMergeSizes(4, 30), ids: []int64{4, 3, 2, 1}, requestedTarget: 100, threshold: 4},
	}
}

func forceMergeKnapsackComparisonNewView(fixture forceMergeKnapsackComparisonFixture) *ForceMergeSegmentView {
	ids := append([]int64(nil), fixture.ids...)
	if len(ids) == 0 {
		ids = make([]int64, len(fixture.sizes))
		for i := range ids {
			ids[i] = int64(i + 1)
		}
	}

	segments := make([]*SegmentView, len(fixture.sizes))
	for i, size := range fixture.sizes {
		segments[i] = &SegmentView{ID: ids[i], Size: size}
	}

	queryNodeCount := max(fixture.queryNodeCount, 1)
	queryNodeMemory := make(map[int64]uint64, queryNodeCount)
	for i := 0; i < queryNodeCount; i++ {
		queryNodeMemory[int64(i+1)] = math.MaxUint64
	}

	return &ForceMergeSegmentView{
		label: &CompactionGroupLabel{
			CollectionID: 1,
			PartitionID:  10,
			Channel:      "force-merge-v1-knapsack-comparison",
		},
		segments:           segments,
		triggerID:          50908,
		configMaxSize:      1,
		expectedTargetSize: fixture.requestedTarget,
		topology: &CollectionTopology{
			QueryNodeMemory: queryNodeMemory,
			DataNodeMemory:  map[int64]uint64{1: math.MaxUint64},
			NumReplicas:     1,
			NumShards:       1,
		},
	}
}

func forceMergeKnapsackComparisonRunPlanner(
	t *testing.T,
	planner string,
	view *ForceMergeSegmentView,
	run func(*ForceMergeSegmentView) ([]CompactionView, string),
) forceMergeKnapsackComparisonPlannerResult {
	t.Helper()

	children, reason := run(view)
	require.Equal(t, "force merge trigger", reason)
	require.NotEmpty(t, children)

	result := forceMergeKnapsackComparisonPlannerResult{
		Planner:     planner,
		OutputPlans: make([]forceMergeKnapsackComparisonOutputPlan, 0, len(children)),
	}
	flattenedIDs := make([]int64, 0, len(view.segments))
	var commonTargetSize int64

	for _, child := range children {
		forceMergeChild, ok := child.(*ForceMergeSegmentView)
		require.True(t, ok, "unexpected comparison output type %T", child)
		require.NotEmpty(t, forceMergeChild.segments)

		targetSize := int64(forceMergeChild.targetSegmentSize)
		require.Equal(t, float64(targetSize), forceMergeChild.targetSegmentSize)
		require.Positive(t, targetSize)
		if commonTargetSize == 0 {
			commonTargetSize = targetSize
		} else {
			require.Equal(t, commonTargetSize, targetSize)
		}

		inputIDs := make([]int64, len(forceMergeChild.segments))
		inputSizes := make([]float64, len(forceMergeChild.segments))
		for i, segment := range forceMergeChild.segments {
			inputIDs[i] = segment.ID
			inputSizes[i] = segment.Size
		}
		flattenedIDs = append(flattenedIDs, inputIDs...)

		inputSize := forceMergeChild.GetTotalSize()
		derivedFinals := plannedForceMergeOutputCount(inputSize, targetSize)
		require.Equal(t, derivedFinals, forceMergeChild.targetSegmentCount)
		result.OutputPlans = append(result.OutputPlans, forceMergeKnapsackComparisonOutputPlan{
			InputSegmentIDs:          inputIDs,
			InputSegmentSizes:        inputSizes,
			InputSize:                inputSize,
			TargetSize:               targetSize,
			DerivedFinalSegmentCount: derivedFinals,
		})
		result.DerivedFinalSegmentCount += derivedFinals
		result.LargestOutputPlanInput = max(result.LargestOutputPlanInput, inputSize)
	}

	result.OutputPlanCount = len(result.OutputPlans)
	result.PreservesReceivedOrder = forceMergeKnapsackComparisonEqualIDs(
		flattenedIDs,
		forceMergeSegmentIDs(view.segments),
	)
	require.True(t, forceMergeKnapsackComparisonSameIDMultiset(flattenedIDs, forceMergeSegmentIDs(view.segments)))
	return result
}

func forceMergeKnapsackComparisonEqualIDs(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func forceMergeKnapsackComparisonSameIDMultiset(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[int64]int, len(left))
	for _, id := range left {
		counts[id]++
	}
	for _, id := range right {
		counts[id]--
		if counts[id] < 0 {
			return false
		}
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func forceMergeKnapsackComparisonTargetSize(
	t *testing.T,
	result forceMergeKnapsackComparisonPlannerResult,
) int64 {
	t.Helper()
	require.NotEmpty(t, result.OutputPlans)
	targetSize := result.OutputPlans[0].TargetSize
	for _, plan := range result.OutputPlans[1:] {
		require.Equal(t, targetSize, plan.TargetSize)
	}
	return targetSize
}

func TestForceMergeV1KnapsackComparisonRecoveredScenarios(t *testing.T) {
	fixtures := forceMergeKnapsackComparisonFixtures()
	require.Len(t, fixtures, 22)
	report := forceMergeKnapsackComparisonReport{
		TargetPolicy:   "shared-current-final-target",
		KnapsackSource: "internal/datacoord/knapsack.go V1 force-compaction path",
		Cases:          make([]forceMergeKnapsackComparisonCaseResult, 0, len(fixtures)),
	}

	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			setForceMergePlanningThreshold(t, strconv.Itoa(fixture.threshold))

			current := forceMergeKnapsackComparisonRunPlanner(
				t,
				"current-fixed-target",
				forceMergeKnapsackComparisonNewView(fixture),
				(*ForceMergeSegmentView).ForceTriggerAll,
			)
			v1Knapsack := forceMergeKnapsackComparisonRunPlanner(
				t,
				"v1-knapsack",
				forceMergeKnapsackComparisonNewView(fixture),
				forceTriggerAllV1KnapsackComparison,
			)

			currentTargetSize := forceMergeKnapsackComparisonTargetSize(t, current)
			require.Equal(t, currentTargetSize, forceMergeKnapsackComparisonTargetSize(t, v1Knapsack))

			planningPath := "exact-dp"
			if len(fixture.sizes) > fixture.threshold {
				planningPath = "bounded-sequential"
			}

			ids := append([]int64(nil), fixture.ids...)
			if len(ids) == 0 {
				ids = make([]int64, len(fixture.sizes))
				for i := range ids {
					ids[i] = int64(i + 1)
				}
			}
			caseResult := forceMergeKnapsackComparisonCaseResult{
				Name:                 fixture.name,
				RequestedTargetSize:  fixture.requestedTarget,
				FinalTargetSize:      currentTargetSize,
				Threshold:            fixture.threshold,
				CurrentPlanningPath:  planningPath,
				OriginalSegmentIDs:   ids,
				OriginalSegmentSizes: append([]float64(nil), fixture.sizes...),
				Current:              current,
				V1Knapsack:           v1Knapsack,
			}
			report.Cases = append(report.Cases, caseResult)

			t.Logf(
				"%s: current plans=%d finals=%d peak=%.0f order=%t; v1-knapsack plans=%d finals=%d peak=%.0f order=%t",
				fixture.name,
				current.OutputPlanCount,
				current.DerivedFinalSegmentCount,
				current.LargestOutputPlanInput,
				current.PreservesReceivedOrder,
				v1Knapsack.OutputPlanCount,
				v1Knapsack.DerivedFinalSegmentCount,
				v1Knapsack.LargestOutputPlanInput,
				v1Knapsack.PreservesReceivedOrder,
			)
		})
	}

	if os.Getenv(forceMergeV1KnapsackReportEnv) != "" {
		document, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)
		t.Logf("force-merge V1 knapsack comparison report:\n%s", document)
	}
}
