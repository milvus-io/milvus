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
	forceMergeV1KnapsackLossDivisor = int64(20)

	forceMergeV1KnapsackRoundOversized    forceMergeV1KnapsackRound = "oversized-singleton"
	forceMergeV1KnapsackRoundOneTarget    forceMergeV1KnapsackRound = "1T"
	forceMergeV1KnapsackRoundTwoTargets   forceMergeV1KnapsackRound = "2T"
	forceMergeV1KnapsackRoundThreeTargets forceMergeV1KnapsackRound = "3T"
)

type forceMergeV1KnapsackRound string

type forceMergeV1KnapsackComparisonGroup struct {
	candidates         []*SegmentInfo
	estimatedInputSize int64
	packingRound       forceMergeV1KnapsackRound
}

type forceMergeV1KnapsackComparisonView struct {
	*ForceMergeSegmentView
	estimatedInputSize int64
	packingRound       forceMergeV1KnapsackRound
}

func forceMergeV1KnapsackRoundCapacity(targetSize, multiplier int64) int64 {
	if targetSize > math.MaxInt64/multiplier {
		return math.MaxInt64
	}
	return targetSize * multiplier
}

func forceMergeV1KnapsackComparisonOutputCount(estimatedInputSize, targetSize int64) int64 {
	if estimatedInputSize <= 0 || targetSize <= 0 {
		return 1
	}
	return 1 + (estimatedInputSize-1)/targetSize
}

func forceMergeV1KnapsackComparisonCandidate(segment *SegmentView) *SegmentInfo {
	return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:        segment.ID,
		NumOfRows: segment.NumOfRows,
		Stats: &datapb.Statistics{
			InsertBinlogSize: int64(segment.Size),
			DeltaBinlogSize:  int64(segment.DeltaSize),
			DeleteNumRows:    int64(segment.DeltaRowCount),
		},
	}}
}

func forceMergeV1KnapsackComparisonEstimatedInputSize(segments []*SegmentView) int64 {
	var total int64
	for _, segment := range segments {
		total += forceMergeV1KnapsackComparisonCandidate(segment).GetResidualSegmentSize()
	}
	return total
}

func forceMergeV1KnapsackMultiRoundGroups(
	candidates []*SegmentInfo,
	targetSize int64,
) []forceMergeV1KnapsackComparisonGroup {
	threeTargetCapacity := forceMergeV1KnapsackRoundCapacity(targetSize, 3)
	groups := make([]forceMergeV1KnapsackComparisonGroup, 0, len(candidates))
	packable := make([]*SegmentInfo, 0, len(candidates))
	for _, candidate := range candidates {
		residualSize := candidate.GetResidualSegmentSize()
		if residualSize > threeTargetCapacity {
			groups = append(groups, forceMergeV1KnapsackComparisonGroup{
				candidates:         []*SegmentInfo{candidate},
				estimatedInputSize: residualSize,
				packingRound:       forceMergeV1KnapsackRoundOversized,
			})
			continue
		}
		packable = append(packable, candidate)
	}

	packer := newSegmentPacker("force-merge-v1-multi-round-comparison", packable, nil)
	lossAllowance := targetSize / forceMergeV1KnapsackLossDivisor
	rounds := []struct {
		capacity     int64
		maxLeftSize  int64
		packingRound forceMergeV1KnapsackRound
	}{
		{capacity: targetSize, maxLeftSize: lossAllowance, packingRound: forceMergeV1KnapsackRoundOneTarget},
		{capacity: forceMergeV1KnapsackRoundCapacity(targetSize, 2), maxLeftSize: lossAllowance, packingRound: forceMergeV1KnapsackRoundTwoTargets},
		{capacity: threeTargetCapacity, maxLeftSize: math.MaxInt64, packingRound: forceMergeV1KnapsackRoundThreeTargets},
	}

	for _, round := range rounds {
		for {
			packed, left := packer.pack(
				round.capacity,
				round.maxLeftSize,
				0,
				forceMergeV1KnapsackMaxSegments,
			)
			if len(packed) == 0 {
				break
			}
			groups = append(groups, forceMergeV1KnapsackComparisonGroup{
				candidates:         packed,
				estimatedInputSize: round.capacity - left,
				packingRound:       round.packingRound,
			})
		}
	}

	if len(packer.candidates) != 0 {
		panic("3T V1 knapsack round did not drain packable candidates")
	}
	return groups
}

// forceTriggerAllV1MultiRoundKnapsackComparison applies the agreed test-only V1
// multi-round prototype without wiring it into production. It deliberately
// shares the current force-merge target calculation so this experiment
// compares grouping policy rather than target-size policy.
func forceTriggerAllV1MultiRoundKnapsackComparison(view *ForceMergeSegmentView) ([]CompactionView, string) {
	if len(view.segments) == 0 {
		return nil, "force merge trigger"
	}

	targetSize, _ := view.calculateTargetSizeCount()
	candidates := make([]*SegmentInfo, 0, len(view.segments))
	segmentViews := make(map[*SegmentInfo]*SegmentView, len(view.segments))
	for _, segment := range view.segments {
		candidate := forceMergeV1KnapsackComparisonCandidate(segment)
		candidates = append(candidates, candidate)
		segmentViews[candidate] = segment
	}

	packedGroups := forceMergeV1KnapsackMultiRoundGroups(candidates, targetSize)
	results := make([]CompactionView, 0, len(packedGroups))
	for _, packedGroup := range packedGroups {
		group := make([]*SegmentView, 0, len(packedGroup.candidates))
		for _, candidate := range packedGroup.candidates {
			group = append(group, segmentViews[candidate])
		}
		results = append(results, &forceMergeV1KnapsackComparisonView{
			ForceMergeSegmentView: &ForceMergeSegmentView{
				label:              view.label,
				segments:           group,
				triggerID:          view.triggerID,
				collectionTTL:      view.collectionTTL,
				configMaxSize:      view.configMaxSize,
				expectedTargetSize: view.expectedTargetSize,
				topology:           view.topology,
				targetSegmentSize:  float64(targetSize),
				targetSegmentCount: forceMergeV1KnapsackComparisonOutputCount(packedGroup.estimatedInputSize, targetSize),
			},
			estimatedInputSize: packedGroup.estimatedInputSize,
			packingRound:       packedGroup.packingRound,
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
	InputSegmentIDs          []int64                   `json:"inputSegmentIds"`
	InputSegmentSizes        []float64                 `json:"inputSegmentSizes"`
	EstimatedInputSize       int64                     `json:"estimatedInputSize"`
	PackingRound             forceMergeV1KnapsackRound `json:"packingRound,omitempty"`
	TargetSize               int64                     `json:"targetSize"`
	DerivedFinalSegmentCount int64                     `json:"derivedFinalSegmentCount"`
}

type forceMergeKnapsackComparisonPlannerResult struct {
	Planner                   string                                   `json:"planner"`
	OutputPlans               []forceMergeKnapsackComparisonOutputPlan `json:"outputPlans"`
	OutputPlanCount           int                                      `json:"outputPlanCount"`
	DerivedFinalSegmentCount  int64                                    `json:"derivedFinalSegmentCount"`
	LargestEstimatedPlanInput int64                                    `json:"largestEstimatedPlanInput"`
	PreservesReceivedOrder    bool                                     `json:"preservesReceivedOrder"`
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
	V1MultiRoundKnapsack forceMergeKnapsackComparisonPlannerResult `json:"v1MultiRoundKnapsack"`
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
		segments[i] = &SegmentView{ID: ids[i], Size: size, NumOfRows: 1}
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
		var (
			forceMergeChild    *ForceMergeSegmentView
			prototypeView      *forceMergeV1KnapsackComparisonView
			estimatedInputSize int64
			packingRound       forceMergeV1KnapsackRound
		)
		switch typed := child.(type) {
		case *ForceMergeSegmentView:
			forceMergeChild = typed
			totalSize := typed.GetTotalSize()
			require.Equal(t, float64(int64(totalSize)), totalSize, "comparison fixture sizes must be whole bytes")
			estimatedInputSize = int64(totalSize)
		case *forceMergeV1KnapsackComparisonView:
			forceMergeChild = typed.ForceMergeSegmentView
			prototypeView = typed
			estimatedInputSize = typed.estimatedInputSize
			packingRound = typed.packingRound
		default:
			require.FailNow(t, "unexpected comparison output type", "%T", child)
		}
		require.NotEmpty(t, forceMergeChild.segments)

		targetSize := int64(forceMergeChild.targetSegmentSize)
		require.Equal(t, float64(targetSize), forceMergeChild.targetSegmentSize)
		require.Positive(t, targetSize)
		if commonTargetSize == 0 {
			commonTargetSize = targetSize
		} else {
			require.Equal(t, commonTargetSize, targetSize)
		}
		if prototypeView != nil {
			forceMergeV1KnapsackComparisonRequireRoundContract(t, prototypeView)
		}

		inputIDs := make([]int64, len(forceMergeChild.segments))
		inputSizes := make([]float64, len(forceMergeChild.segments))
		for i, segment := range forceMergeChild.segments {
			inputIDs[i] = segment.ID
			inputSizes[i] = segment.Size
		}
		flattenedIDs = append(flattenedIDs, inputIDs...)

		derivedFinals := plannedForceMergeOutputCount(float64(estimatedInputSize), targetSize)
		if prototypeView != nil {
			derivedFinals = forceMergeV1KnapsackComparisonOutputCount(prototypeView.estimatedInputSize, targetSize)
		}
		require.Equal(t, derivedFinals, forceMergeChild.targetSegmentCount)
		result.OutputPlans = append(result.OutputPlans, forceMergeKnapsackComparisonOutputPlan{
			InputSegmentIDs:          inputIDs,
			InputSegmentSizes:        inputSizes,
			EstimatedInputSize:       estimatedInputSize,
			PackingRound:             packingRound,
			TargetSize:               targetSize,
			DerivedFinalSegmentCount: derivedFinals,
		})
		result.DerivedFinalSegmentCount += derivedFinals
		result.LargestEstimatedPlanInput = max(result.LargestEstimatedPlanInput, estimatedInputSize)
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

func forceMergeV1KnapsackComparisonRequireView(
	t *testing.T,
	child CompactionView,
) *forceMergeV1KnapsackComparisonView {
	t.Helper()
	prototypeView, ok := child.(*forceMergeV1KnapsackComparisonView)
	require.True(t, ok, "unexpected prototype output type %T", child)
	return prototypeView
}

func forceMergeV1KnapsackComparisonRequireRoundContract(
	t *testing.T,
	view *forceMergeV1KnapsackComparisonView,
) {
	t.Helper()
	require.NotEmpty(t, view.segments)
	targetSize := int64(view.targetSegmentSize)
	require.Equal(t, float64(targetSize), view.targetSegmentSize)
	require.Positive(t, targetSize)
	require.Equal(t, forceMergeV1KnapsackComparisonEstimatedInputSize(view.segments), view.estimatedInputSize)

	switch view.packingRound {
	case forceMergeV1KnapsackRoundOversized:
		require.Len(t, view.segments, 1)
		require.Greater(t, view.estimatedInputSize, forceMergeV1KnapsackRoundCapacity(targetSize, 3))
	case forceMergeV1KnapsackRoundOneTarget:
		require.LessOrEqual(t, view.estimatedInputSize, targetSize)
		require.LessOrEqual(t, targetSize-view.estimatedInputSize, targetSize/forceMergeV1KnapsackLossDivisor)
	case forceMergeV1KnapsackRoundTwoTargets:
		capacity := forceMergeV1KnapsackRoundCapacity(targetSize, 2)
		require.LessOrEqual(t, view.estimatedInputSize, capacity)
		require.LessOrEqual(t, capacity-view.estimatedInputSize, targetSize/forceMergeV1KnapsackLossDivisor)
	case forceMergeV1KnapsackRoundThreeTargets:
		require.GreaterOrEqual(t, view.estimatedInputSize, int64(0))
		require.LessOrEqual(t, view.estimatedInputSize, forceMergeV1KnapsackRoundCapacity(targetSize, 3))
	default:
		require.FailNow(t, "unknown V1 knapsack packing round", "%q", view.packingRound)
	}
}

func TestForceMergeV1KnapsackComparisonUsesExactIntegerDerivedFinalCount(t *testing.T) {
	estimatedInputSize := int64(1<<53) + 1
	require.Equal(t, estimatedInputSize, forceMergeV1KnapsackComparisonOutputCount(estimatedInputSize, 1))

	document, err := json.Marshal(forceMergeKnapsackComparisonOutputPlan{
		EstimatedInputSize: estimatedInputSize,
	})
	require.NoError(t, err)
	require.Contains(t, string(document), `"estimatedInputSize":9007199254740993`)
}

func TestForceMergeV1KnapsackComparisonUsesMultiRoundPacking(t *testing.T) {
	view := forceMergeKnapsackComparisonNewView(forceMergeKnapsackComparisonFixture{
		name:            "three_segments_fill_two_targets",
		sizes:           []float64{70, 70, 70},
		requestedTarget: 100,
		threshold:       3,
	})

	children, reason := forceTriggerAllV1MultiRoundKnapsackComparison(view)
	require.Equal(t, "force merge trigger", reason)
	require.Len(t, children, 1)

	child := forceMergeV1KnapsackComparisonRequireView(t, children[0])
	forceMergeV1KnapsackComparisonRequireRoundContract(t, child)
	require.Equal(t, []int64{1, 2, 3}, forceMergeSegmentIDs(child.segments))
	require.Equal(t, int64(2), child.targetSegmentCount)
	require.Equal(t, forceMergeV1KnapsackRoundTwoTargets, child.packingRound)
}

func TestForceMergeV1KnapsackComparisonUsesResidualSize(t *testing.T) {
	view := forceMergeKnapsackComparisonNewView(forceMergeKnapsackComparisonFixture{
		name:            "delete_adjusted_one_target",
		sizes:           []float64{180, 10},
		requestedTarget: 100,
		threshold:       2,
	})
	view.segments[0].NumOfRows = 100
	view.segments[0].DeltaRowCount = 50
	view.segments[1].NumOfRows = 100

	children, reason := forceTriggerAllV1MultiRoundKnapsackComparison(view)
	require.Equal(t, "force merge trigger", reason)
	require.Len(t, children, 1)

	child := forceMergeV1KnapsackComparisonRequireView(t, children[0])
	forceMergeV1KnapsackComparisonRequireRoundContract(t, child)
	require.Equal(t, []int64{1, 2}, forceMergeSegmentIDs(child.segments))
	require.Equal(t, int64(1), child.targetSegmentCount)
	require.Equal(t, int64(100), child.estimatedInputSize)
	require.Equal(t, forceMergeV1KnapsackRoundOneTarget, child.packingRound)
}

func TestForceMergeV1KnapsackComparisonExtractsOversizedBeforePacking(t *testing.T) {
	view := forceMergeKnapsackComparisonNewView(forceMergeKnapsackComparisonFixture{
		name:            "oversized_before_normal_rounds",
		sizes:           []float64{400, 100},
		requestedTarget: 100,
		threshold:       2,
	})

	children, reason := forceTriggerAllV1MultiRoundKnapsackComparison(view)
	require.Equal(t, "force merge trigger", reason)
	require.Len(t, children, 2)

	first := forceMergeV1KnapsackComparisonRequireView(t, children[0])
	forceMergeV1KnapsackComparisonRequireRoundContract(t, first)
	require.Equal(t, []int64{1}, forceMergeSegmentIDs(first.segments))
	require.Equal(t, int64(4), first.targetSegmentCount)
	require.Equal(t, forceMergeV1KnapsackRoundOversized, first.packingRound)

	second := forceMergeV1KnapsackComparisonRequireView(t, children[1])
	forceMergeV1KnapsackComparisonRequireRoundContract(t, second)
	require.Equal(t, []int64{2}, forceMergeSegmentIDs(second.segments))
	require.Equal(t, forceMergeV1KnapsackRoundOneTarget, second.packingRound)
}

func TestForceMergeV1KnapsackComparisonUsesAbsoluteLossBudget(t *testing.T) {
	view := forceMergeKnapsackComparisonNewView(forceMergeKnapsackComparisonFixture{
		name:            "two_target_loss_exceeds_five_percent_of_one_target",
		sizes:           []float64{80, 60, 60, 50},
		requestedTarget: 100,
		threshold:       4,
	})

	children, reason := forceTriggerAllV1MultiRoundKnapsackComparison(view)
	require.Equal(t, "force merge trigger", reason)
	require.Len(t, children, 1)

	child := forceMergeV1KnapsackComparisonRequireView(t, children[0])
	forceMergeV1KnapsackComparisonRequireRoundContract(t, child)
	require.Equal(t, []int64{1, 2, 3, 4}, forceMergeSegmentIDs(child.segments))
	require.Equal(t, int64(250), child.estimatedInputSize)
	require.Equal(t, forceMergeV1KnapsackRoundThreeTargets, child.packingRound)
}

func TestForceMergeV1KnapsackComparisonAllowsQualifyingSingleton(t *testing.T) {
	view := forceMergeKnapsackComparisonNewView(forceMergeKnapsackComparisonFixture{
		name:            "qualifying_single_input",
		sizes:           []float64{100},
		requestedTarget: 100,
		threshold:       1,
	})

	children, reason := forceTriggerAllV1MultiRoundKnapsackComparison(view)
	require.Equal(t, "force merge trigger", reason)
	require.Len(t, children, 1)

	child := forceMergeV1KnapsackComparisonRequireView(t, children[0])
	forceMergeV1KnapsackComparisonRequireRoundContract(t, child)
	require.Equal(t, []int64{1}, forceMergeSegmentIDs(child.segments))
	require.Equal(t, forceMergeV1KnapsackRoundOneTarget, child.packingRound)
}

func TestForceMergeV1KnapsackComparisonAssignsEveryInputExactlyOnce(t *testing.T) {
	view := forceMergeKnapsackComparisonNewView(forceMergeKnapsackComparisonFixture{
		name:            "all_rounds_exactly_once",
		sizes:           []float64{400, 100, 80, 70, 70, 70, 60, 60, 50},
		requestedTarget: 100,
		threshold:       9,
	})

	children, reason := forceTriggerAllV1MultiRoundKnapsackComparison(view)
	require.Equal(t, "force merge trigger", reason)
	require.NotEmpty(t, children)

	seen := make(map[int64]int, len(view.segments))
	rounds := make(map[forceMergeV1KnapsackRound]bool)
	for _, child := range children {
		prototypeView := forceMergeV1KnapsackComparisonRequireView(t, child)
		forceMergeV1KnapsackComparisonRequireRoundContract(t, prototypeView)
		rounds[prototypeView.packingRound] = true
		for _, segment := range prototypeView.segments {
			seen[segment.ID]++
		}
	}

	require.Equal(t, map[forceMergeV1KnapsackRound]bool{
		forceMergeV1KnapsackRoundOversized:    true,
		forceMergeV1KnapsackRoundOneTarget:    true,
		forceMergeV1KnapsackRoundTwoTargets:   true,
		forceMergeV1KnapsackRoundThreeTargets: true,
	}, rounds)
	require.Len(t, seen, len(view.segments))
	for _, segment := range view.segments {
		require.Equal(t, 1, seen[segment.ID], "segment %d assignment count", segment.ID)
	}
}

func TestForceMergeV1KnapsackComparisonRecoveredScenarios(t *testing.T) {
	fixtures := forceMergeKnapsackComparisonFixtures()
	require.Len(t, fixtures, 22)
	report := forceMergeKnapsackComparisonReport{
		TargetPolicy:   "shared-current-final-target",
		KnapsackSource: "internal/datacoord/knapsack.go V1 greedy primitive with 1T/2T/3T rounds",
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
			v1MultiRoundKnapsack := forceMergeKnapsackComparisonRunPlanner(
				t,
				"v1-multi-round-knapsack",
				forceMergeKnapsackComparisonNewView(fixture),
				forceTriggerAllV1MultiRoundKnapsackComparison,
			)

			currentTargetSize := forceMergeKnapsackComparisonTargetSize(t, current)
			require.Equal(t, currentTargetSize, forceMergeKnapsackComparisonTargetSize(t, v1MultiRoundKnapsack))

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
				V1MultiRoundKnapsack: v1MultiRoundKnapsack,
			}
			report.Cases = append(report.Cases, caseResult)

			t.Logf(
				"%s: current plans=%d finals=%d estimated-peak=%d order=%t; v1-multi-round plans=%d finals=%d estimated-peak=%d order=%t",
				fixture.name,
				current.OutputPlanCount,
				current.DerivedFinalSegmentCount,
				current.LargestEstimatedPlanInput,
				current.PreservesReceivedOrder,
				v1MultiRoundKnapsack.OutputPlanCount,
				v1MultiRoundKnapsack.DerivedFinalSegmentCount,
				v1MultiRoundKnapsack.LargestEstimatedPlanInput,
				v1MultiRoundKnapsack.PreservesReceivedOrder,
			)
		})
	}

	if os.Getenv(forceMergeV1KnapsackReportEnv) != "" {
		document, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)
		t.Logf("force-merge V1 knapsack comparison report:\n%s", document)
	}
}
