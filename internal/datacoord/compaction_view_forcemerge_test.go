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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestForceMergeSegmentView_GetGroupLabel(t *testing.T) {
	label := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch1",
	}

	view := &ForceMergeSegmentView{
		label: label,
	}

	assert.Equal(t, label, view.GetGroupLabel())
}

func TestForceMergeSegmentView_GetSegmentsView(t *testing.T) {
	segments := []*SegmentView{
		{ID: 1, Size: 1024},
		{ID: 2, Size: 2048},
	}

	view := &ForceMergeSegmentView{
		segments: segments,
	}

	assert.Equal(t, segments, view.GetSegmentsView())
	assert.Len(t, view.GetSegmentsView(), 2)
}

func TestForceMergeSegmentView_Append(t *testing.T) {
	view := &ForceMergeSegmentView{
		segments: []*SegmentView{
			{ID: 1, Size: 1024},
		},
	}

	newSegments := []*SegmentView{
		{ID: 2, Size: 2048},
		{ID: 3, Size: 3072},
	}

	view.Append(newSegments...)

	assert.Len(t, view.segments, 3)
	assert.Equal(t, int64(1), view.segments[0].ID)
	assert.Equal(t, int64(2), view.segments[1].ID)
	assert.Equal(t, int64(3), view.segments[2].ID)
}

func TestForceMergeSegmentView_String(t *testing.T) {
	label := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch1",
	}

	view := &ForceMergeSegmentView{
		label: label,
		segments: []*SegmentView{
			{ID: 1},
			{ID: 2},
		},
		triggerID: 12345,
	}

	str := view.String()
	assert.Contains(t, str, "ForceMerge")
	assert.Contains(t, str, "segments=2")
	assert.Contains(t, str, "triggerID=12345")
}

func TestForceMergeSegmentView_Trigger(t *testing.T) {
	view := &ForceMergeSegmentView{
		triggerID: 100,
	}

	assert.Panics(t, func() {
		view.Trigger()
	})
}

func TestForceMergeSegmentView_ForceTrigger(t *testing.T) {
	view := &ForceMergeSegmentView{
		triggerID: 100,
	}

	assert.Panics(t, func() {
		view.ForceTrigger()
	})
}

func TestForceMergeSegmentView_GetTriggerID(t *testing.T) {
	view := &ForceMergeSegmentView{
		triggerID: 12345,
	}

	assert.Equal(t, int64(12345), view.GetTriggerID())
}

func TestForceMergeSegmentView_Complete(t *testing.T) {
	label := &CompactionGroupLabel{
		CollectionID: 100,
		PartitionID:  200,
		Channel:      "test-channel",
	}

	segments := []*SegmentView{
		{ID: 1, Size: 1024 * 1024 * 1024},
		{ID: 2, Size: 512 * 1024 * 1024},
	}

	topology := &CollectionTopology{
		CollectionID:     100,
		NumReplicas:      1,
		IsStandaloneMode: false,
		QueryNodeMemory:  map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		DataNodeMemory:   map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
	}

	view := &ForceMergeSegmentView{
		label:             label,
		segments:          segments,
		triggerID:         99999,
		collectionTTL:     24 * time.Hour,
		targetSegmentSize: 2048 * 1024 * 1024,
		topology:          topology,
	}

	// Test String output
	str := view.String()
	assert.Contains(t, str, "ForceMerge")

	views, r3 := view.ForceTriggerAll()
	assert.Len(t, views, 1)
	assert.NotEmpty(t, r3)
}

func TestForceMergeSegmentView_ForceTriggerAllUsesMultiRoundKnapsack(t *testing.T) {
	t.Run("commits a qualifying 1T singleton", func(t *testing.T) {
		view := newForceMergePlanningView([]int64{1}, []float64{100}, 100)
		targetSize, _ := view.calculateTargetSizeCount()
		groups := groupForceMergeSegments(view.segments, targetSize)

		require.Len(t, groups, 1)
		requireForceMergePackingGroupContract(t, groups[0], targetSize)
		assert.Equal(t, forceMergePackingRoundOneTarget, groups[0].round)
	})

	t.Run("commits three near 0.7T inputs in the 2T round", func(t *testing.T) {
		view := newForceMergePlanningView([]int64{1, 2, 3}, []float64{70, 70, 70}, 100)
		targetSize, _ := view.calculateTargetSizeCount()
		groups := groupForceMergeSegments(view.segments, targetSize)

		require.Len(t, groups, 1)
		requireForceMergePackingGroupContract(t, groups[0], targetSize)
		assert.Equal(t, forceMergePackingRoundTwoTargets, groups[0].round)
		assert.Equal(t, int64(2), plannedForceMergeOutputCount(groups[0].residualSize, targetSize))
	})

	t.Run("uses an absolute 0.05T loss allowance in the 2T round", func(t *testing.T) {
		view := newForceMergePlanningView([]int64{1, 2, 3, 4}, []float64{80, 60, 60, 50}, 100)
		targetSize, _ := view.calculateTargetSizeCount()
		groups := groupForceMergeSegments(view.segments, targetSize)

		require.Len(t, groups, 1)
		requireForceMergePackingGroupContract(t, groups[0], targetSize)
		assert.Equal(t, forceMergePackingRoundThreeTargets, groups[0].round)
		assert.Equal(t, int64(250), groups[0].residualSize)
	})

	t.Run("drains a non-full remainder in the 3T round", func(t *testing.T) {
		view := newForceMergePlanningView([]int64{1}, []float64{40}, 100)
		targetSize, _ := view.calculateTargetSizeCount()
		groups := groupForceMergeSegments(view.segments, targetSize)

		require.Len(t, groups, 1)
		requireForceMergePackingGroupContract(t, groups[0], targetSize)
		assert.Equal(t, forceMergePackingRoundThreeTargets, groups[0].round)
	})
}

func TestForceMergeSegmentView_ForceTriggerAllUsesResidualSize(t *testing.T) {
	view := newForceMergePlanningView([]int64{1, 2}, []float64{160, 10}, 100)
	view.segments[0].DeltaSize = 20
	view.segments[0].NumOfRows = 100
	view.segments[0].DeltaRowCount = 50
	view.segments[1].NumOfRows = 100
	targetSize, _ := view.calculateTargetSizeCount()

	groups := groupForceMergeSegments(view.segments, targetSize)
	require.Len(t, groups, 1)
	requireForceMergePackingGroupContract(t, groups[0], targetSize)
	assert.Equal(t, forceMergePackingRoundOneTarget, groups[0].round)
	assert.Equal(t, int64(100), groups[0].residualSize)

	children := forceMergePlanningChildren(t, view)
	require.Len(t, children, 1)
	assert.Equal(t, int64(1), children[0].GetTargetSegmentCount())
	assertForceMergeChildContract(t, view, children, targetSize)
}

func TestForceMergeSegmentView_ForceTriggerAllExtractsOversizedFirst(t *testing.T) {
	view := newForceMergePlanningView([]int64{1, 2}, []float64{400, 100}, 100)
	targetSize, _ := view.calculateTargetSizeCount()
	groups := groupForceMergeSegments(view.segments, targetSize)

	require.Len(t, groups, 2)
	for _, group := range groups {
		requireForceMergePackingGroupContract(t, group, targetSize)
	}
	assert.Equal(t, forceMergePackingRoundOversized, groups[0].round)
	assert.Equal(t, []int64{1}, forceMergeSegmentIDs(groups[0].segments))
	assert.Equal(t, forceMergePackingRoundOneTarget, groups[1].round)
	assert.Equal(t, []int64{2}, forceMergeSegmentIDs(groups[1].segments))
}

func TestForceMergeSegmentView_ForceTriggerAllAssignsEveryInputExactlyOnce(t *testing.T) {
	view := newForceMergePlanningView(
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		[]float64{400, 100, 80, 70, 70, 70, 60, 60, 50},
		100,
	)
	targetSize, _ := view.calculateTargetSizeCount()
	groups := groupForceMergeSegments(view.segments, targetSize)

	rounds := make(map[forceMergePackingRound]bool)
	seen := make(map[*SegmentView]int, len(view.segments))
	for _, group := range groups {
		requireForceMergePackingGroupContract(t, group, targetSize)
		rounds[group.round] = true
		for _, segment := range group.segments {
			seen[segment]++
		}
	}
	assert.Equal(t, map[forceMergePackingRound]bool{
		forceMergePackingRoundOversized:    true,
		forceMergePackingRoundOneTarget:    true,
		forceMergePackingRoundTwoTargets:   true,
		forceMergePackingRoundThreeTargets: true,
	}, rounds)
	for _, segment := range view.segments {
		assert.Equal(t, 1, seen[segment], "segment %d assignment count", segment.ID)
	}
}

func TestForceMergeSegmentView_ForceTriggerAllMayReorderInputs(t *testing.T) {
	view := newForceMergePlanningView(
		[]int64{4, 3, 2, 1},
		[]float64{30, 30, 30, 30},
		100,
	)
	targetSize, _ := view.calculateTargetSizeCount()
	children := forceMergePlanningChildren(t, view)
	flattened := flattenForceMergeChildIDs(children)

	assert.NotEqual(t, []int64{4, 3, 2, 1}, flattened)
	assert.ElementsMatch(t, []int64{4, 3, 2, 1}, flattened)
	assertForceMergeChildContract(t, view, children, targetSize)
}

func TestGroupForceMergeSegmentsCapsEachPackAt4096Inputs(t *testing.T) {
	segments := make([]*SegmentView, 4097)
	for i := range segments {
		segments[i] = &SegmentView{ID: int64(i + 1), Size: 1, NumOfRows: 1}
	}

	groups := groupForceMergeSegments(segments, 5000)
	require.Len(t, groups, 2)
	for _, group := range groups {
		assert.LessOrEqual(t, len(group.segments), int(forceMergeKnapsackMaxSegments))
		requireForceMergePackingGroupContract(t, group, 5000)
	}
}

func TestForceMergePlanningArithmetic(t *testing.T) {
	largeResidualSize := int64(1<<53) + 1
	assert.Equal(t, largeResidualSize, plannedForceMergeOutputCount(largeResidualSize, 1))
	assert.Equal(t, int64(math.MaxInt64), forceMergeEffectiveSize(float64(math.MaxInt64)))
	assert.Equal(t, int64(math.MaxInt64), forceMergeRoundCapacity(math.MaxInt64, 2))
}

func TestForceMergeSegmentView_ForceTriggerAllRecoveredScenarios(t *testing.T) {
	const gib = float64(1 << 30)
	productionSizes := roundedForceMergeGiBSizes(
		2.40, 2.51, 2.62, 2.73, 2.84, 2.95, 2.36,
		2.47, 2.58, 2.69, 2.80, 2.91, 2.22, 2.33,
		2.44, 2.55, 2.66, 2.77, 2.88, 2.99, 2.91,
	)
	tinyTailSizes := append(repeatForceMergeSizes(40, 10), 5, 12, 18, 20)

	tests := []struct {
		name            string
		sizes           []float64
		ids             []int64
		requestedTarget float64
		threshold       string
		queryNodeCount  int
		expectedTarget  int64
		expectedFinals  int64
	}{
		{name: "01_production_at_threshold", sizes: productionSizes, requestedTarget: 4 * gib, threshold: "100", expectedTarget: 4509715660, expectedFinals: 14},
		{name: "02_production_above_threshold", sizes: productionSizes, requestedTarget: 4 * gib, threshold: "20", expectedTarget: 4509715660, expectedFinals: 14},
		{name: "03_six_equal_at_threshold", sizes: repeatForceMergeSizes(6, 70), requestedTarget: 100, threshold: "6", expectedTarget: 105, expectedFinals: 4},
		{name: "04_six_equal_above_threshold", sizes: repeatForceMergeSizes(6, 70), requestedTarget: 100, threshold: "5", expectedTarget: 105, expectedFinals: 4},
		{name: "05_uniform_1gib_at_threshold", sizes: repeatForceMergeSizes(12, gib), requestedTarget: 3 * gib, threshold: "12", expectedTarget: 3382286745, expectedFinals: 4},
		{name: "06_uniform_1_02gib_at_threshold", sizes: repeatForceMergeSizes(12, 1095216660), requestedTarget: 3 * gib, threshold: "12", expectedTarget: 3382286745, expectedFinals: 4},
		{name: "07_oversized_pair_at_threshold", sizes: []float64{315, 315}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 6},
		{name: "08_uniform_130_at_threshold", sizes: repeatForceMergeSizes(10, 130), requestedTarget: 100, threshold: "10", expectedTarget: 105, expectedFinals: 15},
		{name: "09_mixed_at_threshold", sizes: []float64{37, 162, 23, 276, 31, 249, 162}, requestedTarget: 100, threshold: "7", expectedTarget: 105, expectedFinals: 10},
		{name: "10_mixed_above_threshold", sizes: []float64{37, 162, 23, 276, 31, 249, 162}, requestedTarget: 100, threshold: "6", expectedTarget: 105, expectedFinals: 10},
		{name: "11_three_target_pair_at_threshold", sizes: []float64{150, 150}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 3},
		{name: "12_three_target_pair_plus_one_at_threshold", sizes: []float64{150, 151}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 3},
		{name: "13_equal_total_boundary_at_threshold", sizes: []float64{210, 190}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 4},
		{name: "14_unequal_total_boundary_at_threshold", sizes: []float64{211, 189}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 5},
		{name: "15_near_full_below_5_percent_above_threshold", sizes: []float64{95, 9, 10}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 2},
		{name: "16_near_full_at_5_percent_above_threshold", sizes: []float64{95, 10, 10}, requestedTarget: 100, threshold: "2", expectedTarget: 105, expectedFinals: 2},
		{name: "17_topology_floor_at_threshold", sizes: repeatForceMergeSizes(10, 100), requestedTarget: 1000, threshold: "10", queryNodeCount: 10, expectedTarget: 100, expectedFinals: 10},
		{name: "18_topology_floor_above_threshold", sizes: repeatForceMergeSizes(10, 100), requestedTarget: 1000, threshold: "9", queryNodeCount: 10, expectedTarget: 100, expectedFinals: 10},
		{name: "19_one_hundred_at_threshold", sizes: repeatForceMergeSizes(100, 51), requestedTarget: 100, threshold: "100", expectedTarget: 105, expectedFinals: 50},
		{name: "20_one_hundred_one_above_threshold", sizes: repeatForceMergeSizes(101, 51), requestedTarget: 100, threshold: "100", expectedTarget: 105, expectedFinals: 51},
		{name: "21_tiny_tail_at_threshold", sizes: tinyTailSizes, requestedTarget: 100, threshold: "100", expectedTarget: 105, expectedFinals: 5},
		{name: "22_received_order_at_threshold", sizes: repeatForceMergeSizes(4, 30), ids: []int64{4, 3, 2, 1}, requestedTarget: 100, threshold: "4", expectedTarget: 105, expectedFinals: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setForceMergePlanningThreshold(t, test.threshold)
			ids := append([]int64(nil), test.ids...)
			if len(ids) == 0 {
				ids = make([]int64, len(test.sizes))
				for i := range ids {
					ids[i] = int64(i + 1)
				}
			}
			segments := make([]*SegmentView, len(test.sizes))
			for i, size := range test.sizes {
				segments[i] = &SegmentView{ID: ids[i], Size: size, NumOfRows: 1}
			}

			queryNodeCount := max(test.queryNodeCount, 1)
			queryNodeMemory := make(map[int64]uint64, queryNodeCount)
			for i := 0; i < queryNodeCount; i++ {
				queryNodeMemory[int64(i+1)] = math.MaxUint64
			}
			view := &ForceMergeSegmentView{
				label: &CompactionGroupLabel{
					CollectionID: 1,
					PartitionID:  10,
					Channel:      "force-merge-recovered-scenarios",
				},
				segments:           segments,
				triggerID:          50916,
				configMaxSize:      1,
				expectedTargetSize: test.requestedTarget,
				topology: &CollectionTopology{
					QueryNodeMemory: queryNodeMemory,
					DataNodeMemory:  map[int64]uint64{1: math.MaxUint64},
					NumReplicas:     1,
					NumShards:       1,
				},
			}

			children := forceMergePlanningChildren(t, view)
			assertForceMergeChildContract(t, view, children, test.expectedTarget)
			assert.Equal(t, test.expectedFinals, totalForceMergeChildOutputs(children))
		})
	}
}

func repeatForceMergeSizes(count int, size float64) []float64 {
	result := make([]float64, count)
	for i := range result {
		result[i] = size
	}
	return result
}

func roundedForceMergeGiBSizes(values ...float64) []float64 {
	const gib = float64(1 << 30)
	result := make([]float64, len(values))
	for i, value := range values {
		result[i] = math.Round(value * gib)
	}
	return result
}

func setForceMergePlanningThreshold(t *testing.T, threshold string) {
	t.Helper()
	pt := paramtable.Get()
	require.NoError(t, pt.Save(pt.DataCoordCfg.CompactionMaxFullSegmentThreshold.Key, threshold))
	t.Cleanup(func() {
		pt.Reset(pt.DataCoordCfg.CompactionMaxFullSegmentThreshold.Key)
	})
}

func newForceMergePlanningView(ids []int64, sizes []float64, targetSize int64) *ForceMergeSegmentView {
	segments := make([]*SegmentView, 0, len(sizes))
	for i, size := range sizes {
		segments = append(segments, &SegmentView{ID: ids[i], Size: size, NumOfRows: 1})
	}
	return &ForceMergeSegmentView{
		label: &CompactionGroupLabel{
			CollectionID: 1,
			PartitionID:  10,
			Channel:      "force-merge-planning-test",
		},
		segments:           segments,
		triggerID:          100,
		configMaxSize:      1,
		expectedTargetSize: float64(targetSize),
		topology: &CollectionTopology{
			QueryNodeMemory: map[int64]uint64{1: math.MaxUint64},
			DataNodeMemory:  map[int64]uint64{1: math.MaxUint64},
			NumReplicas:     1,
			NumShards:       1,
		},
	}
}

func forceMergePlanningChildren(t *testing.T, view *ForceMergeSegmentView) []*ForceMergeSegmentView {
	t.Helper()
	children, reason := view.ForceTriggerAll()
	require.Equal(t, "force merge trigger", reason)
	result := make([]*ForceMergeSegmentView, 0, len(children))
	for _, child := range children {
		forceMergeChild, ok := child.(*ForceMergeSegmentView)
		require.True(t, ok)
		result = append(result, forceMergeChild)
	}
	return result
}

func assertForceMergeChildContract(
	t *testing.T,
	view *ForceMergeSegmentView,
	children []*ForceMergeSegmentView,
	targetSize int64,
) {
	t.Helper()
	seen := make(map[*SegmentView]int, len(view.GetSegmentsView()))
	inputCeiling := forceMergeRoundCapacity(targetSize, 3)
	for _, child := range children {
		require.NotEmpty(t, child.GetSegmentsView())
		childTargetSize := int64(child.GetTargetSegmentSize())
		assert.Equal(t, float64(childTargetSize), child.GetTargetSegmentSize())
		assert.Equal(t, targetSize, childTargetSize)
		residualSize := forceMergeResidualSize(child.GetSegmentsView())
		if residualSize > inputCeiling {
			assert.Len(t, child.GetSegmentsView(), 1)
		} else {
			assert.LessOrEqual(t, residualSize, inputCeiling)
		}
		plannedOutputCount := expectedForceMergeOutputCount(residualSize, targetSize)
		assert.Equal(t, plannedOutputCount, child.GetTargetSegmentCount())
		for _, segment := range child.GetSegmentsView() {
			seen[segment]++
		}
	}
	require.Len(t, seen, len(view.GetSegmentsView()))
	for _, segment := range view.GetSegmentsView() {
		assert.Equal(t, 1, seen[segment], "segment %d assignment count", segment.ID)
	}
}

func requireForceMergePackingGroupContract(t *testing.T, group forceMergePackingGroup, targetSize int64) {
	t.Helper()
	require.NotEmpty(t, group.segments)
	require.Equal(t, forceMergeResidualSize(group.segments), group.residualSize)

	switch group.round {
	case forceMergePackingRoundOversized:
		require.Len(t, group.segments, 1)
		require.Greater(t, group.residualSize, forceMergeRoundCapacity(targetSize, 3))
	case forceMergePackingRoundOneTarget:
		require.LessOrEqual(t, group.residualSize, targetSize)
		require.LessOrEqual(t, targetSize-group.residualSize, targetSize/forceMergeKnapsackLossDivisor)
	case forceMergePackingRoundTwoTargets:
		capacity := forceMergeRoundCapacity(targetSize, 2)
		require.LessOrEqual(t, group.residualSize, capacity)
		require.LessOrEqual(t, capacity-group.residualSize, targetSize/forceMergeKnapsackLossDivisor)
	case forceMergePackingRoundThreeTargets:
		require.LessOrEqual(t, group.residualSize, forceMergeRoundCapacity(targetSize, 3))
	default:
		require.FailNow(t, "unknown force-merge packing round", "%q", group.round)
	}
}

func forceMergeSegmentIDs(segments []*SegmentView) []int64 {
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.ID)
	}
	return ids
}

func flattenForceMergeChildIDs(children []*ForceMergeSegmentView) []int64 {
	ids := make([]int64, 0)
	for _, child := range children {
		ids = append(ids, forceMergeSegmentIDs(child.GetSegmentsView())...)
	}
	return ids
}

func totalForceMergeChildOutputs(children []*ForceMergeSegmentView) int64 {
	total := int64(0)
	for _, child := range children {
		total += child.GetTargetSegmentCount()
	}
	return total
}

func forceMergeResidualSize(segments []*SegmentView) int64 {
	var total int64
	for _, segment := range segments {
		total += forceMergeKnapsackCandidate(segment).GetResidualSegmentSize()
	}
	return total
}

func expectedForceMergeOutputCount(residualSize, targetSize int64) int64 {
	if residualSize <= 0 || targetSize <= 0 {
		return 1
	}
	count := residualSize / targetSize
	if residualSize%targetSize != 0 {
		count++
	}
	return max(count, 1)
}

func TestSumSegmentSize(t *testing.T) {
	segments := []*SegmentView{
		{ID: 1, Size: 1024 * 1024 * 1024},
		{ID: 2, Size: 512 * 1024 * 1024},
	}

	totalSize := sumSegmentSize(segments)
	expected := 1.5 * 1024 * 1024 * 1024
	assert.InDelta(t, expected, totalSize, 1)
}

func TestGroupByPartitionChannel(t *testing.T) {
	label1 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch1",
	}
	label2 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  20,
		Channel:      "ch1",
	}

	segments := []*SegmentView{
		{ID: 1, label: label1},
		{ID: 2, label: label1},
		{ID: 3, label: label2},
	}

	groups := groupByPartitionChannel(segments)
	assert.Equal(t, 2, len(groups))

	var count1, count2 int
	for _, segs := range groups {
		if len(segs) == 2 {
			count1++
		} else if len(segs) == 1 {
			count2++
		}
	}
	assert.Equal(t, 1, count1)
	assert.Equal(t, 1, count2)
}

func TestGroupByPartitionChannel_EmptySegments(t *testing.T) {
	groups := groupByPartitionChannel([]*SegmentView{})
	assert.Empty(t, groups)
}

func TestGroupByPartitionChannel_SameLabel(t *testing.T) {
	label := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch1",
	}

	segments := []*SegmentView{
		{ID: 1, label: label},
		{ID: 2, label: label},
		{ID: 3, label: label},
	}

	groups := groupByPartitionChannel(segments)
	assert.Equal(t, 1, len(groups))
	for _, segs := range groups {
		assert.Equal(t, 3, len(segs))
	}
}

func TestCalculateTargetSizeCount_AppliesToleranceBeforeTopology(t *testing.T) {
	t.Run("applies tolerance within machine-safe cap", func(t *testing.T) {
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments:           []*SegmentView{{ID: 1, Size: 100}},
			triggerID:          1,
			configMaxSize:      1000,
			expectedTargetSize: 100,
			topology:           &CollectionTopology{},
		}

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(105), targetSize)
		assert.Equal(t, int64(1), targetCount)
	})

	t.Run("caps tolerance at machine-safe maximum", func(t *testing.T) {
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments:           []*SegmentView{{ID: 1, Size: 100}},
			triggerID:          1,
			configMaxSize:      102,
			expectedTargetSize: 100,
			topology:           &CollectionTopology{},
		}

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(102), targetSize)
		assert.Equal(t, int64(1), targetCount)
	})

	t.Run("applies topology adjustment last with floor division", func(t *testing.T) {
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments:           []*SegmentView{{ID: 1, Size: 1000}},
			triggerID:          1,
			configMaxSize:      100,
			expectedTargetSize: 1000,
			topology: &CollectionTopology{
				QueryNodeMemory: map[int64]uint64{1: 1 << 40, 2: 1 << 40, 3: 1 << 40},
				DataNodeMemory:  map[int64]uint64{1: 1 << 40},
				NumReplicas:     1,
				NumShards:       1,
			},
		}

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(333), targetSize)
		assert.Equal(t, int64(4), targetCount)
	})
}

func TestCalculateTargetSizeCount_QueryNodeParallelism(t *testing.T) {
	t.Run("fractional target count rounds up", func(t *testing.T) {
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 150 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      &CollectionTopology{},
		}

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(2), targetCount)
		assert.Equal(t, int64(100*1024*1024), targetSize)
	})

	t.Run("single QueryNode - no adjustment", func(t *testing.T) {
		topology := &CollectionTopology{
			QueryNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
			DataNodeMemory:  map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(1), targetCount)
		assert.Greater(t, targetSize, int64(0))
	})

	t.Run("two QueryNodes - adjust to 2 segments", func(t *testing.T) {
		topology := &CollectionTopology{
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "Should produce 2 segments for 2 QueryNodes")
		assert.InDelta(t, 1*1024*1024*1024, targetSize, 1024*1024)
	})

	t.Run("three QueryNodes - adjust to 3 segments", func(t *testing.T) {
		topology := &CollectionTopology{
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
				{ID: 3, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(3), targetCount, "Should produce 3 segments for 3 QueryNodes")
		assert.InDelta(t, 1*1024*1024*1024, targetSize, 1024*1024)
	})

	t.Run("two QueryNodes but segments too small - no adjustment", func(t *testing.T) {
		topology := &CollectionTopology{
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 50 * 1024 * 1024},
				{ID: 2, Size: 50 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		_, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(1), targetCount, "Should not split when resulting segments would be below configMaxSize")
	})

	t.Run("already exceeds QueryNode count - no adjustment", func(t *testing.T) {
		topology := &CollectionTopology{
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 500 * 1024 * 1024},
				{ID: 2, Size: 500 * 1024 * 1024},
				{ID: 3, Size: 500 * 1024 * 1024},
				{ID: 4, Size: 500 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		_, targetCount := view.calculateTargetSizeCount()
		assert.GreaterOrEqual(t, targetCount, int64(2), "Should not adjust when already >= QueryNode count")
	})

	t.Run("4 QueryNodes with 2 replicas - adjust to 2 segments", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 2,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
				4: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "4 QNs / 2 replicas = 2 segments for parallelism")
		assert.InDelta(t, 1*1024*1024*1024, targetSize, 1024*1024)
	})

	t.Run("6 QueryNodes with 3 replicas - adjust to 2 segments", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 3,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
				4: 8 * 1024 * 1024 * 1024,
				5: 8 * 1024 * 1024 * 1024,
				6: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "6 QNs / 3 replicas = 2 segments for parallelism")
		assert.InDelta(t, 1*1024*1024*1024, targetSize, 1024*1024)
	})

	t.Run("3 QueryNodes with 2 replicas - perShardParallelism rounds to 1", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 2,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		_, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(1), targetCount, "3 QNs / 2 replicas = 1 (rounded down), no adjustment")
	})

	t.Run("8 QueryNodes, 2 replicas, 2 shards - 2 segments per shard", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 2,
			NumShards:   2,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
				4: 8 * 1024 * 1024 * 1024,
				5: 8 * 1024 * 1024 * 1024,
				6: 8 * 1024 * 1024 * 1024,
				7: 8 * 1024 * 1024 * 1024,
				8: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "8 QNs / (2 replicas * 2 shards) = 2 segments per shard")
		assert.InDelta(t, 1*1024*1024*1024, targetSize, 1024*1024)
	})

	t.Run("4 QueryNodes, 1 replica, 4 shards - 1 segment per shard", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 1,
			NumShards:   4,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
				4: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		_, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(1), targetCount, "4 QNs / (1 replica * 4 shards) = 1 segment per shard (each shard has 1 QN)")
	})

	t.Run("12 QueryNodes, 2 replicas, 3 shards - 2 segments per shard", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 2,
			NumShards:   3,
			QueryNodeMemory: map[int64]uint64{
				1:  8 * 1024 * 1024 * 1024,
				2:  8 * 1024 * 1024 * 1024,
				3:  8 * 1024 * 1024 * 1024,
				4:  8 * 1024 * 1024 * 1024,
				5:  8 * 1024 * 1024 * 1024,
				6:  8 * 1024 * 1024 * 1024,
				7:  8 * 1024 * 1024 * 1024,
				8:  8 * 1024 * 1024 * 1024,
				9:  8 * 1024 * 1024 * 1024,
				10: 8 * 1024 * 1024 * 1024,
				11: 8 * 1024 * 1024 * 1024,
				12: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 1 * 1024 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}
		targetSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "12 QNs / (2 replicas * 3 shards) = 2 segments per shard")
		assert.InDelta(t, 1*1024*1024*1024, targetSize, 1024*1024)
	})

	t.Run("adjusts target count and max safe size when perShardParallelism conditions met", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 1,
			NumShards:   1,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 400 * 1024 * 1024},
				{ID: 2, Size: 500 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(3), targetCount, "targetCount should be adjusted to perShardParallelism (3)")
		expectedTargetSize := (400.0 + 500.0) * 1024 * 1024 / 3.0
		assert.InDelta(t, expectedTargetSize, targetSize, 1024*1024, "targetSize should be totalSize / targetCount")
	})

	t.Run("does not adjust when totalSize/desiredCount < configMaxSize", func(t *testing.T) {
		topology := &CollectionTopology{
			NumReplicas: 1,
			NumShards:   1,
			QueryNodeMemory: map[int64]uint64{
				1: 8 * 1024 * 1024 * 1024,
				2: 8 * 1024 * 1024 * 1024,
				3: 8 * 1024 * 1024 * 1024,
			},
			DataNodeMemory: map[int64]uint64{1: 8 * 1024 * 1024 * 1024},
		}
		view := &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 100 * 1024 * 1024},
				{ID: 2, Size: 150 * 1024 * 1024},
			},
			triggerID:     1,
			configMaxSize: 100 * 1024 * 1024,
			topology:      topology,
		}

		_, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(1), targetCount, "targetCount should not be adjusted when totalSize/desiredCount < configMaxSize")
	})
}

func TestCalculateTargetSizeCount_UserTargetAndMemoryClamp(t *testing.T) {
	Params.Save(Params.DataCoordCfg.CompactionForceMergeQueryNodeMemoryFactor.Key, "4")
	Params.Save(Params.DataCoordCfg.CompactionForceMergeDataNodeMemoryFactor.Key, "4")
	t.Cleanup(func() {
		Params.Reset(Params.DataCoordCfg.CompactionForceMergeQueryNodeMemoryFactor.Key)
		Params.Reset(Params.DataCoordCfg.CompactionForceMergeDataNodeMemoryFactor.Key)
	})

	const (
		mb = float64(1024 * 1024)
		gb = float64(1024 * 1024 * 1024)
	)
	newView := func(expectedTargetSize float64) *ForceMergeSegmentView {
		return &ForceMergeSegmentView{
			label: &CompactionGroupLabel{
				CollectionID: 1,
				PartitionID:  1,
				Channel:      "ch1",
			},
			segments: []*SegmentView{
				{ID: 1, Size: 2.5 * gb},
				{ID: 2, Size: 2.5 * gb},
			},
			triggerID:          1,
			configMaxSize:      64 * mb,
			expectedTargetSize: expectedTargetSize,
			topology: &CollectionTopology{
				NumReplicas: 1,
				NumShards:   1,
				QueryNodeMemory: map[int64]uint64{
					1: 8 * 1024 * 1024 * 1024,
					2: 16 * 1024 * 1024 * 1024,
				},
				DataNodeMemory: map[int64]uint64{
					1: 12 * 1024 * 1024 * 1024,
					2: 20 * 1024 * 1024 * 1024,
				},
			},
		}
	}

	t.Run("user target below safe size gets operating allowance", func(t *testing.T) {
		view := newView(1 * gb)

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(1127428915), targetSize)
		assert.Equal(t, int64(5), targetCount)
	})

	t.Run("user target above smallest node limit is clamped", func(t *testing.T) {
		view := newView(4 * gb)

		targetSize, targetCount := view.calculateTargetSizeCount()

		// The smallest QueryNode is the limiting resource: 8 GiB / factor 4 = 2 GiB.
		assert.Equal(t, int64(2*gb), targetSize)
		assert.Equal(t, int64(3), targetCount)
	})

	t.Run("standalone co-location halves the shared memory limit", func(t *testing.T) {
		view := newView(4 * gb)
		view.topology.IsStandaloneMode = true
		view.topology.QueryNodeMemory = map[int64]uint64{1: 8 * 1024 * 1024 * 1024}

		targetSize, targetCount := view.calculateTargetSizeCount()

		assert.Equal(t, int64(1*gb), targetSize)
		assert.Equal(t, int64(5), targetCount)
	})
}
