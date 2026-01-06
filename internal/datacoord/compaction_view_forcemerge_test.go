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
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
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

func TestGroupingAlgorithmsComparison(t *testing.T) {
	type testCase struct {
		name       string
		segments   []float64
		targetSize float64
	}

	testCases := []testCase{
		{
			name:       "perfect fit - 5x2GB to 5GB",
			segments:   []float64{2, 2, 2, 2, 2},
			targetSize: 5,
		},
		{
			name:       "varying sizes - example from discussion",
			segments:   []float64{1.2, 1.3, 1.4, 1.8, 1.8, 1.8, 1.8, 1.8},
			targetSize: 3,
		},
		{
			name:       "small segments",
			segments:   []float64{0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5},
			targetSize: 2,
		},
		{
			name:       "large segments",
			segments:   []float64{3, 3, 3, 3},
			targetSize: 5,
		},
		{
			name:       "mixed sizes",
			segments:   []float64{0.5, 1, 1.5, 2, 2.5, 3},
			targetSize: 4,
		},
		{
			name:       "many small segments",
			segments:   []float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			targetSize: 3,
		},
		{
			name:       "uneven distribution",
			segments:   []float64{0.3, 0.4, 2.5, 0.3, 2.8, 0.5, 2.2},
			targetSize: 3,
		},
		{
			name:       "single segment",
			segments:   []float64{5},
			targetSize: 3,
		},
		{
			name:       "two segments perfect",
			segments:   []float64{2.5, 2.5},
			targetSize: 5,
		},
		{
			name:       "fibonacci-like sizes",
			segments:   []float64{1, 1, 2, 3, 5, 8},
			targetSize: 10,
		},
		{
			name:       "near-perfect split - tests greedy vs optimal",
			segments:   []float64{1.5, 1.5, 1.5, 1.5, 1.5, 1.5},
			targetSize: 3,
		},
		{
			name:       "strategic grouping - [2.8,0.3] vs [2.8,0.2,0.1]",
			segments:   []float64{2.8, 0.2, 0.1, 2.8, 0.3},
			targetSize: 3,
		},
		{
			name:       "tail optimization - many small + one large",
			segments:   []float64{0.5, 0.5, 0.5, 0.5, 0.5, 2.5},
			targetSize: 3,
		},
		{
			name:       "alternating sizes for different strategies",
			segments:   []float64{1.0, 2.5, 1.0, 2.5, 1.0, 2.5},
			targetSize: 4,
		},
		{
			name:       "edge case - slightly over target creates decision point",
			segments:   []float64{2.1, 2.1, 2.1, 2.1, 2.1},
			targetSize: 4,
		},
		{
			name:       "optimal vs greedy - can fit 3 full or 2 full + small tail",
			segments:   []float64{1.8, 1.8, 1.8, 1.8, 1.8, 1.5},
			targetSize: 3,
		},
		{
			name:       "many segments with complex optimal solution",
			segments:   []float64{0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8},
			targetSize: 2,
		},
		{
			name:       "greedy stops early, optimal continues",
			segments:   []float64{2.8, 0.2, 0.1, 2.8, 0.3},
			targetSize: 3.0,
		},
		{
			name:       "Balanced vs Larger - distribution vs grouping",
			segments:   []float64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
			targetSize: 2.5,
		},
		{
			name: "MaxFull achieves theoretical maximum when possible",
			// Perfect case: 6x1.5GB segments, target=3GB
			// Total=9GB, theoretical max = 3 full segments
			segments:   []float64{1.5, 1.5, 1.5, 1.5, 1.5, 1.5},
			targetSize: 3.0,
		},
		{
			name:       "Larger creates fewer compaction tasks",
			segments:   lo.Times(20, func(i int) float64 { return 0.5 }),
			targetSize: 2.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Convert to SegmentView
			segments := make([]*SegmentView, len(tc.segments))
			for i, size := range tc.segments {
				segments[i] = &SegmentView{
					ID:   int64(i + 1),
					Size: size * 1024 * 1024 * 1024,
				}
			}
			targetSize := tc.targetSize * 1024 * 1024 * 1024
			totalSize := sumSegmentSize(segments)
			theoreticalMaxFull := int(totalSize / targetSize)

			// Test all three algorithms
			groupsBalanced := adaptiveGroupSegments(segments, targetSize)
			groupsLarger := largerGroupingSegments(segments, targetSize)
			groupsMax := maxFullSegmentsGrouping(segments, targetSize)

			// Helper to count full segments and tails
			countMetrics := func(groups [][]*SegmentView) (numGroups, numFull, numTails int) {
				numGroups = len(groups)
				for _, group := range groups {
					groupSize := sumSegmentSize(group)
					full := int(groupSize / targetSize)
					remainder := groupSize - float64(full)*targetSize
					numFull += full
					if remainder > 0.01 {
						numTails++
					}
				}
				return
			}

			// Helper to verify all segments used exactly once
			verifyAllSegmentsUsed := func(groups [][]*SegmentView) bool {
				seen := make(map[int64]int)
				for _, group := range groups {
					for _, seg := range group {
						seen[seg.ID]++
					}
				}
				if len(seen) != len(segments) {
					return false
				}
				for _, count := range seen {
					if count != 1 {
						return false
					}
				}
				return true
			}

			// Verify all algorithms use each segment exactly once
			assert.True(t, verifyAllSegmentsUsed(groupsBalanced), "adaptiveGroupSegments: all segments must be used exactly once")
			assert.True(t, verifyAllSegmentsUsed(groupsLarger), "largerGroupingSegments: all segments must be used exactly once")
			assert.True(t, verifyAllSegmentsUsed(groupsMax), "maxFullSegmentsGrouping: all segments must be used exactly once")

			// Get metrics
			adaptiveGroups, adaptiveFull, adaptiveTails := countMetrics(groupsBalanced)
			largerGroups, largerFull, largerTails := countMetrics(groupsLarger)
			maxGroups, maxFull, maxTails := countMetrics(groupsMax)

			t.Logf("Total size: %.1f GB, Target: %.1f GB, Theoretical max full: %d",
				totalSize/(1024*1024*1024), targetSize/(1024*1024*1024), theoreticalMaxFull)
			t.Logf("Adaptive: %d groups, %d full, %d tails", adaptiveGroups, adaptiveFull, adaptiveTails)
			t.Logf("Larger:   %d groups, %d full, %d tails", largerGroups, largerFull, largerTails)
			t.Logf("MaxFull:  %d groups, %d full, %d tails", maxGroups, maxFull, maxTails)

			// Assertions
			// 1. maxFullSegmentsGrouping should produce most full segments
			assert.GreaterOrEqual(t, maxFull, largerFull, "maxFullSegmentsGrouping should produce >= full segments than largerGroupingSegments")

			// 2. maxFullSegmentsGrouping should not exceed theoretical maximum
			assert.LessOrEqual(t, maxFull, theoreticalMaxFull, "cannot exceed theoretical maximum")

			// 3. All algorithms should process all segments
			for _, groups := range [][][]*SegmentView{groupsBalanced, groupsLarger, groupsMax} {
				totalProcessed := 0
				for _, group := range groups {
					totalProcessed += len(group)
				}
				assert.Equal(t, len(segments), totalProcessed)
			}
		})
	}
}

func TestAdaptiveGroupSegments(t *testing.T) {
	t.Run("empty segments", func(t *testing.T) {
		groups := adaptiveGroupSegments(nil, 5*1024*1024*1024)
		assert.Nil(t, groups)
	})

	t.Run("uses maxFull for small segment count", func(t *testing.T) {
		segments := []*SegmentView{
			{ID: 1, Size: 1.5 * 1024 * 1024 * 1024},
			{ID: 2, Size: 1.5 * 1024 * 1024 * 1024},
			{ID: 3, Size: 1.5 * 1024 * 1024 * 1024},
			{ID: 4, Size: 1.5 * 1024 * 1024 * 1024},
		}
		groups := adaptiveGroupSegments(segments, 3*1024*1024*1024)
		// Should produce 2 groups with 2 full segments
		assert.Equal(t, 2, len(groups))
	})

	t.Run("uses larger for large segment count", func(t *testing.T) {
		// Create 200 segments (> defaultMaxFullSegmentThreshold)
		segments := make([]*SegmentView, 200)
		for i := 0; i < 200; i++ {
			segments[i] = &SegmentView{
				ID:   int64(i),
				Size: 1 * 1024 * 1024 * 1024,
			}
		}
		groups := adaptiveGroupSegments(segments, 3*1024*1024*1024)
		// Should use larger algorithm
		assert.NotNil(t, groups)
		assert.Greater(t, len(groups), 0)
	})
}

func TestLargerGroupingSegments(t *testing.T) {
	t.Run("empty segments", func(t *testing.T) {
		groups := largerGroupingSegments(nil, 5*1024*1024*1024)
		assert.Nil(t, groups)
	})

	t.Run("single segment", func(t *testing.T) {
		segments := []*SegmentView{
			{ID: 1, Size: 3 * 1024 * 1024 * 1024},
		}
		groups := largerGroupingSegments(segments, 5*1024*1024*1024)
		assert.Equal(t, 1, len(groups))
		assert.Equal(t, 1, len(groups[0]))
	})
}

func TestMaxFullSegmentsGrouping(t *testing.T) {
	t.Run("empty segments", func(t *testing.T) {
		groups := maxFullSegmentsGrouping(nil, 5*1024*1024*1024)
		assert.Nil(t, groups)
	})

	t.Run("single segment", func(t *testing.T) {
		segments := []*SegmentView{
			{ID: 1, Size: 3 * 1024 * 1024 * 1024},
		}
		groups := maxFullSegmentsGrouping(segments, 5*1024*1024*1024)
		assert.Equal(t, 1, len(groups))
		assert.Equal(t, 1, len(groups[0]))
	})

	t.Run("perfect fit achieves theoretical maximum", func(t *testing.T) {
		segments := []*SegmentView{
			{ID: 1, Size: 2.5 * 1024 * 1024 * 1024},
			{ID: 2, Size: 2.5 * 1024 * 1024 * 1024},
			{ID: 3, Size: 2.5 * 1024 * 1024 * 1024},
			{ID: 4, Size: 2.5 * 1024 * 1024 * 1024},
		}
		targetSize := 5.0 * 1024 * 1024 * 1024

		groups := maxFullSegmentsGrouping(segments, targetSize)

		totalFull := 0
		for _, group := range groups {
			groupSize := sumSegmentSize(group)
			totalFull += int(groupSize / targetSize)
		}

		// Total is 10GB, should produce exactly 2 full 5GB segments
		assert.Equal(t, 2, totalFull)
	})
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

// Benchmark tests
func BenchmarkLargerGroupingSegments(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			segments := make([]*SegmentView, n)
			for i := 0; i < n; i++ {
				segments[i] = &SegmentView{
					ID:   int64(i),
					Size: float64((i%10+1)*100*1024*1024 + i*1024*1024),
				}
			}
			targetSize := float64(3 * 1024 * 1024 * 1024)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				largerGroupingSegments(segments, targetSize)
			}
		})
	}
}

func BenchmarkMaxFullSegmentsGrouping(b *testing.B) {
	sizes := []int{10, 50, 100}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			segments := make([]*SegmentView, n)
			for i := 0; i < n; i++ {
				segments[i] = &SegmentView{
					ID:   int64(i),
					Size: float64((i%10+1)*100*1024*1024 + i*1024*1024),
				}
			}
			targetSize := float64(3 * 1024 * 1024 * 1024)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				maxFullSegmentsGrouping(segments, targetSize)
			}
		})
	}
}

func BenchmarkGroupingAlgorithmsComparison(b *testing.B) {
	sizes := []int{10, 50, 100, 200, 500}
	targetSize := float64(3 * 1024 * 1024 * 1024)

	for _, n := range sizes {
		segments := make([]*SegmentView, n)
		for i := 0; i < n; i++ {
			segments[i] = &SegmentView{
				ID:   int64(i),
				Size: float64((i%10+1)*100*1024*1024 + i*1024*1024),
			}
		}

		b.Run(fmt.Sprintf("adaptive/n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				adaptiveGroupSegments(segments, targetSize)
			}
		})

		b.Run(fmt.Sprintf("larger/n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				largerGroupingSegments(segments, targetSize)
			}
		})

		// Only test maxFull with smaller sizes due to O(n³) complexity
		if n <= 200 {
			b.Run(fmt.Sprintf("maxFull/n=%d", n), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					maxFullSegmentsGrouping(segments, targetSize)
				}
			})
		}
	}
}

func BenchmarkGroupByPartitionChannel(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			segments := make([]*SegmentView, n)
			for i := 0; i < n; i++ {
				label := &CompactionGroupLabel{
					CollectionID: 1,
					PartitionID:  int64(i % 5),
					Channel:      fmt.Sprintf("ch%d", i%3),
				}
				segments[i] = &SegmentView{
					ID:    int64(i),
					label: label,
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = groupByPartitionChannel(segments)
			}
		})
	}
}

func TestCalculateTargetSizeCount_QueryNodeParallelism(t *testing.T) {
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(1), targetCount)
		assert.Greater(t, maxSafeSize, 0.0)
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "Should produce 2 segments for 2 QueryNodes")
		assert.InDelta(t, 1*1024*1024*1024, maxSafeSize, 1024*1024)
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(3), targetCount, "Should produce 3 segments for 3 QueryNodes")
		assert.InDelta(t, 1*1024*1024*1024, maxSafeSize, 1024*1024)
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

	t.Run("user-provided targetSize respected with QueryNode adjustment", func(t *testing.T) {
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
			triggerID:          1,
			configMaxSize:      100 * 1024 * 1024,
			expectedTargetSize: 500 * 1024 * 1024,
			topology:           topology,
		}
		_, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "Should still adjust for QueryNode parallelism even with user targetSize")
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "4 QNs / 2 replicas = 2 segments for parallelism")
		assert.InDelta(t, 1*1024*1024*1024, maxSafeSize, 1024*1024)
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "6 QNs / 3 replicas = 2 segments for parallelism")
		assert.InDelta(t, 1*1024*1024*1024, maxSafeSize, 1024*1024)
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "8 QNs / (2 replicas * 2 shards) = 2 segments per shard")
		assert.InDelta(t, 1*1024*1024*1024, maxSafeSize, 1024*1024)
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
		maxSafeSize, targetCount := view.calculateTargetSizeCount()
		assert.Equal(t, int64(2), targetCount, "12 QNs / (2 replicas * 3 shards) = 2 segments per shard")
		assert.InDelta(t, 1*1024*1024*1024, maxSafeSize, 1024*1024)
	})
}
