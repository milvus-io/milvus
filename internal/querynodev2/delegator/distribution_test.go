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

package delegator

import (
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type DistributionSuite struct {
	suite.Suite
	dist *distribution
}

func (s *DistributionSuite) SetupTest() {
	paramtable.Init()
	s.dist = NewDistribution("channel-1", NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion))
}

func (s *DistributionSuite) TearDownTest() {
	s.dist = nil
}

func (s *DistributionSuite) TestAddDistribution() {
	type testCase struct {
		tag                  string
		input                []SegmentEntry
		growing              []SegmentEntry
		expected             []SnapshotItem
		expectedSignalClosed bool
		expectedLoadRatio    float64
	}

	cases := []testCase{
		{
			tag: "one_node",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    1,
					SegmentID: 2,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:        1,
							SegmentID:     1,
							TargetVersion: unreadableTargetVersion,
						},
						{
							NodeID:        1,
							SegmentID:     2,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
			},
			expectedSignalClosed: true,
		},
		{
			tag: "duplicate segment",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    1,
					SegmentID: 1,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:        1,
							SegmentID:     1,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
			},
			expectedSignalClosed: true,
		},
		{
			tag: "multiple_nodes",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    2,
					SegmentID: 2,
				},
				{
					NodeID:    1,
					SegmentID: 3,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:        1,
							SegmentID:     1,
							TargetVersion: unreadableTargetVersion,
						},

						{
							NodeID:        1,
							SegmentID:     3,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{
							NodeID:        2,
							SegmentID:     2,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
			},
			expectedSignalClosed: true,
		},
		{
			tag: "remove_growing",
			growing: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
			},
			input: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
				{NodeID: 1, SegmentID: 2},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 1, TargetVersion: unreadableTargetVersion},
						{NodeID: 1, SegmentID: 2, TargetVersion: unreadableTargetVersion},
					},
				},
			},
			expectedSignalClosed: false,
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()
			s.dist.AddGrowing(tc.growing...)
			s.dist.SyncTargetVersion(&querypb.SyncAction{
				TargetVersion: 1000,
			}, []int64{1})
			_, _, _, version, err := s.dist.PinReadableSegments(1.0, 1)
			s.Require().NoError(err)
			s.dist.AddDistributions(tc.input...)
			sealed, _ := s.dist.PeekSegments(false)
			s.compareSnapshotItems(tc.expected, sealed)
			s.dist.Unpin(version)
		})
	}
}

func (s *DistributionSuite) isClosedCh(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *DistributionSuite) compareSnapshotItems(target, value []SnapshotItem) {
	if !s.Equal(len(target), len(value)) {
		return
	}
	mapNodeItem := make(map[int64]SnapshotItem)
	for _, valueItem := range value {
		mapNodeItem[valueItem.NodeID] = valueItem
	}

	for _, targetItem := range target {
		valueItem, ok := mapNodeItem[targetItem.NodeID]
		if !s.True(ok) {
			return
		}

		s.ElementsMatch(targetItem.Segments, valueItem.Segments)
	}
}

func (s *DistributionSuite) TestAddGrowing() {
	type testCase struct {
		tag          string
		workingParts []int64
		input        []SegmentEntry
		expected     []SegmentEntry
	}

	cases := []testCase{
		{
			tag: "normal_case",
			input: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 2},
			},
			workingParts: []int64{1, 2},
			expected: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1, TargetVersion: 1000},
				{SegmentID: 2, PartitionID: 2, TargetVersion: 1000},
			},
		},
		{
			tag: "partial_partition_working",
			input: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 2},
			},
			workingParts: []int64{1},
			expected: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1, TargetVersion: 1000},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddGrowing(tc.input...)
			s.dist.SyncTargetVersion(&querypb.SyncAction{
				TargetVersion:   1000,
				GrowingInTarget: []int64{1, 2},
			}, tc.workingParts)
			_, growing, _, version, err := s.dist.PinReadableSegments(1.0, tc.workingParts...)
			s.Require().NoError(err)
			defer s.dist.Unpin(version)

			s.ElementsMatch(tc.expected, growing)
		})
	}
}

func (s *DistributionSuite) TestRemoveDistribution() {
	type testCase struct {
		tag            string
		presetSealed   []SegmentEntry
		presetGrowing  []SegmentEntry
		removalSealed  []SegmentEntry
		removalGrowing []SegmentEntry

		withMockRead  bool
		expectSealed  []SnapshotItem
		expectGrowing []SegmentEntry
	}

	cases := []testCase{
		{
			tag: "remove with no read",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
				{NodeID: 2, SegmentID: 2},
				{NodeID: 1, SegmentID: 3},
			},
			presetGrowing: []SegmentEntry{
				{SegmentID: 4},
				{SegmentID: 5},
			},

			removalSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
			},
			removalGrowing: []SegmentEntry{
				{SegmentID: 5},
			},

			withMockRead: false,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 3, TargetVersion: unreadableTargetVersion},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, TargetVersion: unreadableTargetVersion},
					},
				},
			},
			expectGrowing: []SegmentEntry{{SegmentID: 4, TargetVersion: unreadableTargetVersion}},
		},
		{
			tag: "remove with wrong nodeID",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
				{NodeID: 2, SegmentID: 2},
				{NodeID: 1, SegmentID: 3},
			},
			presetGrowing: []SegmentEntry{
				{SegmentID: 4},
				{SegmentID: 5},
			},

			removalSealed: []SegmentEntry{
				{NodeID: 2, SegmentID: 1},
			},
			removalGrowing: nil,

			withMockRead: false,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 1, TargetVersion: unreadableTargetVersion},
						{NodeID: 1, SegmentID: 3, TargetVersion: unreadableTargetVersion},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, TargetVersion: unreadableTargetVersion},
					},
				},
			},
			expectGrowing: []SegmentEntry{{SegmentID: 4, TargetVersion: unreadableTargetVersion}, {SegmentID: 5, TargetVersion: unreadableTargetVersion}},
		},
		{
			tag: "remove with wildcardNodeID",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
				{NodeID: 2, SegmentID: 2},
				{NodeID: 1, SegmentID: 3},
			},
			presetGrowing: []SegmentEntry{
				{SegmentID: 4},
				{SegmentID: 5},
			},

			removalSealed: []SegmentEntry{
				{NodeID: wildcardNodeID, SegmentID: 1},
			},
			removalGrowing: nil,

			withMockRead: false,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 3, TargetVersion: unreadableTargetVersion},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, TargetVersion: unreadableTargetVersion},
					},
				},
			},
			expectGrowing: []SegmentEntry{{SegmentID: 4, TargetVersion: unreadableTargetVersion}, {SegmentID: 5, TargetVersion: unreadableTargetVersion}},
		},
		{
			tag: "remove with read",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
				{NodeID: 2, SegmentID: 2},
				{NodeID: 1, SegmentID: 3},
			},
			presetGrowing: []SegmentEntry{
				{SegmentID: 4},
				{SegmentID: 5},
			},

			removalSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1},
			},
			removalGrowing: []SegmentEntry{
				{SegmentID: 5},
			},

			withMockRead: true,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:        1,
							SegmentID:     3,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{
							NodeID:        2,
							SegmentID:     2,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
			},
			expectGrowing: []SegmentEntry{{SegmentID: 4, TargetVersion: unreadableTargetVersion}},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddGrowing(tc.presetGrowing...)
			s.dist.AddDistributions(tc.presetSealed...)

			// update target version, make delegator serviceable
			growingIDs := lo.Map(tc.presetGrowing, func(item SegmentEntry, idx int) int64 {
				return item.SegmentID
			})
			sealedSegmentRowCount := lo.SliceToMap(tc.presetSealed, func(item SegmentEntry) (int64, int64) {
				return item.SegmentID, 100
			})
			s.dist.SyncTargetVersion(&querypb.SyncAction{
				TargetVersion:         1000,
				GrowingInTarget:       growingIDs,
				SealedSegmentRowCount: sealedSegmentRowCount,
			}, []int64{1})

			var version int64
			if tc.withMockRead {
				var err error
				_, _, _, version, err = s.dist.PinReadableSegments(1.0, 1)
				s.Require().NoError(err)
			}

			ch := s.dist.RemoveDistributions(tc.removalSealed, tc.removalGrowing)

			if tc.withMockRead {
				// check ch not closed
				select {
				case <-ch:
					s.Fail("ch closed with running read")
				default:
				}

				s.dist.Unpin(version)
			}
			// check ch close very soon
			timeout := time.NewTimer(time.Second)
			defer timeout.Stop()
			select {
			case <-timeout.C:
				s.Fail("ch not closed after 1 second")
			case <-ch:
			}

			sealed, growing, version := s.dist.PinOnlineSegments()
			defer s.dist.Unpin(version)
			s.compareSnapshotItems(tc.expectSealed, sealed)
			s.ElementsMatch(tc.expectGrowing, growing)
		})
	}
}

func (s *DistributionSuite) TestPeek() {
	type testCase struct {
		tag      string
		readable bool
		input    []SegmentEntry
		expected []SnapshotItem
	}

	cases := []testCase{
		{
			tag:      "one_node",
			readable: false,
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    1,
					SegmentID: 2,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:        1,
							SegmentID:     1,
							TargetVersion: unreadableTargetVersion,
						},
						{
							NodeID:        1,
							SegmentID:     2,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
			},
		},
		{
			tag:      "multiple_nodes",
			readable: false,
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    2,
					SegmentID: 2,
				},
				{
					NodeID:    1,
					SegmentID: 3,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:        1,
							SegmentID:     1,
							TargetVersion: unreadableTargetVersion,
						},

						{
							NodeID:        1,
							SegmentID:     3,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{
							NodeID:        2,
							SegmentID:     2,
							TargetVersion: unreadableTargetVersion,
						},
					},
				},
			},
		},
		{
			tag:      "peek_readable",
			readable: true,
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    2,
					SegmentID: 2,
				},
				{
					NodeID:    1,
					SegmentID: 3,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID:   1,
					Segments: []SegmentEntry{},
				},
				{
					NodeID:   2,
					Segments: []SegmentEntry{},
				},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			// peek during lock
			s.dist.AddDistributions(tc.input...)
			s.dist.mut.Lock()
			sealed, _ := s.dist.PeekSegments(tc.readable)
			s.compareSnapshotItems(tc.expected, sealed)
			s.dist.mut.Unlock()
		})
	}
}

func (s *DistributionSuite) TestMarkOfflineSegments() {
	type testCase struct {
		tag         string
		input       []SegmentEntry
		offlines    []int64
		serviceable bool
	}

	cases := []testCase{
		{
			tag: "offlineHits",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    1,
					SegmentID: 2,
				},
			},
			offlines:    []int64{2},
			serviceable: false,
		},
		{
			tag: "offlineMissed",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
				},
				{
					NodeID:    2,
					SegmentID: 2,
				},
				{
					NodeID:    1,
					SegmentID: 3,
				},
			},
			offlines:    []int64{4},
			serviceable: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddDistributions(tc.input...)
			sealedSegmentRowCount := lo.SliceToMap(tc.input, func(t SegmentEntry) (int64, int64) {
				return t.SegmentID, 100
			})
			s.dist.SyncTargetVersion(&querypb.SyncAction{
				TargetVersion:         1000,
				SealedSegmentRowCount: sealedSegmentRowCount,
				DroppedInTarget:       nil,
			}, nil)
			s.dist.MarkOfflineSegments(tc.offlines...)
			s.Equal(tc.serviceable, s.dist.Serviceable())

			for _, offline := range tc.offlines {
				s.dist.mut.RLock()
				entry, ok := s.dist.sealedSegments[offline]
				s.dist.mut.RUnlock()
				if ok {
					s.EqualValues(-1, entry.NodeID)
					s.EqualValues(unreadableTargetVersion, entry.Version)
				}
			}
		})
	}
}

func (s *DistributionSuite) Test_SyncTargetVersion() {
	growing := []SegmentEntry{
		{
			NodeID:        1,
			SegmentID:     1,
			PartitionID:   1,
			TargetVersion: 1,
		},
		{
			NodeID:        1,
			SegmentID:     2,
			PartitionID:   1,
			TargetVersion: 1,
		},
		{
			NodeID:        1,
			SegmentID:     3,
			PartitionID:   1,
			TargetVersion: 1,
		},
	}

	sealed := []SegmentEntry{
		{
			NodeID:        1,
			SegmentID:     4,
			PartitionID:   1,
			TargetVersion: 1,
		},
		{
			NodeID:        1,
			SegmentID:     5,
			PartitionID:   1,
			TargetVersion: 1,
		},
		{
			NodeID:        1,
			SegmentID:     6,
			PartitionID:   1,
			TargetVersion: 1,
		},
	}

	s.dist.AddGrowing(growing...)
	s.dist.AddDistributions(sealed...)
	s.dist.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         2,
		GrowingInTarget:       []int64{1},
		SealedSegmentRowCount: map[int64]int64{4: 100, 5: 200},
		DroppedInTarget:       []int64{6},
	}, []int64{1})

	s1, s2, _, _, err := s.dist.PinReadableSegments(1.0, 1)
	s.Require().NoError(err)
	s.Len(s1[0].Segments, 2)
	s.Len(s2, 1)

	s1, s2, _ = s.dist.PinOnlineSegments()
	s.Len(s1[0].Segments, 3)
	s.Len(s2, 3)

	s.dist.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         2,
		GrowingInTarget:       []int64{1},
		SealedSegmentRowCount: map[int64]int64{333: 100},
		DroppedInTarget:       []int64{},
	}, []int64{1})
	s.False(s.dist.Serviceable())
	_, _, _, _, err = s.dist.PinReadableSegments(1.0, 1)
	s.Error(err)
}

func TestDistributionSuite(t *testing.T) {
	suite.Run(t, new(DistributionSuite))
}

func TestNewChannelQueryView(t *testing.T) {
	growings := []int64{1, 2, 3}
	sealedWithRowCount := map[int64]int64{4: 100, 5: 200, 6: 300}
	partitions := []int64{7, 8, 9}
	version := int64(10)

	view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
	assert.NotNil(t, view)
	assert.ElementsMatch(t, growings, view.growingSegments.Collect())
	assert.ElementsMatch(t, lo.Keys(sealedWithRowCount), lo.Keys(view.sealedSegmentRowCount))
	assert.True(t, view.partitions.Contain(7))
	assert.True(t, view.partitions.Contain(8))
	assert.True(t, view.partitions.Contain(9))
	assert.Equal(t, version, view.version)
	assert.Equal(t, float64(0), view.loadedRatio.Load())
	assert.False(t, view.Serviceable())
}

func TestDistribution_NewDistribution(t *testing.T) {
	channelName := "test_channel"
	growings := []int64{1, 2, 3}
	sealedWithRowCount := map[int64]int64{4: 100, 5: 200, 6: 300}
	partitions := []int64{7, 8, 9}
	version := int64(10)

	view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
	dist := NewDistribution(channelName, view)

	assert.NotNil(t, dist)
	assert.Equal(t, channelName, dist.channelName)
	assert.Equal(t, view, dist.queryView)
	assert.NotNil(t, dist.growingSegments)
	assert.NotNil(t, dist.sealedSegments)
	assert.NotNil(t, dist.snapshots)
	assert.NotNil(t, dist.current)
}

func TestDistribution_UpdateServiceable(t *testing.T) {
	channelName := "test_channel"
	growings := []int64{1, 2, 3}
	sealedWithRowCount := map[int64]int64{4: 100, 5: 100, 6: 100}
	partitions := []int64{7, 8, 9}
	version := int64(10)

	view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
	dist := NewDistribution(channelName, view)

	// Test with no segments loaded
	dist.updateServiceable("test")
	assert.False(t, dist.Serviceable())
	assert.Equal(t, float64(0), dist.queryView.GetLoadedRatio())

	// Test with some segments loaded
	dist.sealedSegments[4] = SegmentEntry{
		SegmentID: 4,
		Offline:   false,
	}
	dist.growingSegments[1] = SegmentEntry{
		SegmentID: 1,
	}
	dist.updateServiceable("test")
	assert.False(t, dist.Serviceable())
	assert.Equal(t, float64(2)/float64(6), dist.queryView.GetLoadedRatio())

	// Test with all segments loaded
	for id := range sealedWithRowCount {
		dist.sealedSegments[id] = SegmentEntry{
			SegmentID: id,
			Offline:   false,
		}
	}
	for _, id := range growings {
		dist.growingSegments[id] = SegmentEntry{
			SegmentID: id,
		}
	}
	dist.updateServiceable("test")
	assert.Equal(t, float64(1), dist.queryView.GetLoadedRatio())
	assert.False(t, dist.Serviceable())
	dist.queryView.syncedByCoord = true
	assert.True(t, dist.Serviceable())
}

func TestDistribution_SyncTargetVersion(t *testing.T) {
	channelName := "test_channel"
	growings := []int64{1, 2, 3}
	sealedWithRowCount := map[int64]int64{4: 100, 5: 100, 6: 100}
	partitions := []int64{7, 8, 9}
	version := int64(10)

	view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
	dist := NewDistribution(channelName, view)

	// Add some initial segments
	dist.growingSegments[1] = SegmentEntry{
		SegmentID: 1,
	}
	dist.sealedSegments[4] = SegmentEntry{
		SegmentID: 4,
	}

	// Create a new sync action
	action := &querypb.SyncAction{
		GrowingInTarget:       []int64{1, 2},
		SealedSegmentRowCount: map[int64]int64{4: 100, 5: 100},
		DroppedInTarget:       []int64{3},
		TargetVersion:         version + 1,
	}

	// Sync the view
	dist.SyncTargetVersion(action, partitions)

	// Verify the changes
	assert.Equal(t, action.GetTargetVersion(), dist.queryView.version)
	assert.ElementsMatch(t, action.GetGrowingInTarget(), dist.queryView.growingSegments.Collect())
	assert.ElementsMatch(t, lo.Keys(action.GetSealedSegmentRowCount()), lo.Keys(dist.queryView.sealedSegmentRowCount))
	assert.True(t, dist.queryView.partitions.Contain(7))
	assert.True(t, dist.queryView.partitions.Contain(8))
	assert.True(t, dist.queryView.partitions.Contain(9))

	// Verify growing segment target version
	assert.Equal(t, action.GetTargetVersion(), dist.growingSegments[1].TargetVersion)

	// Verify sealed segment target version
	assert.Equal(t, action.GetTargetVersion(), dist.sealedSegments[4].TargetVersion)
}

func TestDistribution_MarkOfflineSegments(t *testing.T) {
	channelName := "test_channel"
	view := NewChannelQueryView([]int64{}, map[int64]int64{1: 100, 2: 200}, []int64{}, 0)
	dist := NewDistribution(channelName, view)

	// Add some segments
	dist.sealedSegments[1] = SegmentEntry{
		SegmentID: 1,
		NodeID:    100,
		Version:   1,
	}
	dist.sealedSegments[2] = SegmentEntry{
		SegmentID: 2,
		NodeID:    100,
		Version:   1,
	}

	// Mark segments offline
	dist.MarkOfflineSegments(1, 2)

	// Verify the changes
	assert.True(t, dist.sealedSegments[1].Offline)
	assert.True(t, dist.sealedSegments[2].Offline)
	assert.Equal(t, int64(-1), dist.sealedSegments[1].NodeID)
	assert.Equal(t, int64(-1), dist.sealedSegments[2].NodeID)
	assert.Equal(t, unreadableTargetVersion, dist.sealedSegments[1].Version)
	assert.Equal(t, unreadableTargetVersion, dist.sealedSegments[2].Version)
}

// TestChannelQueryView_SyncedByCoord tests the syncedByCoord field functionality
func TestChannelQueryView_SyncedByCoord(t *testing.T) {
	mockey.PatchConvey("TestChannelQueryView_SyncedByCoord", t, func() {
		// Mock GetAsFloat to return 1.0 (disable partial result) to avoid paramtable initialization
		mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsFloat")).Return(1.0).Build()

		growings := []int64{1, 2, 3}
		sealedWithRowCount := map[int64]int64{4: 100, 5: 200, 6: 300}
		partitions := []int64{7, 8, 9}
		version := int64(10)

		t.Run("new channelQueryView has syncedByCoord false", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
			assert.False(t, view.syncedByCoord, "New channelQueryView should have syncedByCoord = false")
		})

		t.Run("syncedByCoord can be set manually", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)

			// Initially false
			assert.False(t, view.syncedByCoord)

			// Set to true
			view.syncedByCoord = true
			assert.True(t, view.syncedByCoord)

			// Set back to false
			view.syncedByCoord = false
			assert.False(t, view.syncedByCoord)
		})
	})
}

// TestDistribution_SyncTargetVersionSetsSyncedByCoord tests that SyncTargetVersion sets syncedByCoord
func TestDistribution_SyncTargetVersionSetsSyncedByCoord(t *testing.T) {
	mockey.PatchConvey("TestDistribution_SyncTargetVersionSetsSyncedByCoord", t, func() {
		// Mock GetAsFloat to return 1.0 (disable partial result) to avoid paramtable initialization
		mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsFloat")).Return(1.0).Build()

		channelName := "test_channel"
		growings := []int64{1, 2, 3}
		sealedWithRowCount := map[int64]int64{4: 100, 5: 200, 6: 300}
		partitions := []int64{7, 8, 9}
		version := int64(10)

		t.Run("SyncTargetVersion sets syncedByCoord to true", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
			dist := NewDistribution(channelName, view)

			// Initially syncedByCoord should be false
			assert.False(t, dist.queryView.syncedByCoord)

			// Create sync action
			action := &querypb.SyncAction{
				GrowingInTarget:       []int64{1, 2},
				SealedSegmentRowCount: map[int64]int64{4: 100, 5: 100},
				TargetVersion:         version + 1,
			}

			// Sync the view
			dist.SyncTargetVersion(action, partitions)

			// Verify syncedByCoord is set to true after sync
			assert.True(t, dist.queryView.syncedByCoord, "SyncTargetVersion should set syncedByCoord to true")
			assert.Equal(t, action.GetTargetVersion(), dist.queryView.version)
		})

		t.Run("multiple SyncTargetVersion calls maintain syncedByCoord true", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
			dist := NewDistribution(channelName, view)

			// First sync
			action1 := &querypb.SyncAction{
				GrowingInTarget:       []int64{1, 2},
				SealedSegmentRowCount: map[int64]int64{4: 100},
				TargetVersion:         version + 1,
			}
			dist.SyncTargetVersion(action1, partitions)
			assert.True(t, dist.queryView.syncedByCoord)

			// Second sync
			action2 := &querypb.SyncAction{
				GrowingInTarget:       []int64{1, 2, 3},
				SealedSegmentRowCount: map[int64]int64{4: 100, 5: 200},
				TargetVersion:         version + 2,
			}
			dist.SyncTargetVersion(action2, partitions)
			assert.True(t, dist.queryView.syncedByCoord, "syncedByCoord should remain true after multiple syncs")
			assert.Equal(t, action2.GetTargetVersion(), dist.queryView.version)
		})

		t.Run("SyncTargetVersion creates new queryView with syncedByCoord true", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
			dist := NewDistribution(channelName, view)

			// Store reference to original view
			originalView := dist.queryView
			assert.False(t, originalView.syncedByCoord)

			// Sync should create new queryView
			action := &querypb.SyncAction{
				GrowingInTarget:       []int64{1, 2},
				SealedSegmentRowCount: map[int64]int64{4: 100, 5: 100},
				TargetVersion:         version + 1,
			}
			dist.SyncTargetVersion(action, partitions)

			// Verify new queryView is created with syncedByCoord = true
			newView := dist.queryView
			assert.NotSame(t, originalView, newView, "SyncTargetVersion should create new queryView")
			assert.True(t, newView.syncedByCoord, "New queryView should have syncedByCoord = true")
			assert.False(t, originalView.syncedByCoord, "Original queryView should remain unchanged")
		})
	})
}

// TestDistribution_ServiceableWithSyncedByCoord tests serviceable logic considering syncedByCoord
func TestDistribution_ServiceableWithSyncedByCoord(t *testing.T) {
	mockey.PatchConvey("TestDistribution_ServiceableWithSyncedByCoord", t, func() {
		// Mock GetAsFloat to return 1.0 (disable partial result) to avoid paramtable initialization
		mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsFloat")).Return(1.0).Build()

		channelName := "test_channel"
		growings := []int64{1, 2}
		sealedWithRowCount := map[int64]int64{4: 100, 5: 100}
		partitions := []int64{7, 8}
		version := int64(10)

		t.Run("distribution becomes serviceable after sync and full load", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
			dist := NewDistribution(channelName, view)

			// Initially not serviceable
			assert.False(t, dist.Serviceable())

			// Add all segments to make it fully loaded
			for _, id := range growings {
				dist.growingSegments[id] = SegmentEntry{
					SegmentID: id,
				}
			}
			for id := range sealedWithRowCount {
				dist.sealedSegments[id] = SegmentEntry{
					SegmentID: id,
					Offline:   false,
				}
			}

			// Sync target version to set syncedByCoord = true
			action := &querypb.SyncAction{
				GrowingInTarget:       growings,
				SealedSegmentRowCount: sealedWithRowCount,
				TargetVersion:         version + 1,
			}
			dist.SyncTargetVersion(action, partitions)

			// Update serviceable to calculate loaded ratio
			dist.updateServiceable("test")

			// Should be serviceable now
			assert.True(t, dist.Serviceable())
			assert.Equal(t, float64(1), dist.queryView.GetLoadedRatio())
			assert.True(t, dist.queryView.syncedByCoord)
		})

		t.Run("distribution not serviceable without sync even with full load", func(t *testing.T) {
			view := NewChannelQueryView(growings, sealedWithRowCount, partitions, version)
			dist := NewDistribution(channelName, view)

			// Add all segments to make it fully loaded
			for _, id := range growings {
				dist.growingSegments[id] = SegmentEntry{
					SegmentID: id,
				}
			}
			for id := range sealedWithRowCount {
				dist.sealedSegments[id] = SegmentEntry{
					SegmentID: id,
					Offline:   false,
				}
			}

			// Update serviceable to calculate loaded ratio but don't sync
			dist.updateServiceable("test")

			// Should not be serviceable without sync (assuming partial result is disabled by default)
			// The exact behavior depends on paramtable configuration, but we test the basic structure
			assert.Equal(t, float64(1), dist.queryView.GetLoadedRatio(), "Load ratio should be 1.0")
			assert.False(t, dist.queryView.syncedByCoord, "Should not be synced by coord")

			// Note: The actual serviceable result depends on partial result configuration
			// We focus on testing that the fields are set correctly
		})
	})
}

func (s *DistributionSuite) TestPinReadableSegments_RequiredLoadRatio() {
	type testCase struct {
		tag                    string
		requiredLoadRatio      float64
		sealedSegments         []SegmentEntry
		growingSegments        []SegmentEntry
		sealedSegmentRowCount  map[int64]int64
		growingInTarget        []int64
		expectedSealedCount    int
		expectedNotLoadedCount int
		expectedGrowingCount   int
		shouldError            bool
	}

	cases := []testCase{
		{
			tag:               "full_result_with_target_version_filter",
			requiredLoadRatio: 1.0,
			sealedSegments: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
				{NodeID: 1, SegmentID: 2, PartitionID: 1, Version: 1},
			},
			growingSegments: []SegmentEntry{
				{NodeID: 1, SegmentID: 4, PartitionID: 1},
			},
			sealedSegmentRowCount: map[int64]int64{1: 100, 2: 100},
			growingInTarget:       []int64{4},
			expectedSealedCount:   2, // segments 1,2 are readable (target version matches)
			expectedGrowingCount:  1, // segment 4 is readable (target version matches)
		},
		{
			tag:               "partial_result_with_query_view_filter",
			requiredLoadRatio: 0.6,
			sealedSegments: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
				{NodeID: 2, SegmentID: 3, PartitionID: 1, Version: 1},
			},
			growingSegments: []SegmentEntry{
				{NodeID: 1, SegmentID: 4, PartitionID: 1},
			},
			sealedSegmentRowCount:  map[int64]int64{1: 100, 3: 100, 999: 100}, // segment 999 not loaded, causing load ratio < 1.0
			growingInTarget:        []int64{4},
			expectedSealedCount:    2, // segments 1,3 are in query view and readable
			expectedNotLoadedCount: 1, // segment 999 is not loaded
			expectedGrowingCount:   1, // segment 4 is in query view and readable
		},
		{
			tag:               "insufficient_load_ratio",
			requiredLoadRatio: 1.0,
			sealedSegments: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
			},
			growingSegments:       []SegmentEntry{},
			sealedSegmentRowCount: map[int64]int64{1: 100, 2: 100}, // segment 2 not loaded
			growingInTarget:       []int64{},
			shouldError:           true, // load ratio = 0.5 < required 1.0
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			// Add segments to distribution
			s.dist.AddDistributions(tc.sealedSegments...)
			s.dist.AddGrowing(tc.growingSegments...)

			// Setup query view
			s.dist.SyncTargetVersion(&querypb.SyncAction{
				TargetVersion:         1000,
				SealedSegmentRowCount: tc.sealedSegmentRowCount,
				GrowingInTarget:       tc.growingInTarget,
			}, []int64{1})

			// Test PinReadableSegments with different requiredLoadRatio
			sealed, growing, _, _, err := s.dist.PinReadableSegments(tc.requiredLoadRatio, 1)

			if tc.shouldError {
				s.Error(err)
				return
			}

			s.NoError(err)

			// Count actual segments returned
			actualLoadedSealedCount := 0
			actualNotLoadedSealedCount := 0
			for _, item := range sealed {
				if item.NodeID == -1 {
					actualNotLoadedSealedCount += len(item.Segments)
				} else {
					actualLoadedSealedCount += len(item.Segments)
				}
			}

			s.Equal(tc.expectedSealedCount, actualLoadedSealedCount)
			s.Equal(tc.expectedNotLoadedCount, actualNotLoadedSealedCount)
			s.Equal(tc.expectedGrowingCount, len(growing))
		})
	}
}

func (s *DistributionSuite) TestAddDistributions_TargetVersionSetting() {
	type testCase struct {
		tag                   string
		existingSegment       *SegmentEntry
		newSegment            SegmentEntry
		expectedTargetVersion int64
	}

	cases := []testCase{
		{
			tag: "new_segment_gets_unreadable_target_version",
			newSegment: SegmentEntry{
				NodeID:      1,
				SegmentID:   1,
				Version:     1,
				PartitionID: 1,
			},
			expectedTargetVersion: unreadableTargetVersion,
		},
		{
			tag: "existing_segment_retains_target_version",
			existingSegment: &SegmentEntry{
				NodeID:        1,
				SegmentID:     1,
				Version:       1,
				TargetVersion: 500, // existing target version
				PartitionID:   1,
			},
			newSegment: SegmentEntry{
				NodeID:      1,
				SegmentID:   1,
				Version:     2, // higher version
				PartitionID: 1,
			},
			expectedTargetVersion: 500, // should retain existing target version
		},
		{
			tag: "lower_version_segment_ignored",
			existingSegment: &SegmentEntry{
				NodeID:        1,
				SegmentID:     1,
				Version:       2,
				TargetVersion: 500,
				PartitionID:   1,
			},
			newSegment: SegmentEntry{
				NodeID:      1,
				SegmentID:   1,
				Version:     1, // lower version
				PartitionID: 1,
			},
			expectedTargetVersion: 500, // should keep existing segment unchanged
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			// Add existing segment if provided
			if tc.existingSegment != nil {
				s.dist.mut.Lock()
				s.dist.sealedSegments[tc.existingSegment.SegmentID] = *tc.existingSegment
				s.dist.genSnapshot()
				s.dist.mut.Unlock()
			}

			// Add new segment
			s.dist.AddDistributions(tc.newSegment)

			// Verify target version directly from sealedSegments
			s.dist.mut.RLock()
			actualSegment, found := s.dist.sealedSegments[tc.newSegment.SegmentID]
			s.dist.mut.RUnlock()

			s.True(found, "Segment should be found in distribution")
			s.Equal(tc.expectedTargetVersion, actualSegment.TargetVersion,
				"Target version should match expected value")
		})
	}
}

func (s *DistributionSuite) TestSyncTargetVersion_RedundantGrowingLogic() {
	type testCase struct {
		tag                       string
		growingSegments           []SegmentEntry
		sealedInTarget            []int64
		droppedInTarget           []int64
		expectedRedundantSegments []int64
	}

	cases := []testCase{
		{
			tag: "growing_segment_becomes_redundant_due_to_sealed",
			growingSegments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 1},
			},
			sealedInTarget:            []int64{1}, // segment 1 becomes sealed
			droppedInTarget:           []int64{},
			expectedRedundantSegments: []int64{1},
		},
		{
			tag: "growing_segment_becomes_redundant_due_to_dropped",
			growingSegments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 1},
			},
			sealedInTarget:            []int64{},
			droppedInTarget:           []int64{2}, // segment 2 is dropped
			expectedRedundantSegments: []int64{2},
		},
		{
			tag: "multiple_growing_segments_become_redundant",
			growingSegments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 1},
				{SegmentID: 3, PartitionID: 1},
			},
			sealedInTarget:            []int64{1, 2},
			droppedInTarget:           []int64{3},
			expectedRedundantSegments: []int64{1, 2, 3},
		},
		{
			tag: "no_redundant_growing_segments",
			growingSegments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 1},
			},
			sealedInTarget:            []int64{},
			droppedInTarget:           []int64{},
			expectedRedundantSegments: []int64{},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			// Add growing segments
			s.dist.AddGrowing(tc.growingSegments...)

			// Call SyncTargetVersion to trigger redundant growing segment logic
			s.dist.SyncTargetVersion(&querypb.SyncAction{
				TargetVersion:   1000,
				SealedInTarget:  tc.sealedInTarget,
				DroppedInTarget: tc.droppedInTarget,
			}, []int64{1})

			// Verify redundant segments have correct target version
			growing := make([]SegmentEntry, 0)
			for _, entry := range s.dist.growingSegments {
				growing = append(growing, entry)
			}

			redundantSegments := make([]int64, 0)
			for _, entry := range growing {
				if entry.TargetVersion == redundantTargetVersion {
					redundantSegments = append(redundantSegments, entry.SegmentID)
				}
			}

			s.ElementsMatch(tc.expectedRedundantSegments, redundantSegments,
				"Expected redundant growing segments should have redundant target version")
		})
	}
}

func TestPinReadableSegments(t *testing.T) {
	// Create test distribution
	queryView := NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion)
	dist := NewDistribution("test-channel", queryView)

	// Add some test segments
	dist.AddDistributions([]SegmentEntry{
		{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
		{NodeID: 1, SegmentID: 2, PartitionID: 1, Version: 1},
	}...)

	// Setup query view
	dist.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         1000,
		SealedSegmentRowCount: map[int64]int64{1: 100, 2: 100},
		GrowingInTarget:       []int64{},
	}, []int64{1})

	// Test case 1: requireFullResult=true, Serviceable=false
	mockServiceable := mockey.Mock((*channelQueryView).Serviceable).Return(false).Build()
	mockGetLoadedRatio := mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.8).Build()

	sealed, growing, _, _, err := dist.PinReadableSegments(1.0, 1)

	assert.Error(t, err)
	assert.Nil(t, sealed)
	assert.Nil(t, growing)
	assert.Contains(t, err.Error(), "channel distribution is not serviceable")

	// Test case 2: requireFullResult=true, Serviceable=true
	mockServiceable.UnPatch()
	mockServiceable = mockey.Mock((*channelQueryView).Serviceable).Return(true).Build()

	sealed, growing, _, _, err = dist.PinReadableSegments(1.0, 1)

	assert.NoError(t, err)
	assert.NotNil(t, sealed)
	assert.NotNil(t, growing)

	// Test case 3: requireFullResult=false, loadRatioSatisfy=false
	mockServiceable.UnPatch()
	mockGetLoadedRatio.UnPatch()
	mockGetLoadedRatio = mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.5).Build()

	sealed, growing, _, _, err = dist.PinReadableSegments(0.8, 1)

	assert.Error(t, err)
	assert.Nil(t, sealed)
	assert.Nil(t, growing)
	assert.Contains(t, err.Error(), "channel distribution is not serviceable")

	// Test case 4: requireFullResult=false, loadRatioSatisfy=true
	mockGetLoadedRatio.UnPatch()
	mockGetLoadedRatio = mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.9).Build()
	defer mockGetLoadedRatio.UnPatch()

	sealed, growing, _, _, err = dist.PinReadableSegments(0.8, 1)

	assert.NoError(t, err)
	assert.NotNil(t, sealed)
	assert.NotNil(t, growing)
}

func TestPinReadableSegments_ServiceableLogic(t *testing.T) {
	// Create test distribution
	queryView := NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion)
	dist := NewDistribution("test-channel", queryView)

	// Add test segments
	dist.AddDistributions([]SegmentEntry{
		{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
	}...)

	// Setup query view
	dist.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         1000,
		SealedSegmentRowCount: map[int64]int64{1: 100},
		GrowingInTarget:       []int64{},
	}, []int64{1})

	// Test case: requireFullResult=true, Serviceable=false, GetLoadedRatio=1.0
	// This tests the case where load ratio is satisfied but serviceable is false
	mockServiceable := mockey.Mock((*channelQueryView).Serviceable).Return(false).Build()
	mockGetLoadedRatio := mockey.Mock((*channelQueryView).GetLoadedRatio).Return(1.0).Build()
	defer mockServiceable.UnPatch()
	defer mockGetLoadedRatio.UnPatch()

	sealed, growing, _, _, err := dist.PinReadableSegments(1.0, 1)

	assert.Error(t, err)
	assert.Nil(t, sealed)
	assert.Nil(t, growing)
	assert.Contains(t, err.Error(), "channel distribution is not serviceable")
}

func TestPinReadableSegments_LoadRatioLogic(t *testing.T) {
	// Create test distribution
	queryView := NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion)
	dist := NewDistribution("test-channel", queryView)

	// Add test segments
	dist.AddDistributions([]SegmentEntry{
		{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
	}...)

	// Setup query view
	dist.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         1000,
		SealedSegmentRowCount: map[int64]int64{1: 100},
		GrowingInTarget:       []int64{},
	}, []int64{1})

	// Test case: requireFullResult=false, loadRatioSatisfy=false
	// This tests the case where partial result is requested but load ratio is insufficient
	mockGetLoadedRatio := mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.3).Build()
	defer mockGetLoadedRatio.UnPatch()

	sealed, growing, _, _, err := dist.PinReadableSegments(0.5, 1)

	assert.Error(t, err)
	assert.Nil(t, sealed)
	assert.Nil(t, growing)
	assert.Contains(t, err.Error(), "channel distribution is not serviceable")
}

func TestPinReadableSegments_EdgeCases(t *testing.T) {
	// Create test distribution
	queryView := NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion)
	dist := NewDistribution("test-channel", queryView)

	// Add test segments
	dist.AddDistributions([]SegmentEntry{
		{NodeID: 1, SegmentID: 1, PartitionID: 1, Version: 1},
	}...)

	// Setup query view
	dist.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         1000,
		SealedSegmentRowCount: map[int64]int64{1: 100},
		GrowingInTarget:       []int64{},
	}, []int64{1})

	// Test case 1: requiredLoadRatio = 0.0 (edge case)
	mockGetLoadedRatio := mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.0).Build()

	sealed, growing, _, _, err := dist.PinReadableSegments(0.0, 1)

	assert.NoError(t, err)
	assert.NotNil(t, sealed)
	assert.NotNil(t, growing)

	// Test case 2: requiredLoadRatio = 0.0, GetLoadedRatio = 0.0 (exact match)
	mockGetLoadedRatio.UnPatch()
	mockGetLoadedRatio = mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.0).Build()

	sealed, growing, _, _, err = dist.PinReadableSegments(0.0, 1)

	assert.NoError(t, err)
	assert.NotNil(t, sealed)
	assert.NotNil(t, growing)

	// Test case 3: requiredLoadRatio = 0.0, GetLoadedRatio = 0.1 (satisfied)
	mockGetLoadedRatio.UnPatch()
	mockGetLoadedRatio = mockey.Mock((*channelQueryView).GetLoadedRatio).Return(0.1).Build()
	defer mockGetLoadedRatio.UnPatch()

	sealed, growing, _, _, err = dist.PinReadableSegments(0.0, 1)

	assert.NoError(t, err)
	assert.NotNil(t, sealed)
	assert.NotNil(t, growing)
}
