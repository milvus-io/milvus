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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type DistributionSuite struct {
	suite.Suite
	dist *distribution
}

func (s *DistributionSuite) SetupTest() {
	s.dist = NewDistribution("channel-1", NewChannelQueryView(nil, nil, nil, initialTargetVersion))
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
			s.dist.SyncReadableChannelView(&querypb.SyncAction{
				TargetVersion: 1000,
			}, nil)
			_, _, version, err := s.dist.PinReadableSegments(1.0)
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
			s.dist.SyncReadableChannelView(&querypb.SyncAction{
				TargetVersion:   1000,
				GrowingInTarget: []int64{1, 2},
			}, tc.workingParts)
			_, growing, version, err := s.dist.PinReadableSegments(1.0)
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
			s.dist.SyncReadableChannelView(&querypb.SyncAction{
				TargetVersion:         1000,
				GrowingInTarget:       growingIDs,
				SealedSegmentRowCount: sealedSegmentRowCount,
			}, nil)

			var version int64
			if tc.withMockRead {
				var err error
				_, _, version, err = s.dist.PinReadableSegments(1.0)
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
			s.dist.SyncReadableChannelView(&querypb.SyncAction{
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
	s.dist.SyncReadableChannelView(&querypb.SyncAction{
		TargetVersion:         2,
		GrowingInTarget:       []int64{1},
		SealedSegmentRowCount: map[int64]int64{4: 100, 5: 200},
		DroppedInTarget:       []int64{6},
	}, []int64{1})

	s1, s2, _, err := s.dist.PinReadableSegments(1.0)
	s.Require().NoError(err)
	s.Len(s1[0].Segments, 2)
	s.Len(s2, 1)

	s1, s2, _ = s.dist.PinOnlineSegments()
	s.Len(s1[0].Segments, 3)
	s.Len(s2, 3)

	s.dist.SyncReadableChannelView(&querypb.SyncAction{
		TargetVersion:         2,
		GrowingInTarget:       []int64{1},
		SealedSegmentRowCount: map[int64]int64{333: 100},
		DroppedInTarget:       []int64{},
	}, []int64{1})
	s.False(s.dist.Serviceable())
	_, _, _, err = s.dist.PinReadableSegments(1.0)
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
	assert.True(t, dist.Serviceable())
	assert.Equal(t, float64(1), dist.queryView.GetLoadedRatio())
}

func TestDistribution_SyncReadableChannelView(t *testing.T) {
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
	dist.SyncReadableChannelView(action, partitions)

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
