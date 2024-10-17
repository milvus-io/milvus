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

	"github.com/stretchr/testify/suite"
)

type DistributionSuite struct {
	suite.Suite
	dist *distribution
}

func (s *DistributionSuite) SetupTest() {
	s.dist = NewDistribution()
	s.Equal(initialTargetVersion, s.dist.getTargetVersion())
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
			_, _, version, err := s.dist.PinReadableSegments()
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
		tag      string
		input    []SegmentEntry
		expected []SegmentEntry
	}

	cases := []testCase{
		{
			tag:      "nil input",
			input:    nil,
			expected: []SegmentEntry{},
		},
		{
			tag: "normal case",
			input: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 2},
			},
			expected: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1},
				{SegmentID: 2, PartitionID: 2},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddGrowing(tc.input...)
			_, growing, version, err := s.dist.PinReadableSegments()
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
			expectGrowing: []SegmentEntry{{SegmentID: 4}},
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
			expectGrowing: []SegmentEntry{{SegmentID: 4}, {SegmentID: 5}},
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
			expectGrowing: []SegmentEntry{{SegmentID: 4}, {SegmentID: 5}},
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
			expectGrowing: []SegmentEntry{{SegmentID: 4}},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddGrowing(tc.presetGrowing...)
			s.dist.AddDistributions(tc.presetSealed...)

			var version int64
			if tc.withMockRead {
				var err error
				_, _, version, err = s.dist.PinReadableSegments()
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

func (s *DistributionSuite) TestAddOfflines() {
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
			s.dist.AddOfflines(tc.offlines...)
			s.Equal(tc.serviceable, s.dist.Serviceable())

			// current := s.dist.current.Load()
			for _, offline := range tc.offlines {
				// current.
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
	s.dist.SyncTargetVersion(2, []int64{2, 3}, []int64{6}, []int64{})

	s1, s2, _, err := s.dist.PinReadableSegments()
	s.Require().NoError(err)
	s.Len(s1[0].Segments, 1)
	s.Len(s2, 2)

	s1, s2, _ = s.dist.PinOnlineSegments()
	s.Len(s1[0].Segments, 3)
	s.Len(s2, 3)

	s.dist.serviceable.Store(true)
	s.dist.SyncTargetVersion(2, []int64{222}, []int64{}, []int64{})
	s.True(s.dist.Serviceable())

	s.dist.SyncTargetVersion(2, []int64{}, []int64{333}, []int64{})
	s.False(s.dist.Serviceable())

	s.dist.SyncTargetVersion(2, []int64{}, []int64{333}, []int64{1, 2, 3})
	_, _, _, err = s.dist.PinReadableSegments()
	s.Error(err)
}

func TestDistributionSuite(t *testing.T) {
	suite.Run(t, new(DistributionSuite))
}
