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

package querynode

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
	s.dist = NewDistribution(0)
}

func (s *DistributionSuite) TearDownTest() {
	s.dist = nil
}

func (s *DistributionSuite) TestAddDistribution() {
	type testCase struct {
		tag      string
		input    []SegmentEntry
		expected []SnapshotItem
	}

	cases := []testCase{
		{
			tag: "one node",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
					State:     segmentStateLoaded,
				},
				{
					NodeID:    1,
					SegmentID: 2,
					State:     segmentStateLoaded,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:    1,
							SegmentID: 1,
							State:     segmentStateLoaded,
						},
						{
							NodeID:    1,
							SegmentID: 2,
							State:     segmentStateLoaded,
						},
					},
				},
			},
		},
		{
			tag: "multiple nodes",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
					State:     segmentStateLoaded,
				},
				{
					NodeID:    2,
					SegmentID: 2,
					State:     segmentStateLoaded,
				},
				{
					NodeID:    1,
					SegmentID: 3,
					State:     segmentStateLoaded,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:    1,
							SegmentID: 1,
							State:     segmentStateLoaded,
						},

						{
							NodeID:    1,
							SegmentID: 3,
							State:     segmentStateLoaded,
						},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{
							NodeID:    2,
							SegmentID: 2,
							State:     segmentStateLoaded,
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()
			s.dist.AddDistributions(tc.input...)
			sealed, version := s.dist.GetCurrent()
			defer s.dist.FinishUsage(version)
			s.compareSnapshotItems(tc.expected, sealed)
		})
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

func (s *DistributionSuite) TestRemoveDistribution() {
	type testCase struct {
		tag           string
		presetSealed  []SegmentEntry
		removalSealed []SegmentEntry

		withMockRead bool
		expectSealed []SnapshotItem
	}

	cases := []testCase{
		{
			tag: "remove with no read",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},

			removalSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
			},

			withMockRead: false,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
					},
				},
			},
		},
		{
			tag: "remove with wrong nodeID",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},

			removalSealed: []SegmentEntry{
				{NodeID: 2, SegmentID: 1, State: segmentStateLoaded},
			},

			withMockRead: false,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
						{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
					},
				},
			},
		},
		{
			tag: "remove with wildcardNodeID",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},

			removalSealed: []SegmentEntry{
				{NodeID: wildcardNodeID, SegmentID: 1, State: segmentStateLoaded},
			},

			withMockRead: false,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
					},
				},
			},
		},
		{
			tag: "remove with read",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},

			removalSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
			},

			withMockRead: true,

			expectSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddDistributions(tc.presetSealed...)

			var version int64
			if tc.withMockRead {
				_, version = s.dist.GetCurrent()
			}

			ch := make(chan struct{})
			go func() {
				s.dist.RemoveDistributions(func() {}, tc.removalSealed...)
				close(ch)
			}()
			if tc.withMockRead {
				// check ch not closed
				select {
				case <-ch:
					s.Fail("ch closed with running read")
				default:
				}

				s.dist.FinishUsage(version)
			}
			// check ch close very soon
			timeout := time.NewTimer(time.Second)
			defer timeout.Stop()
			select {
			case <-timeout.C:
				s.Fail("ch not closed after 1 second")
			case <-ch:
			}

			sealed, version := s.dist.GetCurrent()
			defer s.dist.FinishUsage(version)
			s.compareSnapshotItems(tc.expectSealed, sealed)
		})
	}
}

func (s *DistributionSuite) TestNodeDown() {
	type testCase struct {
		tag          string
		presetSealed []SegmentEntry
		nodesDown    []int64

		modification []SegmentEntry
		ok           bool
	}

	cases := []testCase{
		{
			tag:          "no_segment",
			presetSealed: []SegmentEntry{},
			nodesDown:    []int64{1},
			modification: []SegmentEntry{},
			ok:           true,
		},
		{
			tag: "normal_node_down",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},
			nodesDown:    []int64{1},
			modification: []SegmentEntry{},
			ok:           false,
		},
		{
			tag: "normal_node_recover",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},
			nodesDown: []int64{2},
			modification: []SegmentEntry{
				{NodeID: 1, SegmentID: 2, State: segmentStateLoaded},
			},
			ok: true,
		},
		{
			tag: "multiple_down_recover",
			presetSealed: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, State: segmentStateLoaded},
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
				{NodeID: 1, SegmentID: 3, State: segmentStateLoaded},
			},
			nodesDown: []int64{2, 2, 2, 2},
			modification: []SegmentEntry{
				{NodeID: 2, SegmentID: 2, State: segmentStateLoaded},
			},
			ok: true,
		},
	}
	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()

			s.dist.AddDistributions(tc.presetSealed...)

			for _, nodeID := range tc.nodesDown {
				s.dist.NodeDown(nodeID)
			}
			s.dist.UpdateDistribution(tc.modification...)

			s.dist.mut.RLock()
			defer s.dist.mut.RUnlock()
			s.Equal(tc.ok, s.dist.Serviceable())
		})
	}
}

func (s *DistributionSuite) TestPeek() {
	type testCase struct {
		tag      string
		input    []SegmentEntry
		expected []SnapshotItem
	}
	cases := []testCase{
		{
			tag: "one_node",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
					State:     segmentStateLoaded,
				},
				{
					NodeID:    1,
					SegmentID: 2,
					State:     segmentStateLoaded,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:    1,
							SegmentID: 1,
							State:     segmentStateLoaded,
						},
						{
							NodeID:    1,
							SegmentID: 2,
							State:     segmentStateLoaded,
						},
					},
				},
			},
		},
		{
			tag: "multiple_nodes",
			input: []SegmentEntry{
				{
					NodeID:    1,
					SegmentID: 1,
					State:     segmentStateLoaded,
				},
				{
					NodeID:    2,
					SegmentID: 2,
					State:     segmentStateLoaded,
				},
				{
					NodeID:    1,
					SegmentID: 3,
					State:     segmentStateLoaded,
				},
			},
			expected: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{
							NodeID:    1,
							SegmentID: 1,
							State:     segmentStateLoaded,
						},

						{
							NodeID:    1,
							SegmentID: 3,
							State:     segmentStateLoaded,
						},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{
							NodeID:    2,
							SegmentID: 2,
							State:     segmentStateLoaded,
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.SetupTest()
			defer s.TearDownTest()
			s.dist.AddDistributions(tc.input...)
			// peek during lock
			s.dist.mut.Lock()
			sealed := s.dist.Peek()
			s.compareSnapshotItems(tc.expected, sealed)
			s.dist.mut.Unlock()
		})
	}
}

func TestDistributionSuite(t *testing.T) {
	suite.Run(t, new(DistributionSuite))
}
