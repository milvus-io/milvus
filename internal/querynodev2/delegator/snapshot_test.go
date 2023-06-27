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

type SnapshotSuite struct {
	suite.Suite

	snapshot *snapshot
}

func (s *SnapshotSuite) SetupTest() {
	last := NewSnapshot(nil, nil, nil, 0, initialTargetVersion)
	last.Expire(func() {})
	s.Equal(initialTargetVersion, last.GetTargetVersion())

	dist := []SnapshotItem{
		{
			NodeID: 1,
			Segments: []SegmentEntry{
				{
					SegmentID:   1,
					PartitionID: 1,
				},
				{
					SegmentID:   2,
					PartitionID: 2,
				},
			},
		},
		{
			NodeID: 2,
			Segments: []SegmentEntry{
				{
					SegmentID:   3,
					PartitionID: 1,
				},
				{
					SegmentID:   4,
					PartitionID: 2,
				},
			},
		},
	}
	growing := []SegmentEntry{
		{
			SegmentID:   5,
			PartitionID: 1,
		},
		{
			SegmentID:   6,
			PartitionID: 2,
		},
	}

	s.snapshot = NewSnapshot(dist, growing, last, 1, 1)
	s.Equal(int64(1), s.snapshot.GetTargetVersion())
}

func (s *SnapshotSuite) TearDownTest() {
	s.snapshot = nil
}

func (s *SnapshotSuite) TestGet() {
	type testCase struct {
		tag             string
		partitions      []int64
		expectedSealed  []SnapshotItem
		expectedGrowing []SegmentEntry
	}

	cases := []testCase{
		{
			tag:        "nil partition",
			partitions: nil,
			expectedSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{SegmentID: 1, PartitionID: 1},
						{SegmentID: 2, PartitionID: 2},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{SegmentID: 3, PartitionID: 1},
						{SegmentID: 4, PartitionID: 2},
					},
				},
			},
			expectedGrowing: []SegmentEntry{
				{
					SegmentID:   5,
					PartitionID: 1,
				},
				{
					SegmentID:   6,
					PartitionID: 2,
				},
			},
		},
		{
			tag:        "partition_1",
			partitions: []int64{1},
			expectedSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{SegmentID: 1, PartitionID: 1},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{SegmentID: 3, PartitionID: 1},
					},
				},
			},
			expectedGrowing: []SegmentEntry{
				{
					SegmentID:   5,
					PartitionID: 1,
				},
			},
		},
		{
			tag:        "partition_2",
			partitions: []int64{2},
			expectedSealed: []SnapshotItem{
				{
					NodeID: 1,
					Segments: []SegmentEntry{
						{SegmentID: 2, PartitionID: 2},
					},
				},
				{
					NodeID: 2,
					Segments: []SegmentEntry{
						{SegmentID: 4, PartitionID: 2},
					},
				},
			},
			expectedGrowing: []SegmentEntry{
				{
					SegmentID:   6,
					PartitionID: 2,
				},
			},
		},
		{
			tag:        "partition not exists",
			partitions: []int64{3},
			expectedSealed: []SnapshotItem{
				{
					NodeID:   1,
					Segments: []SegmentEntry{},
				},
				{
					NodeID:   2,
					Segments: []SegmentEntry{},
				},
			},
			expectedGrowing: []SegmentEntry{},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			before := s.snapshot.inUse.Load()
			sealed, growing := s.snapshot.Get(tc.partitions...)
			after := s.snapshot.inUse.Load()
			s.ElementsMatch(tc.expectedSealed, sealed)
			s.ElementsMatch(tc.expectedGrowing, growing)
			s.EqualValues(1, after-before)
		})
	}
}

func (s *SnapshotSuite) TestDone() {
	s.Run("done not expired snapshot", func() {
		inUse := s.snapshot.inUse.Load()
		s.Require().EqualValues(0, inUse)
		s.snapshot.Get()
		inUse = s.snapshot.inUse.Load()
		s.Require().EqualValues(1, inUse)

		s.snapshot.Done(func() {})
		inUse = s.snapshot.inUse.Load()
		s.EqualValues(0, inUse)
		// check cleared channel closed
		select {
		case <-s.snapshot.cleared:
			s.Fail("snapshot channel closed in non-expired state")
		default:
		}
	})

	s.Run("done expired snapshot", func() {
		inUse := s.snapshot.inUse.Load()
		s.Require().EqualValues(0, inUse)
		s.snapshot.Get()
		inUse = s.snapshot.inUse.Load()
		s.Require().EqualValues(1, inUse)
		s.snapshot.Expire(func() {})
		// check cleared channel closed
		select {
		case <-s.snapshot.cleared:
			s.FailNow("snapshot channel closed in non-expired state")
		default:
		}

		signal := make(chan struct{})
		s.snapshot.Done(func() { close(signal) })
		inUse = s.snapshot.inUse.Load()
		s.EqualValues(0, inUse)
		timeout := time.NewTimer(time.Second)
		defer timeout.Stop()
		select {
		case <-timeout.C:
			s.FailNow("cleanup never called")
		case <-signal:
		}
		timeout = time.NewTimer(10 * time.Millisecond)
		defer timeout.Stop()
		select {
		case <-timeout.C:
			s.FailNow("snapshot channel not closed after expired and no use")
		case <-s.snapshot.cleared:
		}
	})
}

func TestSnapshot(t *testing.T) {
	suite.Run(t, new(SnapshotSuite))
}
