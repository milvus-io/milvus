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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testGroup() *SegmentChangeGroup {
	return &SegmentChangeGroup{
		GroupID:              1,
		Source:               SegmentChangeSourceMixCompaction,
		CollectionID:         10,
		PartitionID:          100,
		SourceJobID:          999,
		State:                SegmentChangeStateStaged,
		NewSegmentIDs:        []int64{1001, 1002},
		SupersededSegmentIDs: []int64{2001},
		CreateTS:             123,
	}
}

func TestSegmentChangeGroup_Clone(t *testing.T) {
	g := testGroup()
	c := g.Clone()
	require.NotNil(t, c)
	require.NotSame(t, g, c)
	require.Equal(t, g.NewSegmentIDs, c.NewSegmentIDs)
	c.NewSegmentIDs[0] = 7777
	require.NotEqual(t, g.NewSegmentIDs[0], c.NewSegmentIDs[0], "clone must deep-copy slices")
	c.SupersededSegmentIDs[0] = 8888
	require.Equal(t, int64(2001), g.SupersededSegmentIDs[0])
	require.Nil(t, (*SegmentChangeGroup)(nil).Clone())
}

func TestSegmentChangeGroup_ContainsSegment(t *testing.T) {
	g := testGroup()
	require.True(t, g.ContainsSegment(1001))
	require.True(t, g.ContainsSegment(2001))
	require.False(t, g.ContainsSegment(3000))
	require.False(t, (*SegmentChangeGroup)(nil).ContainsSegment(1))
}

func TestSegmentChangeGroup_IsTerminal(t *testing.T) {
	require.False(t, testGroup().IsTerminal())
	require.True(t, (&SegmentChangeGroup{State: SegmentChangeStateReady}).IsTerminal() == false)
	require.True(t, (&SegmentChangeGroup{State: SegmentChangeStateCommitted}).IsTerminal())
	require.True(t, (&SegmentChangeGroup{State: SegmentChangeStateFailed}).IsTerminal())
	require.True(t, (&SegmentChangeGroup{State: SegmentChangeStateAborted}).IsTerminal())
	require.True(t, (*SegmentChangeGroup)(nil).IsTerminal())
}

func TestSegmentChangeGroup_CanTransitionTo(t *testing.T) {
	cases := []struct {
		name  string
		state SegmentChangeState
		next  SegmentChangeState
		want  bool
	}{
		{"staged to ready", SegmentChangeStateStaged, SegmentChangeStateReady, true},
		{"staged to staged (idempotent)", SegmentChangeStateStaged, SegmentChangeStateStaged, true},
		{"staged to failed", SegmentChangeStateStaged, SegmentChangeStateFailed, true},
		{"staged to aborted", SegmentChangeStateStaged, SegmentChangeStateAborted, true},
		{"staged to committed is illegal", SegmentChangeStateStaged, SegmentChangeStateCommitted, false},
		{"ready to committed", SegmentChangeStateReady, SegmentChangeStateCommitted, true},
		{"ready to failed", SegmentChangeStateReady, SegmentChangeStateFailed, true},
		{"ready to ready (idempotent)", SegmentChangeStateReady, SegmentChangeStateReady, true},
		{"ready back to staged is illegal", SegmentChangeStateReady, SegmentChangeStateStaged, false},
		{"committed replay", SegmentChangeStateCommitted, SegmentChangeStateCommitted, true},
		{"committed to staged is illegal", SegmentChangeStateCommitted, SegmentChangeStateStaged, false},
		{"failed replay", SegmentChangeStateFailed, SegmentChangeStateFailed, true},
		{"aborted replay", SegmentChangeStateAborted, SegmentChangeStateAborted, true},
		{"failed to committed is illegal", SegmentChangeStateFailed, SegmentChangeStateCommitted, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &SegmentChangeGroup{State: tc.state}
			require.Equal(t, tc.want, g.CanTransitionTo(tc.next))
		})
	}
	require.False(t, (*SegmentChangeGroup)(nil).CanTransitionTo(SegmentChangeStateReady))
}

func TestSegmentChangeGroup_Validate(t *testing.T) {
	valid := testGroup()
	require.NoError(t, valid.Validate())

	t.Run("nil group", func(t *testing.T) {
		require.Error(t, (*SegmentChangeGroup)(nil).Validate())
	})
	t.Run("non-positive group id", func(t *testing.T) {
		g := testGroup()
		g.GroupID = 0
		require.Error(t, g.Validate())
	})
	t.Run("non-positive collection id", func(t *testing.T) {
		g := testGroup()
		g.CollectionID = 0
		require.Error(t, g.Validate())
	})
	t.Run("no new segments", func(t *testing.T) {
		g := testGroup()
		g.NewSegmentIDs = nil
		require.Error(t, g.Validate())
	})
	t.Run("invalid member id", func(t *testing.T) {
		g := testGroup()
		g.NewSegmentIDs = []int64{0}
		require.Error(t, g.Validate())
	})
	t.Run("duplicate member", func(t *testing.T) {
		g := testGroup()
		g.NewSegmentIDs = []int64{1001, 1001}
		require.Error(t, g.Validate())
	})
	t.Run("segment both new and superseded", func(t *testing.T) {
		g := testGroup()
		g.SupersededSegmentIDs = []int64{1001}
		require.Error(t, g.Validate())
	})
	t.Run("out-of-range state", func(t *testing.T) {
		g := testGroup()
		g.State = SegmentChangeState(99)
		require.Error(t, g.Validate())
	})
	t.Run("out-of-range source", func(t *testing.T) {
		g := testGroup()
		g.Source = SegmentChangeSource(0)
		require.Error(t, g.Validate())
		g.Source = SegmentChangeSource(99)
		require.Error(t, g.Validate())
	})
}

func TestSegmentChangeGroup_MarshalRoundTrip(t *testing.T) {
	g := testGroup()
	data, err := MarshalSegmentChangeGroup(g)
	require.NoError(t, err)
	got, err := UnmarshalSegmentChangeGroup(data)
	require.NoError(t, err)
	require.Equal(t, g, got)
}

func TestUnmarshalSegmentChangeGroup_Corrupt(t *testing.T) {
	_, err := UnmarshalSegmentChangeGroup([]byte("not-json"))
	require.Error(t, err)
}
