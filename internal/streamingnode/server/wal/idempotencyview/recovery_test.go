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

package idempotencyview

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverySnapshotRange(t *testing.T) {
	section := func(vc string, ticks ...uint64) *Snapshot {
		s := &Snapshot{VChannel: vc}
		for _, tt := range ticks {
			s.Records = append(s.Records, &Record{SourceTimeTick: tt})
		}
		return s
	}
	snapshot := &RecoverySnapshot{Chunks: [][]*Snapshot{
		{},
		{section("empty"), section("a", 10, 30), section("b", 20, 40)},
		{section("b", 50), section("a", 60, 80), section("c", 70)},
	}}
	var ticks []uint64
	snapshot.Range(func(_ string, r *Record) bool { ticks = append(ticks, r.SourceTimeTick); return true })
	require.Equal(t, []uint64{10, 20, 30, 40, 50, 60, 70, 80}, ticks)
	ticks = nil
	snapshot.Range(func(_ string, r *Record) bool { ticks = append(ticks, r.SourceTimeTick); return len(ticks) < 2 })
	require.Equal(t, []uint64{10, 20}, ticks, "consumer can stop without visiting later chunks")
}

func TestRecoverySnapshotMergeManySections(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	snapshot := &RecoverySnapshot{}
	var expected []uint64
	for _, count := range []int{1, 2000, 32, 3000, 79, 1} {
		byVC := make(map[int]*Snapshot)
		var sections []*Snapshot
		for i := 0; i < count; i++ {
			vc := random.Intn(23)
			section := byVC[vc]
			if section == nil {
				section = &Snapshot{}
				byVC[vc] = section
				sections = append(sections, section)
			}
			tick := uint64(len(expected) + 1)
			expected = append(expected, tick)
			section.Records = append(section.Records, &Record{SourceTimeTick: tick})
		}
		snapshot.Chunks = append(snapshot.Chunks, sections)
	}
	var actual []uint64
	snapshot.Range(func(_ string, record *Record) bool { actual = append(actual, record.SourceTimeTick); return true })
	require.Equal(t, expected, actual)
}
