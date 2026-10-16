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

package compaction

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

func testConfig() Config {
	return Config{
		L0Trigger: 4, MinMergeRuns: 4, SizeRatio: 4.0, SpaceAmpRatio: 1.0, MaxInputBytes: 1 << 30,
		MaxInputTables: 64, MaxConcurrent: 16, TargetFileSize: 128 << 20, TaskSlot: 4,
	}
}

func ref(id, runID, size int64) *pkindexpb.SSTRef {
	return &pkindexpb.SSTRef{Id: id, RunId: runID, Size: size, MinKey: []byte("a"), MaxKey: []byte("z")}
}

func TestGroupSSTs(t *testing.T) {
	gs := groupSSTs([]*pkindexpb.SSTRef{ref(9, 0, 1), ref(8, 0, 1), ref(7, 5, 2), ref(6, 5, 3), ref(3, 4, 10)})
	require.Equal(t, []group{{0, 0, 1, 1}, {0, 1, 1, 1}, {5, 2, 2, 5}, {4, 4, 1, 10}}, gs)
	require.Equal(t, 2, l0Prefix(gs))
}

func TestRuleL0(t *testing.T) {
	cfg := testConfig()
	// three L0: below the trigger
	_, ok := pickL0(groupSSTs([]*pkindexpb.SSTRef{ref(3, 0, 1), ref(2, 0, 1), ref(1, 0, 1)}), cfg)
	require.False(t, ok)
	// five L0 and a run: all five (the oldest ones first), tombstones kept because a run remains
	gs := groupSSTs([]*pkindexpb.SSTRef{ref(5, 0, 1), ref(4, 0, 1), ref(3, 0, 1), ref(2, 0, 1), ref(1, 0, 1), ref(0, 7, 9)})
	c, ok := pickL0(gs, cfg)
	require.True(t, ok)
	require.Equal(t, choice{rule: ruleL0, score: 1.25, first: 0, last: 4}, c)
	// four L0 and nothing else: all of them, tombstones dropped
	gs = groupSSTs([]*pkindexpb.SSTRef{ref(4, 0, 1), ref(3, 0, 1), ref(2, 0, 1), ref(1, 0, 1)})
	c, ok = pickL0(gs, cfg)
	require.True(t, ok)
	require.True(t, c.dropTombstones)
	require.Equal(t, 0, c.first)
	// the byte cap stops the window but never below two tables
	cfg.MaxInputBytes = 1
	c, ok = pickL0(gs, cfg)
	require.True(t, ok)
	require.Equal(t, 2, c.first)
	require.Equal(t, 3, c.last)
	require.True(t, c.dropTombstones) // only flushed tables exist, so the inputs end the list
	// the table cap
	cfg = testConfig()
	cfg.MaxInputTables = 3
	c, ok = pickL0(gs, cfg)
	require.True(t, ok)
	require.Equal(t, 1, c.first)
	require.True(t, c.dropTombstones)
}

func TestRuleMerge(t *testing.T) {
	cfg := testConfig()
	// one L0, runs 10 (100), 9 (30), 8 (20), 7 (25), 6 (500)
	gs := groupSSTs([]*pkindexpb.SSTRef{ref(50, 0, 1), ref(40, 10, 100), ref(30, 9, 30), ref(20, 8, 20), ref(10, 7, 25), ref(5, 6, 500)})
	// 100/20 = 5 > 4 excludes run 10; 500 excludes run 6; the window is runs 9, 8, 7: three, below minMergeRuns
	_, ok := pickMerge(gs, cfg)
	require.False(t, ok)
	cfg.MinMergeRuns = 3
	c, ok := pickMerge(gs, cfg)
	require.True(t, ok)
	require.Equal(t, choice{rule: ruleMerge, score: 1, first: 2, last: 4}, c)
	// the window that reaches the last run drops tombstones
	gs = groupSSTs([]*pkindexpb.SSTRef{ref(40, 10, 10), ref(30, 9, 10), ref(20, 8, 10), ref(10, 7, 10)})
	cfg.MinMergeRuns = 4
	c, ok = pickMerge(gs, cfg)
	require.True(t, ok)
	require.True(t, c.dropTombstones)
	// more runs win over fewer bytes; equal runs prefer fewer bytes
	gs = groupSSTs([]*pkindexpb.SSTRef{ref(60, 12, 10), ref(50, 11, 10), ref(40, 10, 10), ref(30, 9, 10), ref(20, 8, 100), ref(10, 7, 100), ref(5, 6, 100), ref(1, 5, 100)})
	c, ok = pickMerge(gs, cfg)
	require.True(t, ok)
	require.Equal(t, 0, c.first)
	require.Equal(t, 3, c.last)
	// a window capped by maxInputBytes
	cfg.MaxInputBytes = 35
	_, ok = pickMerge(gs, cfg)
	require.False(t, ok) // 4 runs of 10 are 40 bytes
}

func TestRuleSpaceAmp(t *testing.T) {
	cfg := testConfig()
	// everything above the last run is 60, the last run is 100: no
	gs := groupSSTs([]*pkindexpb.SSTRef{ref(9, 0, 10), ref(8, 3, 50), ref(7, 2, 100)})
	_, ok := pickSpaceAmp(gs, cfg)
	require.False(t, ok)
	// 150 above a last run of 100: yes, everything, tombstones dropped
	gs = groupSSTs([]*pkindexpb.SSTRef{ref(9, 0, 100), ref(8, 3, 50), ref(7, 2, 100)})
	c, ok := pickSpaceAmp(gs, cfg)
	require.True(t, ok)
	require.Equal(t, choice{rule: ruleSpaceAmp, score: 1.5, first: 0, last: 2, dropTombstones: true}, c)
	// a single run never qualifies
	_, ok = pickSpaceAmp(groupSSTs([]*pkindexpb.SSTRef{ref(9, 0, 100), ref(7, 2, 10)}), cfg)
	require.False(t, ok)
}

func TestDegenerateConfig(t *testing.T) {
	cfg := Config{}.normalized()
	gs := groupSSTs([]*pkindexpb.SSTRef{ref(2, 0, 1), ref(1, 0, 1)})
	require.NotPanics(t, func() { evaluate(nil, cfg) })
	c, ok := evaluate(gs, cfg)
	require.True(t, ok)
	require.Equal(t, ruleL0, c.rule)
	_, ok = pickMerge(gs, cfg)
	require.False(t, ok)
	_, ok = pickSpaceAmp(gs, cfg)
	require.False(t, ok)
}

func TestConfigNormalized(t *testing.T) {
	n := Config{
		L0Trigger: -1, MinMergeRuns: 1, MaxInputTables: 0, SizeRatio: 0.5, SpaceAmpRatio: 0,
		MaxInputBytes: 0, TargetFileSize: -5, MaxConcurrent: 0, InspectInterval: 0, FailureCooldown: -time.Second, TaskSlot: 0,
	}.normalized()
	require.Equal(t, Config{
		L0Trigger: 2, MinMergeRuns: 2, MaxInputTables: 2, SizeRatio: 1, SpaceAmpRatio: 1,
		MaxInputBytes: math.MaxInt64, TargetFileSize: 128 << 20, MaxConcurrent: 1,
		InspectInterval: 10 * time.Second, FailureCooldown: 0, TaskSlot: 1,
	}, n)
	// a valid config is unchanged
	require.Equal(t, testConfig().normalized(), testConfig().normalized().normalized())
	cfg := testConfig()
	cfg.InspectInterval, cfg.FailureCooldown = time.Second, time.Minute
	require.Equal(t, cfg, cfg.normalized())
	// the clamps of a single value
	require.Equal(t, 2, Config{L0Trigger: 1}.normalized().L0Trigger)
	require.Equal(t, 5, Config{L0Trigger: 5}.normalized().L0Trigger)
	require.EqualValues(t, 1, Config{SizeRatio: 0.99}.normalized().SizeRatio)
	require.EqualValues(t, 1.5, Config{SizeRatio: 1.5}.normalized().SizeRatio)
}

func TestMergeNeedsTwoRuns(t *testing.T) {
	cfg := testConfig()
	cfg.MinMergeRuns = 1
	_, ok := pickMerge(groupSSTs([]*pkindexpb.SSTRef{ref(2, 7, 10)}), cfg.normalized())
	require.False(t, ok)
}

func TestMergeAroundGiantRun(t *testing.T) {
	cfg := testConfig()
	gs := groupSSTs([]*pkindexpb.SSTRef{ref(6, 6, 10), ref(5, 5, 10), ref(4, 4, 10), ref(3, 3, 1000), ref(2, 2, 10), ref(1, 1, 10)})
	_, ok := pickMerge(gs, cfg)
	require.False(t, ok)
	cfg.MinMergeRuns = 3
	c, ok := pickMerge(gs, cfg)
	require.True(t, ok)
	require.Equal(t, 0, c.first)
	require.Equal(t, 2, c.last)
	require.False(t, c.dropTombstones)
}

func TestEvaluateTieOrder(t *testing.T) {
	cfg := testConfig()
	cfg.SpaceAmpRatio = 100
	// four flushed tables and four equal runs: the L0 and merge scores are both 1
	gs := groupSSTs([]*pkindexpb.SSTRef{
		ref(8, 0, 1), ref(7, 0, 1), ref(6, 0, 1), ref(5, 0, 1),
		ref(4, 4, 10), ref(3, 3, 10), ref(2, 2, 10), ref(1, 1, 10),
	})
	l0, ok := pickL0(gs, cfg)
	require.True(t, ok)
	m, ok := pickMerge(gs, cfg)
	require.True(t, ok)
	require.Equal(t, 1.0, l0.score)
	require.Equal(t, 1.0, m.score)
	c, ok := evaluate(gs, cfg)
	require.True(t, ok)
	require.Equal(t, ruleL0, c.rule)
}

func TestOutputIDCount(t *testing.T) {
	require.EqualValues(t, 8, outputIDCount(256<<20, 128<<20))
	require.EqualValues(t, 4, outputIDCount(0, 128<<20))
}

func TestMakePlanTakesTheHighestScore(t *testing.T) {
	m := &pkindexpb.Manifest{
		CollectionId: 1, Vchannel: "v", Version: 9, SstBasePath: "base",
		Ssts: []*pkindexpb.SSTRef{ref(9, 0, 100), ref(8, 3, 50), ref(7, 2, 100)},
	}
	p, ok := makePlan(m, testConfig())
	require.True(t, ok)
	require.Equal(t, ruleSpaceAmp, p.rule)
	require.Len(t, p.inputs, 3)
	require.EqualValues(t, 250, p.inputBytes)
	require.True(t, p.dropTombstones)
	require.EqualValues(t, 9, p.baseVersion)
	require.Equal(t, "base", p.sstBasePath)
	require.NotSame(t, m.GetSsts()[0], p.inputs[0]) // inputs are copies
	require.EqualValues(t, 6, outputIDCount(250, 128<<20))
	_, ok = makePlan(&pkindexpb.Manifest{Ssts: []*pkindexpb.SSTRef{ref(1, 0, 1)}}, testConfig())
	require.False(t, ok)
}
