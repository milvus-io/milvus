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

package featureusage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// The counter id set is a compile-time constant: every Feature has a
// non-empty, unique name, and the array is exactly numFeatures long.
func TestFeatureNamesComplete(t *testing.T) {
	names := make(map[string]Feature, numFeatures)
	for f := Feature(0); f < numFeatures; f++ {
		require.NotEmpty(t, f.Name(), "feature %d has no name", f)
		// Buckets of one distribution share a name; name and bucket together
		// identify a counter.
		n := f.Name() + "|" + f.Bucket()
		prev, dup := names[n]
		require.False(t, dup, "feature %d and %d share name %q", prev, f, n)
		names[n] = f
	}
	assert.Equal(t, int(numFeatures), NumFeatures())
	assert.Equal(t, int(numFeatures), len(featureNames))
	assert.Equal(t, "", Feature(-1).Name())
	assert.Equal(t, "", numFeatures.Name())
}

func TestPerValueFeaturesFoldUnknown(t *testing.T) {
	// function_score names are user strings at the counting point; anything
	// unrecognized must land in the _other slot and must not create a slot.
	assert.Equal(t, FeatureFunctionScoreRRF, FunctionScoreFeature("rrf"))
	assert.Equal(t, FeatureFunctionScoreRRF, FunctionScoreFeature("RRF"))
	assert.Equal(t, FeatureFunctionScoreWeighted, FunctionScoreFeature("weighted"))
	assert.Equal(t, FeatureFunctionScoreDecay, FunctionScoreFeature("decay"))
	assert.Equal(t, FeatureFunctionScoreModel, FunctionScoreFeature("model"))
	assert.Equal(t, FeatureFunctionScoreBoost, FunctionScoreFeature("boost"))
	assert.Equal(t, FeatureFunctionScoreOther, FunctionScoreFeature("whatever the client sent"))
	assert.Equal(t, FeatureFunctionScoreOther, FunctionScoreFeature(""))

	assert.Equal(t, FeatureRankStrategyRRF, RankStrategyFeature("rrf"))
	assert.Equal(t, FeatureRankStrategyWeighted, RankStrategyFeature(" Weighted "))
	assert.Equal(t, FeatureRankStrategyOther, RankStrategyFeature("anything"))

	f, ok := ConsistencyLevelFeature(commonpb.ConsistencyLevel_Strong)
	assert.True(t, ok)
	assert.Equal(t, FeatureConsistencyStrong, f)
	_, ok = ConsistencyLevelFeature(commonpb.ConsistencyLevel(99))
	assert.False(t, ok)

	f, ok = HighlighterFeature(commonpb.HighlightType_Semantic)
	assert.True(t, ok)
	assert.Equal(t, FeatureHighlighterSemantic, f)
	_, ok = HighlighterFeature(commonpb.HighlightType(99))
	assert.False(t, ok)
}

func TestRolesAndDataNodeMappings(t *testing.T) {
	assert.Equal(t, RoleProxy, FeatureGroupByField.Role())
	assert.Equal(t, RoleProxy, FeatureTextMatch.Role())
	assert.Equal(t, RoleMixCoord, FeatureImportParquet.Role())
	assert.Equal(t, RoleMixCoord, FeatureCompactionOther.Role())
	assert.Equal(t, RoleQueryNode, FeatureTwoStageSearch.Role())
	assert.Equal(t, RoleProxy, Feature(-1).Role())

	f, ok := ImportFileTypeFeature("Parquet")
	assert.True(t, ok)
	assert.Equal(t, FeatureImportParquet, f)
	_, ok = ImportFileTypeFeature("Invalid")
	assert.False(t, ok, "invalid file type creates no slot")
	_, ok = ImportFileTypeFeature("parquet")
	assert.False(t, ok, "names are the FileType String() values, case-sensitive")

	assert.Equal(t, FeatureCompactionMix, CompactionTypeFeature(datapb.CompactionType_MixCompaction))
	assert.Equal(t, FeatureCompactionClustering, CompactionTypeFeature(datapb.CompactionType_ClusteringCompaction))
	assert.Equal(t, FeatureCompactionOther, CompactionTypeFeature(datapb.CompactionType_UndefinedCompaction))
	assert.Equal(t, FeatureCompactionOther, CompactionTypeFeature(datapb.CompactionType(999)))

	// Role-scoped snapshots partition the counter set without overlap.
	c := NewCounters()
	c.Hit(FeatureIterator)
	c.Hit(FeatureImportCSV)
	c.Hit(FeatureSegmentPrune)
	proxy := index(c.SnapshotFor(RoleProxy))
	dn := index(c.SnapshotFor(RoleMixCoord))
	qn := index(c.SnapshotFor(RoleQueryNode))
	assert.Equal(t, NumFeatures(), len(proxy)+len(dn)+len(qn))
	assert.Contains(t, proxy, "search_iterator=v1")
	assert.NotContains(t, proxy, "import_file_type=CSV")
	assert.NotContains(t, proxy, "segment_prune")
	assert.Contains(t, dn, "import_file_type=CSV")
	assert.NotContains(t, dn, "search_iterator=v1")
	assert.Contains(t, qn, "segment_prune")
	assert.EqualValues(t, 1, qn["segment_prune"].Value)
	assert.Len(t, qn, 3, "the three QueryNode execution-path counters")
	assert.EqualValues(t, 1, dn["import_file_type=CSV"].Value)
	assert.EqualValues(t, 1, proxy["search_iterator=v1"].Value)
	for name, e := range dn {
		assert.Equal(t, GroupRequest, e.Group, name)
	}
	assert.Len(t, SnapshotFor(RoleMixCoord), len(dn))
}

func TestCountersHitAndSnapshot(t *testing.T) {
	now := int64(1000)
	c := newCountersWithClock(func() int64 { return now })

	// Fresh: every feature present, all zero.
	snap := c.Snapshot()
	require.Len(t, snap, NumFeatures())
	for _, e := range snap {
		assert.Equal(t, GroupRequest, e.Group)
		assert.Zero(t, e.Value)
		assert.Zero(t, e.LastUsedAt)
	}

	c.Hit(FeatureGroupByField)
	c.Hit(FeatureGroupByField)
	now = 1001
	c.Hit(FeatureIterator)

	byName := index(c.Snapshot())
	assert.EqualValues(t, 2, byName["grouping_search"].Value)
	assert.EqualValues(t, 1000, byName["grouping_search"].LastUsedAt)
	assert.EqualValues(t, 1, byName["search_iterator=v1"].Value)
	assert.EqualValues(t, 1001, byName["search_iterator=v1"].LastUsedAt)
	assert.EqualValues(t, 0, byName["range_search"].Value)

	// A snapshot is a read: nothing changes.
	again := index(c.Snapshot())
	assert.EqualValues(t, 2, again["grouping_search"].Value)

	// Out-of-range features are ignored, not stored.
	c.Hit(Feature(-1))
	c.Hit(numFeatures)
	assert.Len(t, c.Snapshot(), NumFeatures())
}

func TestCountersTimestampStoredAtMostOncePerSecond(t *testing.T) {
	clockReads := 0
	stores := 0
	c := newCountersWithClock(func() int64 { clockReads++; return 5 })
	// Observe stores by wrapping: after the first hit the stored second equals
	// the clock, so subsequent hits in the same second must not store again.
	c.Hit(FeatureNamespace)
	first := c.slots[FeatureNamespace].lastUsedAt.Load()
	for i := 0; i < 100; i++ {
		before := c.slots[FeatureNamespace].lastUsedAt.Load()
		c.Hit(FeatureNamespace)
		if c.slots[FeatureNamespace].lastUsedAt.Load() != before {
			stores++
		}
	}
	assert.EqualValues(t, 5, first)
	assert.Equal(t, 0, stores, "same-second hits must not re-store the timestamp")
	assert.Equal(t, 101, clockReads)
	assert.EqualValues(t, 101, c.slots[FeatureNamespace].value.Load())
}

// A hit whose clock read happened before a later hit stored its second must
// not move last_used_at backwards. The clock here returns 9, then 7: the
// second hit plays the goroutine that read the clock earlier and ran later.
func TestCountersTimestampNeverMovesBackwards(t *testing.T) {
	seconds := []int64{9, 7, 9, 12, 3}
	i := 0
	c := newCountersWithClock(func() int64 { v := seconds[i]; i++; return v })
	want := []int64{9, 9, 9, 12, 12}
	for n := range seconds {
		c.Hit(FeatureNamespace)
		assert.EqualValues(t, want[n], c.slots[FeatureNamespace].lastUsedAt.Load(), "after hit %d", n)
	}
	assert.EqualValues(t, len(seconds), c.slots[FeatureNamespace].value.Load(), "every hit still counts")
}

// Under contention from many goroutines with interleaved clock reads, the
// stored second ends at the largest one any goroutine read.
func TestCountersTimestampConcurrentMonotonic(t *testing.T) {
	var ts atomic.Int64
	var wg sync.WaitGroup
	for g := int64(1); g <= 64; g++ {
		wg.Add(1)
		go func(now int64) {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				advanceTo(&ts, now)
			}
		}(g)
	}
	wg.Wait()
	assert.EqualValues(t, 64, ts.Load())
}

func TestCountersConcurrent(t *testing.T) {
	c := NewCounters()
	var wg sync.WaitGroup
	const goroutines, perG = 16, 1000
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				c.Hit(FeatureRangeSearch)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, goroutines*perG, index(c.Snapshot())["range_search"].Value)
}

func TestGlobalEnabledSwitch(t *testing.T) {
	defer SetEnabled(true)
	before := index(Snapshot())["search_iterator=v2"].Value

	SetEnabled(false)
	assert.False(t, Enabled())
	Hit(FeatureSearchIterV2)
	assert.Equal(t, before, index(Snapshot())["search_iterator=v2"].Value)

	SetEnabled(true)
	assert.True(t, Enabled())
	Hit(FeatureSearchIterV2)
	assert.Equal(t, before+1, index(Snapshot())["search_iterator=v2"].Value)
}
