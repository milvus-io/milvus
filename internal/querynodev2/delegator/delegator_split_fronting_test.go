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
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func pinnedIDs(sealed []SnapshotItem, growing []SegmentEntry) []int64 {
	ids := make([]int64, 0)
	for _, item := range sealed {
		for _, entry := range item.Segments {
			ids = append(ids, entry.SegmentID)
		}
	}
	for _, entry := range growing {
		ids = append(ids, entry.SegmentID)
	}
	return ids
}

// splitTestPartition is the one partition every delegator in these tests holds.
const splitTestPartition = int64(1)

func splitTestEntry(id int64) SegmentEntry {
	return SegmentEntry{NodeID: 1, SegmentID: id, PartitionID: splitTestPartition, Version: 1, TargetVersion: initialTargetVersion, Level: datapb.SegmentLevel_L1}
}

// newSplitScopeDelegator builds a delegator with a real distribution, born the
// way a spawned split child is: unadopted, never synced by querycoord, and so
// not serviceable on its own.
func newSplitScopeDelegator(vchannel string) *shardDelegator {
	return &shardDelegator{
		vchannelName: vchannel,
		lifetime:     lifetime.NewLifetime(lifetime.Working),
		distribution: NewDistribution(vchannel, NewChannelQueryView(nil, nil, []int64{splitTestPartition}, InitialTargetVersion)),
		children:     make(map[string]ShardDelegator),
	}
}

// loadSealed adds sealed segments and makes them readable at the delegator's
// current target version, as a LoadSegments plus the querycoord sync would.
func loadSealed(sd *shardDelegator, ids ...int64) {
	d := sd.distribution
	entries := make([]SegmentEntry, 0, len(ids))
	for _, id := range ids {
		entries = append(entries, splitTestEntry(id))
	}
	d.AddDistributions(entries...)
	d.mut.Lock()
	defer d.mut.Unlock()
	for _, id := range ids {
		entry := d.sealedSegments[id]
		entry.TargetVersion = d.queryView.GetVersion()
		d.sealedSegments[id] = entry
	}
	d.genSnapshot()
}

func loadGrowing(sd *shardDelegator, ids ...int64) {
	entries := make([]SegmentEntry, 0, len(ids))
	for _, id := range ids {
		entries = append(entries, splitTestEntry(id))
	}
	sd.distribution.AddGrowing(entries...)
}

// syncFromCoord stands in for querycoord's SyncTargetVersion: it publishes a
// target version listing the given sealed segments, which marks the view synced
// and, once those segments are loaded, serviceable. A growing segment whose
// flushed twin is in the target is marked redundant by the same call.
func syncFromCoord(sd *shardDelegator, version int64, sealedInTarget ...int64) {
	rowCount := make(map[int64]int64, len(sealedInTarget))
	for _, id := range sealedInTarget {
		rowCount[id] = 1
	}
	sd.distribution.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         version,
		SealedInTarget:        sealedInTarget,
		SealedSegmentRowCount: rowCount,
	}, []int64{splitTestPartition})
	// SyncTargetVersion stamps only the segments the target lists; everything
	// loaded before it keeps the version it had, which the readable filter still
	// admits only at the initial version. Re-stamp what is loaded, as the load
	// path does for a segment that arrives with the target.
	d := sd.distribution
	d.mut.Lock()
	for id := range d.sealedSegments {
		entry := d.sealedSegments[id]
		if entry.TargetVersion != unreadableTargetVersion {
			entry.TargetVersion = version
			d.sealedSegments[id] = entry
		}
	}
	d.genSnapshot()
	d.mut.Unlock()
	d.updateServiceable("test")
}

// adoptAndSync is querycoord adopting a split child and then syncing a target
// version into it: after both it covers its own vchannel on its own.
func adoptAndSync(sd *shardDelegator, version int64, sealedInTarget ...int64) {
	sd.MarkAdopted()
	syncFromCoord(sd, version, sealedInTarget...)
}

// The phase of one read is decided from the family tree that read took, over the
// whole tree and all-or-nothing.
func TestSplitReadPhaseIsDecidedOverTheWholeFamily(t *testing.T) {
	node := func(sd *shardDelegator, children ...*familyNode) *familyNode {
		return &familyNode{sd: sd, children: children}
	}

	t.Run("a read with no fronted delegator is in the fronting phase", func(t *testing.T) {
		assert.Equal(t, splitPhaseFronting, familyPhase(node(newSplitScopeDelegator("v0"))))
		assert.Equal(t, splitPhaseFronting, familyPhase(nil))
	})

	t.Run("an unadopted child keeps the family fronting", func(t *testing.T) {
		child := newSplitScopeDelegator("v1")
		syncFromCoord(child, 7) // synced but never adopted
		assert.False(t, child.adoptedAndSynced())
		assert.Equal(t, splitPhaseFronting, familyPhase(node(newSplitScopeDelegator("v0"), node(child))))
	})

	t.Run("an adopted but unsynced child keeps the family fronting", func(t *testing.T) {
		child := newSplitScopeDelegator("v1")
		child.MarkAdopted()
		assert.False(t, child.adoptedAndSynced())
		assert.Equal(t, splitPhaseFronting, familyPhase(node(newSplitScopeDelegator("v0"), node(child))))
	})

	t.Run("one unsynced sibling keeps the family fronting", func(t *testing.T) {
		synced, unsynced := newSplitScopeDelegator("v1"), newSplitScopeDelegator("v2")
		adoptAndSync(synced, 7)
		require.True(t, synced.adoptedAndSynced())
		assert.Equal(t, splitPhaseFronting,
			familyPhase(node(newSplitScopeDelegator("v0"), node(synced), node(unsynced))))
	})

	t.Run("an unsynced grandchild keeps the family fronting", func(t *testing.T) {
		child, sibling, grandchild := newSplitScopeDelegator("v1"), newSplitScopeDelegator("v2"), newSplitScopeDelegator("v3")
		adoptAndSync(child, 7)
		adoptAndSync(sibling, 7)
		assert.Equal(t, splitPhaseFronting,
			familyPhase(node(newSplitScopeDelegator("v0"), node(child, node(grandchild)), node(sibling))),
			"a cascaded split's grandchild must not be left out of the phase decision")
	})

	t.Run("every delegator below adopted and synced hands over", func(t *testing.T) {
		child, sibling, grandchild := newSplitScopeDelegator("v1"), newSplitScopeDelegator("v2"), newSplitScopeDelegator("v3")
		adoptAndSync(child, 7)
		adoptAndSync(sibling, 7)
		adoptAndSync(grandchild, 7)
		assert.Equal(t, splitPhaseHandover,
			familyPhase(node(newSplitScopeDelegator("v0"), node(child, node(grandchild)), node(sibling))))
	})
}

// Fronting phase: the source reads its own view, and every delegator below it
// adds its growing segments only -- whatever their target version, redundant
// ones included -- minus the IDs the source already pinned.
func TestFrontingPhaseReadsTheSourceViewAndOnlyChildGrowing(t *testing.T) {
	const (
		sourceSealed  = int64(10) // S, the source's own pre-split sealed segment
		sourceGrowing = int64(20)
		childSealed   = int64(11) // O1, the rewrite output, synced into the child
		childGrowing  = int64(21)
		redundant     = int64(30) // growing whose flushed twin is in the child's target
		flushedTwin   = int64(40) // flushed in the child, loaded into the source
		siblingGrow   = int64(22)
	)

	source := newSplitScopeDelegator("v0")
	loadSealed(source, sourceSealed, flushedTwin)
	loadGrowing(source, sourceGrowing)
	syncFromCoord(source, 5, sourceSealed, flushedTwin)
	require.True(t, source.distribution.SyncedAndServiceable())

	// v1 is adopted and synced: it holds O1 and the flushed twins of two of its
	// growing segments.
	child := newSplitScopeDelegator("v1")
	loadSealed(child, childSealed, redundant)
	loadGrowing(child, childGrowing, redundant, flushedTwin)
	adoptAndSync(child, 6, childSealed, redundant)
	require.True(t, child.adoptedAndSynced())

	// v2 has not been adopted yet, which is what keeps the family fronting.
	sibling := newSplitScopeDelegator("v2")
	loadGrowing(sibling, siblingGrow)

	family := &familyNode{sd: source, children: []*familyNode{{sd: child}, {sd: sibling}}}
	scope := frontingSourceScope(family)
	require.Equal(t, splitPhaseFronting, scope.phase)

	sealed, growing, _, version, err := source.pinReadableSegments(scope, 1.0)
	require.NoError(t, err)
	defer source.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{sourceSealed, sourceGrowing, flushedTwin}, pinnedIDs(sealed, growing),
		"the source still reads its own view in the fronting phase")

	sealed, growing, rowCount, version, err := child.pinReadableSegments(scope.forChild(family.children[0]), 1.0)
	require.NoError(t, err)
	defer child.distribution.Unpin(version)
	assert.Empty(t, sealed, "a fronted delegator contributes no sealed segment in the fronting phase")
	assert.Empty(t, rowCount, "and reports no sealed row count for segments it was not asked to read")
	assert.ElementsMatch(t, []int64{childGrowing, redundant}, pinnedIDs(sealed, growing),
		"a redundant growing segment is the only copy of its rows until the source loads its twin, so it is read; "+
			"the twin the source already pinned is not read twice; and the child's own sealed view (O1) is not read at all")

	sealed, growing, _, version, err = sibling.pinReadableSegments(scope.forChild(family.children[1]), 1.0)
	require.NoError(t, err)
	defer sibling.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{siblingGrow}, pinnedIDs(sealed, growing))
}

// Handover phase: every delegator below the source has been adopted and synced,
// so the source contributes nothing of its own and each of them reads its full
// view, each segment exactly once.
func TestHandoverPhaseReadsOnlyTheDelegatorsBelow(t *testing.T) {
	const (
		staleSourceSealed = int64(10) // S, the pre-rewrite copy of the same rows
		child1Sealed      = int64(11) // O1
		child1Growing     = int64(21)
		child2Sealed      = int64(12) // O2
		child2Growing     = int64(22)
	)

	source := newSplitScopeDelegator("v0")
	loadSealed(source, staleSourceSealed)
	syncFromCoord(source, 5, staleSourceSealed)

	child1 := newSplitScopeDelegator("v1")
	loadSealed(child1, child1Sealed)
	loadGrowing(child1, child1Growing)
	adoptAndSync(child1, 6, child1Sealed)

	child2 := newSplitScopeDelegator("v2")
	loadSealed(child2, child2Sealed)
	loadGrowing(child2, child2Growing)
	adoptAndSync(child2, 6, child2Sealed)

	family := &familyNode{sd: source, children: []*familyNode{{sd: child1}, {sd: child2}}}
	scope := frontingSourceScope(family)
	require.Equal(t, splitPhaseHandover, scope.phase)

	sealed, growing, rowCount, version, err := source.pinReadableSegments(scope, 1.0)
	require.NoError(t, err)
	defer source.distribution.Unpin(version)
	assert.Empty(t, pinnedIDs(sealed, growing), "the source reads nothing of its own in the handover phase")
	assert.Empty(t, rowCount)
	assert.Empty(t, scope.pinned, "and records nothing, so the delegators below read with no exclusion")

	var all []int64
	for i, child := range []*shardDelegator{child1, child2} {
		sealed, growing, _, version, err := child.pinReadableSegments(scope.forChild(family.children[i]), 1.0)
		require.NoError(t, err)
		defer child.distribution.Unpin(version)
		all = append(all, pinnedIDs(sealed, growing)...)
	}
	assert.ElementsMatch(t, []int64{child1Sealed, child1Growing, child2Sealed, child2Growing}, all,
		"each delegator below reads its full view, each segment exactly once, and no source segment is served")
}

// Between a cascaded split's adoption and its current-target flip a relabeled
// sealed segment is readable in both a child's view and its own child's, and the
// child keeps fronting it until it is released. A read must count it once: every
// delegator skips what a delegator read before it already pinned.
func TestFrontedReadSkipsSegmentsTheSourceAlreadyPinned(t *testing.T) {
	const (
		shared            = int64(100) // relabeled sealed segment, in both views
		grandchildOnly    = int64(200) // sealed, only the grandchild holds it
		sharedGrowing     = int64(300) // growing, in both views
		grandchildGrowing = int64(400)
	)

	source := newSplitScopeDelegator("v0")
	syncFromCoord(source, 5)

	child := newSplitScopeDelegator("v1")
	loadSealed(child, shared)
	loadGrowing(child, sharedGrowing)
	adoptAndSync(child, 6, shared)

	grandchild := newSplitScopeDelegator("v3")
	loadSealed(grandchild, shared, grandchildOnly)
	loadGrowing(grandchild, sharedGrowing, grandchildGrowing)
	adoptAndSync(grandchild, 6, shared, grandchildOnly)

	childNode := &familyNode{sd: child, children: []*familyNode{{sd: grandchild}}}
	family := &familyNode{sd: source, children: []*familyNode{childNode}}
	scope := frontingSourceScope(family)
	require.Equal(t, splitPhaseHandover, scope.phase)

	childScope := scope.forChild(childNode)
	sealed, growing, _, version, err := child.pinReadableSegments(childScope, 1.0)
	require.NoError(t, err)
	defer child.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{shared, sharedGrowing}, pinnedIDs(sealed, growing))

	sealed, growing, _, version, err = grandchild.pinReadableSegments(childScope.forChild(childNode.children[0]), 1.0)
	require.NoError(t, err)
	defer grandchild.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{grandchildOnly, grandchildGrowing}, pinnedIDs(sealed, growing),
		"a segment a delegator read before already pinned must not be read again")
}

// A read with no fronting in play pins exactly what it pinned before.
func TestUnfrontedReadPinsEverything(t *testing.T) {
	child := newSplitScopeDelegator("v1")
	loadGrowing(child, 7)

	_, growing, _, version, err := child.pinReadableSegments(splitReadScope{asChild: true}, 1.0)
	require.NoError(t, err)
	defer child.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{7}, pinnedIDs(nil, growing))

	_, _, _, _, err = child.pinReadableSegments(splitReadScope{}, 1.0)
	assert.Error(t, err, "without the fronting bypass a non-serviceable child refuses the pin")
}

// The source records its pins only when it has delegators below to exclude them
// from, so an ordinary read allocates nothing extra; and the phase it decided
// travels down unchanged.
func TestFrontingSourceScope(t *testing.T) {
	plain := frontingSourceScope(nil)
	assert.Nil(t, plain.pinned)
	assert.False(t, plain.asChild)
	assert.Equal(t, splitPhaseFronting, plain.phase)

	assert.Nil(t, plain.family.fronted())

	leaf := &familyNode{sd: &shardDelegator{vchannelName: "v0"}}
	assert.Nil(t, frontingSourceScope(leaf).pinned, "a read with no fronted delegator allocates no pin set")

	grandchild := &familyNode{sd: &shardDelegator{vchannelName: "v3"}}
	child := &familyNode{sd: &shardDelegator{vchannelName: "v1"}, children: []*familyNode{grandchild}}
	sibling := &familyNode{sd: &shardDelegator{vchannelName: "v2"}}
	root := &familyNode{sd: &shardDelegator{vchannelName: "v0"}, children: []*familyNode{child, sibling}}
	fronting := frontingSourceScope(root)
	assert.NotNil(t, fronting.pinned)
	assert.False(t, fronting.asChild)
	assert.Same(t, root, fronting.family)
	assert.Equal(t, []*familyNode{child, grandchild, sibling}, root.descendants(), "depth first, grandchildren included")

	childScope := fronting.forChild(child)
	assert.True(t, childScope.asChild)
	assert.Same(t, child, childScope.family)
	assert.Equal(t, fronting.phase, childScope.phase, "the read's phase travels down unchanged")

	handover := splitReadScope{phase: splitPhaseHandover, pinned: typeutil.NewUniqueSet(), family: root}
	assert.Equal(t, splitPhaseHandover, handover.forChild(child).phase)
	// every delegator of the read shares one pin set: each skips what any
	// delegator read before it pinned, and records its own pins for the rest.
	childScope.pinned.Insert(7)
	assert.True(t, fronting.pinned.Contain(7))
	assert.True(t, childScope.exclude.Contain(7))
}

// statsFamily is a source fronting two children, wired for a full public read.
type statsFamily struct {
	source, child1, child2 *shardDelegator
	rows                   map[int64]int64
	asked                  *typeutil.ConcurrentMap[int64, int64]
}

// newStatsFamily builds v0{v1,v2} with a worker that answers GetStatistics with
// the row count of exactly the segments it was asked for, so a duplicated or
// missed segment shows up in the total.
func newStatsFamily(t *testing.T) *statsFamily {
	paramtable.Init()
	paramtable.SetNodeID(1)

	f := &statsFamily{
		source: newSplitScopeDelegator("v0"),
		child1: newSplitScopeDelegator("v1"),
		child2: newSplitScopeDelegator("v2"),
		rows: map[int64]int64{
			10: 1000,        // S, the source's own sealed segment
			20: 200,         // the source's growing
			11: 700, 21: 70, // O1 and v1's growing
			12: 300, 22: 30, // O2 and v2's growing
		},
		asked: typeutil.NewConcurrentMap[int64, int64](),
	}

	worker := &cluster.MockWorker{}
	worker.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
		RunAndReturn(func(_ context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
			total := int64(0)
			for _, id := range req.GetSegmentIDs() {
				count, _ := f.asked.GetOrInsert(id, 0)
				f.asked.Insert(id, count+1)
				total += f.rows[id]
			}
			return &internalpb.GetStatisticsResponse{
				Stats: []*commonpb.KeyValuePair{{Key: "row_count", Value: strconv.FormatInt(total, 10)}},
			}, nil
		})
	manager := &cluster.MockManager{}
	manager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(worker, nil).Maybe()

	for _, sd := range []*shardDelegator{f.source, f.child1, f.child2} {
		sd.workerManager = manager
		sd.latestTsafe = atomic.NewUint64(1)
		sd.latestRequiredMVCCTimeTick = atomic.NewUint64(0)
	}
	f.source.children["v1"] = f.child1
	f.source.children["v2"] = f.child2
	f.child1.SetFrontingParent(f.source)
	f.child2.SetFrontingParent(f.source)

	loadSealed(f.source, 10)
	loadGrowing(f.source, 20)
	syncFromCoord(f.source, 5, 10)
	loadSealed(f.child1, 11)
	loadGrowing(f.child1, 21)
	loadSealed(f.child2, 12)
	loadGrowing(f.child2, 22)
	return f
}

// total is the row count the source returned, summed over the fan-out's
// responses, plus the segment IDs each worker was asked for.
func (f *statsFamily) total(t *testing.T, results []*internalpb.GetStatisticsResponse) int64 {
	sum := int64(0)
	for _, result := range results {
		for _, stat := range result.GetStats() {
			if stat.GetKey() != "row_count" {
				continue
			}
			count, err := strconv.ParseInt(stat.GetValue(), 10, 64)
			require.NoError(t, err)
			sum += count
		}
	}
	f.asked.Range(func(id int64, times int64) bool {
		assert.EqualValues(t, 1, times, "segment %d was read %d times in one statistics request", id, times)
		return true
	})
	return sum
}

// GetStatistics counts every row of the split shard exactly once, in both
// phases: the fan-out never asks two delegators for the same segment, and never
// leaves a segment out.
func TestGetStatisticsCountsEveryRowOnceInBothPhases(t *testing.T) {
	ctx := context.Background()
	req := func() *querypb.GetStatisticsRequest {
		return &querypb.GetStatisticsRequest{Req: &internalpb.GetStatisticsRequest{}, DmlChannels: []string{"v0"}}
	}

	t.Run("fronting", func(t *testing.T) {
		f := newStatsFamily(t)
		// v1 is adopted and synced, v2 is not: the family stays fronting.
		adoptAndSync(f.child1, 6, 11)

		results, err := f.source.GetStatistics(ctx, req())
		require.NoError(t, err)
		// S + the source's growing + both children's growing. O1 is the rewrite
		// of rows S still holds, so it must NOT be counted on top of S.
		assert.EqualValues(t, f.rows[10]+f.rows[20]+f.rows[21]+f.rows[22], f.total(t, results))
	})

	t.Run("handover", func(t *testing.T) {
		f := newStatsFamily(t)
		adoptAndSync(f.child1, 6, 11)
		adoptAndSync(f.child2, 6, 12)

		results, err := f.source.GetStatistics(ctx, req())
		require.NoError(t, err)
		// O1 + O2 and both children's growing, and nothing of the source's own
		// stale view.
		assert.EqualValues(t, f.rows[11]+f.rows[21]+f.rows[12]+f.rows[22], f.total(t, results))
	})
}

// R7: the phase comes from one snapshot per read. A delegator querycoord syncs
// while the read is in flight must not move the rest of the family into the
// handover phase, which would drop the source's view after it was already read.
func TestSplitReadPhaseIsDecidedOnceEvenWhenAChildSyncsMidRead(t *testing.T) {
	f := newStatsFamily(t)
	adoptAndSync(f.child1, 6, 11)

	var mu sync.Mutex
	phases := map[string]splitPhase{}
	var origin func(*shardDelegator, splitReadScope, float64, ...int64) ([]SnapshotItem, []SegmentEntry, map[int64]int64, int64, error)
	synced := atomic.NewBool(false)
	hook := mockey.Mock((*shardDelegator).pinReadableSegments).To(
		func(sd *shardDelegator, scope splitReadScope, ratio float64, partitions ...int64) ([]SnapshotItem, []SegmentEntry, map[int64]int64, int64, error) {
			mu.Lock()
			phases[sd.vchannelName] = scope.phase
			mu.Unlock()
			if sd == f.source && synced.CompareAndSwap(false, true) {
				// querycoord adopts and syncs the last child right after the
				// source has read its own view.
				adoptAndSync(f.child2, 6, 12)
			}
			return origin(sd, scope, ratio, partitions...)
		}).Origin(&origin).Build()
	defer hook.UnPatch()

	results, err := f.source.GetStatistics(context.Background(),
		&querypb.GetStatisticsRequest{Req: &internalpb.GetStatisticsRequest{}, DmlChannels: []string{"v0"}})
	require.NoError(t, err)
	require.True(t, synced.Load())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, map[string]splitPhase{
		"v0": splitPhaseFronting, "v1": splitPhaseFronting, "v2": splitPhaseFronting,
	}, phases, "every delegator of one read uses the phase that read decided")
	assert.EqualValues(t, f.rows[10]+f.rows[20]+f.rows[21]+f.rows[22], f.total(t, results),
		"a mid-read sync must not make the read drop the source's view it had already served")
}

// A cascaded split read: v0 fronts v1 and v2, and v1 fronts v3. The phase covers
// v3 too, and the fan-out contributes each delegator's share once.
func TestCascadedFamilyReadsInOnePhase(t *testing.T) {
	build := func() (source, child, sibling, grandchild *shardDelegator, family *familyNode) {
		source = newSplitScopeDelegator("v0")
		loadSealed(source, 10)
		loadGrowing(source, 20)
		syncFromCoord(source, 5, 10)

		child = newSplitScopeDelegator("v1")
		loadSealed(child, 11)
		loadGrowing(child, 21)

		sibling = newSplitScopeDelegator("v2")
		loadSealed(sibling, 12)
		loadGrowing(sibling, 22)

		grandchild = newSplitScopeDelegator("v3")
		loadSealed(grandchild, 13)
		loadGrowing(grandchild, 23)

		family = &familyNode{sd: source, children: []*familyNode{
			{sd: child, children: []*familyNode{{sd: grandchild}}},
			{sd: sibling},
		}}
		return
	}

	pinAll := func(t *testing.T, family *familyNode) ([]int64, splitPhase) {
		scope := frontingSourceScope(family)
		var ids []int64
		sealed, growing, _, version, err := family.sd.pinReadableSegments(scope, 1.0)
		require.NoError(t, err)
		defer family.sd.distribution.Unpin(version)
		ids = append(ids, pinnedIDs(sealed, growing)...)
		for _, node := range family.descendants() {
			sealed, growing, _, version, err := node.sd.pinReadableSegments(scope.forChild(node), 1.0)
			require.NoError(t, err)
			defer node.sd.distribution.Unpin(version)
			ids = append(ids, pinnedIDs(sealed, growing)...)
		}
		return ids, scope.phase
	}

	t.Run("an unsynced grandchild keeps the whole family fronting", func(t *testing.T) {
		source, child, sibling, _, family := build()
		adoptAndSync(child, 6, 11)
		adoptAndSync(sibling, 6, 12)
		_ = source

		ids, phase := pinAll(t, family)
		assert.Equal(t, splitPhaseFronting, phase)
		assert.ElementsMatch(t, []int64{10, 20, 21, 22, 23}, ids,
			"the source's own view plus the growing of every delegator below it, at any depth")
	})

	t.Run("a fully synced family hands over at every depth", func(t *testing.T) {
		_, child, sibling, grandchild, family := build()
		adoptAndSync(child, 6, 11)
		adoptAndSync(sibling, 6, 12)
		adoptAndSync(grandchild, 6, 13)

		ids, phase := pinAll(t, family)
		assert.Equal(t, splitPhaseHandover, phase)
		assert.ElementsMatch(t, []int64{11, 21, 12, 22, 13, 23}, ids,
			"every delegator below reads its full view and the source contributes nothing")
	})
}
