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

package routing

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// commitShard builds a CollectionShardInfo owning the given residues.
func commitShard(state schemapb.ShardState, buckets ...uint64) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state}
	if len(buckets) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: buckets}}
	}
	return si
}

func commitColl(vchannels []string, modulus uint64, infos ...*model.ShardInfo) *model.Collection {
	coll := &model.Collection{Name: "c", VirtualChannelNames: vchannels, RoutingModulus: modulus, ShardInfos: map[string]*model.ShardInfo{}}
	for _, info := range infos {
		coll.ShardInfos[info.VChannelName] = info
	}
	return coll
}

func modelShard(vchannel string, state schemapb.ShardState, buckets ...uint64) *model.ShardInfo {
	return &model.ShardInfo{VChannelName: vchannel, State: state, Buckets: buckets}
}

// postImage builds a routing post-image from (vchannel, shard info) pairs, in
// order. Each vchannel sits on a pchannel of its own name: the judge asks only
// that the pchannel list be parallel.
func postImage(modulus uint64, taskID int64, entries ...any) *messagespb.AlterCollectionMessageUpdates {
	u := &messagespb.AlterCollectionMessageUpdates{RoutingModulus: modulus, ShardBy: "hash(pk)", SplitTaskId: taskID}
	for i := 0; i+1 < len(entries); i += 2 {
		vchannel, info := entries[i].(string), entries[i+1].(*schemapb.CollectionShardInfo)
		u.VirtualChannelNames = append(u.VirtualChannelNames, vchannel)
		u.PhysicalChannelNames = append(u.PhysicalChannelNames, "p-"+vchannel)
		u.ShardInfos = append(u.ShardInfos, info)
	}
	return u
}

func TestCommitAlreadyApplied(t *testing.T) {
	coll := commitColl([]string{"v0", "v1"}, 2,
		modelShard("v0", schemapb.ShardState_ShardNormal, 0),
		modelShard("v1", schemapb.ShardState_ShardNormal, 1))
	coll.ShardBy = "hash(pk)"
	req := func(modulus uint64, left, right []uint64) *messagespb.AlterCollectionMessageUpdates {
		return &messagespb.AlterCollectionMessageUpdates{
			VirtualChannelNames: []string{"v0", "v1"},
			RoutingModulus:      modulus,
			ShardInfos: []*schemapb.CollectionShardInfo{
				commitShard(schemapb.ShardState_ShardNormal, left...),
				commitShard(schemapb.ShardState_ShardNormal, right...),
			},
		}
	}

	require.True(t, commitAlreadyApplied(coll, req(2, []uint64{0}, []uint64{1})))
	// A rebase onto a doubled modulus leaves every state alone and changes only
	// the residues. Comparing states alone would call this already committed.
	require.False(t, commitAlreadyApplied(coll, req(4, []uint64{0, 2}, []uint64{1, 3})))
	require.False(t, commitAlreadyApplied(coll, req(2, []uint64{1}, []uint64{0})))
	backfill := req(2, []uint64{0}, []uint64{1})
	backfill.ShardBy = NamespaceShardBy
	require.False(t, commitAlreadyApplied(coll, backfill))
	unknown := req(2, []uint64{0}, []uint64{1})
	unknown.VirtualChannelNames = []string{"v0", "v9"}
	require.False(t, commitAlreadyApplied(coll, unknown))
}

func TestShardStateReachableForward(t *testing.T) {
	all := []schemapb.ShardState{
		schemapb.ShardState_ShardNormal,
		schemapb.ShardState_ShardCreating,
		schemapb.ShardState_ShardSplitting,
		schemapb.ShardState_ShardDropped,
	}
	for _, s := range all {
		require.True(t, shardStateReachableForward(s, s), s.String())
	}
	require.True(t, shardStateReachableForward(schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardSplitting))
	require.True(t, shardStateReachableForward(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardNormal))
	// A target is adopted and may then be split: two commits away, still forward.
	require.True(t, shardStateReachableForward(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardSplitting))
	// A fenced source leaves only by being delisted; a listed Dropped is refused
	// before any transition is asked about (CheckNoListedDroppedShard).
	require.False(t, shardStateReachableForward(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardDropped))
	// A target is write-routable from the moment it is published: no abandoning it.
	require.False(t, shardStateReachableForward(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardDropped))
	require.False(t, shardStateReachableForward(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardNormal))
	require.False(t, shardStateReachableForward(schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating))
	require.False(t, shardStateReachableForward(schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardSplitting))
	require.False(t, shardStateReachableForward(schemapb.ShardState(99), schemapb.ShardState_ShardNormal))
}

func TestReexpressResidues(t *testing.T) {
	assert.Equal(t, []uint64{}, reexpressResidues(nil, 2, 2))
	assert.Equal(t, []uint64{1}, reexpressResidues([]uint64{1}, 2, 2))
	assert.Equal(t, []uint64{1, 3}, reexpressResidues([]uint64{1}, 2, 4))
	assert.Equal(t, []uint64{0, 1, 3, 4, 5, 7}, reexpressResidues([]uint64{1, 0, 3}, 4, 8))
	assert.Equal(t, []uint64{2}, reexpressResidues([]uint64{2}, 0, 4), "a legacy modulus is left alone")
}

// requireIncoherent asserts a refusal names an incoherent post-image: System,
// not retriable, and never read as "wait for an earlier commit".
func requireIncoherent(t *testing.T, err error, contains string) {
	t.Helper()
	require.ErrorIs(t, err, merr.ErrServiceInternal, "a planning bug, not the content of a user request")
	require.False(t, merr.IsRetryableErr(err))
	require.False(t, errors.Is(err, ErrCommitAheadOfCollection))
	require.False(t, errors.Is(err, ErrCommitAlreadyApplied))
	if contains != "" {
		assert.Contains(t, err.Error(), contains)
	}
}

// requireAhead asserts a refusal waits for a routing commit this cluster has
// not applied.
func requireAhead(t *testing.T, err error, contains string) {
	t.Helper()
	require.True(t, errors.Is(err, ErrCommitAheadOfCollection), "%v", err)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, merr.IsRetryableErr(err), "the commit it waits for is still being applied")
	if contains != "" {
		assert.Contains(t, err.Error(), contains)
	}
}

func requireApplied(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	require.True(t, errors.Is(err, ErrCommitAlreadyApplied), append([]any{"%v", err}, msgAndArgs...)...)
}

// The fixtures: a two-shard collection [a, b] at modulus 4 (a owns {0,2}, b
// owns {1,3}); split 1 splits a into a1 {0} and a2 {2}; adoption 1 retires a;
// split 2 splits b into b1 {1} and b2 {3}; adoption 2 retires b. Each commit's
// post-image is the primary's meta right after it, in WAL order.
const (
	fa, fb, fa1, fa2, fb1, fb2 = "va", "vb", "va1", "va2", "vb1", "vb2"
	task1, task2               = int64(1), int64(2)
)

var (
	split1Delta    = SplitDelta(fa, []string{fa1, fa2}, false)
	adoption1Delta = AdoptionDelta(fa, []string{fa1, fa2}, true)
	split2Delta    = SplitDelta(fb, []string{fb1, fb2}, false)
	adoption2Delta = AdoptionDelta(fb, []string{fb1, fb2}, true)
)

func initial() *model.Collection {
	coll := commitColl([]string{fa, fb}, 4,
		modelShard(fa, schemapb.ShardState_ShardNormal, 0, 2),
		modelShard(fb, schemapb.ShardState_ShardNormal, 1, 3))
	coll.ShardBy = "hash(pk)"
	return coll
}

func afterSplit1() *model.Collection {
	coll := commitColl([]string{fa, fa1, fa2, fb}, 4,
		modelShard(fa, schemapb.ShardState_ShardSplitting),
		modelShard(fa1, schemapb.ShardState_ShardCreating, 0),
		modelShard(fa2, schemapb.ShardState_ShardCreating, 2),
		modelShard(fb, schemapb.ShardState_ShardNormal, 1, 3))
	coll.ShardBy = "hash(pk)"
	return coll
}

func afterAdoption1() *model.Collection {
	coll := commitColl([]string{fa1, fa2, fb}, 4,
		modelShard(fa1, schemapb.ShardState_ShardNormal, 0),
		modelShard(fa2, schemapb.ShardState_ShardNormal, 2),
		modelShard(fb, schemapb.ShardState_ShardNormal, 1, 3))
	coll.ShardBy = "hash(pk)"
	return coll
}

func afterSplit2() *model.Collection {
	coll := commitColl([]string{fa1, fa2, fb, fb1, fb2}, 4,
		modelShard(fa1, schemapb.ShardState_ShardNormal, 0),
		modelShard(fa2, schemapb.ShardState_ShardNormal, 2),
		modelShard(fb, schemapb.ShardState_ShardSplitting),
		modelShard(fb1, schemapb.ShardState_ShardCreating, 1),
		modelShard(fb2, schemapb.ShardState_ShardCreating, 3))
	coll.ShardBy = "hash(pk)"
	return coll
}

func afterAdoption2() *model.Collection {
	coll := commitColl([]string{fa1, fa2, fb1, fb2}, 4,
		modelShard(fa1, schemapb.ShardState_ShardNormal, 0),
		modelShard(fa2, schemapb.ShardState_ShardNormal, 2),
		modelShard(fb1, schemapb.ShardState_ShardNormal, 1),
		modelShard(fb2, schemapb.ShardState_ShardNormal, 3))
	coll.ShardBy = "hash(pk)"
	return coll
}

func split1Image() *messagespb.AlterCollectionMessageUpdates {
	return postImage(4, 0,
		fa, commitShard(schemapb.ShardState_ShardSplitting),
		fa1, commitShard(schemapb.ShardState_ShardCreating, 0),
		fa2, commitShard(schemapb.ShardState_ShardCreating, 2),
		fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
}

func adoption1Image() *messagespb.AlterCollectionMessageUpdates {
	return postImage(4, task1,
		fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
		fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
		fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
}

func split2Image() *messagespb.AlterCollectionMessageUpdates {
	return postImage(4, 0,
		fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
		fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
		fb, commitShard(schemapb.ShardState_ShardSplitting),
		fb1, commitShard(schemapb.ShardState_ShardCreating, 1),
		fb2, commitShard(schemapb.ShardState_ShardCreating, 3))
}

func adoption2Image() *messagespb.AlterCollectionMessageUpdates {
	return postImage(4, task2,
		fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
		fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
		fb1, commitShard(schemapb.ShardState_ShardNormal, 1),
		fb2, commitShard(schemapb.ShardState_ShardNormal, 3))
}

// TestJudgeCommitInOrder: each commit applied from exactly its pre-image is a
// forward step, and redelivered onto its own post-image is already applied.
func TestJudgeCommitInOrder(t *testing.T) {
	require.NoError(t, JudgeCommit(initial(), split1Image(), split1Delta))
	requireApplied(t, JudgeCommit(afterSplit1(), split1Image(), split1Delta))
	require.NoError(t, JudgeCommit(afterSplit1(), adoption1Image(), adoption1Delta))
	requireApplied(t, JudgeCommit(afterAdoption1(), adoption1Image(), adoption1Delta))
	require.NoError(t, JudgeCommit(afterAdoption1(), split2Image(), split2Delta))
	requireApplied(t, JudgeCommit(afterSplit2(), split2Image(), split2Delta))
	require.NoError(t, JudgeCommit(afterSplit2(), adoption2Image(), adoption2Delta))
	requireApplied(t, JudgeCommit(afterAdoption2(), adoption2Image(), adoption2Delta))

	t.Run("a split of a never-split collection doubles the modulus and re-expresses the rest", func(t *testing.T) {
		legacy := commitColl([]string{fa, fb}, 0)
		split := postImage(4, 0,
			fa, commitShard(schemapb.ShardState_ShardSplitting),
			fa1, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa2, commitShard(schemapb.ShardState_ShardCreating, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
		require.NoError(t, JudgeCommit(legacy, split, split1Delta))
		// A legacy shard with a shard info but no residues serves the same way.
		legacy.ShardInfos[fb] = modelShard(fb, schemapb.ShardState_ShardNormal)
		require.NoError(t, JudgeCommit(legacy, split, split1Delta))
		// The same split at the legacy modulus has nothing to divide: a single
		// residue cannot be shared by two targets, and the judge says so itself.
		same := postImage(2, 0,
			fa, commitShard(schemapb.ShardState_ShardSplitting),
			fa1, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa2, commitShard(schemapb.ShardState_ShardCreating, 0),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1))
		requireIncoherent(t, JudgeCommit(commitColl([]string{fa, fb}, 0), same, split1Delta), "overlap at residue 0")
	})

	t.Run("a split may double the modulus of a split collection", func(t *testing.T) {
		// a1 {0} at modulus 4 splits into a11 {0} and a12 {4} at modulus 8.
		split := postImage(8, 0,
			fa1, commitShard(schemapb.ShardState_ShardSplitting),
			"va11", commitShard(schemapb.ShardState_ShardCreating, 0),
			"va12", commitShard(schemapb.ShardState_ShardCreating, 4),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2, 6),
			fb1, commitShard(schemapb.ShardState_ShardNormal, 1, 5),
			fb2, commitShard(schemapb.ShardState_ShardNormal, 3, 7))
		require.NoError(t, JudgeCommit(afterAdoption2(), split, SplitDelta(fa1, []string{"va11", "va12"}, false)))
		// Growing to a non-multiple (a post-image that tiles at 6, which no
		// residue of the collection's modulus 4 re-expresses to), or
		// re-expressing a shard wrongly (a2 and b2 swapping 6 and 7, so the
		// post-image still tiles).
		nonMultiple := postImage(6, 0,
			fa1, commitShard(schemapb.ShardState_ShardSplitting),
			"va11", commitShard(schemapb.ShardState_ShardCreating, 0),
			"va12", commitShard(schemapb.ShardState_ShardCreating, 3),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 1),
			fb1, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb2, commitShard(schemapb.ShardState_ShardNormal, 4, 5))
		requireIncoherent(t, JudgeCommit(afterAdoption2(), nonMultiple, SplitDelta(fa1, []string{"va11", "va12"}, false)), "does not divide")
		split.ShardInfos[3] = commitShard(schemapb.ShardState_ShardNormal, 2, 7)
		split.ShardInfos[5] = commitShard(schemapb.ShardState_ShardNormal, 3, 6)
		requireIncoherent(t, JudgeCommit(afterAdoption2(), split, SplitDelta(fa1, []string{"va11", "va12"}, false)), "re-expressed at modulus 8")
	})

	t.Run("an adoption may adopt first and retire later, or retire only", func(t *testing.T) {
		keepSource := postImage(4, task1,
			fa, commitShard(schemapb.ShardState_ShardSplitting),
			fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
		require.NoError(t, JudgeCommit(afterSplit1(), keepSource, adoption1Delta))
		adopted := afterSplit1()
		adopted.ShardInfos[fa1].State = schemapb.ShardState_ShardNormal
		adopted.ShardInfos[fa2].State = schemapb.ShardState_ShardNormal
		requireApplied(t, JudgeCommit(adopted, keepSource, adoption1Delta))
		require.NoError(t, JudgeCommit(adopted, adoption1Image(), adoption1Delta), "delist-only")
		// The adopt-first commit redelivered after the source was retired.
		requireApplied(t, JudgeCommit(afterAdoption1(), keepSource, adoption1Delta))
	})
}

// TestJudgeCommitAhead: on a secondary the callbacks of one collection's
// routing commits are not ordered by the broadcaster, so a later commit can
// reach its judge first. Its post-image reflects the earlier commit; applying
// it would apply that commit's delta on its behalf, past that commit's own
// gates. It is refused as ahead, retriably, until the earlier one has applied.
func TestJudgeCommitAhead(t *testing.T) {
	t.Run("split 2 overtakes adoption 1: a delist that is not its own", func(t *testing.T) {
		// The scenario the delta exists for: adoption 1 waits for this cluster's
		// drain, and split 2's post-image already has a retired and a1/a2 Normal.
		requireAhead(t, JudgeCommit(afterSplit1(), split2Image(), split2Delta), "not this commit's to retire")
		require.NoError(t, JudgeCommit(afterAdoption1(), split2Image(), split2Delta), "applies once adoption 1 has")
	})
	t.Run("adoption 2 overtakes adoption 1", func(t *testing.T) {
		requireAhead(t, JudgeCommit(afterSplit1(), adoption2Image(), adoption2Delta), "")
		// With split 2 applied but adoption 1 not, a is still not its to retire.
		coll := afterSplit2()
		coll.VirtualChannelNames = []string{fa, fa1, fa2, fb, fb1, fb2}
		coll.ShardInfos[fa] = modelShard(fa, schemapb.ShardState_ShardSplitting)
		requireAhead(t, JudgeCommit(coll, adoption2Image(), adoption2Delta), "not this commit's to retire")
	})
	t.Run("an adoption before its own split: targets unknown, source not fenced", func(t *testing.T) {
		requireAhead(t, JudgeCommit(initial(), adoption1Image(), adoption1Delta), "not fenced")
		coll := initial()
		coll.ShardInfos[fa].State = schemapb.ShardState_ShardSplitting
		requireAhead(t, JudgeCommit(coll, adoption1Image(), adoption1Delta), "does not carry")
	})
	t.Run("an adoption whose task this cluster has not recorded", func(t *testing.T) {
		requireAhead(t, JudgeCommit(afterSplit1(), adoption1Image(), AdoptionDelta("", nil, false)), "no record of split task 1")
		// Recorded or not, an exact redelivery is recognized without the record.
		requireApplied(t, JudgeCommit(afterAdoption1(), adoption1Image(), AdoptionDelta("", nil, false)))
	})
	t.Run("a split whose post-image reflects an unapplied earlier split", func(t *testing.T) {
		// Split 2 issued while split 1 is still Splitting on the primary.
		both := postImage(4, 0,
			fa, commitShard(schemapb.ShardState_ShardSplitting),
			fa1, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa2, commitShard(schemapb.ShardState_ShardCreating, 2),
			fb, commitShard(schemapb.ShardState_ShardSplitting),
			fb1, commitShard(schemapb.ShardState_ShardCreating, 1),
			fb2, commitShard(schemapb.ShardState_ShardCreating, 3))
		requireAhead(t, JudgeCommit(initial(), both, split2Delta), "not this commit's to move")
		require.NoError(t, JudgeCommit(afterSplit1(), both, split2Delta))
	})
	t.Run("a split of a target not yet adopted here", func(t *testing.T) {
		split := postImage(8, 0,
			fa1, commitShard(schemapb.ShardState_ShardSplitting),
			"va11", commitShard(schemapb.ShardState_ShardCreating, 0),
			"va12", commitShard(schemapb.ShardState_ShardCreating, 4),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2, 6),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3, 5, 7))
		delta := SplitDelta(fa1, []string{"va11", "va12"}, false)
		requireAhead(t, JudgeCommit(afterSplit1(), split, delta), "still Creating here")
		// Its source unknown here, its split not recorded: the split of a is not applied.
		requireAhead(t, JudgeCommit(initial(), split, delta), "no split task is recorded")
		require.NoError(t, JudgeCommit(afterAdoption1(), split, delta))
	})
	t.Run("an adoption at a modulus a later split grew", func(t *testing.T) {
		grown := adoption1Image()
		grown.RoutingModulus = 8
		for _, info := range grown.ShardInfos {
			info.GetHashRouting().Buckets = reexpressResidues(info.GetHashRouting().GetBuckets(), 4, 8)
		}
		requireAhead(t, JudgeCommit(afterSplit1(), grown, adoption1Delta), "grew the modulus")
	})
	t.Run("an untouched shard moved forward", func(t *testing.T) {
		// Split 2's post-image, issued after a later split of a1 fenced it and
		// created its targets at a doubled modulus: a1 is not split 2's to move.
		u := postImage(8, 0,
			fa1, commitShard(schemapb.ShardState_ShardSplitting),
			fa11, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa12, commitShard(schemapb.ShardState_ShardCreating, 4),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2, 6),
			fb, commitShard(schemapb.ShardState_ShardSplitting),
			fb1, commitShard(schemapb.ShardState_ShardCreating, 1, 5),
			fb2, commitShard(schemapb.ShardState_ShardCreating, 3, 7))
		requireAhead(t, JudgeCommit(afterAdoption1(), u, split2Delta), "not this commit's to move")
	})
	t.Run("an adoption's own target retired by a later adoption", func(t *testing.T) {
		// Adoption 1b (delist-only), derived after a1 was split again and its
		// targets adopted, no longer lists a1.
		later := postImage(8, task1,
			"va11", commitShard(schemapb.ShardState_ShardNormal, 0),
			"va12", commitShard(schemapb.ShardState_ShardNormal, 4),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2, 6),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3, 5, 7))
		coll := afterSplit1()
		coll.ShardInfos[fa1].State = schemapb.ShardState_ShardNormal
		coll.ShardInfos[fa2].State = schemapb.ShardState_ShardNormal
		requireAhead(t, JudgeCommit(coll, later, adoption1Delta), "retired after this commit")
		coll.ShardInfos[fa1].State = schemapb.ShardState_ShardSplitting
		requireAhead(t, JudgeCommit(coll, later, adoption1Delta), "retired after this commit")
		coll.ShardInfos[fa1].State = schemapb.ShardState_ShardCreating
		requireAhead(t, JudgeCommit(coll, later, adoption1Delta), "retired after this commit")
	})
}

// TestJudgeCommitBackFillsTheRoutingKey: the one write outside a commit's
// delta. A collection carrying exactly the post-image's topology with no
// routing key takes the post-image's; anything else the redelivery is a no-op.
func TestJudgeCommitBackFillsTheRoutingKey(t *testing.T) {
	coll := afterSplit1()
	coll.ShardBy = ""
	require.NoError(t, JudgeCommit(coll, split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)))
	requireApplied(t, JudgeCommit(afterSplit1(), split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)))
	later := afterAdoption1()
	later.ShardBy = ""
	requireApplied(t, JudgeCommit(later, split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)), "not the same topology: nothing to back-fill onto")
	unset := split1Image()
	unset.ShardBy = ""
	requireApplied(t, JudgeCommit(coll, unset, SplitDelta(fa, []string{fa1, fa2}, true)))
}

// TestJudgeCommitStaleRedelivery: a commit redelivered after later commits
// applied on top of it (a crash before its tombstone, then a later commit with
// free keys) is already applied, not ahead and not incoherent.
func TestJudgeCommitStaleRedelivery(t *testing.T) {
	requireApplied(t, JudgeCommit(afterAdoption1(), split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)))
	requireApplied(t, JudgeCommit(afterSplit2(), split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)))
	requireApplied(t, JudgeCommit(afterAdoption2(), split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)))
	requireApplied(t, JudgeCommit(afterSplit2(), adoption1Image(), adoption1Delta))
	requireApplied(t, JudgeCommit(afterAdoption2(), adoption1Image(), adoption1Delta))
	// Without the record, a retired source reads as a split never applied here.
	requireAhead(t, JudgeCommit(afterAdoption1(), split1Image(), split1Delta), "no split task is recorded")
}

// After adoption 1, split 3 splits a1 into a11 {0} and a12 {4} at modulus 8,
// and adoption 3 retires a1: commit 1's own target is no longer listed.
const fa11, fa12 = "va11", "va12"

func afterSplit3() *model.Collection {
	coll := commitColl([]string{fa1, fa11, fa12, fa2, fb}, 8,
		modelShard(fa1, schemapb.ShardState_ShardSplitting),
		modelShard(fa11, schemapb.ShardState_ShardCreating, 0),
		modelShard(fa12, schemapb.ShardState_ShardCreating, 4),
		modelShard(fa2, schemapb.ShardState_ShardNormal, 2, 6),
		modelShard(fb, schemapb.ShardState_ShardNormal, 1, 3, 5, 7))
	coll.ShardBy = "hash(pk)"
	return coll
}

func afterAdoption3() *model.Collection {
	coll := commitColl([]string{fa11, fa12, fa2, fb}, 8,
		modelShard(fa11, schemapb.ShardState_ShardNormal, 0),
		modelShard(fa12, schemapb.ShardState_ShardNormal, 4),
		modelShard(fa2, schemapb.ShardState_ShardNormal, 2, 6),
		modelShard(fb, schemapb.ShardState_ShardNormal, 1, 3, 5, 7))
	coll.ShardBy = "hash(pk)"
	return coll
}

// TestJudgeCommitRedeliveredAfterItsTargetsWereRetired: a commit redelivered
// after one of its own targets was split again and retired (on a secondary,
// adoption 1 stuck retrying its post-apply steps while split 3 and adoption 3,
// under another name key, applied) is already applied: its own source is
// delisted with the task recorded, so an unlisted own target was retired
// later, not never created here. An unlisted own target with the own source
// still listed keeps meaning "its split is not applied here".
func TestJudgeCommitRedeliveredAfterItsTargetsWereRetired(t *testing.T) {
	recordedSplit1 := SplitDelta(fa, []string{fa1, fa2}, true)
	t.Run("adoption redelivered after its target was split and adopted away", func(t *testing.T) {
		requireApplied(t, JudgeCommit(afterSplit3(), adoption1Image(), adoption1Delta), "a1 fenced again: past adoption 1")
		requireApplied(t, JudgeCommit(afterAdoption3(), adoption1Image(), adoption1Delta), "a1 retired: past adoption 1")
		// The delist-only form of adoption 1, which keeps a1 listed Splitting.
		keep := postImage(4, task1,
			fa, commitShard(schemapb.ShardState_ShardSplitting),
			fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
		requireApplied(t, JudgeCommit(afterAdoption3(), keep, adoption1Delta))
	})
	t.Run("split redelivered after its target was split and adopted away", func(t *testing.T) {
		requireApplied(t, JudgeCommit(afterSplit3(), split1Image(), recordedSplit1))
		requireApplied(t, JudgeCommit(afterAdoption3(), split1Image(), recordedSplit1))
		// Without the record, the retired source still reads as a split never
		// applied here, whatever the targets say.
		requireAhead(t, JudgeCommit(afterAdoption3(), split1Image(), split1Delta), "no split task is recorded")
	})
	t.Run("negative: own target unlisted while the own source is still fenced", func(t *testing.T) {
		// The split wrote its source and targets in one catalog write, so a
		// fenced source without its targets is not a state a later commit
		// produces: the adoption waits for the split, the split is incoherent.
		coll := initial()
		coll.ShardInfos[fa].State = schemapb.ShardState_ShardSplitting
		requireAhead(t, JudgeCommit(coll, adoption1Image(), adoption1Delta), "does not carry")
		requireIncoherent(t, JudgeCommit(coll, split1Image(), recordedSplit1), "past another part of it")
		// The source still Normal: the split is simply pending, whatever the
		// record says.
		require.NoError(t, JudgeCommit(initial(), split1Image(), recordedSplit1))
		requireAhead(t, JudgeCommit(initial(), adoption1Image(), adoption1Delta), "not fenced")
	})
}

// TestJudgeCommitIncoherent: a post-image behind the collection, or one no
// commit produces, is a Milvus bug and stays an error.
func TestJudgeCommitIncoherent(t *testing.T) {
	t.Run("backward: a fenced source listed Normal", func(t *testing.T) {
		// Un-fencing the source makes it writable again, and a writable shard
		// without residues is a dead shard: the tiling check names it before
		// the source's own state is judged (judgeSource's refusal shields the
		// same shape should the tiling check ever be relaxed).
		u := postImage(4, task1,
			fa, commitShard(schemapb.ShardState_ShardNormal),
			fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "owns no residue")
	})
	t.Run("backward: an adopted target listed Creating by its adoption", func(t *testing.T) {
		coll := afterAdoption1()
		u := postImage(4, task1,
			fa1, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
		requireIncoherent(t, JudgeCommit(coll, u, adoption1Delta), "cannot go from ShardNormal to ShardCreating")
	})
	t.Run("backward: an untouched shard behind the collection", func(t *testing.T) {
		u := adoption2Image()
		u.ShardInfos[0] = commitShard(schemapb.ShardState_ShardCreating, 0)
		requireIncoherent(t, JudgeCommit(afterSplit2(), u, adoption2Delta), "cannot go from ShardNormal to ShardCreating")
	})
	t.Run("backward: the modulus shrinks or is revoked", func(t *testing.T) {
		// A shrink that still tiles (a1 {0}, a2 {2}, b {1} at modulus 3), so
		// the modulus is what the refusal names.
		u := postImage(3, task1,
			fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1))
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "cannot take it down to")
		u = adoption1Image()
		u.RoutingModulus = 0
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "back to none")
		s := split1Image()
		s.RoutingModulus = 0
		requireIncoherent(t, JudgeCommit(commitColl([]string{fa, fb}, 0), s, split1Delta), "sets no routing modulus")
	})
	t.Run("a listed Dropped shard, whatever the collection holds (F4)", func(t *testing.T) {
		u := adoption1Image()
		u.VirtualChannelNames = append([]string{fa}, u.VirtualChannelNames...)
		u.PhysicalChannelNames = append([]string{"p-" + fa}, u.PhysicalChannelNames...)
		u.ShardInfos = append([]*schemapb.CollectionShardInfo{commitShard(schemapb.ShardState_ShardDropped)}, u.ShardInfos...)
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "reaches Dropped only by being delisted")
		s := split1Image()
		s.ShardInfos[0] = commitShard(schemapb.ShardState_ShardDropped)
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "reaches Dropped only by being delisted")
		requireIncoherent(t, JudgeCommit(afterSplit1(), s, split1Delta), "reaches Dropped only by being delisted")
	})
	t.Run("a split's own shape", func(t *testing.T) {
		s := split1Image()
		s.ShardInfos[1] = commitShard(schemapb.ShardState_ShardNormal, 0)
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "born Creating")
		// The source left writable, with residues so that the post-image still
		// tiles (at a grown modulus) and its state is what the refusal names.
		s = postImage(8, 0,
			fa, commitShard(schemapb.ShardState_ShardNormal, 4, 6),
			fa1, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa2, commitShard(schemapb.ShardState_ShardCreating, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3, 5, 7))
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "does not list it as Splitting")
		// A target born without residues is a dead shard: the tiling check
		// names it (judgeTarget's own refusal shields the same shape).
		s = split1Image()
		s.ShardInfos[1] = commitShard(schemapb.ShardState_ShardCreating)
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "owns no residue")
		s = split1Image()
		s.ShardInfos = s.ShardInfos[:3]
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "parallel and non-empty")
	})
	t.Run("a split's delta partly applied", func(t *testing.T) {
		coll := initial()
		coll.VirtualChannelNames = append(coll.VirtualChannelNames, fa1)
		coll.ShardInfos[fa1] = modelShard(fa1, schemapb.ShardState_ShardCreating, 0)
		requireIncoherent(t, JudgeCommit(coll, split1Image(), split1Delta), "past another part of it")
	})
	t.Run("an adoption changes no residues", func(t *testing.T) {
		u := adoption1Image()
		u.ShardInfos[0] = commitShard(schemapb.ShardState_ShardNormal, 2)
		u.ShardInfos[1] = commitShard(schemapb.ShardState_ShardNormal, 0)
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "created with")
	})
	t.Run("an adoption names its split task", func(t *testing.T) {
		u := adoption1Image()
		u.SplitTaskId = 0
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "does not name its split task")
	})
	t.Run("an adoption fencing its target", func(t *testing.T) {
		// Adoption 1 redelivered with a post-image that already reflects a
		// later split of a1 (a1 fenced, its targets created at a doubled
		// modulus, so the post-image still tiles): ahead, for the target's
		// own state before the modulus is compared.
		u := postImage(8, task1,
			fa1, commitShard(schemapb.ShardState_ShardSplitting),
			fa11, commitShard(schemapb.ShardState_ShardCreating, 0),
			fa12, commitShard(schemapb.ShardState_ShardCreating, 4),
			fa2, commitShard(schemapb.ShardState_ShardNormal, 2, 6),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3, 5, 7))
		requireAhead(t, JudgeCommit(afterSplit1(), u, adoption1Delta), "not yet adopted it")
		coll := afterAdoption1()
		requireAhead(t, JudgeCommit(coll, u, adoption1Delta), "later split of it")
	})
	t.Run("a collection listing no vchannel", func(t *testing.T) {
		// Meta no create or routing commit produces; a never-split collection's
		// modulus is its vchannel count, which every comparison divides by.
		empty := commitColl(nil, 0)
		requireIncoherent(t, JudgeCommit(empty, split1Image(), split1Delta), "lists no vchannel")
		requireIncoherent(t, JudgeCommit(empty, split1Image(), SplitDelta(fa, []string{fa1, fa2}, true)), "lists no vchannel")
		requireIncoherent(t, JudgeCommit(empty, adoption1Image(), adoption1Delta), "lists no vchannel")
	})
}

// The namespace routing key is valid only for a collection whose rows have
// ALWAYS been placed by it -- namespace.sharding.enabled=true in partition_key
// mode -- and that is decidable from the collection's own properties because
// both are immutable after creation.
func TestJudgeCommitRefusesTheNamespaceKeyForAPrimaryKeyPlacedCollection(t *testing.T) {
	commit := func(shardBy string) *messagespb.AlterCollectionMessageUpdates {
		return &messagespb.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"v0", "v1", "v2"},
			PhysicalChannelNames: []string{"p0", "p0", "p0"},
			RoutingModulus:       2,
			ShardBy:              shardBy,
			ShardInfos: []*schemapb.CollectionShardInfo{
				commitShard(schemapb.ShardState_ShardSplitting),
				commitShard(schemapb.ShardState_ShardCreating, 0),
				commitShard(schemapb.ShardState_ShardCreating, 1),
			},
		}
	}
	delta := SplitDelta("v0", []string{"v1", "v2"}, false)
	collWith := func(props ...*commonpb.KeyValuePair) *model.Collection {
		return &model.Collection{Name: "c", VirtualChannelNames: []string{"v0"}, Properties: props}
	}
	kv := func(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }

	requireIncoherent(t, JudgeCommit(collWith(kv(common.NamespaceShardingEnabledKey, "false"), kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)), commit(NamespaceShardBy), delta), "placed by primary key")
	requireIncoherent(t, JudgeCommit(collWith(kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartition)), commit(NamespaceShardBy), delta), "")
	requireIncoherent(t, JudgeCommit(collWith(kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)), commit(NamespaceShardBy), delta), "")
	requireIncoherent(t, JudgeCommit(collWith(kv(common.NamespaceShardingEnabledKey, "yes")), commit(NamespaceShardBy), delta), common.NamespaceShardingEnabledKey)
	require.NoError(t, JudgeCommit(
		collWith(kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)),
		commit(NamespaceShardBy), delta))
	require.NoError(t, JudgeCommit(collWith(kv(common.NamespaceShardingEnabledKey, "false")), commit("hash(pk)"), delta))
}

func TestCheckNoListedDroppedShard(t *testing.T) {
	require.NoError(t, CheckNoListedDroppedShard([]string{"v0", "v1"}, []*schemapb.CollectionShardInfo{
		commitShard(schemapb.ShardState_ShardSplitting),
		commitShard(schemapb.ShardState_ShardNormal, 0, 1),
	}))
	require.NoError(t, CheckNoListedDroppedShard([]string{"v0"}, nil))
	err := CheckNoListedDroppedShard([]string{"v0", "v1"}, []*schemapb.CollectionShardInfo{
		commitShard(schemapb.ShardState_ShardNormal, 0, 1),
		commitShard(schemapb.ShardState_ShardDropped),
	})
	requireIncoherent(t, err, `"v1"`)
	// A shape mismatch is someone else's refusal; the shard is still named if it can be.
	requireIncoherent(t, CheckNoListedDroppedShard(nil, []*schemapb.CollectionShardInfo{commitShard(schemapb.ShardState_ShardDropped)}), "Dropped")
}

// TestJudgeCommitRefusesAMalformedPostImage: what the message alone can
// answer is refused before the collection is compared, and before an adoption
// asks for its task record -- so a malformed adoption never reaches the drain
// gate. The delta is the one the adoption callback judges with first, before
// datacoord is asked; "ahead" is what that pass answers for a well-formed
// adoption.
func TestJudgeCommitRefusesAMalformedPostImage(t *testing.T) {
	firstPass := AdoptionDelta("", nil, false)
	requireAhead(t, JudgeCommit(afterSplit1(), adoption1Image(), firstPass), "no record of split task")

	t.Run("a short pchannel list", func(t *testing.T) {
		u := adoption1Image()
		u.PhysicalChannelNames = u.PhysicalChannelNames[:2]
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, firstPass), "parallel and non-empty")
		u.PhysicalChannelNames = nil
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, firstPass), "3 vchannels, 0 pchannels and 3 shard infos")
		s := split1Image()
		s.PhysicalChannelNames = append(s.PhysicalChannelNames, "p-extra")
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "parallel and non-empty")
	})
	t.Run("a duplicated vchannel", func(t *testing.T) {
		u := postImage(4, task1,
			fa1, commitShard(schemapb.ShardState_ShardNormal, 0),
			fa1, commitShard(schemapb.ShardState_ShardNormal, 2),
			fb, commitShard(schemapb.ShardState_ShardNormal, 1, 3))
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, firstPass), `lists vchannel "va1" twice`)
		// A pchannel listed twice is what a split produces: targets live on
		// their source's pchannel.
		shared := adoption1Image()
		shared.PhysicalChannelNames = []string{"p", "p", "p"}
		requireAhead(t, JudgeCommit(afterSplit1(), shared, firstPass), "no record of split task")
		require.NoError(t, JudgeCommit(afterSplit1(), shared, adoption1Delta))
	})
	t.Run("a shard info naming another vchannel", func(t *testing.T) {
		u := adoption1Image()
		u.ShardInfos[0].VchannelName = fa2
		requireIncoherent(t, JudgeCommit(afterSplit1(), u, firstPass), "names vchannel")
	})
	t.Run("writable shards that do not tile", func(t *testing.T) {
		overlap := adoption1Image()
		overlap.ShardInfos[1] = commitShard(schemapb.ShardState_ShardNormal, 0)
		requireIncoherent(t, JudgeCommit(afterSplit1(), overlap, firstPass), "overlap at residue 0")
		gap := adoption1Image()
		gap.ShardInfos[2] = commitShard(schemapb.ShardState_ShardNormal, 1)
		requireIncoherent(t, JudgeCommit(afterSplit1(), gap, firstPass), "gap: residue 3")
		over := adoption1Image()
		over.ShardInfos[2] = commitShard(schemapb.ShardState_ShardNormal, 1, 4)
		requireIncoherent(t, JudgeCommit(afterSplit1(), over, firstPass), "not below the modulus")
		capped := adoption1Image()
		capped.RoutingModulus = maxModulus * 2
		requireIncoherent(t, JudgeCommit(afterSplit1(), capped, firstPass), "exceeds the cap")
		// A split's post-image is judged by the same checks.
		s := split1Image()
		s.ShardInfos[2] = commitShard(schemapb.ShardState_ShardCreating, 0)
		requireIncoherent(t, JudgeCommit(initial(), s, split1Delta), "overlap at residue 0")
	})
	t.Run("a well-formed post-image passes the message-only checks", func(t *testing.T) {
		require.NoError(t, CheckPostImageShape(adoption1Image()))
		require.NoError(t, CheckPostImageTiling(adoption1Image()))
		require.NoError(t, CheckPostImageShape(split1Image()))
		require.NoError(t, CheckPostImageTiling(split1Image()))
	})
}
