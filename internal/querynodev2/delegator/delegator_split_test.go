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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// fakeChildSpawner records every target it was asked to spawn and returns a
// stub child delegator, so a test can assert the spawn fan-out without a real
// querynode/pipeline. ProcessSplitShard spawns in the background, so it is
// accessed concurrently and guarded by a mutex.
type fakeChildSpawner struct {
	mu         sync.Mutex
	spawned    []string
	aborted    []string
	lastParent ShardDelegator
	err        error
	// failures makes the first failures spawn attempts fail, then succeed.
	failures int
	// foreign makes a successful spawn return a delegator fronted by nobody,
	// like one querycoord watched for the target on its own.
	foreign bool
}

func (f *fakeChildSpawner) SpawnSplitChild(_ context.Context, params SpawnChildParams) (ShardDelegator, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spawned = append(f.spawned, params.TargetVChannel)
	f.lastParent = params.Parent
	if f.err != nil {
		return nil, f.err
	}
	if len(f.spawned) <= f.failures {
		return nil, errors.New("transient spawn failure")
	}
	// like the querynode, wire the source as the child's fronting parent.
	child := &shardDelegator{vchannelName: params.TargetVChannel}
	if !f.foreign {
		child.SetFrontingParent(params.Parent)
	}
	return child, nil
}

// attempts is how many spawns were attempted, failed ones included.
func (f *fakeChildSpawner) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.spawned)
}

func (f *fakeChildSpawner) AbortSplitChild(_ context.Context, _ ShardDelegator, _ int64, vchannel string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborted = append(f.aborted, vchannel)
}

func (f *fakeChildSpawner) spawnedVChannels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.spawned...)
}

func (f *fakeChildSpawner) abortedVChannels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.aborted...)
}

func (f *fakeChildSpawner) parent() ShardDelegator {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastParent
}

func newSplitTargets(vchannels ...string) []string {
	return vchannels
}

// childVChannels reads the source delegator's registered children under the
// child lock (ProcessSplitShard publishes them from a background goroutine).
func childVChannels(sd *shardDelegator) []string {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	out := make([]string, 0, len(sd.children))
	for vchannel := range sd.children {
		out = append(out, vchannel)
	}
	return out
}

func TestSplitChildVisibilityLifecycle(t *testing.T) {
	source := &shardDelegator{vchannelName: "v0", children: make(map[string]ShardDelegator)}
	child := &shardDelegator{vchannelName: "v1"}
	child.SetFrontingParent(source)
	source.children["v1"] = child

	// born fronted + un-adopted: invisible to querycoord, fronted by the source.
	assert.True(t, child.IsUnadoptedSplitChild())
	assert.Contains(t, childVChannels(source), "v1")

	// adoption makes it visible to querycoord but does NOT detach it: the source
	// keeps fronting it (reads + delete forwarding) until it actually becomes
	// serviceable, so the split key range is never left unserved.
	child.MarkAdopted()
	assert.False(t, child.IsUnadoptedSplitChild())
	assert.Equal(t, ShardDelegator(source), child.FrontingParent())
	assert.Contains(t, childVChannels(source), "v1")
}

func TestSourceFrontsChildrenUntilDetached(t *testing.T) {
	newChild := func(vchannel string, serviceable bool) *shardDelegator {
		version := InitialTargetVersion
		if serviceable {
			version = 1
		}
		qv := NewChannelQueryView(nil, nil, nil, version)
		if serviceable {
			qv.loadedRatio.Store(1.0)
			qv.syncedByCoord = true
		}
		return &shardDelegator{vchannelName: vchannel, distribution: NewDistribution(vchannel, qv)}
	}
	frontedNames := func(sd *shardDelegator) []string {
		out := make([]string, 0)
		for _, c := range sd.frontingChildren() {
			out = append(out, c.vchannelName)
		}
		return out
	}
	source := &shardDelegator{
		vchannelName: "v0",
		children: map[string]ShardDelegator{
			"v1": newChild("v1", false), // not yet serviceable
			"v2": newChild("v2", true),  // already serviceable, but the proxy may not have re-routed yet
		},
	}

	// the source fronts BOTH children: a child becomes serviceable (next-target
	// synced) strictly before the proxy re-routes the range onto it, so the source
	// must keep fronting it to avoid an unserved window.
	assert.ElementsMatch(t, []string{"v1", "v2"}, frontedNames(source))

	// only at source release (DetachSplitChild) does the source stop fronting it.
	source.DetachSplitChild("v2")
	assert.ElementsMatch(t, []string{"v1"}, frontedNames(source))
}

func TestSourceServesAtMinChildTSafe(t *testing.T) {
	child := func(tsafe uint64) *shardDelegator {
		// non-serviceable query view so the source keeps fronting it.
		qv := NewChannelQueryView(nil, nil, nil, InitialTargetVersion)
		return &shardDelegator{
			vchannelName:               "child",
			latestTsafe:                atomic.NewUint64(tsafe),
			latestRequiredMVCCTimeTick: atomic.NewUint64(0),
			distribution:               NewDistribution("child", qv),
		}
	}
	source := &shardDelegator{
		vchannelName: "v0",
		latestTsafe:  atomic.NewUint64(150),
		children: map[string]ShardDelegator{
			"v1": child(100),
			"v2": child(200),
		},
	}

	// the serviceable timestamp is the min over the family: here a child's 100.
	assert.Equal(t, uint64(100), source.GetTSafe())

	// waiting for a guarantee below everyone returns the same min.
	got, err := source.waitTSafe(context.Background(), 40)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), got)
}

// The source's own tsafe is part of the family's serviceable timestamp. After
// the fence the source takes no new DML, but its delete node can still be
// behind the filter node that consumed the fence, and a bulk delete replay in
// loadStreamDelete stalls it; until its own tsafe passes t, deletes <= t may
// not be applied to what it serves. Answering at the children's tsafe alone
// would return those rows.
func TestSourceWaitsForItsOwnTSafeAsWellAsItsChildren(t *testing.T) {
	child := func(tsafe uint64) *shardDelegator {
		return &shardDelegator{
			vchannelName:               "child",
			latestTsafe:                atomic.NewUint64(tsafe),
			latestRequiredMVCCTimeTick: atomic.NewUint64(0),
			distribution:               NewDistribution("child", NewChannelQueryView(nil, nil, nil, InitialTargetVersion)),
		}
	}
	source := &shardDelegator{
		vchannelName:               "v0",
		latestTsafe:                atomic.NewUint64(50),
		latestRequiredMVCCTimeTick: atomic.NewUint64(0),
		tsCond:                     syncutil.NewContextCond(&sync.Mutex{}),
		lifetime:                   lifetime.NewLifetime(lifetime.Working),
		catchingUpStreamingData:    atomic.NewBool(false),
		children: map[string]ShardDelegator{
			"v1": child(100),
			"v2": child(200),
		},
	}

	// the source's own 50 is behind both children: it bounds the family.
	assert.Equal(t, uint64(50), source.GetTSafe())

	done := make(chan uint64, 1)
	go func() {
		got, err := source.waitTSafe(context.Background(), 80)
		assert.NoError(t, err)
		done <- got
	}()
	select {
	case got := <-done:
		t.Fatalf("served at %d before the source's own tsafe reached the guarantee", got)
	case <-time.After(100 * time.Millisecond):
	}
	source.UpdateTSafe(90)
	select {
	case got := <-done:
		assert.Equal(t, uint64(90), got, "served at min(own, children)")
	case <-time.After(5 * time.Second):
		t.Fatal("the read never woke up once the source's own tsafe passed the guarantee")
	}
}

// A delegator built with WithChildSpawner fronts its own split with that
// spawner.
func TestWithChildSpawnerWiresTheSpawner(t *testing.T) {
	spawner := &fakeChildSpawner{}
	sd := &shardDelegator{vchannelName: "v1", children: make(map[string]ShardDelegator), lifetime: lifetime.NewLifetime(lifetime.Working)}
	WithChildSpawner(spawner)(sd)

	assert.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v3")))
	assert.Eventually(t, func() bool { return len(childVChannels(sd)) == 1 }, time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"v3"}, spawner.spawnedVChannels())
}

func TestProcessSplitShard(t *testing.T) {
	t.Run("spawns one child per target", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1", "v2"))
		assert.NoError(t, err)
		// spawning is asynchronous: the children appear shortly after.
		assert.Eventually(t, func() bool { return len(childVChannels(sd)) == 2 }, time.Second, 5*time.Millisecond)
		assert.ElementsMatch(t, []string{"v1", "v2"}, spawner.spawnedVChannels())
		assert.ElementsMatch(t, []string{"v1", "v2"}, childVChannels(sd))
	})

	t.Run("passes the source delegator as the child's fronting parent", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1"))
		assert.NoError(t, err)
		// the child must forward its deletes back to this source delegator.
		assert.Eventually(t, func() bool { return spawner.parent() != nil }, time.Second, 5*time.Millisecond)
		assert.Equal(t, ShardDelegator(sd), spawner.parent())
	})

	t.Run("idempotent: an existing child is not re-spawned", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     map[string]ShardDelegator{"v1": &MockShardDelegator{}},
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1", "v2"))
		assert.NoError(t, err)
		// only the missing target v2 is spawned; v1 is left untouched.
		assert.Eventually(t, func() bool { return len(childVChannels(sd)) == 2 }, time.Second, 5*time.Millisecond)
		assert.Equal(t, []string{"v2"}, spawner.spawnedVChannels())
	})

	t.Run("a failed spawn stays pending, refusing reads, and is retried until the child is published", func(t *testing.T) {
		spawner := &fakeChildSpawner{failures: 1}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		// the failure is handled in the background, not returned (the spawn does
		// not block the flow-graph goroutine).
		require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
		require.Eventually(t, func() bool { return spawner.attempts() == 1 }, time.Second, time.Millisecond)

		// The fence has been consumed, so the target's writes are no longer in the
		// source's view. Until a child fronts them a read through the source must
		// be refused, not answered from the incomplete family.
		assert.Never(t, func() bool {
			_, err := sd.frontingFamily()
			return err == nil
		}, 300*time.Millisecond, 5*time.Millisecond, "a read was served while the failed target had no child")
		_, err := sd.frontingFamily()
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)

		// the spawn is retried and the child is published.
		assert.Eventually(t, func() bool { return len(childVChannels(sd)) == 1 }, 10*time.Second, 10*time.Millisecond)
		assert.Equal(t, 2, spawner.attempts())
		_, err = sd.frontingFamily()
		assert.NoError(t, err)
	})

	t.Run("a failing spawn stops retrying and clears its slot once the source is releasing", func(t *testing.T) {
		spawner := &fakeChildSpawner{err: errors.New("spawn boom")}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
		require.Eventually(t, func() bool { return spawner.attempts() >= 1 }, time.Second, time.Millisecond)
		sd.MarkReleasing()

		assert.Eventually(t, func() bool {
			sd.childMut.Lock()
			defer sd.childMut.Unlock()
			return len(sd.spawning) == 0
		}, 10*time.Second, 10*time.Millisecond)
		attempts := spawner.attempts()
		assert.LessOrEqual(t, attempts, 2)
		assert.Empty(t, childVChannels(sd))
	})

	t.Run("a failing spawn stops retrying and clears its slot once the delegator is closed", func(t *testing.T) {
		spawner := &fakeChildSpawner{err: errors.New("spawn boom")}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
		require.Eventually(t, func() bool { return spawner.attempts() >= 1 }, time.Second, time.Millisecond)
		sd.lifetime.SetState(lifetime.Stopped)

		assert.Eventually(t, func() bool {
			sd.childMut.Lock()
			defer sd.childMut.Unlock()
			return len(sd.spawning) == 0
		}, 10*time.Second, 10*time.Millisecond)
		assert.LessOrEqual(t, spawner.attempts(), 2)
		assert.Empty(t, childVChannels(sd))
	})

	t.Run("a spawn that returns a delegator this source does not front is never published", func(t *testing.T) {
		// e.g. querycoord adopted the target and watched a delegator of its own
		// while this spawn was retrying: that delegator forwards no delete to
		// this source, so fronting it would serve rows it has deleted.
		spawner := &fakeChildSpawner{foreign: true}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
		require.Eventually(t, func() bool { return spawner.attempts() == 1 }, time.Second, time.Millisecond)
		assert.Never(t, func() bool { return len(childVChannels(sd)) > 0 }, 300*time.Millisecond, 5*time.Millisecond,
			"a delegator that does not forward its deletes to this source was fronted")
		_, err := sd.frontingFamily()
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable, "the target stays pending, so reads through the source stay refused")
		assert.Empty(t, spawner.abortedVChannels(), "the source must not tear down a delegator it does not own")
	})

	t.Run("a spawn refused because another delegator serves the target is not retried", func(t *testing.T) {
		spawner := &fakeChildSpawner{err: merr.WrapErrChannelReduplicate("v1", "served by a delegator this source does not front")}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}

		require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
		require.Eventually(t, func() bool { return spawner.attempts() == 1 }, time.Second, time.Millisecond)
		// the first retry would come after one second of backoff.
		assert.Never(t, func() bool { return spawner.attempts() > 1 }, 1500*time.Millisecond, 10*time.Millisecond)
		_, err := sd.frontingFamily()
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("a child spawned after the delegator stopped is aborted, not fronted", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}
		sd.lifetime.SetState(lifetime.Stopped)

		require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
		assert.Eventually(t, func() bool { return len(spawner.abortedVChannels()) == 1 }, time.Second, 5*time.Millisecond)
		assert.Empty(t, childVChannels(sd))
	})

	t.Run("an empty target vchannel is rejected", func(t *testing.T) {
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: &fakeChildSpawner{},
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets(""))
		assert.Error(t, err)
	})

	t.Run("a missing spawner is an internal error and leaves the targets pending, refusing reads", func(t *testing.T) {
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1"))
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		// the fence was consumed but nothing can front its target: reads through
		// the source are refused rather than served without the target's writes.
		_, err = sd.frontingFamily()
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("a child spawned after the source is releasing is aborted, not fronted", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
			lifetime:     lifetime.NewLifetime(lifetime.Working),
		}
		// the source is being released while a spawn is launched.
		sd.MarkReleasing()

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1"))
		assert.NoError(t, err)
		// the spawn completes but the child is torn down (aborted), never fronted,
		// so it cannot orphan onto the gone source.
		assert.Eventually(t, func() bool { return len(spawner.abortedVChannels()) == 1 }, time.Second, 5*time.Millisecond)
		assert.Equal(t, []string{"v1"}, spawner.abortedVChannels())
		assert.Empty(t, childVChannels(sd))
	})
}

// TestFrontedChildAcceptsTheSourcesChannel pins the one exception to the
// misroute guard.
//
// Every read entry point refuses a request that does not name its own vchannel
// -- the check that catches a proxy addressing the wrong shard. A split child
// answers on its SOURCE's behalf, so the request it is handed names the source,
// and the guard refused it: once the fence finally reached the delegator, the
// very first fronted read of a split failed with "channel misrouted", which is
// a louder version of the silent hole it replaced.
func TestFrontedChildAcceptsTheSourcesChannel(t *testing.T) {
	child := &shardDelegator{vchannelName: "target-v2"}
	source := &shardDelegator{vchannelName: "source-v0"}
	addressedToSource := []string{"source-v0"}

	// serving its own shard: a request naming another shard is a misroute.
	assert.True(t, child.misroutedFor(addressedToSource, false))
	// fronted by its source: the same request is exactly what it must answer.
	assert.False(t, child.misroutedFor(addressedToSource, true))

	// the source itself is unaffected either way.
	assert.False(t, source.misroutedFor(addressedToSource, false))
	assert.False(t, source.misroutedFor(addressedToSource, true))

	// and a request naming neither is still a misroute for the source.
	assert.True(t, source.misroutedFor([]string{"unrelated-v9"}, false))
}

func TestSourceHandsItsPartitionsDownToFrontedChildren(t *testing.T) {
	// querycoord syncs the source and does not know the children exist, so this
	// is the only way a child hears about a partition created after the fence.
	// Without it the child rejects a request the source has already admitted.
	newChild := func(vchannel string, partitions ...int64) *shardDelegator {
		qv := NewChannelQueryView(nil, nil, partitions, InitialTargetVersion)
		return &shardDelegator{vchannelName: vchannel, distribution: NewDistribution(vchannel, qv)}
	}
	child1 := newChild("v1", 1)
	child2 := newChild("v2", 1)
	source := &shardDelegator{
		vchannelName: "v0",
		distribution: NewDistribution("v0", NewChannelQueryView(nil, nil, []int64{1}, InitialTargetVersion)),
		children:     map[string]ShardDelegator{"v1": child1, "v2": child2},
		// the sync makes the source serviceable, which then trims its delete buffer
		deleteBuffer: deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](0, 0, []string{"1", "v0"}),
	}

	for _, child := range []*shardDelegator{child1, child2} {
		_, _, _, _, err := child.distribution.PinReadableSegmentsAsChild(1.0, 2)
		assert.Error(t, err, "partition 2 did not exist at the fence")
	}

	source.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion: 1,
		Checkpoint:    &msgpb.MsgPosition{Timestamp: 1},
	}, []int64{1, 2})

	for _, child := range []*shardDelegator{child1, child2} {
		_, _, _, version, err := child.distribution.PinReadableSegmentsAsChild(1.0, 2)
		assert.NoError(t, err, "every fronted child must follow the source's partition set")
		child.distribution.Unpin(version)
	}
}

// A source the collection no longer lists was retired by an adoption; its
// children went to their own delegators, and a delegator watched for it now has
// none to front. It must never answer from its own view alone: that misses the
// targets' writes and returns rows they deleted. Every public read is refused
// retriably, so the proxy retries until its shard leaders move to the targets.
func TestARetiredSourceWithoutItsFamilyRefusesReads(t *testing.T) {
	sd := &shardDelegator{
		vchannelName: "v0",
		children:     make(map[string]ShardDelegator),
		lifetime:     lifetime.NewLifetime(lifetime.Working),
	}
	_, err := sd.frontingFamily()
	require.NoError(t, err)

	sd.RefuseReadsAsRetiredSource(context.Background())
	_, err = sd.frontingFamily()
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))

	for name, read := range map[string]func() error{
		"search": func() error {
			_, err := sd.Search(context.Background(), &querypb.SearchRequest{DmlChannels: []string{"v0"}})
			return err
		},
		"query": func() error {
			_, err := sd.Query(context.Background(), &querypb.QueryRequest{DmlChannels: []string{"v0"}})
			return err
		},
		"query stream": func() error {
			return sd.QueryStream(context.Background(), &querypb.QueryRequest{DmlChannels: []string{"v0"}}, nil)
		},
		"statistics": func() error {
			_, err := sd.GetStatistics(context.Background(), &querypb.GetStatisticsRequest{DmlChannels: []string{"v0"}})
			return err
		},
	} {
		assert.ErrorIs(t, read(), merr.ErrServiceUnavailable, name)
	}
}
