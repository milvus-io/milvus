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
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
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
}

func (f *fakeChildSpawner) SpawnSplitChild(_ context.Context, params SpawnChildParams) (ShardDelegator, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spawned = append(f.spawned, params.Target.GetVchannel())
	f.lastParent = params.Parent
	if f.err != nil {
		return nil, f.err
	}
	return &MockShardDelegator{}, nil
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

func newSplitTargets(vchannels ...string) []*messagespb.SplitShardTarget {
	targets := make([]*messagespb.SplitShardTarget, 0, len(vchannels))
	for _, vchannel := range vchannels {
		targets = append(targets, &messagespb.SplitShardTarget{Vchannel: vchannel})
	}
	return targets
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
			vchannelName: "child",
			latestTsafe:  atomic.NewUint64(tsafe),
			distribution: NewDistribution("child", qv),
		}
	}
	// the source's own tsafe is frozen at T_switch (50) after the fence; the two
	// children have advanced past it.
	source := &shardDelegator{
		vchannelName: "v0",
		latestTsafe:  atomic.NewUint64(50),
		children: map[string]ShardDelegator{
			"v1": child(100),
			"v2": child(200),
		},
	}

	// the serviceable timestamp is min(child tsafes), not the source's frozen 50.
	assert.Equal(t, uint64(100), source.GetTSafe())

	// waiting for a guarantee below both children returns the same min, never
	// blocking on the source's frozen tsafe.
	got, err := source.waitTSafe(context.Background(), 40)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), got)
}

func TestProcessSplitShard(t *testing.T) {
	t.Run("spawns one child per target", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
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
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1", "v2"))
		assert.NoError(t, err)
		// only the missing target v2 is spawned; v1 is left untouched.
		assert.Eventually(t, func() bool { return len(childVChannels(sd)) == 2 }, time.Second, 5*time.Millisecond)
		assert.Equal(t, []string{"v2"}, spawner.spawnedVChannels())
	})

	t.Run("a spawn failure leaves no child and clears the in-flight slot", func(t *testing.T) {
		spawner := &fakeChildSpawner{err: errors.New("spawn boom")}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
		}

		// the failure is logged in the background, not returned (the spawn no
		// longer blocks the flow-graph goroutine).
		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1"))
		assert.NoError(t, err)
		assert.Eventually(t, func() bool { return len(spawner.spawnedVChannels()) == 1 }, time.Second, 5*time.Millisecond)
		assert.Empty(t, childVChannels(sd))
		// the in-flight slot is cleared so a later fence re-consume can retry.
		sd.childMut.Lock()
		_, stillSpawning := sd.spawning["v1"]
		sd.childMut.Unlock()
		assert.False(t, stillSpawning)
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

	t.Run("a missing spawner is an internal error", func(t *testing.T) {
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
		}

		err := sd.ProcessSplitShard(context.Background(), newSplitTargets("v1"))
		assert.Error(t, err)
	})

	t.Run("a child spawned after the source is releasing is aborted, not fronted", func(t *testing.T) {
		spawner := &fakeChildSpawner{}
		sd := &shardDelegator{
			vchannelName: "v0",
			children:     make(map[string]ShardDelegator),
			childSpawner: spawner,
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
