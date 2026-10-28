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

package pkindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// TestIndexCreatedBeforeStartWaitsForTheRecovery checks that an Index opened
// before Start is not ready until the recovery marks it, does not hand over,
// and that an Index opened after Start is ready at once.
func TestIndexCreatedBeforeStartWaitsForTheRecovery(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.recoveredChannel(0, "v0")
	v0 := ch.get("v0")
	assert.False(t, v0.Ready())
	insert(t, ch, v0, 10, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	assert.True(t, inHeap(ch, v0), "a dirty Index holds LastAcked back while it waits")
	assert.Empty(t, picks(ch, farFuture), "no handover before ready")

	ch.start(false)
	assert.False(t, v0.Ready(), "Start does not mark an existing Index, the recovery does")
	v1, created, err := ch.CreateCollection(context.Background(), "v1", testCollectionID, testIndexID)
	require.NoError(t, err)
	require.True(t, created)
	assert.True(t, v1.Ready(), "a collection created after Start is ready at once")

	v0.MarkReady(context.Background())
	assert.True(t, v0.Ready())
	assert.Equal(t, []*index.Index{v0}, picks(ch, farFuture))
	assert.Empty(t, env.fatalList())
}

func TestAddOfAnExistingIndexReportsNotCreated(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 10, 1)

	_, created, err := ch.CreateCollection(context.Background(), "v0", testCollectionID, testIndexID)
	require.NoError(t, err)
	assert.False(t, created, "a caller that rolls back must not remove the Index in use")
	assert.Same(t, idx, ch.get("v0"))
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
}

func TestDropRemovesTheLocalFiles(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 10, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)
	require.DirExists(t, idx.CommittedDirForTest())

	require.NoError(t, ch.DropCollection(context.Background(), "v0"))
	assert.Nil(t, ch.get("v0"))
	assert.NoDirExists(t, filepath.Join(env.root, "v0"))
	require.NoError(t, ch.DropCollection(context.Background(), "v0"), "a drop of a vchannel with no Index is a no-op")

	idx = env.add(ch, "v0")
	assert.Zero(t, segmentOf(t, idx, 1), "the old key is gone")
	assert.Empty(t, env.fatalList())
}

func TestDropOfAClosedChannelFails(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	ch.Close()

	err := ch.DropCollection(context.Background(), "v0")
	require.ErrorIs(t, err, engine.ErrClosed)
	assert.DirExists(t, filepath.Join(env.root, "v0"))
}

func TestRemoveWaitsForARunningHandover(t *testing.T) {
	env := newTestEnv(t, asyncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	env.client.set(func(c *FakeCommitter) {
		c.commitEntered = entered
		c.commitBlock = block
	})
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)
	<-entered

	removed := make(chan struct{})
	go func() {
		ch.remove(context.Background(), "v0")
		close(removed)
	}()
	select {
	case <-removed:
		t.Fatal("Remove returned while the handover runs")
	case <-time.After(200 * time.Millisecond):
	}
	close(block)
	select {
	case <-removed:
	case <-time.After(waitTimeout):
		t.Fatal("Remove did not return after the handover ended")
	}
	assert.Nil(t, ch.get("v0"))
	assert.False(t, scheduleOf(ch, idx).running.Load())
}

func TestClosedChannelAndIndex(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.Close()

	_, _, err := ch.CreateCollection(context.Background(), "v1", testCollectionID, testIndexID)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	_, err = idx.Decider().Decide(context.Background(), insertRequest(idx, 1))
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable))
	assert.Nil(t, ch.get("v0"))
}

func TestManagerCloseClosesChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	env.mgr.Close()
	assert.Nil(t, ch.get("v0"))
	closed := env.channel(0)
	_, _, err := closed.CreateCollection(context.Background(), "v0", testCollectionID, testIndexID)
	assert.True(t, errors.Is(err, engine.ErrClosed))
}

func TestFencedCommitStopsTheHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{errors.Wrap(index.ErrFenced, "v0")} })
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, index.ErrFenced))
	assert.True(t, scheduleOf(ch, idx).stopped.Load())
	assert.Len(t, idx.EngineForTest().FrozenGenerations(), 1, "the generations stay frozen")
	// the Index keeps serving writes, but it does not hand over again
	insert(t, ch, idx, 2, 2)
	ch.ObserveTimeTick(20, WALTailSnapshot{})
	assert.True(t, idx.Dirty())
	assert.Empty(t, picks(ch, farFuture))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
}

func TestRejectedCommitIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{errors.Wrap(index.ErrRejected, "duplicate table")} })
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, index.ErrRejected))
	assert.True(t, scheduleOf(ch, idx).stopped.Load())
	assert.Zero(t, scheduleOf(ch, idx).failures, "no retry is scheduled")
	assert.Len(t, idx.EngineForTest().FrozenGenerations(), 1, "the generations stay frozen")
	assert.Empty(t, picks(ch, farFuture))
	assert.Len(t, env.client.requestList(), 1)
}

// dropCommittedFiles makes the committed tables of idx missing on this node.
func dropCommittedFiles(t *testing.T, idx *index.Index) {
	require.NoError(t, os.RemoveAll(idx.CommittedDirForTest()))
	idx.ForgetInstalledPathsForTest()
}

func TestMissingObjectIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)
	require.EqualValues(t, 10, idx.LastCommittedTimeTick())

	// the next install needs the first tables, which are gone everywhere
	dropCommittedFiles(t, idx)
	env.store.missing.Store(true)
	insert(t, ch, idx, 2, 2)
	ch.ObserveTimeTick(20, WALTailSnapshot{})
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted))
	assert.True(t, scheduleOf(ch, idx).stopped.Load())
}

func TestObserveTimeTickOnlyMovesLastObservedForward(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(7)
	assert.EqualValues(t, 7, ch.LastObserved(), "the seed is the global checkpoint")
	ch.ObserveTimeTick(20, WALTailSnapshot{})
	assert.EqualValues(t, 20, ch.LastObserved())
	ch.ObserveTimeTick(15, WALTailSnapshot{})
	assert.EqualValues(t, 20, ch.LastObserved(), "never goes back")
}

func TestObserveTimeTickRemovesInDoubtEntriesOfIdleVChannels(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	intent, err := idx.Decider().Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(1)})
	require.NoError(t, err)
	intent.InDoubt(10)
	require.Equal(t, 1, dedup.InflightLenForTest(idx.Decider()))

	ch.ObserveTimeTick(9, WALTailSnapshot{})
	assert.Equal(t, 1, dedup.InflightLenForTest(idx.Decider()), "the write may still be applied after 9")
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	assert.Equal(t, 0, dedup.InflightLenForTest(idx.Decider()), "a TimeTick reaches an idle vchannel")
}

func TestManagerApplyAddsADirtiedIndexToTheHeap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	assert.EqualValues(t, 10, ch.LastAcked(), "a clean Index follows lastObserved")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 11}))
	ch.ObserveTimeTick(12, WALTailSnapshot{})
	assert.EqualValues(t, 0, ch.lastAckedUnclamped(), "a dirty Index holds LastAcked at its last commit")
	assert.Contains(t, ch.dirtyIndexes(), idx)

	require.NoError(t, ch.Apply(ctx, "no-such-vchannel", index.Write{Puts: puts(1, 1), TimeTick: 11}), "a vchannel without an Index is ignored")
}

// TestManagerApplyIgnoresADroppedIndex: the observe looks the Index up and
// then writes it. A DropCollection in between closes the Index, and the
// write is moot: Apply reports no error, so lastObserved keeps moving. A
// closed Index that is still attached reports the closed engine.
func TestManagerApplyIgnoresADroppedIndex(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	dropped := env.add(ch, "v0")
	require.NoError(t, ch.DropCollection(ctx, "v0"))
	require.NoError(t, ch.apply(ctx, dropped, index.Write{Puts: puts(1, 1), TimeTick: 5}))

	closed := env.add(ch, "v1")
	closed.Close(ctx)
	err := ch.Apply(ctx, "v1", index.Write{Puts: puts(1, 1), TimeTick: 5})
	require.True(t, errors.Is(err, engine.ErrClosed), "%v", err)
}

// refNames returns the vchannels of refs.
func refNames(refs []VChannelRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.VChannel)
	}
	return out
}

// TestIndexesOfOneVChannelUseSeparateDirs checks that the local directory
// and the object keys of an Index carry its index ID.
func TestIndexesOfOneVChannelUseSeparateDirs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	env := NewFakeEnv(t, root)
	c := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	idx, _, err := c.CreateCollection(ctx, "v1", 1, 7)
	require.NoError(t, err)
	assert.EqualValues(t, 7, idx.IndexID())
	assert.Equal(t, "root/pk_index/1/7/v1/sst", idx.SSTBasePathForTest())
	assert.DirExists(t, filepath.Join(index.Dir(root, "v1", 7), "engine"))
	require.NoError(t, c.DropCollection(ctx, "v1"))
	assert.NoDirExists(t, index.VChannelDir(root, "v1"))

	idx, _, err = c.CreateCollection(ctx, "v1", 1, 8)
	require.NoError(t, err)
	assert.Equal(t, "root/pk_index/1/8/v1/sst", idx.SSTBasePathForTest())
	assert.DirExists(t, filepath.Join(index.Dir(root, "v1", 8), "engine"))
	assert.NoDirExists(t, index.Dir(root, "v1", 7))
	assert.NotEqual(t, index.Dir(root, "v1", 7), index.Dir(root, "v1", 8))
	c.Close()
}

