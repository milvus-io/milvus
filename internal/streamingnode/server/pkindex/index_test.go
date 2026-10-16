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
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTwoHandoversInOneProcess(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	insert(t, idx, 10, 1, 2, 3)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	first := env.client.manifest("v0")
	require.EqualValues(t, 1, first.Version, "the first flush creates the manifest")
	require.EqualValues(t, 10, first.CheckpointTimeTick)
	require.NotEmpty(t, first.Tables)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.EngineForTest().FrozenGenerations())

	insert(t, idx, 20, 4, 5)
	ch.ObserveTimeTick(20)
	ch.step(farFuture)
	second := env.client.manifest("v0")
	require.EqualValues(t, 2, second.Version)
	// the second install holds the tables of the first, which come from installedPaths
	assert.Subset(t, sstIDs(second), sstIDs(first))
	assert.Equal(t, len(second.Tables), idx.EngineForTest().Stats().CommittedTables)
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
	assert.EqualValues(t, 20, segmentOf(t, idx, 4))
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
	assert.Zero(t, env.store.downloads.Load())
	assert.False(t, idx.Dirty())
}

func TestLostResponseRetriesTheSameRequest(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(10)
	env.client.set(func(c *fakeClient) { c.loseNext = true })
	ch.step(farFuture)
	require.NotNil(t, idx.PendingForTest())
	assert.Equal(t, 1, scheduleOf(ch, idx).failures)
	assert.Len(t, idx.EngineForTest().FrozenGenerations(), 1)

	// new writes and a new tick do not change the retried request
	insert(t, idx, 20, 3)
	ch.ObserveTimeTick(20)
	ch.step(farFuture)
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
	// the second request took the applied retry branch of the manifest service
	assert.Equal(t, []bool{true, false}, env.client.appliedList())
	assert.EqualValues(t, 1, env.client.manifest("v0").Version)
	assert.Nil(t, idx.PendingForTest())
	assert.Zero(t, scheduleOf(ch, idx).failures)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.EngineForTest().FrozenGenerations())
	assert.True(t, idx.Dirty(), "the write after the rotate is not committed")

	ch.step(farFuture)
	reqs = env.client.requestList()
	require.Len(t, reqs, 3)
	assert.EqualValues(t, 20, reqs[2].CheckpointTimeTick)
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
}

func TestReopenedIndexStartsEmpty(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(10)
	// a frozen generation from a failed handover is left on disk too
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	ch.step(farFuture)
	require.Len(t, idx.EngineForTest().FrozenGenerations(), 1)
	ch.remove(context.Background(), "v0")
	require.Empty(t, env.client.manifest("v0").Tables)

	idx = env.add(ch, "v0")
	assert.Empty(t, idx.EngineForTest().FrozenGenerations())
	assert.DirExists(t, idx.CommittedDirForTest(), "the committed dir is kept")
	assert.Zero(t, segmentOf(t, idx, 1), "the old key is gone")

	insert(t, idx, 20, 3)
	ch.ObserveTimeTick(20)
	assert.EqualValues(t, 20, segmentOf(t, idx, 3))
	assert.Empty(t, env.fatalList())
}

// inHeap reports whether idx is in the LastAcked heap of its PChannelIndexManager.
func inHeap(ch *PChannelIndexManager, idx *index.Index) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.acked.contains(idx)
}

// TestLeftoverInEngineDirOnlyIsRemovedAtOpen checks that a file left in the
// engine directory is removed at open, the committed directory is kept, and
// the Index starts empty.
func TestLeftoverInEngineDirOnlyIsRemovedAtOpen(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	engineDir := filepath.Join(env.root, "v0", "engine")
	committed := filepath.Join(env.root, "v0", "committed")
	require.NoError(t, os.MkdirAll(engineDir, 0o755))
	require.NoError(t, os.MkdirAll(committed, 0o755))
	stale := filepath.Join(engineDir, "stale")
	require.NoError(t, os.WriteFile(stale, []byte("partial"), 0o600))
	kept := filepath.Join(committed, "1001.sst")
	require.NoError(t, os.WriteFile(kept, []byte("table"), 0o600))

	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.NoFileExists(t, stale)
	assert.FileExists(t, kept)
	assert.Empty(t, idx.EngineForTest().FrozenGenerations())
	assert.Zero(t, segmentOf(t, idx, 1))
	assert.Empty(t, env.fatalList())
}

// TestFirstHandoverCreatesTheManifest checks that a ready Index uploads under
// the SST base path that it computes, and creates the manifest with its first
// handover.
func TestFirstHandoverCreatesTheManifest(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.Equal(t, "root/pk_index/100/v0/sst", idx.SSTBasePathForTest())

	insert(t, idx, 10, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	reqs := env.client.requestList()
	require.Len(t, reqs, 1)
	assert.Equal(t, schemapb.DataType_Int64, reqs[0].PKDataType)
	m := env.client.manifest("v0")
	assert.EqualValues(t, 1, m.Version)
	assert.Equal(t, idx.SSTBasePathForTest(), m.SSTBasePath)
	keys := env.store.Keys()
	for _, ref := range m.Tables {
		assert.Contains(t, keys, pkroot.SSTKey(idx.SSTBasePathForTest(), int64(ref.ID)))
	}
	assert.Empty(t, env.fatalList())
}

func TestDecideErrorsReachTheCallerAndReleaseTheWriter(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ctx := context.Background()
	require.NoError(t, idx.EngineForTest().Write(ctx, []engine.Mutation{{Key: codec.EncodeInt64PK(1), Value: []byte{0xff, 0xff}}}))

	// Corrupted data is reported to the caller. The WAL interceptor, not the
	// Index, decides that it makes the WAL unavailable.
	_, err := idx.Authority().Decide(ctx, insertRequest(idx, 1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted))
	assert.Empty(t, env.fatalList())

	// The engine of another Index is closed while the Index stays open, so
	// Decide reaches the lookup and it fails with a closed error.
	other := env.add(ch, "v1")
	require.NoError(t, other.EngineForTest().Close())
	_, err = other.Authority().Decide(ctx, insertRequest(other, 2))
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.False(t, errors.Is(err, pkroot.ErrCorrupted))
	// the failed Decide released the writer
	ch.ObserveTimeTick(30)
	assert.EqualValues(t, 30, ch.wm.lastApplied())
}

func TestUploadFailuresKeepThePending(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(10)
	env.store.FailUploads(3)

	ch.step(farFuture)
	p := idx.PendingForTest()
	require.NotNil(t, p, "the pending is recorded before the upload")
	gens := idx.EngineForTest().FrozenGenerations()
	require.Len(t, gens, 1)
	for i := 0; i < 2; i++ {
		insert(t, idx, 20, int64(3+i))
		ch.ObserveTimeTick(uint64(20 + i))
		ch.step(farFuture)
		assert.Same(t, p, idx.PendingForTest())
		assert.Equal(t, gens, idx.EngineForTest().FrozenGenerations(), "a failed upload does not rotate again")
	}
	assert.Equal(t, 3, scheduleOf(ch, idx).failures)
	assert.Empty(t, env.client.requestList())

	ch.step(farFuture)
	assert.Nil(t, idx.PendingForTest())
	reqs := env.client.requestList()
	require.Len(t, reqs, 1)
	assert.EqualValues(t, 10, reqs[0].CheckpointTimeTick)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.EngineForTest().FrozenGenerations(), "exactly one rotate happened")
	assert.Equal(t, len(reqs[0].AddTables), env.store.Len())
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
	assert.Empty(t, env.fatalList())
}

func TestInstallDownloadsMissingTables(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	first := env.client.manifest("v0")
	require.NotEmpty(t, first.Tables)

	// the next install needs the first tables, which are not on this node
	dropCommittedFiles(t, idx)
	insert(t, idx, 20, 3)
	ch.ObserveTimeTick(20)
	ch.step(farFuture)

	assert.EqualValues(t, len(first.Tables), env.store.downloads.Load())
	for _, id := range sstIDs(first) {
		assert.FileExists(t, filepath.Join(idx.CommittedDirForTest(), sstName(id)))
	}
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
	assert.Empty(t, env.fatalList())
}

func TestInstallIfNewerIgnoresAnOlderVersion(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	insert(t, idx, 20, 2)
	ch.ObserveTimeTick(20)
	ch.step(farFuture)
	require.EqualValues(t, 2, idx.InstalledVersionForTest())
	paths := idx.InstalledPathsForTest()
	tables := idx.EngineForTest().Stats().CommittedTables

	older := env.client.version("v0", 1)
	require.NotNil(t, older)
	require.NoError(t, idx.InstallIfNewerForTest(context.Background(), *older, nil))
	assert.EqualValues(t, 2, idx.InstalledVersionForTest())
	assert.Equal(t, paths, idx.InstalledPathsForTest())
	assert.Equal(t, tables, idx.EngineForTest().Stats().CommittedTables)
	assert.EqualValues(t, 20, segmentOf(t, idx, 2))
}

func sstName(id int64) string {
	return strconv.FormatInt(id, 10) + sst.Extension
}

func TestRetryReuploadsOldTables(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(10)
	env.client.set(func(c *fakeClient) {
		c.commitErrs = []error{merr.WrapErrServiceUnavailable("down"), merr.WrapErrServiceUnavailable("down")}
	})
	ch.step(farFuture)
	p := idx.PendingForTest()
	require.NotNil(t, p)
	uploads := env.store.uploads.Load()
	objects := env.store.Len()
	keys := env.store.Keys()
	require.EqualValues(t, len(p.Request().AddTables), uploads)

	// a recent upload is not repeated
	ch.step(farFuture)
	require.Same(t, p, idx.PendingForTest())
	assert.Equal(t, uploads, env.store.uploads.Load())

	env.setParams(func(p *Params) { p.ReuploadAfter = 0 })
	ch.step(farFuture)
	assert.Nil(t, idx.PendingForTest())
	assert.EqualValues(t, uploads+int64(len(p.Request().AddTables)), env.store.uploads.Load())
	assert.Equal(t, objects, env.store.Len())
	assert.Equal(t, keys, env.store.Keys(), "the upload repeats under the same keys")
	reqs := env.client.requestList()
	require.Len(t, reqs, 3)
	assert.Equal(t, reqs[0], reqs[2])
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
}

func TestParamsFromConfig(t *testing.T) {
	paramtable.Init()
	p := ParamsFromConfig()
	assert.Zero(t, p.ReuploadAfter, "the caller that injects the Committer sets it")
	assert.Equal(t, time.Second, p.BackoffBase)
	assert.Equal(t, 60*time.Second, p.MaxBackoff)
	assert.EqualValues(t, 64<<20, p.ActiveBytes)
	assert.EqualValues(t, 2<<30, p.TailBytes)
	assert.EqualValues(t, 8<<30, p.SoftWatermark)
	assert.EqualValues(t, 4<<20, p.MemTableSize)
	assert.Positive(t, p.LockStripes)
}

func TestNoCommitterDedupsButNeverHandsOver(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.mgr.cfg.Committer = nil
	ch := env.channel(0)
	idx, created, err := ch.CreateCollection(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	require.NotNil(t, idx)

	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(10)
	// the second insert of key 1 is a duplicate and moves it to segment 20
	insert(t, idx, 20, 1)
	ch.ObserveTimeTick(20)
	assert.EqualValues(t, 20, segmentOf(t, idx, 1))
	assert.EqualValues(t, 10, segmentOf(t, idx, 2))

	assert.False(t, idx.HasCommitter())
	assert.False(t, inHeap(ch, idx))
	assert.Empty(t, ch.pickHandovers(farFuture))
	ch.step(farFuture)
	assert.Empty(t, env.client.requestList())
	assert.Zero(t, env.client.manifest("v0").Version, "no claim reached the manifest service")
	assert.EqualValues(t, uint64(math.MaxUint64), ch.LastAcked(), "a node without a Committer never holds LastAcked back")
	assert.Empty(t, env.fatalList())

	ch.remove(context.Background(), "v0")
	assert.Nil(t, ch.get("v0"))
}

// TestNotReadyIndexDecidesDegraded checks that an Index whose committed table
// set is not installed treats every key as present: an insert gets a
// companion delete for every key and a delete writes a tombstone for every
// key, so that no record that the install brings back comes back to life.
func TestNotReadyIndexDecidesDegraded(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.recoveredChannel(0, "v0")
	idx := ch.get("v0")
	ctx := context.Background()
	require.False(t, idx.Ready())

	guard, err := idx.Authority().Decide(ctx, insertRequest(idx, 1, 2))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1, 2), guard.Decision().CompanionDeletePKs, "absent keys count as present")
	guard.Apply(ctx, authority.Applied{SegmentID: 10})

	guard, err = idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindDelete, VChannel: idx.VChannel(), PKs: int64PKs(2, 3)})
	require.NoError(t, err)
	assert.Equal(t, int64PKs(2, 3), guard.Decision().PresentPKs, "a tombstone for every key")
	guard.Apply(ctx, authority.Applied{})
	assert.Zero(t, segmentOf(t, idx, 2), "the tombstone removed key 2")

	idx.MarkReady(ctx, false)
	guard, err = idx.Authority().Decide(ctx, insertRequest(idx, 1, 3))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1), guard.Decision().CompanionDeletePKs, "ready: only the present key")
	guard.Discard(ctx, nil)
}

func TestOpenKeepsCommittedDirAndWipesEngineDir(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	env := NewFakeEnv(t, root)
	c := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	idx, _, err := c.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	committed := idx.CommittedDirForTest()
	require.NoError(t, os.MkdirAll(committed, 0o755))
	kept := filepath.Join(committed, "7.sst")
	require.NoError(t, os.WriteFile(kept, []byte("table"), 0o644))
	idx.MarkReady(ctx, false)
	insert(t, idx, 1, 5)
	engineDir := filepath.Join(index.Dir(root, "v1"), "engine")
	c.RemoveForTest(ctx, "v1") // keeps the files, like a restart
	require.DirExists(t, engineDir)

	idx, _, err = c.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	assert.FileExists(t, kept)
	idx.MarkReady(ctx, false)
	g, err := idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindDelete, VChannel: "v1", PKs: []authority.PK{authority.Int64PK(5)}})
	require.NoError(t, err)
	assert.Empty(t, g.Decision().PresentPKs, "the engine dir was wiped")
	g.Discard(ctx, nil)
	c.Close()
}

func TestResolveLocalRedownloadsATableWithTheWrongSize(t *testing.T) {
	ctx := context.Background()
	env := NewFakeEnv(t, t.TempDir())
	c := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	t.Cleanup(c.Close)
	idx, _, err := c.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	idx.MarkReady(ctx, false)
	// a real table in the store, built by one handover
	insert(t, idx, 1, 1, 2, 3)
	c.ObserveTimeTick(12)
	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.Request().AddTables[0]
	key := pkroot.SSTKey(idx.SSTBasePathForTest(), int64(ref.ID))
	store := env.Manager.StoreForTest().(*index.FakeStore)
	require.NoError(t, store.UploadFile(ctx, key, p.Local()[ref.ID]))
	// corrupt the local copy: wrong size
	local := index.CommittedPathForTest(idx.CommittedDirForTest(), ref.ID)
	require.NoError(t, os.WriteFile(local, []byte("short"), 0o644))

	view := index.ManifestView{Version: 1, SSTBasePath: idx.SSTBasePathForTest(), CheckpointTimeTick: 11, Tables: []index.TableRef{ref}}
	require.NoError(t, idx.InstallIfNewerForTest(ctx, view, nil))
	fi, err := os.Stat(local)
	require.NoError(t, err)
	assert.Equal(t, ref.Size, fi.Size())
}

func TestResolveLocalReusesATableWithTheRightSize(t *testing.T) {
	ctx := context.Background()
	env := NewFakeEnv(t, t.TempDir())
	c := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	t.Cleanup(c.Close)
	idx, _, err := c.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	idx.MarkReady(ctx, false)
	insert(t, idx, 1, 1, 2, 3)
	c.ObserveTimeTick(12)
	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.Request().AddTables[0]
	key := pkroot.SSTKey(idx.SSTBasePathForTest(), int64(ref.ID))
	store := env.Manager.StoreForTest().(*index.FakeStore)
	require.NoError(t, store.UploadFile(ctx, key, p.Local()[ref.ID]))
	// a download would fail, so the install must use the local file
	store.FailDownloads(1)

	view := index.ManifestView{Version: 1, SSTBasePath: idx.SSTBasePathForTest(), CheckpointTimeTick: 11, Tables: []index.TableRef{ref}}
	require.NoError(t, idx.InstallIfNewerForTest(ctx, view, nil))
}
