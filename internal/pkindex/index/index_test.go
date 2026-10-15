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

package index

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/objpath"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTwoHandoversInOneProcess(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	insert(t, idx, 10, 1, 2, 3)
	ch.Tick(10)
	ch.step(farFuture)
	first := env.client.manifest("v0")
	require.EqualValues(t, 1, first.Version, "the first flush creates the manifest")
	require.EqualValues(t, 10, first.CheckpointTimeTick)
	require.NotEmpty(t, first.Tables)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.eng.FrozenGenerations())

	insert(t, idx, 20, 4, 5)
	ch.Tick(20)
	ch.step(farFuture)
	second := env.client.manifest("v0")
	require.EqualValues(t, 2, second.Version)
	// the second install holds the tables of the first, which come from installedPaths
	assert.Subset(t, sstIDs(second), sstIDs(first))
	assert.Equal(t, len(second.Tables), idx.eng.Stats().CommittedTables)
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
	assert.EqualValues(t, 20, segmentOf(t, idx, 4))
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
	assert.Zero(t, env.store.downloads.Load())
	assert.False(t, idx.dirty())
	assert.True(t, idx.fresh())
}

func TestLostResponseRetriesTheSameRequest(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	insert(t, idx, 10, 1, 2)
	ch.Tick(10)
	env.client.set(func(c *fakeClient) { c.loseNext = true })
	ch.step(farFuture)
	require.NotNil(t, idx.pending.Load())
	assert.Equal(t, 1, idx.backoff.Failures())
	assert.Len(t, idx.eng.FrozenGenerations(), 1)

	// new writes and a new tick do not change the retried request
	insert(t, idx, 20, 3)
	ch.Tick(20)
	ch.step(farFuture)
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
	assert.Nil(t, idx.pending.Load())
	assert.Zero(t, idx.backoff.Failures())
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.eng.FrozenGenerations())
	assert.True(t, idx.dirty(), "the write after the rotate is not committed")

	ch.step(farFuture)
	reqs = env.client.requestList()
	require.Len(t, reqs, 3)
	assert.EqualValues(t, 20, reqs[2].CheckpointTimeTick)
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
}

func TestReopenedIndexStartsEmptyAndDoesNotHandOver(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.Tick(10)
	// a frozen generation from a failed handover is left on disk too
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	ch.step(farFuture)
	require.Len(t, idx.eng.FrozenGenerations(), 1)
	ch.Remove(context.Background(), "v0")
	require.Empty(t, env.client.manifest("v0").Tables)

	idx = env.add(ch, "v0")
	assert.False(t, idx.fresh())
	assert.Empty(t, idx.eng.FrozenGenerations())
	assert.NoDirExists(t, idx.committedDir)
	assert.Zero(t, segmentOf(t, idx, 1), "the old key is gone")

	insert(t, idx, 20, 3)
	ch.Tick(20)
	assert.EqualValues(t, 20, segmentOf(t, idx, 3))
	assert.Empty(t, ch.pickHandovers(farFuture))
	ch.step(farFuture)
	assert.Len(t, env.client.requestList(), 1, "only the request from before the reopen")
	assert.False(t, inHeap(idx))
	assert.EqualValues(t, 20, ch.LastAcked(), "the Index does not hold LastAcked back")
	assert.Empty(t, env.fatalList())
}

// inHeap reports whether idx is in the LastAcked heap of its Channel.
func inHeap(idx *Index) bool {
	idx.ch.mu.Lock()
	defer idx.ch.mu.Unlock()
	return idx.heapPos >= 0
}

func TestFoundManifestStopsTheHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)
	ch.Tick(10)
	ch.step(farFuture)
	require.NotEmpty(t, env.client.manifest("v0").Tables)

	// the pchannel moves to a node with no local files, and the WAL opens again
	ch.Close()
	require.NoError(t, os.RemoveAll(filepath.Join(env.root, "v0")))
	block := make(chan struct{})
	env.client.set(func(c *fakeClient) { c.lookupBlock = block })
	ch = env.recoveredChannel(0, "v0")
	idx = ch.Get("v0")
	assert.True(t, idx.fresh(), "no local leftovers")
	// a write before the lookup puts the Index into the heap
	insert(t, idx, 20, 2)
	require.True(t, inHeap(idx))

	close(block)
	waitReady(t, idx)
	assert.False(t, idx.fresh())
	assert.False(t, inHeap(idx), "the lookup removes the Index from the heap")
	assert.EqualValues(t, 20, segmentOf(t, idx, 2), "the lookup keeps the new writes")
	insert(t, idx, 30, 3)
	ch.Tick(30)
	assert.False(t, inHeap(idx))
	assert.Empty(t, ch.pickHandovers(farFuture))
	ch.step(farFuture)
	assert.Len(t, env.client.requestList(), 1, "no CommitFlush after the move")
	assert.Empty(t, env.fatalList())
}

func TestCommittedLeftoverOnlyMakesTheIndexNotFresh(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	committed := filepath.Join(env.root, "v0", "committed")
	require.NoError(t, os.MkdirAll(committed, 0o755))
	// a download that did not finish, and no engine directory
	leftover := filepath.Join(committed, "1001.sst.download-123")
	require.NoError(t, os.WriteFile(leftover, []byte("partial"), 0o600))

	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.False(t, idx.fresh())
	assert.NoDirExists(t, committed)

	insert(t, idx, 10, 1)
	ch.Tick(10)
	assert.Empty(t, ch.pickHandovers(farFuture))
	ch.step(farFuture)
	assert.Empty(t, env.client.requestList())
	assert.Empty(t, env.fatalList())
}

// TestAnyManifestMakesTheIndexNotFresh checks that a manifest of any version
// makes the Index of an existing collection not fresh: an owner committed it,
// and the Index can miss the writes of that owner.
func TestAnyManifestMakesTheIndexNotFresh(t *testing.T) {
	cases := map[string]snapshot.ManifestView{
		"version 1 with no tables":    {Version: 1},
		"a later version with tables": {Version: 7, CheckpointTimeTick: 5, Tables: []snapshot.TableRef{{ID: 900}}},
	}
	for name, view := range cases {
		t.Run(name, func(t *testing.T) {
			env := newTestEnv(t, syncScheduler{})
			env.client.seed("v0", view)

			ch := env.recoveredChannel(0, "v0")
			idx := ch.Get("v0")
			waitReady(t, idx)
			assert.False(t, idx.fresh())

			insert(t, idx, 10, 1)
			ch.Tick(10)
			assert.False(t, inHeap(idx))
			assert.Empty(t, ch.pickHandovers(farFuture))
			ch.step(farFuture)
			assert.Empty(t, env.client.requestList(), "no CommitFlush")
			assert.EqualValues(t, 10, segmentOf(t, idx, 1), "the Index still deduplicates")
			assert.Empty(t, env.fatalList())
		})
	}
}

// TestNoManifestAndNoLeftoverIsFresh checks that the Index of an existing
// collection without a manifest and without local files from before is
// fresh, uploads under the SST base path that it computes, and creates the
// manifest with its first handover.
func TestNoManifestAndNoLeftoverIsFresh(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.recoveredChannel(0, "v0")
	idx := ch.Get("v0")
	waitReady(t, idx)
	assert.True(t, idx.fresh())
	assert.Equal(t, "root/pk_index/100/v0/sst", idx.SSTBasePath())

	insert(t, idx, 10, 1)
	ch.Tick(10)
	ch.step(farFuture)
	reqs := env.client.requestList()
	require.Len(t, reqs, 1)
	assert.Equal(t, schemapb.DataType_Int64, reqs[0].PKDataType)
	m := env.client.manifest("v0")
	assert.EqualValues(t, 1, m.Version)
	assert.Equal(t, idx.SSTBasePath(), m.SSTBasePath)
	keys := env.store.Keys()
	for _, ref := range m.Tables {
		assert.Contains(t, keys, objpath.SSTKey(idx.SSTBasePath(), int64(ref.ID)))
	}
	assert.Empty(t, env.fatalList())
}

func TestDecideCorruptionIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ctx := context.Background()
	require.NoError(t, idx.EngineForTest().Write(ctx, []engine.Mutation{{Key: codec.EncodeInt64PK(1), Value: []byte{0xff, 0xff}}}))

	_, err := idx.Decide(ctx, insertRequest(idx, 1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
	fatals := env.fatalList()
	require.Len(t, fatals, 1)
	assert.True(t, errors.Is(fatals[0], pkindex.ErrCorrupted))

	// A decider error that is not corruption is not fatal. The engine of
	// another Index is closed while the Index stays open, so Decide reaches
	// the decider and its lookup fails.
	other := env.add(ch, "v1")
	require.NoError(t, other.eng.Close())
	_, err = other.Decide(ctx, insertRequest(other, 2))
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.False(t, errors.Is(err, pkindex.ErrCorrupted))
	assert.Len(t, env.fatalList(), 1)
	// the failed Decide released the writer
	ch.Tick(30)
	assert.EqualValues(t, 30, other.LastAppliedTimeTick())
}

func TestUploadFailuresKeepThePending(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.Tick(10)
	env.store.FailUploads(3)

	ch.step(farFuture)
	p := idx.pending.Load()
	require.NotNil(t, p, "the pending is recorded before the upload")
	gens := idx.eng.FrozenGenerations()
	require.Len(t, gens, 1)
	for i := 0; i < 2; i++ {
		insert(t, idx, 20, int64(3+i))
		ch.Tick(uint64(20 + i))
		ch.step(farFuture)
		assert.Same(t, p, idx.pending.Load())
		assert.Equal(t, gens, idx.eng.FrozenGenerations(), "a failed upload does not rotate again")
	}
	assert.Equal(t, 3, idx.backoff.Failures())
	assert.Empty(t, env.client.requestList())

	ch.step(farFuture)
	assert.Nil(t, idx.pending.Load())
	reqs := env.client.requestList()
	require.Len(t, reqs, 1)
	assert.EqualValues(t, 10, reqs[0].CheckpointTimeTick)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.eng.FrozenGenerations(), "exactly one rotate happened")
	assert.Equal(t, len(reqs[0].AddTables), env.store.Len())
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
	assert.Empty(t, env.fatalList())
}

func TestInstallDownloadsMissingTables(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1, 2)
	ch.Tick(10)
	ch.step(farFuture)
	first := env.client.manifest("v0")
	require.NotEmpty(t, first.Tables)

	// the next install needs the first tables, which are not on this node
	dropCommittedFiles(t, idx)
	insert(t, idx, 20, 3)
	ch.Tick(20)
	ch.step(farFuture)

	assert.EqualValues(t, len(first.Tables), env.store.downloads.Load())
	for _, id := range sstIDs(first) {
		assert.FileExists(t, filepath.Join(idx.committedDir, sstName(id)))
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
	ch.Tick(10)
	ch.step(farFuture)
	insert(t, idx, 20, 2)
	ch.Tick(20)
	ch.step(farFuture)
	require.EqualValues(t, 2, idx.installedVersion)
	paths := idx.installedPaths
	tables := idx.eng.Stats().CommittedTables

	older := env.client.version("v0", 1)
	require.NotNil(t, older)
	require.NoError(t, idx.InstallIfNewer(context.Background(), *older, nil))
	assert.EqualValues(t, 2, idx.installedVersion)
	assert.Equal(t, paths, idx.installedPaths)
	assert.Equal(t, tables, idx.eng.Stats().CommittedTables)
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
	ch.Tick(10)
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	ch.step(farFuture)
	p := idx.pending.Load()
	require.NotNil(t, p)
	uploads := env.store.uploads.Load()
	objects := env.store.Len()

	env.setParams(func(p *Params) { p.ReuploadAfter = 0 })
	ch.step(farFuture)
	assert.Nil(t, idx.pending.Load())
	assert.EqualValues(t, uploads+int64(len(p.Request.AddTables)), env.store.uploads.Load())
	assert.Equal(t, objects, env.store.Len())
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
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
	created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	idx := ch.Get("v0")
	require.NotNil(t, idx)

	insert(t, idx, 10, 1, 2)
	ch.Tick(10)
	// the second insert of key 1 is a duplicate and moves it to segment 20
	insert(t, idx, 20, 1)
	ch.Tick(20)
	assert.EqualValues(t, 20, segmentOf(t, idx, 1))
	assert.EqualValues(t, 10, segmentOf(t, idx, 2))

	assert.False(t, idx.readyDone(), "no claim")
	assert.False(t, idx.fresh())
	assert.False(t, inHeap(idx))
	assert.Empty(t, ch.pickHandovers(farFuture))
	ch.step(farFuture)
	assert.Empty(t, env.client.requestList())
	assert.Zero(t, env.client.manifest("v0").Version, "no claim reached the manifest service")
	assert.EqualValues(t, 20, ch.LastAcked(), "the Index does not hold LastAcked back")
	assert.Empty(t, env.fatalList())

	ch.Remove(context.Background(), "v0")
	assert.Nil(t, ch.Get("v0"))
}
