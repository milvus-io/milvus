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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReopenedIndexStartsEmpty(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	// a frozen generation from a failed handover is left on disk too
	idx.committer.set(func(c *fakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	_, err := idx.Handover(ctx)
	require.Error(t, err)
	require.Len(t, idx.eng.FrozenGenerations(), 1)
	idx.reopen(t)
	require.Empty(t, idx.committer.manifest().Tables)

	assert.Empty(t, idx.eng.FrozenGenerations(), "the engine directory is wiped at the open")
	assert.Zero(t, idx.segmentOf(t, 1), "the old key is gone")

	idx.insert(t, 20, 3)
	assert.EqualValues(t, 20, idx.segmentOf(t, 3))
}

// TestLeftoverInCommittedOnlyIsKeptAtOpen checks that a file left in the
// committed directory alone survives the open: the install checks the
// committed directory against the manifest and removes what it does not
// list, so a restart keeps the tables it already has.
func TestLeftoverInCommittedOnlyIsKeptAtOpen(t *testing.T) {
	ctx := context.Background()
	idx := openTestIndex(t)
	idx.Close(ctx)
	require.NoError(t, os.RemoveAll(Dir(idx.root, testVChannel, testIndexID)))
	committed := filepath.Join(Dir(idx.root, testVChannel, testIndexID), "committed")
	require.NoError(t, os.MkdirAll(committed, 0o755))
	leftover := filepath.Join(committed, "1001.sst.download-123")
	require.NoError(t, os.WriteFile(leftover, []byte("partial"), 0o600))

	idx.open(t)
	assert.FileExists(t, leftover, "the open keeps the committed directory")
	assert.Empty(t, idx.eng.FrozenGenerations())
	assert.Zero(t, idx.segmentOf(t, 1))
	idx.SetCommitted(ManifestView{Version: 1})
	require.NoError(t, idx.Install(ctx))
	assert.NoFileExists(t, leftover, "the install removes what the manifest does not list")
}

func TestOpenKeepsCommittedDirAndWipesEngineDir(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	require.NoError(t, os.MkdirAll(idx.committedDir, 0o755))
	kept := filepath.Join(idx.committedDir, "7.sst")
	require.NoError(t, os.WriteFile(kept, []byte("table"), 0o600))
	idx.insert(t, 1, 5)
	engineDir := filepath.Join(Dir(idx.root, testVChannel, testIndexID), "engine")
	idx.Close(ctx) // keeps the files, like a restart
	require.DirExists(t, engineDir)

	idx.open(t)
	assert.FileExists(t, kept)
	idx.MarkReady(ctx)
	g, err := idx.Decider().Decide(ctx, insertRequest(5))
	require.NoError(t, err)
	assert.Empty(t, g.CompanionDeletePKs(), "the engine dir was wiped")
	g.NotAppended()
}

// corruptedEngine stands for an engine that finds a damaged table file: every
// read fails with an error marked ErrCorrupted.
type corruptedEngine struct {
	engine.RW
}

func (corruptedEngine) MultiGet(context.Context, [][]byte) ([][]byte, error) {
	return nil, errors.Mark(errors.New("damaged table file"), pkroot.ErrCorrupted)
}

func TestDecideErrorsReachTheCaller(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.auth = dedup.New(idx.vchannel, corruptedEngine{RW: idx.eng}, dedup.Config{Ready: idx.ready})

	// Corrupted data is reported to the caller. The WAL interceptor, not the
	// Index, decides that it makes the WAL unavailable.
	_, err := idx.Decider().Decide(ctx, insertRequest(1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted))

	// The engine of another Index is closed while the Index stays open, so
	// Decide reaches the lookup and it fails with a closed error.
	other := newTestIndex(t)
	require.NoError(t, other.eng.Close())
	_, err = other.Decider().Decide(ctx, insertRequest(2))
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.False(t, errors.Is(err, pkroot.ErrCorrupted))
}

// TestNotReadyIndexWaits checks that the writes of an Index whose committed
// table set is not installed wait for the install, and that the observe
// still applies the WAL to the engine meanwhile. Once the Index is ready,
// the decision is exact.
func TestNotReadyIndexWaits(t *testing.T) {
	ctx := context.Background()
	idx := openTestIndex(t)
	require.False(t, idx.Ready())

	_, err := idx.Decider().Decide(ctx, insertRequest(1, 3))
	var wait *dedup.WaitError
	require.ErrorAs(t, err, &wait)
	assert.Zero(t, wait.TimeTick, "the write waits only for the install")
	_, err = idx.Apply(ctx, Write{Puts: puts(10, 1, 2), TimeTick: 5})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- wait.Wait(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("Wait returned before the Index was ready: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	idx.MarkReady(ctx)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after MarkReady")
	}

	intent, err := idx.Decider().Decide(ctx, insertRequest(1, 3))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1), intent.CompanionDeletePKs(), "ready: only the present key")
	intent.NotAppended()
}

func TestInstallDownloadsMissingTables(t *testing.T) {
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	require.True(t, idx.handover(t))
	first := idx.committer.manifest()
	require.NotEmpty(t, first.Tables)

	// the next install needs the first tables, which are not on this node
	require.NoError(t, os.RemoveAll(idx.committedDir))
	idx.installedPaths = nil
	idx.insert(t, 20, 3)
	idx.observed.Store(20)
	require.True(t, idx.handover(t))

	assert.EqualValues(t, len(first.Tables), idx.store.downloads.Load())
	for _, id := range sstIDs(first) {
		assert.FileExists(t, committedPath(idx.committedDir, sst.ID(id)))
	}
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.EqualValues(t, 10, idx.segmentOf(t, 1))
}

func TestInstallIfNewerIgnoresAnOlderVersion(t *testing.T) {
	idx := newTestIndex(t)
	idx.insert(t, 10, 1)
	idx.observed.Store(10)
	require.True(t, idx.handover(t))
	idx.insert(t, 20, 2)
	idx.observed.Store(20)
	require.True(t, idx.handover(t))
	require.EqualValues(t, 2, idx.currentInstalledVersion())
	paths := idx.installedPaths
	tables := idx.eng.Stats().CommittedTables

	older := idx.committer.version(1)
	require.NotNil(t, older)
	require.NoError(t, idx.installIfNewer(context.Background(), *older, nil))
	assert.EqualValues(t, 2, idx.currentInstalledVersion())
	assert.Equal(t, paths, idx.installedPaths)
	assert.Equal(t, tables, idx.eng.Stats().CommittedTables)
	assert.EqualValues(t, 20, idx.segmentOf(t, 2))
}

func TestResolveLocalRedownloadsATableWithTheWrongSize(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	// a real table in the store, built by one handover
	idx.insert(t, 1, 1, 2, 3)
	idx.observed.Store(12)
	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.request.AddTables[0]
	require.NoError(t, idx.store.UploadFile(ctx, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)), p.local[ref.ID]))
	// corrupt the local copy: wrong size
	local := committedPath(idx.committedDir, ref.ID)
	require.NoError(t, os.WriteFile(local, []byte("short"), 0o600))

	view := ManifestView{Version: 1, SSTBasePath: idx.sstBasePath, CheckpointTimeTick: 11, Tables: []TableRef{ref}}
	require.NoError(t, idx.installIfNewer(ctx, view, nil))
	fi, err := os.Stat(local)
	require.NoError(t, err)
	assert.Equal(t, ref.Size, fi.Size())
}

func TestResolveLocalReusesATableWithTheRightSize(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 1, 1, 2, 3)
	idx.observed.Store(12)
	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.request.AddTables[0]
	require.NoError(t, idx.store.UploadFile(ctx, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)), p.local[ref.ID]))
	// a download would fail, so the install must use the local file
	idx.store.FailDownloads(1)

	view := ManifestView{Version: 1, SSTBasePath: idx.sstBasePath, CheckpointTimeTick: 11, Tables: []TableRef{ref}}
	require.NoError(t, idx.installIfNewer(ctx, view, nil))
}
