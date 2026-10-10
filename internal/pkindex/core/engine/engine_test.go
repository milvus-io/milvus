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

package engine

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

func TestWarmRestartRestoresGenerations(t *testing.T) {
	shared := NewSharedResources(SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	dir := t.TempDir()
	var nextID atomic.Int64
	nextID.Store(1000)
	cfg := Config{
		Dir: dir, VChannel: "vc", Shared: shared,
		AllocID: func(context.Context) (int64, error) { return nextID.Add(1), nil },
	}
	ctx := context.Background()

	e, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))

	// leave a cycle half-finished: frozen, flushed, but never dropped
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	_, err = e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(2), Value: entry(2)}}))
	_, err = e.FlushDraining(ctx, gen) // flushing twice is harmless on a frozen DB
	require.NoError(t, err)
	require.NoError(t, e.Close())

	// reopening restores both DBs, so the interrupted cycle can be resumed
	// instead of leaking a directory
	e2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer e2.Close()
	assert.Equal(t, []Generation{gen}, e2.DrainingGenerations())
	requireSegment(t, e2, pk(1), 1)

	infos, err := e2.FlushDraining(ctx, gen)
	require.NoError(t, err)
	var total int64
	for _, info := range infos {
		total += info.Info.NumEntries
	}
	assert.Equal(t, int64(1), total)
}

func TestUnflushedWritesDoNotSurviveRestart(t *testing.T) {
	// the engine's own WAL is disabled on purpose: unflushed state is expected
	// to be lost and rebuilt by WAL replay, so the recovery layer must never
	// assume otherwise
	shared := NewSharedResources(SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	dir := t.TempDir()
	var nextID atomic.Int64
	nextID.Store(1000)
	cfg := Config{
		Dir: dir, VChannel: "vc", Shared: shared,
		AllocID: func(context.Context) (int64, error) { return nextID.Add(1), nil },
	}
	ctx := context.Background()

	e, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	require.NoError(t, e.Close())

	e2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer e2.Close()
	assert.Nil(t, getOne(t, e2, pk(1)))
}

func TestSharedConfigTableCacheSize(t *testing.T) {
	files, shards := SharedConfig{}.tableCacheSize()
	assert.Equal(t, DefaultMaxOpenFiles, files)
	assert.Equal(t, runtime.GOMAXPROCS(0), shards)

	files, shards = SharedConfig{MaxOpenFiles: 100, TableCacheShards: 4}.tableCacheSize()
	assert.Equal(t, 100, files)
	assert.Equal(t, 4, shards)

	// a shard never gets a zero quota
	files, shards = SharedConfig{MaxOpenFiles: 2, TableCacheShards: 16}.tableCacheSize()
	assert.Equal(t, 2, files)
	assert.Equal(t, 2, shards)

	// explicit sizes build a usable cache pair
	shared := NewSharedResources(SharedConfig{CacheBytes: 1 << 20, MaxOpenFiles: 64, TableCacheShards: 2})
	shared.Release()
}

func TestStats(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	s := e.Stats()
	assert.Positive(t, s.MemTableBytes)
	assert.Zero(t, s.DrainingGenerations)

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, e.Stats().DrainingGenerations)

	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	tables := commitTables(t, e, t.TempDir(), infos, nil)
	assert.Equal(t, len(tables), e.Stats().CommittedTables)
}

func TestDestroy(t *testing.T) {
	shared := NewSharedResources(SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	dir := t.TempDir()
	ctx := context.Background()
	e, err := Open(ctx, Config{
		Dir: dir, VChannel: "vc", Shared: shared,
		AllocID: func(context.Context) (int64, error) { return 1, nil },
	})
	require.NoError(t, err)
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	_, err = e.RotateIncrement(ctx)
	require.NoError(t, err)

	require.NoError(t, e.Destroy())
	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err), "destroy must drop every generation")
}

// Every method rejecting work after Close must carry both the sentinel a
// caller branches on and the category the boundary translates.
func TestClosedEngineRejects(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	require.NoError(t, e.Close())

	requireClosed := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrClosed), "want ErrClosed, got %v", err)
		assert.True(t, errors.Is(err, pkerr.ErrUnavailable), "want the unavailable category, got %v", err)
	}
	requireClosed(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	_, err = e.MultiGet(ctx, [][]byte{pk(1)})
	requireClosed(t, err)
	_, err = e.RotateIncrement(ctx)
	requireClosed(t, err)
	_, err = e.FlushDraining(ctx, gen)
	requireClosed(t, err)
	requireClosed(t, e.DropDraining(ctx, gen))
	requireClosed(t, e.InstallCommitted(ctx, nil))
	assert.NoError(t, e.Close(), "closing twice is a no-op")
}

// A missing SharedResources is a wiring bug in Milvus, so it carries no
// category: nothing downstream should retry it or treat it as bad data.
func TestOpenWithoutSharedResourcesIsCallerBug(t *testing.T) {
	_, err := Open(context.Background(), Config{Dir: t.TempDir(), VChannel: "test-vchannel-v0"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNoSharedResources), "got %v", err)
	assert.False(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.False(t, errors.Is(err, pkerr.ErrIO))
	assert.False(t, errors.Is(err, pkerr.ErrCorrupted))
}

// An ID allocator is not optional: without one a flush could not name its
// output, and that is a wiring bug rather than a condition to act on.
func TestOpenWithoutAllocIDIsCallerBug(t *testing.T) {
	shared := NewSharedResources(SharedConfig{CacheBytes: 1 << 20})
	t.Cleanup(shared.Release)
	_, err := Open(context.Background(), Config{Dir: t.TempDir(), VChannel: "vc", Shared: shared})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNoAllocID), "got %v", err)
	assert.False(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.False(t, errors.Is(err, pkerr.ErrIO))
}
