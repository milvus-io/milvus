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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
)

// A retirement interrupted after its rename must not bring the generation
// back: a frozen generation is read before the committed set, so a stale one
// would hide newer committed data. Open deletes it, even half removed, and
// the staging it leaves behind.
func TestOpenDeletesInterruptedRetirement(t *testing.T) {
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
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	staged, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, staged)
	require.NoError(t, e.Close())

	// the rename happened, then the deletion stopped partway
	genDir := filepath.Join(dir, fmt.Sprintf("%s%d", incrementDirPrefix, gen))
	retired := filepath.Join(dir, fmt.Sprintf("%s%d", retiredDirPrefix, gen))
	require.NoError(t, os.Rename(genDir, retired))
	ssts, err := filepath.Glob(filepath.Join(retired, "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, ssts)
	require.NoError(t, os.Remove(ssts[0]))

	e2, err := Open(ctx, cfg)
	require.NoError(t, err, "a half-removed retired generation must not stop Open")
	defer e2.Close()
	assert.Empty(t, e2.FrozenGenerations())
	assert.Nil(t, getOne(t, e2, pk(1)), "a retired generation must not serve reads again")
	for _, gone := range []string{retired, filepath.Join(dir, fmt.Sprintf("%s%d", stagedDirPrefix, gen))} {
		_, err := os.Stat(gone)
		assert.True(t, os.IsNotExist(err), "%s must be removed", gone)
	}
}

// A completed retirement leaves neither the generation nor its retired name.
func TestRetireLeavesNothingBehind(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	_, err = e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.NoError(t, e.DropFrozen(ctx, gen))

	for _, prefix := range []string{incrementDirPrefix, retiredDirPrefix, stagedDirPrefix} {
		_, err := os.Stat(filepath.Join(e.cfg.Dir, fmt.Sprintf("%s%d", prefix, gen)))
		assert.True(t, os.IsNotExist(err), "%s%d must be removed", prefix, gen)
	}
}

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
	_, err = e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(2), Value: entry(2)}}))
	_, err = e.StageFrozen(ctx, gen) // flushing twice is harmless on a frozen DB
	require.NoError(t, err)
	require.NoError(t, e.Close())

	// reopening restores both DBs, so the interrupted cycle can be resumed
	// instead of leaking a directory
	e2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer e2.Close()
	assert.Equal(t, []Generation{gen}, e2.FrozenGenerations())
	requireSegment(t, e2, pk(1), 1)

	infos, err := e2.StageFrozen(ctx, gen)
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

// A zero SharedConfig must still give a block cache that keeps what Preload
// put in it. A zero-byte pebble cache retains nothing, and every lookup would
// then fault the index and the filter in under the engine's locks.
func TestSharedConfigDefaultCacheBytes(t *testing.T) {
	assert.Equal(t, int64(DefaultCacheBytes), SharedConfig{}.cacheBytes())
	assert.Equal(t, int64(DefaultCacheBytes), SharedConfig{CacheBytes: -1}.cacheBytes())
	assert.Equal(t, int64(1<<20), SharedConfig{CacheBytes: 1 << 20}.cacheBytes())

	shared := NewSharedResources(SharedConfig{})
	t.Cleanup(shared.Release)
	e := newTestEngine(t, func(c *Config) { c.Shared = shared })
	ctx := context.Background()
	table := buildCommitted(t, t.TempDir(), 1, map[int64][]byte{1: entry(10), 100: entry(20)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{table}))

	before := shared.cache.Metrics().Misses
	assert.Nil(t, getOne(t, e, pk(50)), "in range, but no table holds it")
	assert.Equal(t, before, shared.cache.Metrics().Misses,
		"the default cache must hold the index and the filter that Preload read")
}

func TestStats(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	assert.Zero(t, e.Stats().FrozenGenerations)

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, e.Stats().FrozenGenerations)

	infos, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	tables := commitTables(t, e, t.TempDir(), infos, nil)
	assert.Equal(t, len(tables), e.Stats().CommittedTables)
}

// ActiveBytes counts len(key)+len(value) per applied mutation and len(key)
// for a delete.
func TestActiveBytesCountsAppliedMutations(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	assert.Zero(t, e.Stats().ActiveBytes)

	require.NoError(t, e.Write(ctx, []Mutation{
		{Key: pk(1), Value: entry(1)},
		{Key: pk(2), Value: entry(2)},
	}))
	want := uint64(2 * (len(pk(1)) + len(entry(1))))
	assert.Equal(t, want, e.Stats().ActiveBytes)

	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Delete: true}}))
	want += uint64(len(pk(1)))
	assert.Equal(t, want, e.Stats().ActiveBytes)
}

// A batch that Write rejects is not applied, so it adds nothing, not even for
// the mutations before the one that was rejected.
func TestActiveBytesIgnoresRejectedBatch(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	before := e.Stats().ActiveBytes

	err := e.Write(ctx, []Mutation{
		{Key: pk(2), Value: entry(2)},
		{Key: pk(3), Value: codec.EncodeTombstone()},
	})
	require.Error(t, err)
	assert.Equal(t, before, e.Stats().ActiveBytes)
}

// The generation that RotateIncrement starts begins at 0; the frozen one keeps
// what it was written.
func TestActiveBytesRestartsAfterRotate(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	written := e.Stats().ActiveBytes
	require.Positive(t, written)

	_, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	assert.Zero(t, e.Stats().ActiveBytes)
	assert.Equal(t, written, e.frozen[0].written())
}

// Writes racing with rotations are each counted once, on the generation that
// took them, so the counts summed over all generations equal what was written.
// After Open the reopened active generation starts at 0, even though it still
// holds what was written before the restart. This is a documented limit.
func TestActiveBytesStartsAtZeroAfterOpen(t *testing.T) {
	shared := NewSharedResources(SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	cfg := Config{
		Dir: t.TempDir(), VChannel: "vc", Shared: shared,
		AllocID: func(context.Context) (int64, error) { return 1, nil },
	}
	ctx := context.Background()

	e, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	// flush so the data survives the restart; the active DB keeps no WAL
	require.NoError(t, e.active.db.Flush())
	require.Positive(t, e.Stats().ActiveBytes)
	require.NoError(t, e.Close())

	e2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer e2.Close()
	requireSegment(t, e2, pk(1), 1)
	assert.Zero(t, e2.Stats().ActiveBytes)
}

func TestActiveBytesUnderConcurrentRotate(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	const writers, perWriter, rotations = 4, 200, 10
	perMutation := uint64(len(pk(0)) + len(entry(0)))

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := int64(w*perWriter + i)
				assert.NoError(t, e.Write(ctx, []Mutation{{Key: pk(k), Value: entry(k)}}))
			}
		}(w)
	}
	for r := 0; r < rotations; r++ {
		_, err := e.RotateIncrement(ctx)
		require.NoError(t, err)
	}
	wg.Wait()

	e.mu.RLock()
	sum := e.active.written()
	for _, g := range e.frozen {
		sum += g.written()
	}
	e.mu.RUnlock()
	assert.Equal(t, uint64(writers*perWriter)*perMutation, sum)
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
		assert.True(t, errors.Is(err, pkindex.ErrUnavailable), "want the unavailable category, got %v", err)
	}
	requireClosed(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	_, err = e.MultiGet(ctx, [][]byte{pk(1)})
	requireClosed(t, err)
	_, err = e.RotateIncrement(ctx)
	requireClosed(t, err)
	_, err = e.StageFrozen(ctx, gen)
	requireClosed(t, err)
	requireClosed(t, e.DropFrozen(ctx, gen))
	requireClosed(t, e.InstallCommitted(ctx, nil))
	assert.NoError(t, e.Close(), "closing twice is a no-op")
}

// A missing SharedResources is a wiring bug in Milvus, so it carries no
// category: nothing downstream should retry it or treat it as bad data.
func TestOpenWithoutSharedResourcesIsCallerBug(t *testing.T) {
	_, err := Open(context.Background(), Config{Dir: t.TempDir(), VChannel: "test-vchannel-v0"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNoSharedResources), "got %v", err)
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.False(t, errors.Is(err, pkindex.ErrIO))
	assert.False(t, errors.Is(err, pkindex.ErrCorrupted))
}

// An ID allocator is not optional: without one a flush could not name its
// output, and that is a wiring bug rather than a condition to act on.
func TestOpenWithoutAllocIDIsCallerBug(t *testing.T) {
	shared := NewSharedResources(SharedConfig{CacheBytes: 1 << 20})
	t.Cleanup(shared.Release)
	_, err := Open(context.Background(), Config{Dir: t.TempDir(), VChannel: "vc", Shared: shared})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNoAllocID), "got %v", err)
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.False(t, errors.Is(err, pkindex.ErrIO))
}
