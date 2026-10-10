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
	"path/filepath"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
)

func TestHandoverCycle(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()

	const n = 1000
	muts := make([]Mutation, 0, n)
	for i := int64(0); i < n; i++ {
		muts = append(muts, Mutation{Key: pk(i), Value: entry(i)})
	}
	require.NoError(t, e.Write(ctx, muts))

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	assert.Equal(t, []Generation{gen}, e.DrainingGenerations())
	// data stays readable while its DB is frozen
	requireSegment(t, e, pk(42), 42)

	flushed, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, flushed)
	var total int64
	for _, f := range flushed {
		total += f.Info.NumEntries
	}
	assert.Equal(t, int64(n), total, "the frozen generation must hold everything written to it")

	tables := commitTables(t, e, baseDir, flushed, nil, gen)
	assert.Empty(t, e.DrainingGenerations())

	// the committed set answers now, and the generation is gone from disk
	requireSegment(t, e, pk(42), 42)
	_, err = os.Stat(filepath.Join(e.cfg.Dir, "increment-1"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(e.cfg.Dir, "staged-1"))
	assert.True(t, os.IsNotExist(err), "retiring a generation removes its staging")
	assert.Len(t, tables, len(flushed))

	// the fresh active DB keeps taking writes
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(n), Value: entry(n)}}))
	requireSegment(t, e, pk(n), n)
}

// TestFlushDrainingOrdersNewestFirst covers one generation holding several
// overlapping L0 SSTs, which happens whenever a memtable fills up before the
// rotation. The drained set must come back newest first, because that is the
// order InstallCommitted resolves overlapping keys in.
func TestFlushDrainingOrdersNewestFirst(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()
	// stands in for a memtable filling up mid-generation
	flushActive := func() { require.NoError(t, e.active.db.Flush()) }

	require.NoError(t, e.Write(ctx, []Mutation{
		{Key: pk(1), Value: entry(10)},
		{Key: pk(2), Value: entry(20)},
	}))
	flushActive()
	require.NoError(t, e.Write(ctx, []Mutation{
		{Key: pk(1), Delete: true},
		{Key: pk(2), Value: entry(21)},
	}))
	flushActive()
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(2), Value: entry(22)}}))

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.Len(t, infos, 3)

	// the newest table holds only pk(2)
	assert.Equal(t, int64(1), infos[0].Info.NumEntries)
	assert.Equal(t, pk(2), infos[0].Info.MinKey)

	commitTables(t, e, baseDir, infos, nil)
	require.NoError(t, e.DropDraining(ctx, gen))

	// answered by the baseline alone now: the delete must still win over the
	// older put, and the latest put over the earlier ones
	assert.Nil(t, getOne(t, e, pk(1)), "a deleted pk must not resurrect from an older table")
	requireSegment(t, e, pk(2), 22)
}

// TestRotateKeepsConcurrentWrites is the reason rotation freezes instead of
// discarding: writes racing a snapshot cycle land in the new active DB, and a
// write that lands in the frozen DB just before the swap is still drained.
func TestRotateKeepsConcurrentWrites(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()

	const writers = 4
	const perWriter = 500
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := int64(w*perWriter + i)
				assert.NoError(t, e.Write(ctx, []Mutation{{Key: pk(key), Value: entry(key)}}))
			}
		}(w)
	}

	// run full handover cycles while the writers are running
	var tables []CommittedTable
	for round := 0; round < 3; round++ {
		gen, err := e.RotateIncrement(ctx)
		require.NoError(t, err)
		infos, err := e.FlushDraining(ctx, gen)
		require.NoError(t, err)
		tables = commitTables(t, e, baseDir, infos, tables, gen)
	}
	wg.Wait()

	// one final cycle to absorb whatever the writers wrote after the last one
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	commitTables(t, e, baseDir, infos, tables, gen)

	// every key written during the cycles survived
	for key := int64(0); key < writers*perWriter; key++ {
		requireSegment(t, e, pk(key), key)
	}
}

func TestDropDrainingRequiresRotate(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	_, err := e.FlushDraining(ctx, 1)
	assert.Error(t, err, "the active generation is not drainable")
	assert.Error(t, e.DropDraining(ctx, 1))

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	require.NoError(t, e.DropDraining(ctx, gen))
	assert.Error(t, e.DropDraining(ctx, gen), "dropping twice must fail")
}

// A generation the engine is not draining is a caller mistake it must be able
// to tell apart from a failure of the drain itself.
func TestDrainUnknownGeneration(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	const unknown = Generation(42)

	_, err := e.FlushDraining(ctx, unknown)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotDraining), "want ErrNotDraining, got %v", err)
	assert.Contains(t, err.Error(), "42")

	err = e.DropDraining(ctx, unknown)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotDraining), "want ErrNotDraining, got %v", err)
	assert.Contains(t, err.Error(), "42")

	// a generation that was drained already is no longer draining either
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	require.NoError(t, e.DropDraining(ctx, gen))
	assert.True(t, errors.Is(e.DropDraining(ctx, gen), ErrNotDraining))
}

func TestRotateOpensNewGenerationOutsideLock(t *testing.T) {
	e := newTestEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var origin func(*Engine, Generation) (*generation, error)
	mocker := mockey.Mock((*Engine).openGeneration).To(func(pe *Engine, gen Generation) (*generation, error) {
		close(entered)
		<-release
		return origin(pe, gen)
	}).Origin(&origin).Build()
	defer mocker.UnPatch()

	rotated := make(chan error, 1)
	go func() {
		_, err := e.RotateIncrement(context.Background())
		rotated <- err
	}()
	<-entered
	defer func() {
		close(release)
		require.NoError(t, <-rotated)
		requireSegment(t, e, pk(7), 70)
	}()
	requireReadWriteUnblocked(t, e)
}

func TestDropRemovesGenerationOutsideLock(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)

	entered, release := make(chan struct{}), make(chan struct{})
	var origin func(*generation) error
	mocker := mockey.Mock((*generation).retire).To(func(g *generation) error {
		close(entered)
		<-release
		return origin(g)
	}).Origin(&origin).Build()
	defer mocker.UnPatch()

	dropped := make(chan error, 1)
	go func() { dropped <- e.DropDraining(ctx, gen) }()
	<-entered
	defer func() {
		close(release)
		require.NoError(t, <-dropped)
	}()
	requireReadWriteUnblocked(t, e)
}

// The SSTs a memtable flush produces become baseline tables as they are, so
// they must carry the bloom filter too.
func TestDrainedTablesCarryBloomFilter(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	const n = 100000
	muts := make([]Mutation, 0, n)
	for i := int64(0); i < n; i++ {
		muts = append(muts, Mutation{Key: pk(i * 2), Value: entry(i)})
	}
	require.NoError(t, e.Write(ctx, muts))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	commitTables(t, e, t.TempDir(), infos, nil)
	require.NoError(t, e.DropDraining(ctx, gen))

	c := e.cfg.Shared.cache
	before := c.Metrics().Misses
	for i := int64(0); i < 500; i++ {
		require.Nil(t, getOne(t, e, pk(i*(2*n/500)+1)))
	}
	assert.Less(t, c.Metrics().Misses-before, int64(50), "baseline misses must be answered by the bloom filter")
	requireSegment(t, e, pk(4242), 2121)
}

// A memtable flush writes its SSTs through pebble, not through sst.Writer, so
// only a shared options template keeps the two producers on one table format.
// A baseline table is served to every other role, so a divergence here would
// only surface on another node.
func TestFlushOutputUsesPinnedTableFormat(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{
		{Key: pk(1), Value: entry(10)},
		{Key: pk(2), Value: entry(20)},
	}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, infos)

	for _, f := range infos {
		assert.Equal(t, sst.TableFormat, f.Info.TableFormat, "flush output %d", f.Info.ID)
		// the Info is read back from the file, so this is the file's own format
		r, err := sst.OpenReader(f.Path, nil, sst.ExpectSize(f.Info.Size))
		require.NoError(t, err)
		format, err := r.TableFormat()
		require.NoError(t, err)
		assert.Equal(t, sst.TableFormat, format)
		require.NoError(t, r.Close())
	}
}

// Installing and retiring are one step, so a reader never sees the window
// where the frozen generation is already gone and the tables that replace it
// are not yet in: that window would report a live key as absent.
func TestInstallAndRetireAreOneStep(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	dstDir := t.TempDir()
	require.NoError(t, e.Write(ctx, []Mutation{put(pk(1), 10)}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	flushed, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	tables := publishFlushed(t, flushed, dstDir)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				vs, err := e.MultiGet(ctx, [][]byte{pk(1)})
				if !assert.NoError(t, err) {
					return
				}
				if !assert.NotNil(t, vs[0], "the key must never read as absent during the swap") {
					return
				}
			}
		}()
	}
	require.NoError(t, e.InstallCommitted(ctx, tables, gen))
	close(stop)
	wg.Wait()
	requireSegment(t, e, pk(1), 10)
}

// A generation named in retire that is not draining is a caller mistake, and
// it must be caught before anything has changed.
func TestInstallCommittedRejectsUnknownRetire(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	table := buildCommitted(t, t.TempDir(), 7, map[int64][]byte{1: entry(10)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{table}))
	requireSegment(t, e, pk(1), 10)

	replacement := buildCommitted(t, t.TempDir(), 8, map[int64][]byte{1: entry(20)})
	err := e.InstallCommitted(ctx, []CommittedTable{replacement}, Generation(999))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotDraining), "got %v", err)

	// nothing changed: neither the committed set nor the generations
	requireSegment(t, e, pk(1), 10)
	assert.Equal(t, 1, e.Stats().CommittedTables)
	assert.Empty(t, e.DrainingGenerations())
}

// InstallCommitted promises that a lookup never pays to fault in an index or a
// filter, so the tables it installs are warm by the time it returns.
func TestInstallCommittedPreloadsNewTables(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	// the probed key has to fall inside the table's range, or pruning skips
	// the reader and the lookup proves nothing
	table := buildCommitted(t, t.TempDir(), 1, map[int64][]byte{1: entry(10), 100: entry(20)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{table}))

	c := e.cfg.Shared.cache
	before := c.Metrics().Misses
	assert.Nil(t, getOne(t, e, pk(50)), "in range, but no table holds it")
	assert.Equal(t, before, c.Metrics().Misses,
		"a lookup after the install must not fault in the index or the filter")
}

// A committed set usually repeats most of its tables. Reopening them would
// throw away what their blocks had cached, so the readers are kept.
func TestInstallCommittedReusesReaders(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	dir := t.TempDir()
	kept := buildCommitted(t, dir, 1, map[int64][]byte{1: entry(10)})
	replaced := buildCommitted(t, dir, 2, map[int64][]byte{2: entry(20)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{kept, replaced}))

	keptReader := e.committed[0].reader
	requireSegment(t, e, pk(1), 10)

	// swap the second table for a different one; the first must be untouched
	fresh := buildCommitted(t, dir, 3, map[int64][]byte{3: entry(30)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{kept, fresh}))
	assert.Same(t, keptReader, e.committed[0].reader, "a table that stayed keeps its reader")

	c := e.cfg.Shared.cache
	before := c.Metrics().Misses
	requireSegment(t, e, pk(1), 10)
	assert.Equal(t, before, c.Metrics().Misses,
		"the kept table's blocks must still be cached after the swap")
	requireSegment(t, e, pk(3), 30)
	assert.Nil(t, getOne(t, e, pk(2)), "the replaced table is gone from the set")
}

// Opening a table can fail, and until every new table is open the engine must
// not have changed anything.
func TestInstallCommittedLeavesStateOnOpenFailure(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	dir := t.TempDir()
	good := buildCommitted(t, dir, 1, map[int64][]byte{1: entry(10)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{good}))

	missing := CommittedTable{Info: good.Info, Path: filepath.Join(dir, "404.sst")}
	missing.Info.ID = 404
	err := e.InstallCommitted(ctx, []CommittedTable{missing, good})
	require.Error(t, err)

	assert.Equal(t, 1, e.Stats().CommittedTables)
	requireSegment(t, e, pk(1), 10)
}
