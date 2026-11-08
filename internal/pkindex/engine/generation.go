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
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// generation is one container of writes, backed today by a pebble DB.
type generation struct {
	gen        Generation
	dir        string
	stagedDir  string
	retiredDir string
	db         *pebble.DB
	// writtenBytes counts the logical bytes write applied to this generation;
	// only write adds to it. See Stats.ActiveBytes.
	writtenBytes atomic.Uint64
}

// retire closes the generation and deletes both its own directory and the
// tables staged out of it.
func (g *generation) retire() error {
	// a failed close still lets the rename below take the generation out of
	// every future Open, which is what retiring has to guarantee
	closeErr := g.db.Close()
	// The rename is the commit point. Once it is durable, no Open brings this
	// generation back, however far the deletion below gets: Open deletes
	// retired directories instead of reopening them.
	if err := os.Rename(g.dir, g.retiredDir); err != nil {
		return pkindex.MarkIO(err, "retire generation %s", g.dir)
	}
	if err := syncDir(filepath.Dir(g.dir)); err != nil {
		return pkindex.MarkIO(err, "sync engine dir after retiring %s", g.dir)
	}
	if err := os.RemoveAll(g.retiredDir); err != nil {
		return pkindex.MarkIO(err, "remove retired generation %s", g.retiredDir)
	}
	if err := os.RemoveAll(g.stagedDir); err != nil {
		return pkindex.MarkIO(err, "remove staged tables %s", g.stagedDir)
	}
	if closeErr != nil {
		return sst.MarkPebbleErr(errors.Wrapf(closeErr, "close generation %s", g.dir))
	}
	return nil
}

// syncDir makes the entries of a directory, such as a rename into it, durable.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func generationDir(dir string, gen Generation) string {
	return filepath.Join(dir, fmt.Sprintf("%s%d", incrementDirPrefix, gen))
}

func retiredDir(dir string, gen Generation) string {
	return filepath.Join(dir, fmt.Sprintf("%s%d", retiredDirPrefix, gen))
}

func stagedDir(dir string, gen Generation) string {
	return filepath.Join(dir, fmt.Sprintf("%s%d", stagedDirPrefix, gen))
}

// openGeneration opens, or creates, the pebble DB that backs generation gen.
func openGeneration(cfg Config, gen Generation) (*generation, error) {
	dir := generationDir(cfg.Dir, gen)
	// the shared template pins the format major version, the comparer and the
	// bloom filter, so a flush output is byte-compatible with a Writer output
	opts := sst.PebbleOptions()
	opts.DisableWAL = true
	opts.DisableAutomaticCompactions = true
	// L0 is bounded by external compaction on datanode and by rotation, not by
	// stalling writers: a stall here would block the WAL append path.
	opts.L0StopWritesThreshold = 1 << 20
	opts.Cache = cfg.Shared.cache
	opts.TableCache = cfg.Shared.tableCache
	if cfg.MemTableSize > 0 {
		opts.MemTableSize = cfg.MemTableSize
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, sst.MarkPebbleErr(errors.Wrapf(err, "open generation %s", dir))
	}
	return &generation{
		gen:        gen,
		dir:        dir,
		stagedDir:  stagedDir(cfg.Dir, gen),
		retiredDir: retiredDir(cfg.Dir, gen),
		db:         db,
	}, nil
}

// write applies muts to this generation as one batch and, once the batch is
// applied, adds their logical size to the generation's count. A rejected batch
// changes nothing.
func (g *generation) write(muts []Mutation) error {
	b := g.db.NewBatch()
	defer b.Close()
	var size uint64
	for _, m := range muts {
		v, err := storedValue(m)
		if err != nil {
			return err
		}
		if err := b.Set(m.Key, v, nil); err != nil {
			return sst.MarkPebbleErr(errors.Wrapf(err, "stage write to generation %d", g.gen))
		}
		size += m.size()
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return sst.MarkPebbleErr(errors.Wrapf(err, "commit write to generation %d", g.gen))
	}
	g.writtenBytes.Add(size)
	return nil
}

// written is the logical bytes write applied to this generation.
func (g *generation) written() uint64 { return g.writtenBytes.Load() }

// get reads key from this generation. found is true once the generation
// holds the key, as a value or as a tombstone; a tombstone resolves to a nil
// value so that older layers cannot resurrect it.
func (g *generation) get(key []byte) (value []byte, found bool, err error) {
	v, closer, err := g.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, sst.MarkPebbleErr(errors.Wrapf(err, "read generation %s", g.dir))
	}
	defer closer.Close()
	if codec.IsTombstone(v) {
		return nil, true, nil
	}
	// the value is only valid until closer.Close, so hand back a copy
	return append([]byte{}, v...), true, nil
}

// storedValue is what pebble keeps for m: a codec tombstone for a delete, so
// that it stays visible to get and masks entries in the layers below, and the
// value otherwise. A value equal to the tombstone is a caller bug.
func storedValue(m Mutation) ([]byte, error) {
	if m.Delete {
		return codec.EncodeTombstone(), nil
	}
	if codec.IsTombstone(m.Value) {
		return nil, errors.Wrapf(errReservedTombstoneValue, "key %x", m.Key)
	}
	return m.Value, nil
}

// size is the logical bytes a mutation adds: the key, plus the value unless it
// is a delete.
func (m Mutation) size() uint64 {
	if m.Delete {
		return uint64(len(m.Key))
	}
	return uint64(len(m.Key) + len(m.Value))
}
