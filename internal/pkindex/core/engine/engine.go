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
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	incrementDirPrefix = "increment-"
	stagedDirPrefix    = "staged-"
	// stagedManifestName marks a staging directory as complete and records the
	// order its tables were produced in.
	stagedManifestName = "MANIFEST"
)

// Conditions that are bugs in the calling code rather than signals to act on.
// They are unexported because nothing outside should branch on them, and they
// carry no pkerr category so that no boundary reports them as retriable or as
// damaged data.
var (
	errNoSharedResources      = errors.New("pkindex engine opened without shared resources")
	errNoAllocID              = errors.New("pkindex engine opened without an ID allocator")
	errReservedTombstoneValue = errors.New("pkindex value is the reserved tombstone encoding")
)

// errClosed is ErrClosed in the form the methods return: a closed engine is
// something a retry against a reopened engine can clear, so it also carries
// the unavailable category for the boundary translation.
var errClosed = errors.Mark(ErrClosed, pkerr.ErrUnavailable)

var (
	_ RW        = (*Engine)(nil)
	_ Lifecycle = (*Engine)(nil)
)

// SharedResources are the node-level pebble resources every engine instance on
// the node attaches to: one block cache and one table (file handle) cache.
type SharedResources struct {
	cache      *pebble.Cache
	tableCache *pebble.TableCache
}

// NewSharedResources creates the node-level caches. cacheBytes is the total
// block cache budget shared by all engines on the node.
func NewSharedResources(cacheBytes int64) *SharedResources {
	c := pebble.NewCache(cacheBytes)
	return &SharedResources{
		cache:      c,
		tableCache: pebble.NewTableCache(c, 8, 4096),
	}
}

// Release drops the references held by SharedResources; call it after every
// engine using it is closed.
func (s *SharedResources) Release() {
	s.tableCache.Unref()
	s.cache.Unref()
}

// Config configures one engine instance.
type Config struct {
	// Dir is the engine's private data directory.
	Dir string
	// VChannel is the owning vchannel, for logging only.
	VChannel string
	// Shared are the node-level caches; required.
	Shared *SharedResources
	// AllocID hands out Milvus global IDs for the tables a flush produces;
	// required. On a streaming node this is the node's ID allocator, which
	// draws batches from the coordinator.
	AllocID func(ctx context.Context) (int64, error)
	// MemTableSize caps one memtable's size in bytes; 0 keeps pebble's
	// default. Transitional: a node-level write budget on SharedResources
	// replaces it once generations stop being pebble DBs.
	MemTableSize uint64
}

// generation is one container of writes, backed today by a pebble DB.
type generation struct {
	gen       Generation
	dir       string
	stagedDir string
	db        *pebble.DB
}

// retire closes the generation and deletes both its own directory and the
// tables staged out of it.
func (g *generation) retire() error {
	if err := g.db.Close(); err != nil {
		return sst.MarkPebbleErr(errors.Wrapf(err, "close generation %s", g.dir))
	}
	if err := os.RemoveAll(g.dir); err != nil {
		return pkerr.MarkIO(err, "remove generation %s", g.dir)
	}
	if err := os.RemoveAll(g.stagedDir); err != nil {
		return pkerr.MarkIO(err, "remove staged tables %s", g.stagedDir)
	}
	return nil
}

type committedReader struct {
	info   sst.Info
	reader *sst.Reader
}

// Engine is one per-vchannel pkindex engine, implementing both RW and
// Lifecycle over one set of state. All methods are safe for concurrent use.
type Engine struct {
	cfg    Config
	logger *mlog.Logger

	// cycleMu serializes the structural operations (RotateIncrement,
	// FlushDraining, InstallCommitted, DropDraining, Close). They do their disk
	// IO holding only cycleMu, which MultiGet and Write never take, and hold mu
	// or commitMu just long enough to swap pointers.
	// Lock order: cycleMu, mu, commitMu.
	cycleMu sync.Mutex

	// mu guards the generations and closed. They are written holding both
	// cycleMu and mu.Lock, so either lock suffices to read them. MultiGet and
	// Write hold RLock for their whole call, so once a structural operation
	// has taken Lock to unlink a generation, no reader can still be using it.
	mu       sync.RWMutex
	active   *generation
	draining []*generation // newest first
	closed   bool

	// commitMu guards the committed set, separately from mu so that installing
	// tables alone does not block writes. InstallCommitted takes both, which is
	// what makes swapping the set and retiring generations one step to a reader
	// holding them both.
	commitMu  sync.RWMutex
	committed []committedReader
}

// Open opens (or creates) the engine under cfg.Dir. Every generation left on
// disk is reopened: the newest becomes active, the rest are restored as
// frozen, so a cycle interrupted by a crash can be resumed rather than leaking
// its directory.
func Open(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Shared == nil {
		return nil, errNoSharedResources
	}
	if cfg.AllocID == nil {
		return nil, errNoAllocID
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, pkerr.MarkIO(err, "create engine dir %s", cfg.Dir)
	}
	e := &Engine{
		cfg:    cfg,
		logger: mlog.With(mlog.FieldModule("pkindex"), mlog.FieldVChannel(cfg.VChannel)),
	}
	gens, err := existingGenerations(cfg.Dir)
	if err != nil {
		return nil, err
	}
	if len(gens) == 0 {
		gens = []Generation{1}
	}
	// descending: the newest generation is the active one
	sort.Slice(gens, func(i, j int) bool { return gens[i] > gens[j] })
	opened := make([]*generation, 0, len(gens))
	for _, gen := range gens {
		g, err := e.openGeneration(gen)
		if err != nil {
			for _, o := range opened {
				o.db.Close()
			}
			return nil, err
		}
		opened = append(opened, g)
	}
	e.active, e.draining = opened[0], opened[1:]
	if err := e.removeOrphanStaging(gens); err != nil {
		for _, g := range opened {
			g.db.Close()
		}
		return nil, err
	}
	e.logger.Info(ctx, "pkindex engine opened",
		mlog.String("dir", cfg.Dir),
		mlog.Int64("activeGen", int64(e.active.gen)),
		mlog.Int("drainingGenerations", len(e.draining)))
	return e, nil
}

func existingGenerations(dir string) ([]Generation, error) {
	gens, err := dirsWithPrefix(dir, incrementDirPrefix)
	if err != nil {
		return nil, err
	}
	return gens, nil
}

func dirsWithPrefix(dir, prefix string) ([]Generation, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, pkerr.MarkIO(err, "list engine dir %s", dir)
	}
	gens := make([]Generation, 0, len(ents))
	for _, ent := range ents {
		if !ent.IsDir() || !strings.HasPrefix(ent.Name(), prefix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimPrefix(ent.Name(), prefix), 10, 64)
		if err != nil {
			continue
		}
		gens = append(gens, Generation(n))
	}
	return gens, nil
}

// removeOrphanStaging deletes staging left by a generation that no longer
// exists. Its tables were either committed, in which case the manifest names
// the uploaded copies, or never committed, in which case nothing refers to
// them.
func (e *Engine) removeOrphanStaging(live []Generation) error {
	staged, err := dirsWithPrefix(e.cfg.Dir, stagedDirPrefix)
	if err != nil {
		return err
	}
	alive := make(map[Generation]struct{}, len(live))
	for _, gen := range live {
		alive[gen] = struct{}{}
	}
	for _, gen := range staged {
		if _, ok := alive[gen]; ok {
			continue
		}
		dir := e.stagedDir(gen)
		if err := os.RemoveAll(dir); err != nil {
			return pkerr.MarkIO(err, "remove orphan staged tables %s", dir)
		}
	}
	return nil
}

func (e *Engine) generationDir(gen Generation) string {
	return filepath.Join(e.cfg.Dir, fmt.Sprintf("%s%d", incrementDirPrefix, gen))
}

func (e *Engine) stagedDir(gen Generation) string {
	return filepath.Join(e.cfg.Dir, fmt.Sprintf("%s%d", stagedDirPrefix, gen))
}

func (e *Engine) openGeneration(gen Generation) (*generation, error) {
	dir := e.generationDir(gen)
	// the shared template pins the format major version, the comparer and the
	// bloom filter, so a flush output is byte-compatible with a Writer output
	opts := sst.PebbleOptions()
	opts.DisableWAL = true
	opts.DisableAutomaticCompactions = true
	// L0 is bounded by external compaction on datanode and by rotation, not by
	// stalling writers: a stall here would block the WAL append path.
	opts.L0StopWritesThreshold = 1 << 20
	opts.Cache = e.cfg.Shared.cache
	opts.TableCache = e.cfg.Shared.tableCache
	if e.cfg.MemTableSize > 0 {
		opts.MemTableSize = e.cfg.MemTableSize
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, sst.MarkPebbleErr(errors.Wrapf(err, "open generation %s", dir))
	}
	return &generation{gen: gen, dir: dir, stagedDir: e.stagedDir(gen), db: db}, nil
}

func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := Stats{}
	if !e.closed {
		m := e.active.db.Metrics()
		s.MemTableBytes = m.MemTable.Size
		s.L0Tables = m.Levels[0].NumFiles
		s.DrainingGenerations = len(e.draining)
	}
	e.commitMu.RLock()
	s.CommittedTables = len(e.committed)
	e.commitMu.RUnlock()
	return s
}

func (e *Engine) Close() error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return nil
	}
	// once Lock is granted every in-flight MultiGet and Write has returned, and
	// later ones see closed, so the handles can be released outside mu
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	err := e.active.db.Close()
	for _, g := range e.draining {
		if cerr := g.db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	e.commitMu.Lock()
	old := e.committed
	e.committed = nil
	e.commitMu.Unlock()
	for _, cr := range old {
		cr.reader.Close()
	}
	if err != nil {
		return sst.MarkPebbleErr(errors.Wrapf(err, "close engine %s", e.cfg.Dir))
	}
	return nil
}

func (e *Engine) Destroy() error {
	if err := e.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(e.cfg.Dir); err != nil {
		return pkerr.MarkIO(err, "remove engine dir %s", e.cfg.Dir)
	}
	return nil
}
