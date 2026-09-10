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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func (e *Engine) RotateIncrement(ctx context.Context) (Generation, error) {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return 0, errClosed
	}
	// only structural operations replace e.active, and cycleMu excludes them
	frozen := e.active
	g, err := e.openGeneration(frozen.gen + 1)
	if err != nil {
		return 0, err
	}
	// the frozen generation stays in the read path; it only stops taking writes
	e.mu.Lock()
	e.active = g
	e.draining = append([]*generation{frozen}, e.draining...)
	drainingCount := len(e.draining)
	e.mu.Unlock()
	e.logger.Info(ctx, "pkindex generation rotated",
		mlog.Int64("activeGen", int64(g.gen)),
		mlog.Int64("frozenGen", int64(frozen.gen)),
		mlog.Int("drainingGenerations", drainingCount))
	return frozen.gen, nil
}

// FlushDraining writes a frozen generation out and stages the result.
//
// Staging is what makes a retry safe. Pebble numbers files per DB, so two
// generations produce the same file names, and the generation's own directory
// is deleted when it retires. The engine therefore hard-links each output into
// a directory of its own under the table's allocated ID, and records the order
// in a manifest written last. A second call for the same generation finds that
// manifest and returns exactly the same tables under exactly the same IDs, so
// an upload that already put some of them in object storage is idempotent. A
// staging directory without the manifest is the debris of an interrupted
// attempt and is discarded.
func (e *Engine) FlushDraining(ctx context.Context, gen Generation) ([]FlushedTable, error) {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return nil, errClosed
	}
	g := e.findDraining(gen)
	if g == nil {
		return nil, errors.Wrapf(ErrNotDraining, "generation %d", gen)
	}
	ids, staged, err := readStagedManifest(g.stagedDir)
	if err != nil {
		return nil, err
	}
	if !staged {
		if ids, err = e.stage(ctx, g); err != nil {
			return nil, err
		}
	}
	return e.describeStaged(ctx, g, ids)
}

func (e *Engine) DrainingGenerations() []Generation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	gens := make([]Generation, 0, len(e.draining))
	for _, g := range e.draining {
		gens = append(gens, g.gen)
	}
	return gens
}

// InstallCommitted swaps in the committed set and retires the frozen
// generations it covers, in one step.
func (e *Engine) InstallCommitted(ctx context.Context, tables []CommittedTable, retire ...Generation) error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return errClosed
	}
	// resolve every generation before touching anything, so a bad argument
	// leaves the engine exactly as it was
	retiring := make([]*generation, 0, len(retire))
	for _, gen := range retire {
		g := e.findDraining(gen)
		if g == nil {
			return errors.Wrapf(ErrNotDraining, "generation %d", gen)
		}
		retiring = append(retiring, g)
	}
	// a set usually repeats most of its tables, so reuse the readers already
	// open under those IDs: reopening would drop what their blocks had cached
	open := make(map[sst.ID]*sst.Reader, len(e.committed))
	for _, cr := range e.committed {
		open[cr.info.ID] = cr.reader
	}
	// opening and preloading read from disk, so they happen under cycleMu
	// alone, before anything is swapped
	readers := make([]committedReader, 0, len(tables))
	opened := make([]*sst.Reader, 0, len(tables))
	keep := make(map[sst.ID]struct{}, len(tables))
	for _, t := range tables {
		keep[t.Info.ID] = struct{}{}
		if r, ok := open[t.Info.ID]; ok {
			readers = append(readers, committedReader{info: t.Info, reader: r})
			continue
		}
		r, err := sst.OpenReader(t.Path, e.cfg.Shared.cache, sst.ExpectSize(t.Info.Size))
		if err != nil {
			closeAll(opened)
			return err
		}
		// the caller's lookups must not be the ones that fault these in
		if err := r.Preload(); err != nil {
			r.Close()
			closeAll(opened)
			return err
		}
		opened = append(opened, r)
		readers = append(readers, committedReader{info: t.Info, reader: r})
	}

	// one critical section: a reader holds both locks for its whole call, so
	// it never sees the generations already gone and the tables not yet in
	e.mu.Lock()
	e.commitMu.Lock()
	old := e.committed
	e.committed = readers
	if len(retiring) > 0 {
		e.draining = withoutGenerations(e.draining, retiring)
	}
	e.commitMu.Unlock()
	e.mu.Unlock()

	// nothing references the tables that dropped out of the set any more; the
	// ones that stayed are still in use under their existing reader
	for _, cr := range old {
		if _, ok := keep[cr.info.ID]; !ok {
			cr.reader.Close()
		}
	}
	for _, g := range retiring {
		if err := g.retire(); err != nil {
			// the generation is out of the read path either way, and the next
			// Open deletes whatever is left, so this does not fail the install
			e.logger.Warn(ctx, "pkindex failed to remove retired generation",
				mlog.Int64("gen", int64(g.gen)), mlog.Err(err))
		}
	}
	e.logger.Info(ctx, "pkindex committed set installed",
		mlog.Int("tables", len(tables)), mlog.Int("retired", len(retiring)))
	return nil
}

// DropDraining retires one frozen generation without installing anything.
func (e *Engine) DropDraining(ctx context.Context, gen Generation) error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return errClosed
	}
	g := e.findDraining(gen)
	if g == nil {
		return errors.Wrapf(ErrNotDraining, "generation %d", gen)
	}
	e.mu.Lock()
	e.draining = withoutGenerations(e.draining, []*generation{g})
	e.mu.Unlock()
	if err := g.retire(); err != nil {
		return err
	}
	e.logger.Info(ctx, "pkindex generation dropped", mlog.Int64("gen", int64(gen)))
	return nil
}

func (e *Engine) findDraining(gen Generation) *generation {
	for _, g := range e.draining {
		if g.gen == gen {
			return g
		}
	}
	return nil
}

// withoutGenerations returns a fresh slice without the given generations, so
// that a reader already iterating the old one is unaffected.
func withoutGenerations(all []*generation, remove []*generation) []*generation {
	drop := make(map[Generation]struct{}, len(remove))
	for _, g := range remove {
		drop[g.gen] = struct{}{}
	}
	kept := make([]*generation, 0, len(all))
	for _, g := range all {
		if _, ok := drop[g.gen]; ok {
			continue
		}
		kept = append(kept, g)
	}
	return kept
}

func closeAll(readers []*sst.Reader) {
	for _, r := range readers {
		r.Close()
	}
}
