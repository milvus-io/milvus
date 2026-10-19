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

	"github.com/milvus-io/milvus/internal/pkindex/sst"
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
	g, err := openGeneration(e.cfg, frozen.gen+1)
	if err != nil {
		return 0, err
	}
	// the frozen generation stays in the read path; it only stops taking writes
	e.mu.Lock()
	e.active = g
	e.frozen = append([]*generation{frozen}, e.frozen...)
	frozenCount := len(e.frozen)
	e.mu.Unlock()
	e.logger.Info(ctx, "pkindex generation rotated",
		mlog.Int64("activeGen", int64(g.gen)),
		mlog.Int64("frozenGen", int64(frozen.gen)),
		mlog.Int("frozenGenerations", frozenCount))
	return frozen.gen, nil
}

// StageFrozen writes a frozen generation out and stages the result.
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
func (e *Engine) StageFrozen(ctx context.Context, gen Generation) ([]StagedTable, error) {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return nil, errClosed
	}
	g := e.findFrozen(gen)
	if g == nil {
		return nil, errors.Wrapf(ErrNotFrozen, "generation %d", gen)
	}
	ids, staged, err := readStagedList(g.stagedDir)
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

func (e *Engine) FrozenGenerations() []Generation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	gens := make([]Generation, 0, len(e.frozen))
	for _, g := range e.frozen {
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
	seenGen := make(map[Generation]struct{}, len(retire))
	for _, gen := range retire {
		if _, dup := seenGen[gen]; dup {
			// retiring a generation twice would close its DB twice, which
			// pebble answers with a panic
			return errors.Wrapf(errDuplicateRetire, "generation %d", gen)
		}
		seenGen[gen] = struct{}{}
		g := e.findFrozen(gen)
		if g == nil {
			return errors.Wrapf(ErrNotFrozen, "generation %d", gen)
		}
		retiring = append(retiring, g)
	}
	// the same table twice would open two readers for one file and leak the
	// one that the next install does not find under its ID
	keep := make(map[sst.ID]struct{}, len(tables))
	for _, t := range tables {
		if _, dup := keep[t.Info.ID]; dup {
			return errors.Wrapf(errDuplicateTable, "sst %d", int64(t.Info.ID))
		}
		keep[t.Info.ID] = struct{}{}
	}
	// a set usually repeats most of its tables, so reuse the readers already
	// open under those IDs: reopening would drop what their blocks had cached
	open := make(map[sst.ID]*sst.Reader, len(e.committed))
	for _, cr := range e.committed {
		open[cr.info.ID] = cr.reader
	}
	// opening and preloading read from disk, so they happen under cycleMu
	// alone, before anything is swapped
	readers := make([]installedTable, 0, len(tables))
	opened := make([]*sst.Reader, 0, len(tables))
	for _, t := range tables {
		if r, ok := open[t.Info.ID]; ok {
			readers = append(readers, installedTable{info: t.Info, reader: r})
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
		readers = append(readers, installedTable{info: t.Info, reader: r})
	}

	// one critical section: a reader holds both locks for its whole call, so
	// it never sees the generations already gone and the tables not yet in
	e.mu.Lock()
	e.commitMu.Lock()
	old := e.committed
	e.committed = readers
	if len(retiring) > 0 {
		e.frozen = withoutGenerations(e.frozen, retiring)
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
			// The generation is out of the read path either way. If the rename
			// to retired-N happened, the next Open deletes what is left. If it
			// did not, the next Open restores the generation as frozen, and the
			// warm start retires it again because the committed manifest
			// covers it. Either way this does not fail the install.
			e.logger.Warn(ctx, "pkindex failed to remove retired generation",
				mlog.Int64("gen", int64(g.gen)), mlog.Err(err))
		}
	}
	e.logger.Info(ctx, "pkindex committed set installed",
		mlog.Int("tables", len(tables)), mlog.Int("retired", len(retiring)))
	return nil
}

// DropFrozen retires one frozen generation without installing anything.
func (e *Engine) DropFrozen(ctx context.Context, gen Generation) error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return errClosed
	}
	g := e.findFrozen(gen)
	if g == nil {
		return errors.Wrapf(ErrNotFrozen, "generation %d", gen)
	}
	e.mu.Lock()
	e.frozen = withoutGenerations(e.frozen, []*generation{g})
	e.mu.Unlock()
	if err := g.retire(); err != nil {
		return err
	}
	e.logger.Info(ctx, "pkindex generation dropped", mlog.Int64("gen", int64(gen)))
	return nil
}

func (e *Engine) findFrozen(gen Generation) *generation {
	for _, g := range e.frozen {
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
