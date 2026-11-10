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
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// stagedTableTargetBytes caps the size of one staged table. A larger
// generation is staged as several tables with disjoint key ranges. It matches
// the default target size of a compaction output table. It is a var only so
// that tests can lower it.
var stagedTableTargetBytes uint64 = 128 << 20

// stage writes a frozen generation out and stages it as merged tables:
//
//	increment-7/                         staged-7/
//	  000005.sst  (auto-flushed)  ─┐
//	  memtable ─flush─▶ 000007.sst ─┼─merge▶ 1001.sst   newest value per key,
//	                               ─┘                   tombstones kept
//	                                       STAGED     "1001\n"
//
//	1. Clear staged-7/: debris of an earlier attempt that never finished.
//	2. Flush the memtable, so that every write of the generation is in an SST.
//	3. Iterate the generation in key order. The iterator yields the newest
//	   value of each key; a delete is a tombstone value and is written as is.
//	4. Write the entries into tables. Each table gets a new ID when its first
//	   key arrives, and is closed once it reaches stagedTableTargetBytes, so
//	   the tables are in key order and their key ranges do not overlap. Each
//	   table file is fsynced before it is renamed to <id>.sst.
//	5. Fsync staged-7/ so the renames are durable, then write STAGED (temp
//	   file, fsync, rename, fsync the directory). It records the order, and by
//	   existing at all it marks the staging complete.
//
// An empty generation gives no tables and an empty STAGED.
//
// A later call for the same generation finds STAGED and returns the same
// tables under the same IDs without redoing any step. Without STAGED, the
// attempt was interrupted; its IDs were never handed out, so step 1 discards
// it and the whole staging runs again under new IDs.
func (e *Engine) stage(ctx context.Context, g *generation) ([]sst.ID, error) {
	// an earlier attempt may have written some tables before failing; their
	// IDs were never reported to anyone, so the whole attempt is discarded
	if err := os.RemoveAll(g.stagedDir); err != nil {
		return nil, pkindex.MarkIO(err, "clear staged tables %s", g.stagedDir)
	}
	if err := os.MkdirAll(g.stagedDir, 0o755); err != nil {
		return nil, pkindex.MarkIO(err, "create staged tables %s", g.stagedDir)
	}
	if err := syncDir(filepath.Dir(g.stagedDir)); err != nil {
		return nil, pkindex.MarkIO(err, "sync engine dir after creating %s", g.stagedDir)
	}
	if err := g.db.Flush(); err != nil {
		return nil, sst.MarkPebbleErr(errors.Wrapf(err, "flush generation %s", g.dir))
	}
	ids, err := e.mergeGeneration(ctx, g)
	if err != nil {
		return nil, err
	}
	// the renames must be durable before STAGED marks them complete, or a
	// crash can keep a list that names tables which are gone
	if err := syncDir(g.stagedDir); err != nil {
		return nil, pkindex.MarkIO(err, "sync staged tables %s", g.stagedDir)
	}
	if err := writeStagedList(g.stagedDir, ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// mergeGeneration writes the newest value of every key of g, in key order,
// into tables under g.stagedDir, and returns their IDs in key order. On error
// the open table is aborted; tables already closed stay behind until the next
// attempt clears staged-N/.
func (e *Engine) mergeGeneration(ctx context.Context, g *generation) (ids []sst.ID, err error) {
	it, err := g.db.NewIter(nil)
	if err != nil {
		return nil, sst.MarkPebbleErr(errors.Wrapf(err, "iterate generation %s", g.dir))
	}
	var w *sst.Writer
	defer func() {
		if w != nil {
			w.Abort()
		}
		if cerr := it.Close(); cerr != nil && err == nil {
			ids, err = nil, sst.MarkPebbleErr(errors.Wrapf(cerr, "iterate generation %s", g.dir))
		}
	}()
	for valid := it.First(); valid; valid = it.Next() {
		value, verr := it.ValueAndErr()
		if verr != nil {
			return nil, sst.MarkPebbleErr(errors.Wrapf(verr, "read generation %s", g.dir))
		}
		if w == nil {
			raw, aerr := e.cfg.AllocID(ctx)
			if aerr != nil {
				return nil, errors.Wrapf(aerr, "allocate sst id for generation %d", g.gen)
			}
			if w, err = sst.NewWriter(g.stagedDir, sst.ID(raw)); err != nil {
				return nil, err
			}
		}
		if err := w.Add(it.Key(), value); err != nil {
			return nil, err
		}
		if w.EstimatedSize() >= stagedTableTargetBytes {
			info, cerr := w.Close()
			w = nil
			if cerr != nil {
				return nil, cerr
			}
			ids = append(ids, info.ID)
		}
	}
	if ierr := it.Error(); ierr != nil {
		return nil, sst.MarkPebbleErr(errors.Wrapf(ierr, "iterate generation %s", g.dir))
	}
	if w != nil {
		info, cerr := w.Close()
		w = nil
		if cerr != nil {
			return nil, cerr
		}
		ids = append(ids, info.ID)
	}
	return ids, nil
}

// describeStaged reads back the Info of every staged table, in the recorded
// order.
func (e *Engine) describeStaged(ctx context.Context, g *generation, ids []sst.ID) ([]StagedTable, error) {
	out := make([]StagedTable, 0, len(ids))
	for _, id := range ids {
		path := filepath.Join(g.stagedDir, sst.FileName(id))
		info, err := sst.ReadInfo(id, path)
		if err != nil {
			return nil, err
		}
		out = append(out, StagedTable{Info: info, Path: path})
	}
	return out, nil
}

// writeStagedList records the staged IDs in order and, by existing at all,
// marks the directory complete. It is renamed into place so that a crash
// cannot leave a half-written list behind, and the directory is synced so the
// rename survives a power loss.
func writeStagedList(dir string, ids []sst.ID) error {
	var sb strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&sb, "%d\n", int64(id))
	}
	tmp, err := os.CreateTemp(dir, stagedListName+".tmp-*")
	if err != nil {
		return pkindex.MarkIO(err, "create staged manifest in %s", dir)
	}
	if _, err := tmp.WriteString(sb.String()); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return pkindex.MarkIO(err, "write staged manifest %s", tmp.Name())
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return pkindex.MarkIO(err, "sync staged manifest %s", tmp.Name())
	}
	tmp.Close()
	if err := os.Rename(tmp.Name(), filepath.Join(dir, stagedListName)); err != nil {
		os.Remove(tmp.Name())
		return pkindex.MarkIO(err, "publish staged manifest in %s", dir)
	}
	if err := syncDir(dir); err != nil {
		return pkindex.MarkIO(err, "sync staged manifest in %s", dir)
	}
	return nil
}

// readStagedList returns the staged IDs in order; ok is false when the
// directory holds no completed staging.
func readStagedList(dir string) (ids []sst.ID, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, stagedListName))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, pkindex.MarkIO(err, "read staged manifest in %s", dir)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		n, perr := strconv.ParseInt(line, 10, 64)
		if perr != nil {
			return nil, false, errors.Mark(
				errors.Wrapf(perr, "staged manifest in %s is malformed", dir),
				pkindex.ErrCorrupted)
		}
		ids = append(ids, sst.ID(n))
	}
	return ids, true, nil
}
