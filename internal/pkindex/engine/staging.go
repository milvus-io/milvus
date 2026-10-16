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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// stage writes a frozen generation out and stages its tables:
//
//		increment-7/                       staged-7/
//		  000005.sst  (auto-flushed)   ══▶  1002.sst   hard links, so no copy and
//		  memtable ─flush─▶ 000007.sst ══▶  1001.sst   they outlive increment-7/
//		                                    STAGED     "1001\n1002\n"
//
//	 1. Clear staged-7/: debris of an earlier attempt that never finished.
//	 2. Flush the memtable, then list every SST of the generation, including
//	    the ones pebble flushed earlier when a memtable filled up.
//	 3. Order them newest first by largest sequence number. Their key ranges
//	    can overlap, so this order decides which value a read sees.
//	 4. Give each one a new ID and hard-link it as <id>.sst.
//	 5. Fsync staged-7/ so the links are durable, then write STAGED (temp file,
//	    fsync, rename, fsync the directory). It records the order, and by
//	    existing at all it marks the staging complete.
//
// A later call for the same generation finds STAGED and returns the same
// tables under the same IDs without redoing any step. Without STAGED, the
// attempt was interrupted; its IDs were never handed out, so step 1 discards
// it and the whole staging runs again under new IDs.
func (e *Engine) stage(ctx context.Context, g *generation) ([]sst.ID, error) {
	// an earlier attempt may have linked some tables before failing; their IDs
	// were never reported to anyone, so the whole attempt is discarded
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
	levels, err := g.db.SSTables()
	if err != nil {
		return nil, sst.MarkPebbleErr(errors.Wrapf(err, "list ssts of generation %s", g.dir))
	}
	var tables []pebble.SSTableInfo
	for _, level := range levels {
		tables = append(tables, level...)
	}
	// Flushes of one DB are serial and nothing compacts or ingests, so tables
	// from different flushes have disjoint sequence number ranges, and the
	// later flush sorts first. One flush can be split by key into several
	// files whose sequence number ranges overlap, but their key ranges do not,
	// so their relative order does not matter; the STAGED list fixes whatever
	// order this sort picked.
	sort.Slice(tables, func(i, j int) bool { return tables[i].LargestSeqNum > tables[j].LargestSeqNum })

	ids := make([]sst.ID, 0, len(tables))
	for _, t := range tables {
		raw, err := e.cfg.AllocID(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "allocate sst id for generation %d", g.gen)
		}
		id := sst.ID(raw)
		src := filepath.Join(g.dir, fmt.Sprintf("%s%s", t.FileNum, sst.Extension))
		if err := os.Link(src, filepath.Join(g.stagedDir, sst.FileName(id))); err != nil {
			return nil, pkindex.MarkIO(err, "stage sst %s of generation %d", src, g.gen)
		}
		ids = append(ids, id)
	}
	// the links must be durable before STAGED marks them complete, or a
	// crash can keep a list that names tables which are gone
	if err := syncDir(g.stagedDir); err != nil {
		return nil, pkindex.MarkIO(err, "sync staged tables %s", g.stagedDir)
	}
	if err := writeStagedList(g.stagedDir, ids); err != nil {
		return nil, err
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
