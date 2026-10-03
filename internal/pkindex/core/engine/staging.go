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

	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// stage flushes the generation and hard-links every SST it holds into the
// staging directory, newest first.
func (e *Engine) stage(ctx context.Context, g *generation) ([]sst.ID, error) {
	// an earlier attempt may have linked some tables before failing; their IDs
	// were never reported to anyone, so the whole attempt is discarded
	if err := os.RemoveAll(g.stagedDir); err != nil {
		return nil, pkerr.MarkIO(err, "clear staged tables %s", g.stagedDir)
	}
	if err := os.MkdirAll(g.stagedDir, 0o755); err != nil {
		return nil, pkerr.MarkIO(err, "create staged tables %s", g.stagedDir)
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
	// Flushes of one DB are serial and nothing compacts or ingests, so the
	// tables' sequence number ranges are disjoint and ordering by them is
	// ordering by recency.
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
			return nil, pkerr.MarkIO(err, "stage sst %s of generation %d", src, g.gen)
		}
		ids = append(ids, id)
	}
	if err := writeStagedManifest(g.stagedDir, ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// describeStaged reads back the Info of every staged table, in the recorded
// order.
func (e *Engine) describeStaged(ctx context.Context, g *generation, ids []sst.ID) ([]FlushedTable, error) {
	out := make([]FlushedTable, 0, len(ids))
	for _, id := range ids {
		path := filepath.Join(g.stagedDir, sst.FileName(id))
		info, err := sst.ReadInfo(id, path)
		if err != nil {
			return nil, err
		}
		out = append(out, FlushedTable{Info: info, Path: path})
	}
	return out, nil
}

// writeStagedManifest records the staged IDs in order and, by existing at all,
// marks the directory complete. It is renamed into place so that a crash
// cannot leave a half-written list behind.
func writeStagedManifest(dir string, ids []sst.ID) error {
	var sb strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&sb, "%d\n", int64(id))
	}
	tmp, err := os.CreateTemp(dir, stagedManifestName+".tmp-*")
	if err != nil {
		return pkerr.MarkIO(err, "create staged manifest in %s", dir)
	}
	if _, err := tmp.WriteString(sb.String()); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return pkerr.MarkIO(err, "write staged manifest %s", tmp.Name())
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return pkerr.MarkIO(err, "sync staged manifest %s", tmp.Name())
	}
	tmp.Close()
	if err := os.Rename(tmp.Name(), filepath.Join(dir, stagedManifestName)); err != nil {
		os.Remove(tmp.Name())
		return pkerr.MarkIO(err, "publish staged manifest in %s", dir)
	}
	return nil
}

// readStagedManifest returns the staged IDs in order; ok is false when the
// directory holds no completed staging.
func readStagedManifest(dir string) (ids []sst.ID, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, stagedManifestName))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, pkerr.MarkIO(err, "read staged manifest in %s", dir)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		n, perr := strconv.ParseInt(line, 10, 64)
		if perr != nil {
			return nil, false, errors.Mark(
				errors.Wrapf(perr, "staged manifest in %s is malformed", dir),
				pkerr.ErrCorrupted)
		}
		ids = append(ids, sst.ID(n))
	}
	return ids, true, nil
}
