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

package compaction

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// task is one compaction on this node, from Create until Drop or the TTL.
type task struct {
	req    *pkindexpb.CompactionRequest
	dir    string
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	state     taskcommon.State
	err       error
	outputs   []*pkindexpb.SSTRef
	processed atomic.Int64
	// lastQueried is the unix time in nanoseconds of the last Query, or of
	// Create. The executor deletes a finished or failed task that was not
	// queried for Config.TerminalTaskTTL.
	lastQueried atomic.Int64
}

func (t *task) getState() (taskcommon.State, error, []*pkindexpb.SSTRef) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state, t.err, t.outputs
}

func (t *task) setDone(outputs []*pkindexpb.SSTRef, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err != nil {
		t.state, t.err = taskcommon.Failed, err
		return
	}
	t.state, t.outputs = taskcommon.Finished, outputs
}

// retriable reports whether another attempt may succeed: the remote store or
// the local disk failed, as opposed to a corrupt input or a bad request.
func retriable(err error) bool {
	if errors.Is(err, pkindex.ErrCorrupted) {
		return false
	}
	return errors.Is(err, pkindex.ErrUnavailable) || errors.Is(err, pkindex.ErrIO)
}

// run downloads the inputs, merges them into tables of about
// target_file_size each and uploads every table as it is closed. It returns
// the outputs in ascending key order.
func (t *task) run(ctx context.Context, store Store) ([]*pkindexpb.SSTRef, error) {
	req := t.req
	in, out := filepath.Join(t.dir, "in"), filepath.Join(t.dir, "out")
	for _, d := range []string{in, out} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, pkindex.MarkIO(err, "create %s", d)
		}
	}
	// The inputs are not needed after the merge: free the disk when run
	// returns, after the readers close (defers run last in, first out).
	defer func() { _ = os.RemoveAll(in) }()
	sources := make([]Source, 0, len(req.GetInputs()))
	readers := make([]*sst.Reader, 0, len(req.GetInputs()))
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()
	for _, ref := range req.GetInputs() {
		path := filepath.Join(in, sst.FileName(sst.ID(ref.GetId())))
		if err := store.DownloadFile(ctx, pkindex.SSTKey(req.GetSstBasePath(), ref.GetId()), path); err != nil {
			return nil, errors.Wrapf(err, "download input %d", ref.GetId())
		}
		r, err := sst.OpenReader(path, nil, sst.ExpectSize(ref.GetSize()))
		if err != nil {
			return nil, errors.Wrapf(err, "open input %d", ref.GetId())
		}
		readers = append(readers, r)
		sources = append(sources, r)
	}

	up := startUploader(ctx, store, req.GetSstBasePath(), out)
	o := &splitOutput{dir: out, nextID: req.GetOutputIdBegin(), endID: req.GetOutputIdEnd(), maxBytes: req.GetTargetFileSize(), onClose: up.enqueue}
	if _, err := Merge(ctx, sources, nil, req.GetDropTombstones(), o, &t.processed); err != nil {
		o.Abort()
		_ = up.finish()
		return nil, err
	}
	infos, err := o.Finish()
	if err != nil {
		_ = up.finish()
		return nil, err
	}
	if err := up.finish(); err != nil {
		return nil, err
	}
	outputs := make([]*pkindexpb.SSTRef, 0, len(infos))
	for _, info := range infos {
		outputs = append(outputs, &pkindexpb.SSTRef{
			Id: int64(info.ID), TableFormat: uint32(info.TableFormat), Size: info.Size,
			MinKey: info.MinKey, MaxKey: info.MaxKey, NumEntries: info.NumEntries, RunId: req.GetRunId(),
		})
	}
	sort.Slice(outputs, func(a, b int) bool { return sst.Comparer.Compare(outputs[a].MinKey, outputs[b].MinKey) < 0 })
	return outputs, nil
}
