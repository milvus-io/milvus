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

package index

import (
	"context"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
)

// SetCommitted records the manifest of the vchannel at the open. The writes
// up to its checkpoint are in its tables, so Replay skips them, and Install
// installs the tables. The Index keeps the checkpoint as its
// LastCommittedTimeTick until the next handover. It runs once, at the open,
// before any Replay and before any write of the Index. The manifest version
// is at least 1.
func (idx *Index) SetCommitted(v ManifestView) {
	idx.installMu.Lock()
	view := v
	idx.recovered = &view
	idx.installMu.Unlock()
	idx.lastCommitted.Store(v.CheckpointTimeTick)
	idx.cfg.OnRefresh(idx)
}

// Install brings the committed table set of the recovered manifest into the
// engine and marks the Index ready. It removes the files of the committed
// dir that the manifest does not list, reuses the listed files that have the
// right size and downloads the rest. An Index without a recovered manifest
// only becomes ready. The recovery runs it once after the replay; a failure
// returns and the caller retries, so every step is idempotent. It ends with
// the error of ctx when ctx ends, and with an error that wraps
// engine.ErrClosed when the Index closes.
func (idx *Index) Install(ctx context.Context) error {
	ctx, cancel := contextutil.MergeContext(ctx, idx.ctx)
	defer cancel()
	if err := idx.closedOr(nil); err != nil {
		return err
	}
	idx.installMu.Lock()
	view := idx.recovered
	idx.installMu.Unlock()
	if view == nil {
		// nothing to install, see MarkReady
		idx.MarkReady(ctx)
		return nil
	}
	if err := idx.reconcileCommittedDir(ctx, view); err != nil {
		return idx.closedOr(err)
	}
	if err := idx.installIfNewer(ctx, *view, nil); err != nil {
		return idx.closedOr(err)
	}
	idx.MarkReady(ctx)
	return nil
}

// closedOr returns the closed error of the Index when the Index closed, else
// err. The closed error wraps engine.ErrClosed and is marked
// pkroot.ErrUnavailable.
func (idx *Index) closedOr(err error) error {
	if idx.ctx.Err() != nil {
		return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of vchannel %s is closed", idx.vchannel), pkroot.ErrUnavailable)
	}
	return err
}

// reconcileCommittedDir removes the files of the committed dir that the
// manifest does not list: tables that a compaction replaced, and the
// temporary files of downloads that did not finish.
func (idx *Index) reconcileCommittedDir(ctx context.Context, view *ManifestView) error {
	ents, err := os.ReadDir(idx.committedDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return pkroot.MarkIO(err, "list committed dir %s", idx.committedDir)
	}
	listed := make(map[string]struct{}, len(view.Tables))
	for _, ref := range view.Tables {
		listed[filepath.Base(committedPath(idx.committedDir, ref.ID))] = struct{}{}
	}
	for _, ent := range ents {
		name := ent.Name()
		if _, ok := listed[name]; ok {
			continue
		}
		path := filepath.Join(idx.committedDir, name)
		if err := os.RemoveAll(path); err != nil {
			return pkroot.MarkIO(err, "remove %s", path)
		}
		idx.logger.Info(ctx, "removed a local file the primary key index manifest does not list", mlog.String("file", name))
	}
	return nil
}
