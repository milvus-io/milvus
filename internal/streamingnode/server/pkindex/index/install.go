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
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
)

// SetCommitted records the manifest of the vchannel at the open. The writes
// up to its checkpoint are in its tables, so Replay skips them, and Install
// installs the tables. The Index keeps the checkpoint as its
// LastCommittedTimeTick until the next handover. It runs once, at the open,
// before any Replay and before any write of the Index. The manifest version
// is at least 1.
func (idx *Index) SetCommitted(v manifest.View) {
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
func (idx *Index) reconcileCommittedDir(ctx context.Context, view *manifest.View) error {
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

// currentInstalledVersion returns the version of the installed manifest, 0
// when no manifest is installed.
func (idx *Index) currentInstalledVersion() int64 {
	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	return idx.installedVersion
}

// installIfNewer installs the tables of m when m is newer than the installed
// manifest, and retires gens in the same step. It installs nothing from a
// manifest whose checkpoint is before the last Reset: its tables hold keys
// that the reset removed. local maps the tables of this
// handover to their local files.
//
// It finds the local file of every table of m in this order: local, the
// tables that the Index installed before, the committed directory (the links
// survive a restart), and last a download from the store. It downloads
// outside installMu and checks the version again under it.
func (idx *Index) installIfNewer(ctx context.Context, m manifest.View, local map[sst.ID]string, gens ...engine.Generation) error {
	idx.installMu.Lock()
	if m.CheckpointTimeTick < idx.resetTT.Load() {
		idx.installMu.Unlock()
		return nil
	}
	newer := m.Version > idx.installedVersion
	known := make(map[sst.ID]string, len(m.Tables))
	for _, ref := range m.Tables {
		if p, ok := idx.installedPaths[ref.ID]; ok {
			known[ref.ID] = p
		}
	}
	idx.installMu.Unlock()

	if !newer {
		return idx.retireCovered(ctx, m, gens)
	}

	tables := make([]engine.CommittedTable, 0, len(m.Tables))
	for _, ref := range m.Tables {
		path, err := idx.resolveLocal(ctx, m.SSTBasePath, ref, local, known)
		if err != nil {
			return err
		}
		tables = append(tables, engine.CommittedTable{Info: ref, Path: path})
	}

	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	if m.CheckpointTimeTick < idx.resetTT.Load() {
		return nil
	}
	if m.Version <= idx.installedVersion {
		return idx.retireCoveredLocked(ctx, m, gens)
	}
	if err := idx.eng.InstallCommitted(ctx, tables, gens...); err != nil {
		return err
	}
	// TODO: no code deletes a local committed file. A compaction must delete
	// the files of the tables that leave the set.
	paths := make(map[sst.ID]string, len(tables))
	for _, t := range tables {
		paths[t.Info.ID] = t.Path
	}
	idx.installedVersion = m.Version
	idx.installedPaths = paths
	return nil
}

// retireCovered drops gens when a newer manifest than m is installed.
func (idx *Index) retireCovered(ctx context.Context, m manifest.View, gens []engine.Generation) error {
	if len(gens) == 0 {
		return nil
	}
	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	return idx.retireCoveredLocked(ctx, m, gens)
}

// retireCoveredLocked drops gens when the installed manifest is not older
// than m. m covers gens, and a later version keeps covering what m covers,
// so the installed tables hold the data of gens. Without the drop, gens stay
// frozen, and the next handover sends their committed tables again. The
// caller holds installMu.
func (idx *Index) retireCoveredLocked(ctx context.Context, m manifest.View, gens []engine.Generation) error {
	if len(gens) == 0 {
		return nil
	}
	// one call: the engine retires only the oldest frozen generations as a
	// whole, a younger one left behind would mask the installed tables
	if err := idx.eng.DropFrozen(ctx, gens...); err != nil {
		idx.logger.Warn(ctx, "failed to drop the frozen generations that a newer manifest covers",
			mlog.Int("generations", len(gens)), mlog.Int64("manifestVersion", m.Version), mlog.Err(err))
	}
	return nil
}

// resolveLocal returns the local file of table ref. A file in the committed
// dir is reused when its size is the size in the manifest, else it is
// replaced by a download. The paths of local and known were installed by
// this process under the same table ID, so they are reused without a size
// check.
func (idx *Index) resolveLocal(ctx context.Context, basePath string, ref manifest.TableRef, local, known map[sst.ID]string) (string, error) {
	id := ref.ID
	if p, ok := local[id]; ok {
		return p, nil
	}
	if p, ok := known[id]; ok {
		return p, nil
	}
	path := committedPath(idx.committedDir, id)
	fi, err := os.Stat(path)
	switch {
	case err == nil && fi.Size() == ref.Size:
		return path, nil
	case err == nil:
		idx.logger.Warn(ctx, "a local committed primary key index table has the wrong size, downloading it again",
			mlog.Int64("sstID", int64(id)), mlog.Int64("localSize", fi.Size()), mlog.Int64("size", ref.Size))
		if err := os.Remove(path); err != nil {
			return "", pkroot.MarkIO(err, "remove committed sst %s", path)
		}
	case !os.IsNotExist(err):
		return "", pkroot.MarkIO(err, "stat committed sst %s", path)
	}
	if err := os.MkdirAll(idx.committedDir, 0o755); err != nil {
		return "", pkroot.MarkIO(err, "create committed dir %s", idx.committedDir)
	}
	if err := idx.cfg.Store.DownloadFile(ctx, manifest.SSTKey(basePath, int64(id)), path); err != nil {
		return "", err
	}
	idx.logger.Info(ctx, "downloaded a committed primary key index table", mlog.Int64("sstID", int64(id)))
	return path, nil
}
