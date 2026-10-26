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
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// pendingHandover is a prepared handover. Every retry sends the same request,
// so the manifest service sees an exact retry (checkpoint not advanced, every
// SST present) and returns the committed manifest after a lost response.
type pendingHandover struct {
	// gens are the frozen generations that the handover retires.
	gens []engine.Generation
	// local maps the tables of request to their committed local files.
	local map[sst.ID]string
	// request is nil when there is nothing to publish.
	request *FlushArgs
	// covered is the checkpoint that the handover commits.
	covered uint64
	// uploadedAt is when the last complete upload of the tables started. It
	// is zero until the tables are uploaded once.
	uploadedAt time.Time
}

// Handover runs one handover step under the lifetime of the Index: it
// prepares a handover when none is pending, else it uploads and commits the
// pending one. It reports whether a commit succeeded. It returns
// context.Canceled when the Index or taskCtx ends during the step, and the
// error of the step otherwise; the caller classifies it with IsFatal. The
// handover driver is the only caller, and it runs at most one step of an
// Index at a time.
func (idx *Index) Handover(taskCtx context.Context) (committed bool, err error) {
	if !idx.lifetime.Add(typeutil.LifetimeStateWorking) {
		return false, context.Canceled
	}
	defer idx.lifetime.Done()
	ctx, cancel := context.WithCancel(idx.ctx)
	defer cancel()
	stop := context.AfterFunc(taskCtx, cancel)
	defer stop()

	committed, err = idx.handover(ctx)
	idx.cfg.OnRefresh(idx)
	if err != nil && ctx.Err() != nil {
		// the Index or the scheduler is closing, this is not a failure
		return false, context.Canceled
	}
	return committed, err
}

// handover prepares a handover when none is pending, then uploads and
// commits the pending one. It reports whether a commit succeeded. A pending
// request without tables only retires its generations.
func (idx *Index) handover(ctx context.Context) (bool, error) {
	p := idx.pending.Load()
	if p == nil {
		// Clear the flag before the rotate. A write between the clear and
		// the rotate lands in the frozen generation and sets the flag again,
		// which only causes one extra handover.
		idx.dirtyFlag.Store(false)
		var err error
		p, err = idx.prepare(ctx)
		if err != nil || p == nil {
			// the writes may still be only in the active generation; the
			// OnRefresh after this step puts the Index back into the heap
			idx.markDirty()
			return false, err
		}
		// Record the pending before the upload. A failed upload then keeps
		// it, and the retry does not rotate again.
		idx.pending.Store(p)
	}
	if p.request == nil {
		// Nothing to publish: the generations hold no write, so covered is
		// committed as soon as they retire. One call for all generations,
		// the engine retires only the oldest ones as a whole. A failed drop
		// must not be retried, the next engine open cleans up.
		dropErr := idx.eng.DropFrozen(ctx, p.gens...)
		idx.lastCommitted.Store(p.covered)
		idx.pending.Store(nil)
		if dropErr != nil {
			idx.logger.Warn(ctx, "failed to drop empty frozen generations", mlog.Err(dropErr))
		}
		return true, nil
	}
	// keep the pending request on every failure below: a retry must send
	// the same request
	if err := idx.upload(ctx, p); err != nil {
		return false, err
	}
	if err := idx.commit(ctx, p); err != nil {
		return false, err
	}
	idx.pending.Store(nil)
	idx.logger.Debug(ctx, "primary key index handover committed",
		mlog.Uint64("checkpointTimeTick", p.covered), mlog.Int("ssts", len(p.request.AddTables)))
	return true, nil
}

// prepare freezes the active generation, stages all frozen generations,
// links the tables, and builds the request. It does not upload: upload does.
// The caller records the result as pending right away, so a failed upload
// does not cause a new rotate. The caller calls prepare only when the Index
// has no pending handover. It returns nil when lastObserved did not move
// past the last commit.
//
// On error the generations stay frozen and the caller keeps no pending. A
// later prepare stages them again under the same table IDs.
//
// TODO: a failed StageFrozen or link makes the retry rotate again, so each
// retry adds one frozen generation. The cause is a local disk error. The
// design accepts it because such an error is rare, but a disk that keeps
// failing adds one generation at each retry.
func (idx *Index) prepare(ctx context.Context) (*pendingHandover, error) {
	// Read lastObserved before the rotate: the frozen generations then hold
	// every change with tt <= covered. The changes above covered that the
	// observe writes between the read and the swap land in the frozen
	// generation too; a restart replays them again with the same result.
	covered := idx.cfg.LastObserved()
	// The manifest service treats a flush of the same term whose checkpoint
	// does not move forward as an applied retry, and does not add its tables.
	// So wait: the writes stay in the engine, and a later handover covers them.
	if covered <= idx.LastCommittedTimeTick() {
		return nil, nil
	}

	if _, err := idx.eng.RotateIncrement(ctx); err != nil {
		return nil, err
	}
	// newest first, with the generations that earlier failed handovers left
	gens := idx.eng.FrozenGenerations()
	local := make(map[sst.ID]string)
	var refs []TableRef
	for _, g := range gens {
		staged, err := idx.eng.StageFrozen(ctx, g)
		if err != nil {
			return nil, err
		}
		for _, s := range staged {
			path, err := linkCommitted(idx.committedDir, s)
			if err != nil {
				return nil, err
			}
			local[s.Info.ID] = path
			refs = append(refs, s.Info)
		}
	}

	p := &pendingHandover{gens: gens, local: local, covered: covered}
	if len(refs) > 0 {
		p.request = &FlushArgs{
			CollectionID:       idx.collectionID,
			VChannel:           idx.vchannel,
			PChannelTerm:       idx.cfg.Term,
			PKDataType:         idx.pkDataType,
			AddTables:          refs,
			CheckpointTimeTick: covered,
		}
	}
	return p, nil
}

// upload uploads the tables of the pending request p when needed: when no
// upload finished yet, or when the last upload is older than
// Params.ReuploadAfter. The second case keeps the tables safe from the
// garbage collection of the manifest service (grace window) before the
// commit. The keys and files do not change, so the request does not change
// either. It records the start of a complete upload in p. A request without
// tables has nothing to upload.
func (idx *Index) upload(ctx context.Context, p *pendingHandover) error {
	if p.request == nil {
		return nil
	}
	cfg := idx.cfg
	if !p.uploadedAt.IsZero() && time.Since(p.uploadedAt) <= cfg.Params().ReuploadAfter {
		return nil
	}
	start := time.Now()
	for _, ref := range p.request.AddTables {
		if err := cfg.Store.UploadFile(ctx, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)), p.local[ref.ID]); err != nil {
			return err
		}
	}
	p.uploadedAt = start
	return nil
}

// commit sends the pending request p, whose tables upload uploaded, and
// installs the result. The caller keeps p and calls upload and commit again
// with the same p after a retryable error.
//
// commit installs the returned manifest and retires the generations only when
// the manifest has the request in effect: its version is not below the
// version installed before the flush, and its checkpoint is not below
// p.covered. Else it returns ErrRejected and keeps the generations. It also
// returns ErrRejected when the manifest records another SST base path than
// the one the tables are uploaded under. A manifest service that takes the
// request as an applied retry by mistake can not make the generations retire
// while their tables are not committed.
//
// commit does not read lastObserved again and does not rotate. Only one
// prepare, upload or commit of one Index runs at a time.
func (idx *Index) commit(ctx context.Context, p *pendingHandover) error {
	before := idx.currentInstalledVersion()
	m, err := idx.cfg.Committer.CommitFlush(ctx, *p.request)
	if err != nil {
		return err
	}
	if m.Version < before || m.CheckpointTimeTick < p.covered {
		return errors.Wrapf(ErrRejected, "vchannel %s: the manifest after the flush has version %d and checkpoint %d, want at least version %d and checkpoint %d",
			idx.vchannel, m.Version, m.CheckpointTimeTick, before, p.covered)
	}
	if m.SSTBasePath != idx.sstBasePath {
		return errors.Wrapf(ErrRejected, "vchannel %s: the manifest has the SST base path %s, the tables are uploaded under %s",
			idx.vchannel, m.SSTBasePath, idx.sstBasePath)
	}
	if err := idx.installIfNewer(ctx, m, p.local, p.gens...); err != nil {
		return err
	}
	idx.lastCommitted.Store(p.covered)
	return nil
}

// committedPath returns the local file of committed table id under dir.
func committedPath(dir string, id sst.ID) string {
	return filepath.Join(dir, strconv.FormatInt(int64(id), 10)+sst.Extension)
}

// linkCommitted hard-links a staged table into dir and returns the path of
// the link. The staged file goes away when its generation retires, and the
// link keeps the table on local disk. It reuses a link from an earlier attempt.
func linkCommitted(dir string, s engine.StagedTable) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", pkroot.MarkIO(err, "create committed dir %s", dir)
	}
	path := committedPath(dir, s.Info.ID)
	if err := os.Link(s.Path, path); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", pkroot.MarkIO(err, "link sst %d to %s", s.Info.ID, path)
	}
	return path, nil
}
