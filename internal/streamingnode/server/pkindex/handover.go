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

package pkindex

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

// prepare freezes the active generation, stages all frozen generations,
// links the tables, and builds the request. It does not upload: commit does.
// The caller records the result as pending right away, so a failed upload
// does not cause a new rotate. The caller calls prepare only when the Index
// has no pending handover. It returns nil when the watermark did not move
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
	// Read the watermark before the rotate, so that the frozen generations
	// hold at least all writes up to it.
	covered := idx.ch.wm.lastApplied()
	// The manifest service treats a flush of the same term whose checkpoint
	// does not move forward as an applied retry, and does not add its tables.
	// So wait: the writes stay in the engine, and a later handover covers them.
	if covered <= idx.lastCommittedTimeTick() {
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
			PChannelTerm:       idx.ch.info.Term,
			PKDataType:         idx.pkDataType,
			AddTables:          refs,
			CheckpointTimeTick: covered,
		}
	}
	return p, nil
}

// commit uploads the tables of p when needed, sends the pending request and
// installs the result. The caller keeps p and calls commit again with the
// same p after a retryable error.
//
// It uploads every table of the request when no upload finished yet, or when
// the last upload is older than Params.ReuploadAfter. The second case keeps
// the tables safe from the coordinator garbage collection (grace window)
// before the commit. The keys and files do not change, so the request does
// not change either.
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
// commit does not read the watermark again and does not rotate. Only one
// prepare or commit of one Index runs at a time.
func (idx *Index) commit(ctx context.Context, p *pendingHandover) error {
	cfg := idx.ch.m.cfg
	if p.request != nil && (p.uploadedAt.IsZero() || time.Since(p.uploadedAt) > cfg.Params().ReuploadAfter) {
		start := time.Now()
		for _, ref := range p.request.AddTables {
			if err := cfg.Store.UploadFile(ctx, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)), p.local[ref.ID]); err != nil {
				return err
			}
		}
		p.uploadedAt = start
	}
	if p.request == nil {
		var dropErr error
		for _, g := range p.gens {
			// a failed drop must not be retried, the next Open cleans up
			if err := idx.eng.DropFrozen(ctx, g); err != nil && dropErr == nil {
				dropErr = err
			}
		}
		// nothing was written up to covered, so it is committed even when a drop failed
		idx.lastCommitted.Store(p.covered)
		return dropErr
	}

	before := idx.currentInstalledVersion()
	m, err := cfg.Committer.CommitFlush(ctx, *p.request)
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
