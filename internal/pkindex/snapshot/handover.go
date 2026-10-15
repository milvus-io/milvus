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

// Package snapshot moves the writes of a vchannel from the engine's
// generations to the committed table set.
package snapshot

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/objpath"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// Target is the vchannel side that a handover needs.
type Target interface {
	CollectionID() int64
	VChannel() string
	PChannelTerm() int64
	PKDataType() schemapb.DataType
	// SSTBasePath is the complete object key prefix of the SSTs.
	SSTBasePath() string
	// CommittedDir is the local directory of the committed tables.
	CommittedDir() string
	// LastAppliedTimeTick is the time tick that the engine holds all writes up to.
	LastAppliedTimeTick() uint64
	// LastCommittedTimeTick is the checkpoint of the last successful handover.
	LastCommittedTimeTick() uint64
	Lifecycle() engine.Lifecycle
	// InstalledVersion is the version of the installed manifest, 0 when no
	// manifest is installed.
	InstalledVersion() int64
	// InstallIfNewer installs m when its version is newer than the installed
	// one, and retires gens in the same step. local maps the tables of this
	// handover to their local files.
	InstallIfNewer(ctx context.Context, m ManifestView, local map[sst.ID]string, gens ...engine.Generation) error
	SetLastCommittedTimeTick(tt uint64)
}

// Pending is a prepared handover. Every retry sends the same request, so the
// manifest service sees an exact retry (checkpoint not advanced, every SST
// present) and returns the committed manifest after a lost response.
type Pending struct {
	// Gens are the frozen generations that the handover retires.
	Gens []engine.Generation
	// Local maps the tables of Request to their committed local files.
	Local map[sst.ID]string
	// Request is nil when there is nothing to publish.
	Request *FlushArgs
	// Covered is the checkpoint that the handover commits.
	Covered uint64
	// UploadedAt is when the last complete upload of the tables started. It
	// is zero until the tables are uploaded once.
	UploadedAt time.Time
}

// Prepare freezes the active generation, stages all frozen generations,
// links the tables, and builds the request. It does not upload: Commit does.
// The caller records the result as pending right away, so a failed upload
// does not cause a new rotate. The caller calls Prepare only when the target
// has no pending handover. It returns nil when the watermark did not move
// past the last commit.
//
// On error the generations stay frozen and the caller keeps no pending. A
// later Prepare stages them again under the same table IDs.
//
// TODO: a failed StageFrozen or link makes the retry rotate again, so each
// retry adds one frozen generation. The cause is a local disk error. The
// design accepts it because such an error is rare, but a disk that keeps
// failing adds one generation at each retry.
func Prepare(ctx context.Context, t Target) (*Pending, error) {
	// Read the watermark before the rotate, so that the frozen generations
	// hold at least all writes up to it.
	covered := t.LastAppliedTimeTick()
	// The manifest service treats a flush of the same term whose checkpoint
	// does not move forward as an applied retry, and does not add its tables.
	// So wait: the writes stay in the engine, and a later handover covers them.
	if covered <= t.LastCommittedTimeTick() {
		return nil, nil
	}

	lc := t.Lifecycle()
	if _, err := lc.RotateIncrement(ctx); err != nil {
		return nil, err
	}
	// newest first, with the generations that earlier failed handovers left
	gens := lc.FrozenGenerations()
	local := make(map[sst.ID]string)
	var refs []TableRef
	for _, g := range gens {
		staged, err := lc.StageFrozen(ctx, g)
		if err != nil {
			return nil, err
		}
		for _, s := range staged {
			path, err := linkCommitted(t.CommittedDir(), s)
			if err != nil {
				return nil, err
			}
			local[s.Info.ID] = path
			refs = append(refs, s.Info)
		}
	}

	p := &Pending{Gens: gens, Local: local, Covered: covered}
	if len(refs) > 0 {
		p.Request = &FlushArgs{
			CollectionID:       t.CollectionID(),
			VChannel:           t.VChannel(),
			PChannelTerm:       t.PChannelTerm(),
			PKDataType:         t.PKDataType(),
			AddTables:          refs,
			CheckpointTimeTick: covered,
		}
	}
	return p, nil
}

// Commit uploads the tables of p when needed, sends the pending request and
// installs the result. The caller keeps p and calls Commit again with the
// same p after a retryable error.
//
// It uploads every table of the request when no upload finished yet, or when
// the last upload is older than reuploadAfter. The second case keeps the
// tables safe from the coordinator garbage collection (grace window) before
// the commit. The keys and files do not change, so the request does not
// change either.
//
// Commit installs the returned manifest and retires the generations only when
// the manifest has the request in effect: its version is not below the
// installed version, and its checkpoint is not below p.Covered. Else it
// returns ErrRejected and keeps the generations. It also returns ErrRejected
// when the manifest records another SST base path than the one the tables
// are uploaded under. A manifest service that
// takes the request as an applied retry by mistake can not make the
// generations retire while their tables are not committed.
//
// Commit does not read the watermark again and does not rotate. Only one
// Prepare or Commit for one Target can run at a time.
func Commit(ctx context.Context, t Target, store TableStore, c Committer, p *Pending, reuploadAfter time.Duration) error {
	if p.Request != nil && (p.UploadedAt.IsZero() || time.Since(p.UploadedAt) > reuploadAfter) {
		start := time.Now()
		for _, ref := range p.Request.AddTables {
			if err := store.UploadFile(ctx, objpath.SSTKey(t.SSTBasePath(), int64(ref.ID)), p.Local[ref.ID]); err != nil {
				return err
			}
		}
		p.UploadedAt = start
	}
	if p.Request == nil {
		lc := t.Lifecycle()
		var dropErr error
		for _, g := range p.Gens {
			// a failed drop must not be retried, the next Open cleans up
			if err := lc.DropFrozen(ctx, g); err != nil && dropErr == nil {
				dropErr = err
			}
		}
		// nothing was written up to Covered, so it is committed even when a drop failed
		t.SetLastCommittedTimeTick(p.Covered)
		return dropErr
	}

	before := t.InstalledVersion()
	m, err := c.CommitFlush(ctx, *p.Request)
	if err != nil {
		return err
	}
	if m.Version < before || m.CheckpointTimeTick < p.Covered {
		return errors.Wrapf(ErrRejected, "vchannel %s: the manifest after the flush has version %d and checkpoint %d, want at least version %d and checkpoint %d",
			t.VChannel(), m.Version, m.CheckpointTimeTick, before, p.Covered)
	}
	if m.SSTBasePath != t.SSTBasePath() {
		return errors.Wrapf(ErrRejected, "vchannel %s: the manifest has the SST base path %s, the tables are uploaded under %s",
			t.VChannel(), m.SSTBasePath, t.SSTBasePath())
	}
	if err := t.InstallIfNewer(ctx, m, p.Local, p.Gens...); err != nil {
		return err
	}
	t.SetLastCommittedTimeTick(p.Covered)
	return nil
}

// sstFileName is the local file name of a committed table.
func sstFileName(id sst.ID) string {
	return strconv.FormatInt(int64(id), 10) + sst.Extension
}

// linkCommitted hard-links a staged table into dir and returns the path of
// the link. The staged file goes away when its generation retires, and the
// link keeps the table on local disk. It reuses a link from an earlier attempt.
func linkCommitted(dir string, s engine.StagedTable) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", pkindex.MarkIO(err, "create committed dir %s", dir)
	}
	path := filepath.Join(dir, sstFileName(s.Info.ID))
	if err := os.Link(s.Path, path); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", pkindex.MarkIO(err, "link sst %d to %s", s.Info.ID, path)
	}
	return path, nil
}
