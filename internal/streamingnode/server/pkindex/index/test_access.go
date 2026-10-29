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

//go:build test

package index

import (
	"context"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// This file is compiled only with the "test" build tag. It lets the tests of
// other packages read and seed the engine of an Index.

// EngineForTest returns the engine of the Index. A write through it does not
// mark the Index dirty.
func (idx *Index) EngineForTest() *engine.Engine {
	return idx.eng
}

// SetDeciderEngineForTest makes the Decider of the Index read eng instead of
// the engine of the Index. Call it before the first decision.
func (idx *Index) SetDeciderEngineForTest(eng engine.RW) {
	idx.auth = dedup.New(idx.vchannel, eng, dedup.Config{Ready: idx.ready})
}

// PendingHandover is the prepared handover of an Index, for the tests of
// the two phases.
type PendingHandover = pendingHandover

// PrepareForTest runs the prepare phase of a handover.
func (idx *Index) PrepareForTest(ctx context.Context) (*PendingHandover, error) {
	return idx.prepare(ctx)
}

// CommitForTest runs the commit phase of a handover.
func (idx *Index) CommitForTest(ctx context.Context, p *PendingHandover) error {
	return idx.commit(ctx, p)
}

// PendingForTest returns the prepared handover that waits for its commit,
// nil when there is none.
func (idx *Index) PendingForTest() *PendingHandover {
	return idx.pending.Load()
}

// SetLastCommittedForTest sets the LastCommittedTimeTick of the Index.
func (idx *Index) SetLastCommittedForTest(tt uint64) {
	idx.lastCommitted.Store(tt)
}

// InstalledVersionForTest returns the version of the installed table set.
func (idx *Index) InstalledVersionForTest() int64 {
	return idx.currentInstalledVersion()
}

// ForgetInstalledPathsForTest drops the local paths of the installed tables,
// as if the Index did not know where they are.
func (idx *Index) ForgetInstalledPathsForTest() {
	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	idx.installedPaths = nil
}

// SSTBasePathForTest returns the object key prefix of the tables.
func (idx *Index) SSTBasePathForTest() string { return idx.sstBasePath }

// CommittedDirForTest returns the local directory of the committed tables.
func (idx *Index) CommittedDirForTest() string { return idx.committedDir }

// ReadyForTest returns a channel that is closed when the Index is ready.
func (idx *Index) ReadyForTest() <-chan struct{} { return idx.ready }

// DirtyForTest reports whether the Index has writes since its last rotate.
func (idx *Index) DirtyForTest() bool {
	return idx.dirtyFlag.Load()
}

// LastObservedForTest returns the lastObserved of the pchannel of the Index.
func (idx *Index) LastObservedForTest() uint64 {
	return idx.cfg.LastObserved()
}

// The fields of a PendingHandover, for the tests of the two phases.

func (p *PendingHandover) Request() *FlushArgs      { return p.request }
func (p *PendingHandover) Covered() uint64          { return p.covered }
func (p *PendingHandover) Local() map[sst.ID]string { return p.local }

// CommittedPathForTest returns the local path of a committed table.
func CommittedPathForTest(dir string, id sst.ID) string { return committedPath(dir, id) }
