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
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// This file is compiled only with the "test" build tag. It lets the tests of
// other packages read and seed the engine of an Index.

// EngineForTest returns the engine of the Index. A write through it does not
// mark the Index dirty.
func EngineForTest(idx Index) *engine.Engine {
	return idx.(*indexImpl).eng
}

// SetDeciderEngineForTest makes the Decider of the Index read eng instead of
// the engine of the Index. Call it before the first decision.
func SetDeciderEngineForTest(idx Index, eng engine.RW) {
	idx.(*indexImpl).auth = dedup.New(idx.(*indexImpl).vchannel, eng, dedup.Config{Ready: idx.(*indexImpl).ready})
}

// PendingHandover is the prepared handover of an Index, for the tests of
// the two phases.
type PendingHandover = pendingHandover

// PrepareForTest runs the prepare phase of a handover.
func PrepareForTest(ctx context.Context, idx Index) (*PendingHandover, error) {
	return idx.(*indexImpl).prepare(ctx)
}

// CommitForTest runs the commit phase of a handover.
func CommitForTest(ctx context.Context, idx Index, p *PendingHandover) error {
	return idx.(*indexImpl).commit(ctx, p)
}

// PendingForTest returns the prepared handover that waits for its commit,
// nil when there is none.
func PendingForTest(idx Index) *PendingHandover {
	return idx.(*indexImpl).pending.Load()
}

// SetLastCommittedForTest sets the LastCommittedTimeTick of the Index.
func SetLastCommittedForTest(idx Index, tt uint64) {
	idx.(*indexImpl).lastCommitted.Store(tt)
}

// InstalledVersionForTest returns the version of the installed table set.
func InstalledVersionForTest(idx Index) int64 {
	return idx.(*indexImpl).currentInstalledVersion()
}

// ForgetInstalledPathsForTest drops the local paths of the installed tables,
// as if the Index did not know where they are.
func ForgetInstalledPathsForTest(idx Index) {
	idx.(*indexImpl).installMu.Lock()
	defer idx.(*indexImpl).installMu.Unlock()
	idx.(*indexImpl).installedPaths = nil
}

// SSTBasePathForTest returns the object key prefix of the tables.
func SSTBasePathForTest(idx Index) string { return idx.(*indexImpl).cfg.SSTBasePath }

// CommittedDirForTest returns the local directory of the committed tables.
func CommittedDirForTest(idx Index) string { return idx.(*indexImpl).committedDir }

// ReadyForTest returns a channel that is closed when the Index is ready.
func ReadyForTest(idx Index) <-chan struct{} { return idx.(*indexImpl).ready }

// DirtyForTest reports whether the Index has writes since its last rotate.
func DirtyForTest(idx Index) bool {
	return idx.(*indexImpl).dirtyFlag.Load()
}

// LastObservedForTest returns the lastObserved of the pchannel of the Index.
func LastObservedForTest(idx Index) uint64 {
	return idx.(*indexImpl).cfg.LastObserved()
}

// The fields of a PendingHandover, for the tests of the two phases.

func (p *PendingHandover) Request() *manifest.FlushArgs { return p.request }
func (p *PendingHandover) Covered() uint64              { return p.covered }
func (p *PendingHandover) Local() map[sst.ID]string     { return p.local }

// CommittedPathForTest returns the local path of a committed table.
func CommittedPathForTest(dir string, id sst.ID) string { return committedPath(dir, id) }

// FailWritesForTest makes every engine write of Apply fail with err, nil
// lets the writes through again.
func FailWritesForTest(idx Index, err error) {
	if err == nil {
		idx.(*indexImpl).failWrites.Store(nil)
		return
	}
	idx.(*indexImpl).failWrites.Store(&err)
}

// OnWriteForTest runs hook before every engine write of Apply with the
// batch, nil removes it.
func OnWriteForTest(idx Index, hook func(muts []engine.Mutation)) {
	if hook == nil {
		idx.(*indexImpl).onWrite.Store(nil)
		return
	}
	idx.(*indexImpl).onWrite.Store(&hook)
}

// CheckpointTimeTickForTest returns the enabling vchannel metadata checkpoint.
func CheckpointTimeTickForTest(idx Index) uint64 { return idx.(*indexImpl).cfg.CheckpointTimeTick }
