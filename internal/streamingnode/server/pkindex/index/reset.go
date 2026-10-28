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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Reset empties the Index at tt, the time tick of a TruncateCollection of
// its vchannel. The observe of the WAL recovery calls it in time tick order,
// so the writes before tt are in the engine before it, and the writes after
// tt reach the engine after it. It keeps the Decider: its in-flight set may
// hold writes after tt, which the append path decided already.
//
// It empties the engine (see engine.Lifecycle.Reset), forgets the recovered
// manifest and the installed tables, and drops a prepared handover: its
// tables are from before tt. The next handover commits the reset: its flush
// replaces the committed table set (see FlushArgs.ResetTimeTick). A handover
// that runs at the same time installs nothing, see commit.
//
// It does nothing when tt <= LastCommittedTimeTick: a committed manifest
// covers tt, and the flush that committed it carried the reset or came after
// the flush that did. This happens when the recovery replays the WAL from a
// checkpoint before a truncate whose reset is committed. It also does nothing
// for a truncate before EnableTimeTick: the Index holds no write before it.
//
// It reports whether the Index went from clean to dirty, so that the caller
// adds it to the LastAcked heap of the pchannel.
func (idx *Index) Reset(ctx context.Context, tt uint64) (dirtied bool, err error) {
	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	if tt <= idx.LastCommittedTimeTick() || tt < idx.cfg.EnableTimeTick {
		return false, nil
	}
	if err := idx.closedOr(nil); err != nil {
		return false, err
	}
	if err := idx.eng.Reset(ctx); err != nil {
		return false, err
	}
	idx.resetTT.Store(tt)
	idx.resetEpoch++
	idx.recovered = nil
	idx.installedPaths = nil
	idx.pending.Store(nil)
	idx.logger.Info(ctx, "primary key index reset at a truncate", mlog.Uint64("resetTimeTick", tt))
	return idx.markDirty(), nil
}
