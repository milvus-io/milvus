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

package datanode

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	taskRetention     = 24 * time.Hour
	taskSweepInterval = time.Hour
)

func (node *DataNode) taskSweepLoop() {
	ticker := time.NewTicker(taskSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-node.ctx.Done():
			return
		case now := <-ticker.C:
			node.removeExpiredTasks(now)
		}
	}
}

func (node *DataNode) removeExpiredTasks(now time.Time) {
	cutoff := now.Add(-taskRetention)
	var importCount, indexCount, compactionCount, externalCount int
	if node.importTaskMgr != nil {
		importCount = node.importTaskMgr.RemoveExpiredTasks(node.ctx, cutoff)
	}
	if node.taskManager != nil {
		indexCount = node.taskManager.RemoveExpiredTasks(node.ctx, cutoff)
	}
	if node.externalCollectionManager != nil {
		externalCount = node.externalCollectionManager.RemoveExpiredTasks(node.ctx, cutoff)
	}
	// Compactor.Stop waits for the running plan to exit. Keep it last: the
	// executor detaches the Stop goroutine (it does not block the sweep), but
	// the ordering keeps even the bookkeeping of a wedged plan off the other
	// managers' path.
	if node.compactionExecutor != nil {
		compactionCount = node.compactionExecutor.RemoveExpiredTasks(node.ctx, cutoff)
	}

	total := importCount + indexCount + compactionCount + externalCount
	if total == 0 {
		return
	}
	mlog.Info(node.ctx, "processed expired datanode tasks",
		mlog.Int("total", total),
		mlog.Int("importCopy", importCount),
		mlog.Int("indexAnalyzeStats", indexCount),
		mlog.Int("compaction", compactionCount),
		mlog.Int("externalRefresh", externalCount),
		mlog.Time("cutoff", cutoff))
}
