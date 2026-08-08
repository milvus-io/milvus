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

package writebuffer

import (
	"context"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
)

// runSyncTaskInline runs both phases the way the dispatcher does, for tests that
// stub the sync manager and only need the task's end-to-end effect.
func runSyncTaskInline(ctx context.Context, task syncmgr.Task) error {
	if err := task.Prepare(ctx); err != nil {
		return err
	}
	return task.Commit(ctx)
}

// anyGrowingRetryArmed replaces the old channel-wide growingSourceRetryScheduled
// flag: the clock is per segment now, so "is a growing retry pending" is a
// question about the set.
func anyGrowingRetryArmed(wb *writeBufferBase) bool {
	for _, progress := range wb.growingSourceProgress {
		if progress.intent.owes {
			return true
		}
	}
	return false
}
