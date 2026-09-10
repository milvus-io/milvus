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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// shardSplitTasks is datacoord's record of the shard splits it has been told
// about, keyed by split task id.
//
// Every replica's datacoord holds one. On the primary it is written by the
// planner as well; on a secondary the SplitShard ack callback
// (Server.CommitShardSplit) is the only writer, and the record it leaves is
// what lets that replica answer the drain check and later adopt the targets.
// The catalog behind it is authoritative: the cache exists so the drain check,
// which runs on a loop, does not walk etcd.
type shardSplitTasks struct {
	tasks *typeutil.ConcurrentMap[int64, *datapb.SplitShardTask]
}

func newShardSplitTasks() *shardSplitTasks {
	return &shardSplitTasks{tasks: typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask]()}
}

// load fills the cache from the catalog. Called once at datacoord start, so an
// in-flight split resumes across a restart instead of stranding its sources
// half-retired.
func (s *shardSplitTasks) load(ctx context.Context, catalog metastore.DataCoordCatalog) error {
	tasks, err := catalog.ListSplitShardTask(ctx)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		s.tasks.Insert(task.GetTaskId(), task)
	}
	return nil
}

func (s *shardSplitTasks) get(taskID int64) (*datapb.SplitShardTask, bool) {
	return s.tasks.Get(taskID)
}

// upsert persists the task and only then caches it.
//
// The order matters in one direction only: a task cached but not persisted
// disappears on the next restart while its sources have already been treated as
// splitting, whereas a task persisted but not cached is read back by load. So
// the catalog goes first and a failed save leaves the cache untouched.
//
// That leaves one narrow window: an etcd put that actually landed but whose
// response timed out returns an error here, so the catalog holds the new task
// while the cache still holds the old one --- which for a freshly fenced source
// means a stale zero T_switch. Both exits close it: the callback is retried to
// success and the retry merges onto the same id, and a restart's load reads the
// catalog's copy. Nothing acts on the stale record in between, because the drain
// check refuses a source whose T_switch is zero.
func (s *shardSplitTasks) upsert(ctx context.Context, catalog metastore.DataCoordCatalog, task *datapb.SplitShardTask) error {
	if err := catalog.SaveSplitShardTask(ctx, task); err != nil {
		return err
	}
	s.tasks.Insert(task.GetTaskId(), task)
	return nil
}

// splitSourceVChannels lists a task's source vchannel names.
func splitSourceVChannels(task *datapb.SplitShardTask) []string {
	out := make([]string, 0, len(task.GetSources()))
	for _, source := range task.GetSources() {
		out = append(out, source.GetVchannel())
	}
	return out
}
