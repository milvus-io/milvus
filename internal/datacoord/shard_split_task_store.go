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
	"sync"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	// sourceIndex maps a source vchannel to the ids of the tasks that name it,
	// so the per-vchannel flush-state checks (sourceSwitchTimeTick) never walk
	// every task ever recorded. Entries are only ever added: a task keeps its
	// source for life (CommitShardSplit refuses a different one under the same
	// id) and tasks are never removed from the store.
	sourceMu    sync.RWMutex
	sourceIndex map[string][]int64

	// taskLocks serializes the read-modify-write of one task id
	// (lockTask/unlockTask): a writer reads the record, merges its own
	// fields into it and upserts the result, and two such writers on the
	// same id must not interleave or the second's upsert drops what the
	// first merged in. It is held around the whole sequence, and only ever
	// outside sourceMu, which cache takes on its own for the index update.
	taskLocks *lock.KeyLock[int64]
}

func newShardSplitTasks() *shardSplitTasks {
	return &shardSplitTasks{
		tasks:       typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask](),
		sourceIndex: make(map[string][]int64),
		taskLocks:   lock.NewKeyLock[int64](),
	}
}

// lockTask takes the write lock of one task id. Every read-merge-upsert of a
// task must run under it, and release it with unlockTask.
func (s *shardSplitTasks) lockTask(taskID int64) {
	s.taskLocks.Lock(taskID)
}

func (s *shardSplitTasks) unlockTask(taskID int64) {
	s.taskLocks.Unlock(taskID)
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
		s.cache(task)
	}
	return nil
}

// cache stores the task and indexes its sources.
func (s *shardSplitTasks) cache(task *datapb.SplitShardTask) {
	s.tasks.Insert(task.GetTaskId(), task)
	s.sourceMu.Lock()
	defer s.sourceMu.Unlock()
	for _, source := range task.GetSources() {
		ids := s.sourceIndex[source.GetVchannel()]
		if !lo.Contains(ids, task.GetTaskId()) {
			s.sourceIndex[source.GetVchannel()] = append(ids, task.GetTaskId())
		}
	}
}

// sourceSwitchTimeTick returns the recorded T_switch of vchannel as the source
// of a shard split, or false when no recorded task has fenced it: it is not
// the source of any task, or every task naming it still carries a zero tick
// (its fence is not on record, so it may still be accepting writes). Should
// more than one task name it with a non-zero tick, the largest is returned,
// which is the conservative choice for a caller asking "has the checkpoint
// passed the fence".
func (s *shardSplitTasks) sourceSwitchTimeTick(vchannel string) (uint64, bool) {
	s.sourceMu.RLock()
	ids := s.sourceIndex[vchannel]
	s.sourceMu.RUnlock()

	var tick uint64
	for _, id := range ids {
		task, ok := s.tasks.Get(id)
		if !ok {
			continue
		}
		for _, source := range task.GetSources() {
			if source.GetVchannel() == vchannel && source.GetSwitchTimeTick() > tick {
				tick = source.GetSwitchTimeTick()
			}
		}
	}
	return tick, tick != 0
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
	s.cache(task)
	return nil
}

// modify applies mutate to a private copy of the latest record of taskID and
// persists the result through upsert, all under the task's write lock
// (lockTask), the same lock CommitShardSplit holds around its own
// read-merge-upsert. So the split manager and the SplitShard ack callback, the
// two writers of one record, never write back a stale copy over each other: a
// manager write that raced the callback cannot revert the T_switch and the
// fence the callback recorded, and a redelivered callback cannot drop what the
// manager wrote.
//
// mutate returns false to leave the record untouched, and nothing is persisted.
// It returns the record as it stands afterwards. A task the store does not hold
// is a System error: every caller acts on a task it has already observed, and
// records are never removed.
func (s *shardSplitTasks) modify(
	ctx context.Context,
	catalog metastore.DataCoordCatalog,
	taskID int64,
	mutate func(task *datapb.SplitShardTask) bool,
) (*datapb.SplitShardTask, error) {
	s.lockTask(taskID)
	defer s.unlockTask(taskID)
	latest, ok := s.tasks.Get(taskID)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("shard split task %d is not in the store", taskID)
	}
	cloned := proto.Clone(latest).(*datapb.SplitShardTask)
	if !mutate(cloned) {
		return latest, nil
	}
	if err := s.upsert(ctx, catalog, cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

// create persists a task the store does not hold yet, under its write lock.
// Refused when the id is already recorded: a task id is allocated once, so an
// existing record under it is either this very task (a retry, which must not
// overwrite what has happened since) or another split's, and neither may be
// replaced.
func (s *shardSplitTasks) create(ctx context.Context, catalog metastore.DataCoordCatalog, task *datapb.SplitShardTask) error {
	s.lockTask(task.GetTaskId())
	defer s.unlockTask(task.GetTaskId())
	if _, ok := s.tasks.Get(task.GetTaskId()); ok {
		return merr.WrapErrServiceInternalMsg("shard split task %d is already recorded", task.GetTaskId())
	}
	return s.upsert(ctx, catalog, task)
}

// list returns every recorded task. The records are the store's own and must
// not be mutated; modify is the only way to change one.
func (s *shardSplitTasks) list() []*datapb.SplitShardTask {
	return s.tasks.Values()
}

// splitSourceVChannels lists a task's source vchannel names.
func splitSourceVChannels(task *datapb.SplitShardTask) []string {
	out := make([]string, 0, len(task.GetSources()))
	for _, source := range task.GetSources() {
		out = append(out, source.GetVchannel())
	}
	return out
}
