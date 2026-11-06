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

package compaction

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// recoverCompactionTask reconciles a persisted task with its durable output
// group before the scheduler can query or resubmit work to a DataNode.
func (c *Inspector) recoverCompactionTask(t *datapb.CompactionTask) (ctask.CompactionTask, error) {
	task, err := c.newCompactionTask(t)
	if err != nil {
		return nil, err
	}
	group, err := c.meta.GetCompactionChangeGroup(t)
	if err != nil {
		return nil, err
	}
	if group == nil {
		// No output transaction exists yet. Retain the original input and
		// snapshot validation, including admission of unfinished inner sorts.
		// Do not apply the new-task L0 exclusion here: executing L0 tasks must
		// remain tracked, and schedule() handles stale queued L0 tasks.
		if err := c.claimCompactionInputs(t); err != nil {
			return nil, err
		}
		return task, nil
	}

	// The durable transaction already owns its parents. Reconstruct the
	// checkpoint instead of claiming them again or rerunning the worker.
	restored := proto.Clone(t).(*datapb.CompactionTask)
	switch restored.GetState() {
	case datapb.CompactionTaskState_failed, datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_completed:
		// Keep terminal decisions so Clean can abort unpublished output.
	default:
		restored.State = datapb.CompactionTaskState_meta_saved
		if restored.GetType() == datapb.CompactionType_ClusteringCompaction {
			restored.State = datapb.CompactionTaskState_indexing
		}
	}
	restored.ResultSegments = meta.CompactionGroupResultIDs(group, t)
	task.SetTask(restored)
	c.meta.SetSegmentsCompacting(context.TODO(), t.GetInputSegments(), true)
	return task, nil
}

func (c *Inspector) LoadMeta() error {
	triggers := c.compactionTaskMeta.GetCompactionTasks()
	var failedTasks []*datapb.CompactionTask
	for _, tasks := range triggers {
		for _, task := range tasks {
			if ctask.IsCompactionTaskCleaned(task) {
				mlog.Info(context.TODO(), "compactionInspector loadMeta abandon compactionTask",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.String("type", task.GetType().String()),
					mlog.String("state", task.GetState().String()))
				continue
			}

			t, err := c.recoverCompactionTask(task)
			if err != nil {
				// Invalid or obsolete plans can be discarded, but a recovery
				// failure must not erase the durable task or start scheduling
				// with an unverified group lineage.
				if !errors.Is(err, merr.ErrIllegalCompactionPlan) &&
					!errors.Is(err, merr.ErrCompactionPlanConflict) &&
					!errors.Is(err, merr.ErrSegmentNotFound) &&
					!errors.Is(err, merr.ErrCompactionBlocked) {
					return errors.Wrapf(err, "recover compaction task %d", task.GetPlanID())
				}
				mlog.Info(context.TODO(), "compactionInspector loadMeta obsolete compactionTask, defer cleanup",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.String("type", task.GetType().String()),
					mlog.String("state", task.GetState().String()),
					mlog.Err(err),
				)
				failedTasks = append(failedTasks, task)
				continue
			}
			if t.NeedReAssignNodeID() {
				if err = c.submitTask(t); err != nil {
					mlog.Info(context.TODO(), "compactionInspector loadMeta submit task failed, defer cleanup",
						mlog.Int64("planID", task.GetPlanID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", task.GetState().String()),
						mlog.Err(err),
					)
					failedTasks = append(failedTasks, task)
					continue
				}
				mlog.Info(context.TODO(), "compactionInspector loadMeta submitTask",
					mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
					mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
					mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
					mlog.String("type", task.GetType().String()),
					mlog.String("state", t.GetTaskProto().GetState().String()))
			} else {
				c.restoreTask(t)
				mlog.Info(context.TODO(), "compactionInspector loadMeta restoreTask",
					mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
					mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
					mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
					mlog.String("type", task.GetType().String()),
					mlog.String("state", t.GetTaskProto().GetState().String()))
			}
		}
	}

	if len(failedTasks) > 0 {
		c.stopWg.Add(1)
		go c.cleanupFailedCompactionTasks(failedTasks)
	}
	return nil
}

func (c *Inspector) cleanupFailedCompactionTasks(tasks []*datapb.CompactionTask) {
	defer c.stopWg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	total := len(tasks)
	mlog.Info(ctx, "start cleaning up failed compaction tasks", mlog.Int("total", total))
	cleaned := 0
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "failed compaction task cleanup aborted",
				mlog.Int("cleaned", cleaned),
				mlog.Int("remaining", total-cleaned))
			return
		default:
		}
		if err := c.compactionTaskMeta.DropCompactionTask(ctx, task); err != nil {
			mlog.Warn(ctx, "drop failed compaction task failed",
				mlog.Int64("planID", task.GetPlanID()), mlog.Err(err))
			continue
		}
		cleaned++
	}
	mlog.Info(ctx, "failed compaction task cleanup finished",
		mlog.Int("cleaned", cleaned),
		mlog.Int("total", total))
}
