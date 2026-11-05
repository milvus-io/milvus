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

package copysegment

import (
	"fmt"
	"maps"
	"sort"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// materializeRestoreDeletes runs only after every copy task has durably
// completed. Importing stays set until all fixed-target L0 plans have committed.
func (c *Checker) materializeRestoreDeletes(job CopySegmentJob) (bool, error) {
	recipe := job.(*Job)
	persisted := make(map[int64]*datapb.CompactionTask)
	if len(recipe.GetRestoreCompactionPlans()) > 0 {
		for _, task := range c.CompactionTasks.GetCompactionTasksByTriggerID(job.GetJobId()) {
			persisted[task.GetPlanID()] = task
		}
		complete := true
		for _, id := range recipe.GetRestoreCompactionPlans() {
			complete = complete && persisted[id].GetTerminalState() == datapb.CompactionTaskState_completed
		}
		if complete {
			return true, nil
		}
	}

	groups := make(map[string]*datapb.CompactionTask)
	for _, mapping := range job.GetIdMappings() {
		segment := c.Meta.GetSegment(c.Ctx, mapping.GetTargetSegmentId())
		if segment == nil {
			return false, merr.WrapErrDataIntegrityMsg("restore segment %d missing before delete materialization", mapping.GetTargetSegmentId())
		}
		if segment.GetLevel() != datapb.SegmentLevel_L0 {
			continue
		}
		key := fmt.Sprintf("%s/%d", segment.GetInsertChannel(), segment.GetPartitionID())
		plan := groups[key]
		if plan == nil {
			plan = &datapb.CompactionTask{
				TriggerID: job.GetJobId(), CollectionID: job.GetCollectionId(),
				PartitionID: segment.GetPartitionID(), Channel: segment.GetInsertChannel(),
				Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, State: datapb.CompactionTaskState_pipelining,
				NodeID: meta.NullNodeID,
			}
			groups[key] = plan
		}
		plan.InputSegments = append(plan.InputSegments, segment.GetID())
		plan.TotalRows += segment.GetNumOfRows()
	}
	if len(groups) == 0 {
		if len(recipe.GetRestoreCompactionPlans()) != 0 {
			return false, merr.WrapErrDataIntegrityMsg("restore compaction inputs disappeared before completion")
		}
		return true, nil
	}
	snapshot, err := loadCopySnapshot(c.Ctx, job, c.SnapshotSource)
	if err != nil {
		return false, err
	}
	keys := make([]string, 0, len(groups))
	for key, plan := range groups {
		keys = append(keys, key)
		plan.Schema = snapshot.Collection.GetSchema()
		sort.Slice(plan.InputSegments, func(i, j int) bool { return plan.InputSegments[i] < plan.InputSegments[j] })
	}
	sort.Strings(keys)
	if len(recipe.GetRestoreCompactionPlans()) == 0 {
		first, _, err := c.alloc.AllocN(int64(len(keys)))
		if err != nil {
			return false, err
		}
		ids := make(map[string]int64, len(keys))
		for i, key := range keys {
			ids[key] = first + int64(i)
		}
		if err := c.CopyMeta.UpdateJob(c.Ctx, job.GetJobId(), func(job CopySegmentJob) {
			job.(*Job).RestoreCompactionPlans = maps.Clone(ids)
		}); err != nil {
			return false, err
		}
		recipe = c.CopyMeta.GetJob(c.Ctx, job.GetJobId()).(*Job)
	}
	if c.Compactions == nil {
		return false, merr.WrapErrServiceUnavailableMsg("restore compaction scheduler is not ready")
	}
	for _, key := range keys {
		plan := groups[key]
		plan.PlanID = recipe.GetRestoreCompactionPlans()[key]
		if plan.PlanID == 0 {
			return false, merr.WrapErrDataIntegrityMsg("restore compaction group %s has no durable plan ID", key)
		}
		if previous := persisted[plan.PlanID]; previous != nil {
			if previous.GetTerminalState() == datapb.CompactionTaskState_completed {
				continue
			}
			switch previous.GetState() {
			case datapb.CompactionTaskState_cleaned:
				// Cleanup has released the previous attempt's input locks. Reuse its
				// durable identity; no concurrent attempt may publish to these bases.
			case datapb.CompactionTaskState_pipelining:
				if c.Compactions.HasCompactionTask(plan.PlanID) {
					continue
				}
				// Admission persisted the plan but a full queue rejected it.
				// Resubmit the same durable plan after its input lock is released.
			default:
				continue
			}
		}
		if err := c.Compactions.EnqueueCompaction(plan); err != nil {
			return false, err
		}
	}
	return false, nil
}
