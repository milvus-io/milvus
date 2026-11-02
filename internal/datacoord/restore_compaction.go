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
	"fmt"
	"maps"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// materializeRestoreDeletes runs only after every copy task has durably
// completed. Importing stays set until all fixed-target L0 plans have committed.
func (c *copySegmentChecker) materializeRestoreDeletes(job CopySegmentJob) (bool, error) {
	recipe := job.(*copySegmentJob)
	persisted := make(map[int64]*datapb.CompactionTask)
	if len(recipe.GetRestoreCompactionPlans()) > 0 {
		for _, task := range c.meta.GetCompactionTasksByTriggerID(c.ctx, job.GetJobId()) {
			persisted[task.GetPlanID()] = task
		}
		complete := true
		for _, id := range recipe.GetRestoreCompactionPlans() {
			complete = complete && persisted[id].GetRestoreCompleted()
		}
		if complete {
			return true, nil
		}
	}

	groups := make(map[string]*datapb.CompactionTask)
	targets := make([]*SegmentInfo, 0, len(job.GetIdMappings()))
	for _, mapping := range job.GetIdMappings() {
		segment := c.meta.GetSegment(c.ctx, mapping.GetTargetSegmentId())
		if segment == nil {
			return false, merr.WrapErrDataIntegrityMsg("restore segment %d missing before delete materialization", mapping.GetTargetSegmentId())
		}
		if segment.GetLevel() != datapb.SegmentLevel_L0 {
			targets = append(targets, segment)
			continue
		}
		key := fmt.Sprintf("%s/%d", segment.GetInsertChannel(), segment.GetPartitionID())
		plan := groups[key]
		if plan == nil {
			plan = &datapb.CompactionTask{
				TriggerID: job.GetJobId(), RestoreJobId: job.GetJobId(), CollectionID: job.GetCollectionId(),
				PartitionID: segment.GetPartitionID(), Channel: segment.GetInsertChannel(),
				Type: datapb.CompactionType_Level0DeleteCompaction, State: datapb.CompactionTaskState_pipelining,
				NodeID: NullNodeID,
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
	snapshot, err := loadCopySnapshot(c.ctx, job, c.meta.snapshotMeta)
	if err != nil {
		return false, err
	}
	keys := make([]string, 0, len(groups))
	for key, plan := range groups {
		keys = append(keys, key)
		plan.Schema = snapshot.Collection.GetSchema()
		for _, segment := range targets {
			if segment.GetInsertChannel() == plan.GetChannel() && (plan.GetPartitionID() == common.AllPartitionsID || segment.GetPartitionID() == plan.GetPartitionID()) {
				plan.RestoreTargetSegments = append(plan.RestoreTargetSegments, segment.GetID())
			}
		}
		sort.Slice(plan.InputSegments, func(i, j int) bool { return plan.InputSegments[i] < plan.InputSegments[j] })
		sort.Slice(plan.RestoreTargetSegments, func(i, j int) bool { return plan.RestoreTargetSegments[i] < plan.RestoreTargetSegments[j] })
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
		if err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(), func(job CopySegmentJob) {
			job.(*copySegmentJob).RestoreCompactionPlans = maps.Clone(ids)
		}); err != nil {
			return false, err
		}
		recipe = c.copyMeta.GetJob(c.ctx, job.GetJobId()).(*copySegmentJob)
	}
	if c.compactions == nil {
		return false, merr.WrapErrServiceUnavailableMsg("restore compaction scheduler is not ready")
	}
	for _, key := range keys {
		plan := groups[key]
		plan.PlanID = recipe.GetRestoreCompactionPlans()[key]
		if plan.PlanID == 0 {
			return false, merr.WrapErrDataIntegrityMsg("restore compaction group %s has no durable plan ID", key)
		}
		if previous := persisted[plan.PlanID]; previous != nil {
			if previous.GetRestoreCompleted() {
				continue
			}
			switch previous.GetState() {
			case datapb.CompactionTaskState_cleaned:
				// Cleanup has released the previous attempt's input locks. Reuse its
				// durable identity; no concurrent attempt may publish to these bases.
			case datapb.CompactionTaskState_pipelining:
				inspector, ok := c.compactions.(*compactionInspector)
				if !ok || inspector.getCompactionTask(plan.PlanID) != nil {
					continue
				}
				// Admission persisted the plan but a full queue rejected it.
				// Resubmit the same durable plan after its input lock is released.
			default:
				continue
			}
		}
		if err := c.compactions.enqueueCompaction(plan); err != nil {
			return false, err
		}
	}
	return false, nil
}

// A restore L0 transaction retires all inputs together with every target delta.
// This closes the crash window between that commit and the task-state write.
func (t *l0CompactionTask) restoreMutationApplied() bool {
	if t.GetTaskProto().GetRestoreJobId() == 0 || len(t.GetTaskProto().GetInputSegments()) == 0 {
		return false
	}
	for _, id := range t.GetTaskProto().GetInputSegments() {
		segment := t.meta.GetSegment(t.context(), id)
		if segment == nil || segment.GetState() != commonpb.SegmentState_Dropped || !segment.GetCompacted() {
			return false
		}
	}
	return true
}

func (t *l0CompactionTask) restoreTargets() ([]*SegmentInfo, error) {
	plan := t.GetTaskProto()
	targets := make([]*SegmentInfo, 0, len(plan.GetRestoreTargetSegments()))
	for _, id := range plan.GetRestoreTargetSegments() {
		segment := t.meta.GetSegment(t.context(), id)
		if !isSegmentHealthy(segment) || !isFlushState(segment.GetState()) || segment.GetCollectionID() != plan.GetCollectionID() ||
			segment.GetInsertChannel() != plan.GetChannel() || segment.GetLevel() == datapb.SegmentLevel_L0 ||
			(plan.GetPartitionID() != common.AllPartitionsID && segment.GetPartitionID() != plan.GetPartitionID()) {
			return nil, merr.WrapErrDataIntegrityMsg("restore compaction target %d is no longer valid", id)
		}
		targets = append(targets, segment)
	}
	return targets, nil
}
