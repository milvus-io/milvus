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

package task

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A restore L0 transaction retires all inputs together with every target delta.
// This closes the crash window between that commit and the task-state write.
func (t *L0CompactionTask) restoreMutationApplied() bool {
	if t.GetTaskProto().GetType() != datapb.CompactionType_RestoreLevel0DeleteCompaction || len(t.GetTaskProto().GetInputSegments()) == 0 {
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

func (t *L0CompactionTask) restoreTargets() ([]*meta.SegmentInfo, error) {
	plan := t.GetTaskProto()
	job := t.LookupRestoreJob(t.context(), plan.GetTriggerID())
	if job == nil {
		return nil, merr.WrapErrDataIntegrityMsg("restore job %d missing for compaction plan %d", plan.GetTriggerID(), plan.GetPlanID())
	}
	if job.GetCollectionId() != plan.GetCollectionID() {
		return nil, merr.WrapErrDataIntegrityMsg("restore job %d does not own compaction collection %d", plan.GetTriggerID(), plan.GetCollectionID())
	}
	targets := make([]*meta.SegmentInfo, 0)
	for _, mapping := range job.GetIdMappings() {
		segment := t.meta.GetSegment(t.context(), mapping.GetTargetSegmentId())
		if segment == nil {
			return nil, merr.WrapErrDataIntegrityMsg("restore segment %d missing for compaction plan %d", mapping.GetTargetSegmentId(), plan.GetPlanID())
		}
		if segment.GetLevel() == datapb.SegmentLevel_L0 || segment.GetInsertChannel() != plan.GetChannel() ||
			(plan.GetPartitionID() != common.AllPartitionsID && segment.GetPartitionID() != plan.GetPartitionID()) {
			continue
		}
		if !meta.IsSegmentHealthy(segment) || !meta.IsFlushState(segment.GetState()) || segment.GetCollectionID() != plan.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg("restore compaction target %d is no longer valid", segment.GetID())
		}
		targets = append(targets, segment)
	}
	return targets, nil
}
