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

func (t *L0CompactionTask) restoreTargets() ([]*meta.SegmentInfo, error) {
	plan := t.GetTaskProto()
	targets := make([]*meta.SegmentInfo, 0, len(plan.GetRestoreTargetSegments()))
	for _, id := range plan.GetRestoreTargetSegments() {
		segment := t.meta.GetSegment(t.context(), id)
		if !meta.IsSegmentHealthy(segment) || !meta.IsFlushState(segment.GetState()) || segment.GetCollectionID() != plan.GetCollectionID() ||
			segment.GetInsertChannel() != plan.GetChannel() || segment.GetLevel() == datapb.SegmentLevel_L0 ||
			(plan.GetPartitionID() != common.AllPartitionsID && segment.GetPartitionID() != plan.GetPartitionID()) {
			return nil, merr.WrapErrDataIntegrityMsg("restore compaction target %d is no longer valid", id)
		}
		targets = append(targets, segment)
	}
	return targets, nil
}
