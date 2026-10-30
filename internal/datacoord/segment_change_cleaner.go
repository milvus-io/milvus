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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// segmentChangeCleaner removes terminal identities only after the producer can
// no longer replay them. A COMMITTED group also waits for physical retirement
// of its superseded segments. Physical removal follows the existing segment
// GC's snapshot, load and checkpoint protections; it is a conservative
// retirement condition for the group's parent metadata.
type segmentChangeCleaner struct {
	mt          *meta
	importMeta  ImportMeta
	copyMeta    CopySegmentMeta
	refreshMeta *externalCollectionRefreshMeta
}

func (c *segmentChangeCleaner) clean(ctx context.Context, groups []*model.SegmentChangeGroup) {
	if c == nil || c.mt == nil {
		return
	}
	needsCompactionTasks := false
	for _, g := range groups {
		if !g.IsTerminal() {
			continue
		}
		switch g.Source {
		case model.SegmentChangeSourceSortCompaction, model.SegmentChangeSourceMixCompaction,
			model.SegmentChangeSourceClustering, model.SegmentChangeSourceForceMerge,
			model.SegmentChangeSourceStorageVersion, model.SegmentChangeSourceBumpSchema:
			needsCompactionTasks = true
		}
	}
	// Build one task index for the sweep. Looking up each compaction group by
	// scanning every task would make cleanup quadratic in long-lived metadata.
	var compactionTasks map[int64]*datapb.CompactionTask
	if needsCompactionTasks && c.mt.compactionTaskMeta != nil {
		compactionTasks = make(map[int64]*datapb.CompactionTask)
		for _, tasks := range c.mt.GetCompactionTasks(ctx) {
			for _, task := range tasks {
				compactionTasks[task.GetPlanID()] = task
			}
		}
	}
	for _, g := range groups {
		if !g.IsTerminal() || !c.producerFinished(ctx, g, compactionTasks) || !c.segmentsRetired(ctx, g) {
			continue
		}
		if err := c.mt.DeleteSegmentChangeGroup(ctx, g.CollectionID, g.GroupID); err != nil {
			mlog.Warn(ctx, "failed to delete terminal segment change group, will retry",
				mlog.Int64("groupID", g.GroupID), mlog.Err(err))
		}
	}
}

func (c *segmentChangeCleaner) producerFinished(ctx context.Context, g *model.SegmentChangeGroup, tasks map[int64]*datapb.CompactionTask) bool {
	switch g.Source {
	case model.SegmentChangeSourceSortCompaction, model.SegmentChangeSourceMixCompaction,
		model.SegmentChangeSourceClustering, model.SegmentChangeSourceForceMerge,
		model.SegmentChangeSourceStorageVersion, model.SegmentChangeSourceBumpSchema:
		if c.mt.compactionTaskMeta == nil || !compactionTaskFinished(tasks[g.SourceJobID]) {
			return false
		}
		for planID := range g.SortJobs {
			if !compactionTaskFinished(tasks[planID]) {
				return false
			}
		}
		return true
	case model.SegmentChangeSourceImportJob, model.SegmentChangeSourceCDCReplicated:
		if c.importMeta == nil {
			return false
		}
		job := c.importMeta.GetJob(ctx, g.SourceJobID)
		return job == nil || job.GetState() == internalpb.ImportJobState_Completed || job.GetState() == internalpb.ImportJobState_Failed
	case model.SegmentChangeSourceCopySegment:
		if c.copyMeta == nil {
			return false
		}
		// Copy groups use the task ID as SourceJobID. A task is removed
		// only by the copy job's terminal GC.
		task := c.copyMeta.GetTask(ctx, g.SourceJobID)
		if task == nil {
			return true
		}
		if task.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskCompleted &&
			task.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskFailed {
			return false
		}
		if task.GetCleanupRequired() {
			return false
		}
		job := c.copyMeta.GetJob(ctx, task.GetJobId())
		return job == nil || job.GetState() == datapb.CopySegmentJobState_CopySegmentJobCompleted || job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed
	case model.SegmentChangeSourceExternalRefresh:
		if c.refreshMeta == nil {
			return false
		}
		job := c.refreshMeta.GetJob(g.SourceJobID)
		return job == nil || job.GetState() == indexpb.JobState_JobStateFinished || job.GetState() == indexpb.JobState_JobStateFailed
	default:
		return false
	}
}

// A missing task cannot resume on coordinator restart; terminal task GC can
// also remove it before the superseded segment reaches physical GC.
func compactionTaskFinished(task *datapb.CompactionTask) bool {
	if task == nil {
		return true
	}
	switch task.GetState() {
	case datapb.CompactionTaskState_completed, datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_cleaned:
		return true
	default:
		return false
	}
}

func (c *segmentChangeCleaner) segmentsRetired(ctx context.Context, g *model.SegmentChangeGroup) bool {
	if g.State == model.SegmentChangeStateCommitted {
		for _, id := range g.SupersededSegmentIDs {
			if c.mt.GetSegment(ctx, id) != nil {
				return false
			}
		}
		return true
	}
	for _, id := range g.UnpublishedSegmentIDs() {
		if seg := c.mt.GetSegment(ctx, id); seg != nil && seg.GetState() != commonpb.SegmentState_Dropped {
			return false
		}
	}
	return true
}
