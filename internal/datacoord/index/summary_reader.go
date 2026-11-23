// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// segmentIndexSummaries returns the newest build per index ID. For the same
// build, a retained task takes precedence until retirement.
func (m *IndexMeta) segmentIndexSummaries(segmentID int64) map[int64]*model.SegmentIndexSummary {
	usesSummaries := m.UsesSegmentSummaries()
	if usesSummaries {
		// Completion publishes the summary before retiring the task under this lock.
		locks := m.getSegmentIndexMapLocks()
		locks.RLock(segmentID)
		defer locks.RUnlock(segmentID)
	}
	var tasks *typeutil.ConcurrentMap[int64, *model.SegmentIndex]
	if m.SegmentIndexes != nil {
		tasks, _ = m.SegmentIndexes.Get(segmentID)
	}
	var segment *datapb.SegmentInfo
	if usesSummaries {
		segment, _ = m.summarySource.GetSegment(segmentID)
	}
	completed := segment.GetIndexSummaries()
	if tasks == nil && len(completed) == 0 {
		return nil
	}
	summaries := make(map[int64]*model.SegmentIndexSummary, len(completed))
	for _, summary := range completed {
		summaries[summary.GetIndexId()] = model.SegmentIndexSummaryFromProto(segment, summary)
	}
	if tasks != nil {
		tasks.Range(func(id int64, task *model.SegmentIndex) bool {
			if current := summaries[id]; current == nil || task.BuildID >= current.BuildID {
				summaries[id] = model.SummarizeSegmentIndex(task)
			}
			return true
		})
	}
	return summaries
}

// GetSegmentIndexSummaries returns the current builds for live index definitions.
func (m *IndexMeta) GetSegmentIndexSummaries(collectionID, segmentID int64) map[int64]*model.SegmentIndexSummary {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	indexes := m.Indexes[collectionID]
	if len(indexes) == 0 {
		return nil
	}
	summaries := m.segmentIndexSummaries(segmentID)
	for id, summary := range summaries {
		index := indexes[id]
		if index == nil || index.IsDeleted || summary.IsDeleted {
			delete(summaries, id)
		}
	}
	return summaries
}

// GetSegmentIndexSummary returns detached inventory and task state without file metadata.
func (m *IndexMeta) GetSegmentIndexSummary(segmentID, buildID int64) (*model.SegmentIndexSummary, bool) {
	if record, ok := m.GetIndexJob(buildID); ok {
		return model.SummarizeSegmentIndex(record), record.SegmentID == segmentID
	}
	return m.completedIndexSummary(segmentID, buildID)
}
