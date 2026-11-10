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
    "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
    "github.com/milvus-io/milvus/internal/datacoord/meta"
    "github.com/milvus-io/milvus/internal/metacache"
    "github.com/milvus-io/milvus/internal/metastore/model"
    "github.com/milvus-io/milvus/pkg/v3/util/merr"
    "github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (m *IndexMeta) UsesSegmentSummaries() bool { return m.summarySource != nil }

// UseSegmentSummaries runs once during recovery, before metadata is published.
func (m *IndexMeta) UseSegmentSummaries(source metacache.MetaView) error {
    for _, record := range m.SegmentBuildInfo.List() {
        if record.IndexState == commonpb.IndexState_Finished {
            return merr.Wrapf(merr.ErrDataIntegrity, "summary mode cannot recover completed task row %d", record.BuildID)
        }
    }
    m.summarySource = source
    for _, segment := range source.GetAllSegments() {
        for _, summary := range segment.GetIndexSummaries() {
            record := meta.SegmentIndexFromSummary(segment, summary)
            m.taskCounts.replaceTask(nil, record)
        }
    }
    return nil
}

// segmentIndexRecords is a request-scoped projection, never a resident cache.
func (m *IndexMeta) segmentIndexRecords(segmentID int64) (*typeutil.ConcurrentMap[int64, *model.SegmentIndex], bool) {
    active, found := m.SegmentIndexes.Get(segmentID)
    if !m.UsesSegmentSummaries() { return active, found }
    records := typeutil.NewConcurrentMap[int64, *model.SegmentIndex]()
    if segment, ok := m.summarySource.GetSegment(segmentID); ok {
        for _, summary := range segment.GetIndexSummaries() {
            records.Insert(summary.GetIndexId(), meta.SegmentIndexFromSummary(segment, summary))
        }
    }
    if found {
        active.Range(func(id int64, record *model.SegmentIndex) bool { records.Insert(id, record); return true })
    }
    return records, records.Len() != 0
}

func (m *IndexMeta) summaryRecord(segmentID, buildID int64) (*model.SegmentIndex, bool) {
    if !m.UsesSegmentSummaries() { return nil, false }
    segment, ok := m.summarySource.GetSegment(segmentID)
    if !ok { return nil, false }
    for _, summary := range segment.GetIndexSummaries() {
        if summary.GetBuildId() == buildID { return meta.SegmentIndexFromSummary(segment, summary), true }
    }
    return nil, false
}

func (m *IndexMeta) GetSegmentIndexBuild(segmentID, buildID int64) (*model.SegmentIndex, bool) {
    if record, ok := m.GetIndexJob(buildID); ok { return record, record.SegmentID == segmentID }
    return m.summaryRecord(segmentID, buildID)
}

