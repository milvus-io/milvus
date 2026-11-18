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
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *IndexMeta) UsesSegmentSummaries() bool { return m != nil && m.summarySource != nil }

// UseSegmentSummaries runs once during recovery, before metadata is published.
func (m *IndexMeta) UseSegmentSummaries(source metacache.MetaView) error {
	for _, record := range m.SegmentBuildInfo.List() {
		if record.IndexState == commonpb.IndexState_Finished {
			segment, exists := source.GetSegment(record.SegmentID)
			if !exists || segment.GetState() == commonpb.SegmentState_Dropped || !m.IsIndexExist(record.CollectionID, record.IndexID) {
				continue
			}
			return merr.Wrapf(merr.ErrDataIntegrity, "summary mode cannot recover completed task row %d", record.BuildID)
		}
	}
	m.summarySource = source
	for _, segment := range source.GetAllSegments() {
		for _, summary := range segment.GetIndexSummaries() {
			m.taskCounts.adjustSummary(model.SegmentIndexSummaryFromProto(segment, summary), 1)
		}
	}
	go m.taskCounts.publishStoredSizes()
	return nil
}

func (m *IndexMeta) completedIndexSummary(segmentID, buildID int64) (*model.SegmentIndexSummary, bool) {
	if !m.UsesSegmentSummaries() {
		return nil, false
	}
	segment, ok := m.summarySource.GetSegment(segmentID)
	if !ok {
		return nil, false
	}
	for _, summary := range segment.GetIndexSummaries() {
		if summary.GetBuildId() == buildID {
			return model.SegmentIndexSummaryFromProto(segment, summary), true
		}
	}
	return nil, false
}

// AccountCopiedIndexes runs after a fresh importing target becomes durable.
func (m *IndexMeta) AccountCopiedIndexes(segment *datapb.SegmentInfo) {
	for _, summary := range segment.GetIndexSummaries() {
		m.taskCounts.adjustSummary(model.SegmentIndexSummaryFromProto(segment, summary), 1)
	}
	m.taskCounts.publishStoredSizes()
}
