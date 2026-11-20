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
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func (m *IndexMeta) UsesSegmentSummaries() bool { return m != nil && m.summarySource != nil }

// ConfigureSegmentSummaries attaches the catalog-backed inventory before serving.
func (m *IndexMeta) ConfigureSegmentSummaries(source metacache.MetaView, publish bool) {
	m.publishSummaries = publish
	if m.summarySource != nil {
		return
	}
	m.summarySource = source
	for _, segment := range source.GetAllSegments() {
		m.accountIndexSummaries(segment, segment.GetIndexSummaries())
	}
	go m.taskCounts.publishStoredSizes()
}

func (m *IndexMeta) UseSegmentSummaries(source metacache.MetaView) error {
	m.ConfigureSegmentSummaries(source, true)
	return nil
}

func (m *IndexMeta) PublishesSegmentSummaries(segmentID int64) bool {
	if !m.UsesSegmentSummaries() {
		return false
	}
	if m.publishSummaries {
		return true
	}
	// An in-flight manifest task must maintain inventory already published
	// for this segment even if new builds have switched back to etcd.
	segment, exists := m.summarySource.GetSegment(segmentID)
	return exists && (segment.GetIndexSummaryVersion() != 0 || len(segment.GetIndexSummaries()) != 0)
}

func (m *IndexMeta) SummaryRetirementVersion() uint64 {
	return m.summaryRetirementVersion.Load()
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

func (m *IndexMeta) completedIndexSummaryForIndex(segmentID, indexID int64) (*model.SegmentIndexSummary, bool) {
	if !m.UsesSegmentSummaries() {
		return nil, false
	}
	segment, ok := m.summarySource.GetSegment(segmentID)
	if !ok {
		return nil, false
	}
	for _, summary := range segment.GetIndexSummaries() {
		if summary.GetIndexId() == indexID {
			return model.SegmentIndexSummaryFromProto(segment, summary), true
		}
	}
	return nil, false
}

// AccountIndexSummaries tracks newly published summaries after catalog commit.
func (m *IndexMeta) AccountIndexSummaries(segment *datapb.SegmentInfo, summaries []*datapb.SegmentIndexSummary) {
	m.accountIndexSummaries(segment, summaries)
	m.taskCounts.publishStoredSizes()
}

func (m *IndexMeta) accountIndexSummaries(segment *datapb.SegmentInfo, summaries []*datapb.SegmentIndexSummary) {
	for _, summary := range summaries {
		m.taskCounts.adjustSummary(model.SegmentIndexSummaryFromProto(segment, summary), 1)
	}
}
