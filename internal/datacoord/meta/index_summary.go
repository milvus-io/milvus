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

package meta

import (
	"cmp"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const CurrentIndexSummaryVersion uint32 = 1

func HasIndexSummaryInventory(segment *datapb.SegmentInfo) bool {
	return segment.GetIndexSummaryVersion() != 0 || len(segment.GetIndexSummaries()) != 0
}

// MarkIndexSummaryCoverage carries a verified inventory into the next revision.
func MarkIndexSummaryCoverage(previous, updated *datapb.SegmentInfo) {
	if previous.GetIndexSummaryVersion() == CurrentIndexSummaryVersion || !previous.GetManifestHasIndex() {
		updated.IndexSummaryVersion = CurrentIndexSummaryVersion
	}
}

// ApplyIndexSummary updates a cloned segment before its catalog publication.
// A stale removal must not erase the replacement build's inventory entry.
func ApplyIndexSummary(segment *datapb.SegmentInfo, record *model.SegmentIndexSummary, fieldID int64, present bool) {
	segment.IndexSummaries = slices.DeleteFunc(segment.IndexSummaries, func(s *datapb.SegmentIndexSummary) bool {
		return s.GetIndexId() == record.IndexID && (present || s.GetBuildId() == record.BuildID)
	})
	if !present {
		return
	}
	segment.IndexSummaries = append(segment.IndexSummaries, model.MarshalSegmentIndexSummary(record, fieldID))
	sortIndexSummaries(segment.IndexSummaries)
}

func sortIndexSummaries(summaries []*datapb.SegmentIndexSummary) {
	slices.SortFunc(summaries, func(a, b *datapb.SegmentIndexSummary) int {
		return cmp.Compare(a.GetIndexId(), b.GetIndexId())
	})
}

func manifestSummaryFieldID(commit SegmentManifestCommit, buildID int64) int64 {
	if commit.Mutation.Updates != nil {
		for _, entry := range commit.Mutation.Updates.Indexes {
			if entry.BuildID == buildID {
				return entry.FieldID
			}
		}
	}
	return 0
}

func UpdateIndexSummaries(segmentID int64, summaries []*datapb.SegmentIndexSummary) UpdateOperator {
	return func(pack *UpdateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil {
			return pack.Fail(merr.WrapErrSegmentNotFound(segmentID))
		}
		segment.IndexSummaries = make([]*datapb.SegmentIndexSummary, len(summaries))
		for i, summary := range summaries {
			segment.IndexSummaries[i] = proto.Clone(summary).(*datapb.SegmentIndexSummary)
		}
		sortIndexSummaries(segment.IndexSummaries)
		segment.IndexSummaryVersion = CurrentIndexSummaryVersion
		return true
	}
}
