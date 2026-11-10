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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
"github.com/milvus-io/milvus/internal/metastore/model"
"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// ApplyIndexSummary updates a cloned segment before its catalog publication.
// A stale removal must not erase the replacement build's inventory entry.
func ApplyIndexSummary(segment *datapb.SegmentInfo, record *model.SegmentIndex, fieldID int64, present bool) {
	segment.IndexSummaries = slices.DeleteFunc(segment.IndexSummaries, func(s *datapb.SegmentIndexSummary) bool {
		return s.GetIndexId() == record.IndexID && (present || s.GetBuildId() == record.BuildID)
	})
	if !present {
		return
	}
	segment.IndexSummaries = append(segment.IndexSummaries, &datapb.SegmentIndexSummary{
		IndexId: record.IndexID, FieldId: fieldID, BuildId: record.BuildID,
		HasArtifact: len(record.IndexFileKeys) > 0, IndexVersion: record.IndexVersion,
		SerializedSize: record.IndexSerializedSize, MemSize: record.IndexMemSize,
		VectorEngineVersion: record.CurrentIndexVersion, ScalarEngineVersion: record.CurrentScalarIndexVersion,
		IndexType: record.IndexType,
	})
	slices.SortFunc(segment.IndexSummaries, func(a, b *datapb.SegmentIndexSummary) int {
		if a.GetIndexId() < b.GetIndexId() {
			return -1
		}
		if a.GetIndexId() > b.GetIndexId() {
			return 1
		}
		return 0
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

// SegmentIndexFromSummary carries coordinator decisions without any file keys.
func SegmentIndexFromSummary(segment *datapb.SegmentInfo, summary *datapb.SegmentIndexSummary) *model.SegmentIndex {
    return &model.SegmentIndex{
        SegmentID: segment.GetID(), CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(),
        NumRows: segment.GetNumOfRows(), IndexID: summary.GetIndexId(), BuildID: summary.GetBuildId(),
        IndexVersion: summary.GetIndexVersion(), IndexState: commonpb.IndexState_Finished,
        IndexSerializedSize: summary.GetSerializedSize(), IndexMemSize: summary.GetMemSize(),
        CurrentIndexVersion: summary.GetVectorEngineVersion(), CurrentScalarIndexVersion: summary.GetScalarEngineVersion(),
        IndexType: summary.GetIndexType(), ManifestPublished: true, ArtifactPresent: summary.GetHasArtifact(),
        IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
    }
}
