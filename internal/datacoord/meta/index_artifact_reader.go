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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ResolveSegmentIndexes combines live task rows with the completed artifacts in
// the selected segment revision. The caller transfers ownership of the task map.
func (m *Meta) ResolveSegmentIndexes(ctx context.Context, segment *datapb.SegmentInfo, records map[int64]*model.SegmentIndex, live func(int64) bool) (map[int64]*model.SegmentIndex, error) {
	records, artifacts, err := MergeSegmentIndexSummaries(segment, records, live)
	if err != nil || len(artifacts) == 0 {
		return records, err
	}
	entries, err := m.ReadManifestIndexesWithIO(ctx, nil, segment.GetManifestPath(), CreateStorageConfig())
	if err != nil {
		return nil, merr.Wrapf(err, "read index artifacts for segment %d", segment.GetID())
	}
	return resolveSegmentIndexEntries(m.ChunkManager.RootPath(), segment, records, artifacts, entries)
}

// MergeSegmentIndexSummaries projects the selected segment inventory without I/O.
// A finished catalog record for the same or a newer build supplies its files.
func MergeSegmentIndexSummaries(segment *datapb.SegmentInfo, records map[int64]*model.SegmentIndex, live func(int64) bool) (map[int64]*model.SegmentIndex, map[int64]int64, error) {
	if records == nil {
		records = make(map[int64]*model.SegmentIndex, len(segment.GetIndexSummaries()))
	}
	artifacts := make(map[int64]int64)
	seen := make(map[int64]struct{}, len(segment.GetIndexSummaries()))
	for _, summary := range segment.GetIndexSummaries() {
		if summary.GetBuildId() <= 0 || summary.GetIndexId() <= 0 {
			return nil, nil, merr.Wrapf(merr.ErrDataIntegrity, "invalid index summary for segment %d", segment.GetID())
		}
		id := summary.GetIndexId()
		if live != nil && !live(id) {
			continue
		}
		if _, exists := seen[id]; exists {
			return nil, nil, merr.Wrapf(merr.ErrDataIntegrity, "duplicate index summary for segment %d", segment.GetID())
		}
		seen[id] = struct{}{}
		// A finished record can replace an older manifest artifact or carry
		// the source files of a build whose migration just committed.
		if task := records[id]; task != nil && task.BuildID >= summary.GetBuildId() && task.IndexState == commonpb.IndexState_Finished {
			continue
		}
		records[id] = SegmentIndexFromSummary(segment, summary)
		if summary.GetHasArtifact() {
			artifacts[id] = summary.GetFieldId()
		}
	}
	if len(artifacts) != 0 && (segment.GetManifestPath() == "" || !segment.GetManifestHasIndex()) {
		return nil, nil, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has index artifacts without a manifest", segment.GetID())
	}
	return records, artifacts, nil
}

func resolveSegmentIndexEntries(root string, segment *datapb.SegmentInfo, records map[int64]*model.SegmentIndex, artifacts map[int64]int64, entries []packed.ManifestIndexInfo) (map[int64]*model.SegmentIndex, error) {
	resolved := make(map[int64]bool)
	for _, entry := range entries {
		fieldID, needsArtifact := artifacts[entry.IndexID]
		if !needsArtifact {
			continue
		}
		record := records[entry.IndexID]
		if resolved[entry.IndexID] || entry.BuildID != record.BuildID || entry.IndexVersion != record.IndexVersion || entry.FieldID != fieldID {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "manifest index identity disagrees with summary for segment %d index %d", segment.GetID(), entry.IndexID)
		}
		if _, valid := ManifestIndexFilePathInfoForSegment(root, segment, entry); !valid {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "invalid manifest index for segment %d index %d", segment.GetID(), entry.IndexID)
		}
		loaded := segmentIndexFromManifest(&SegmentInfo{SegmentInfo: segment}, entry)
		loaded.ArtifactPresent = true
		records[entry.IndexID] = loaded
		resolved[entry.IndexID] = true
	}
	for id := range artifacts {
		if !resolved[id] {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "manifest missing summary build %d for segment %d", records[id].BuildID, segment.GetID())
		}
	}
	return records, nil
}

// SegmentIndexFromSummary carries coordinator decisions without any file keys.
func SegmentIndexFromSummary(segment *datapb.SegmentInfo, summary *datapb.SegmentIndexSummary) *model.SegmentIndex {
	inventory := model.SegmentIndexSummaryFromProto(segment, summary)
	return &model.SegmentIndex{
		SegmentID: segment.GetID(), CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(),
		NumRows: inventory.NumRows, IndexID: summary.GetIndexId(), BuildID: summary.GetBuildId(),
		IndexVersion: summary.GetIndexVersion(), IndexState: commonpb.IndexState_Finished,
		IndexSerializedSize: summary.GetSerializedSize(), IndexMemSize: summary.GetMemSize(),
		CurrentIndexVersion: summary.GetVectorEngineVersion(), CurrentScalarIndexVersion: summary.GetScalarEngineVersion(),
		IndexType: summary.GetIndexType(), ManifestPublished: true, ArtifactPresent: summary.GetHasArtifact(),
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
}
