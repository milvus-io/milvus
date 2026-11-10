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

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadCompletedIndexes resolves only the immutable revision paired with segment.
// Its returned file lists belong to this call and are never installed in meta.
func (m *Meta) ReadCompletedIndexes(ctx context.Context, segment *datapb.SegmentInfo) (map[int64]*model.SegmentIndex, error) {
	records := make(map[int64]*model.SegmentIndex, len(segment.GetIndexSummaries()))
	needManifest := false
	for _, summary := range segment.GetIndexSummaries() {
		if summary.GetBuildId() <= 0 || summary.GetIndexId() <= 0 {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "invalid index summary for segment %d", segment.GetID())
		}
		if _, exists := records[summary.GetIndexId()]; exists {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "duplicate index summary for segment %d", segment.GetID())
		}
		records[summary.GetIndexId()] = SegmentIndexFromSummary(segment, summary)
		needManifest = needManifest || summary.GetHasArtifact()
	}
	if !needManifest {
		return records, nil
	}
	if segment.GetManifestPath() == "" || !segment.GetManifestHasIndex() {
		return nil, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has index artifacts without a manifest", segment.GetID())
	}
	io := packed.NewManifestIOContext(1)
	defer io.Close()
	entries, err := m.ReadManifestIndexesWithIO(ctx, io, segment.GetManifestPath(), CreateStorageConfig())
	if err != nil {
		return nil, merr.Wrapf(err, "read index artifacts for segment %d", segment.GetID())
	}
	resolved := make(map[int64]bool)
	for _, entry := range entries {
		record, ok := records[entry.IndexID]
		if !ok || !record.ArtifactPresent {
			continue
		}
		if resolved[entry.IndexID] || entry.BuildID != record.BuildID || entry.IndexVersion != record.IndexVersion {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "manifest index identity disagrees with summary for segment %d index %d", segment.GetID(), entry.IndexID)
		}
		if _, valid := ManifestIndexFilePathInfoForSegment(m.ChunkManager.RootPath(), segment, entry); !valid {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "invalid manifest index for segment %d index %d", segment.GetID(), entry.IndexID)
		}
		records[entry.IndexID] = segmentIndexFromManifest(NewSegmentInfo(segment), entry)
		resolved[entry.IndexID] = true
	}
	for id, record := range records {
		if record.ArtifactPresent && !resolved[id] {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "manifest missing summary build %d for segment %d", record.BuildID, segment.GetID())
		}
	}
	return records, nil
}
