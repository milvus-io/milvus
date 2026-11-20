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

package gc

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

func (gc *GarbageCollector) recycleSummaryIndexes(ctx context.Context, io *packed.ManifestIOContext, signal <-chan gcCmd) {
	records := gc.Option.IndexMeta.GetAllSegIndexes()
	tasksBySegment := make(map[int64][]*model.SegmentIndexSummary)
	for _, record := range records {
		if record != nil {
			tasksBySegment[record.SegmentID] = append(tasksBySegment[record.SegmentID], model.SummarizeSegmentIndex(record))
		}
	}
	for _, segment := range gc.Meta.SelectSegments(ctx) {
		if ctx.Err() != nil {
			return
		}
		candidates := tasksBySegment[segment.GetID()]
		for _, summary := range segment.GetIndexSummaries() {
			if _, exists := records[summary.GetBuildId()]; !exists {
				candidates = append(candidates, model.SegmentIndexSummaryFromProto(segment.SegmentInfo, summary))
			}
		}
		gc.recycleUnusedSegIndexesForSegment(ctx, io, segment.GetID(), candidates, signal)
		delete(tasksBySegment, segment.GetID())
	}
	// Task rows may outlive their segment row.
	for segmentID, candidates := range tasksBySegment {
		if ctx.Err() != nil {
			return
		}
		if gc.Meta.GetSegment(ctx, segmentID) == nil {
			gc.recycleUnusedSegIndexesForSegment(ctx, io, segmentID, candidates, signal)
		}
	}
}

func (gc *GarbageCollector) removeIndexRecordLocked(ctx context.Context, record *model.SegmentIndexSummary) error {
	segment := gc.Meta.GetSegment(ctx, record.SegmentID)
	if segment != nil {
		for _, summary := range segment.GetIndexSummaries() {
			if summary.GetBuildId() == record.BuildID {
				return gc.Meta.RetireIndexSummaryLocked(ctx, record.SegmentID, record.BuildID)
			}
		}
	}
	return gc.Option.IndexMeta.RemoveSegmentIndex(ctx, record.BuildID)
}

func (gc *GarbageCollector) artifactSummaryBuildIDs(ctx context.Context) map[int64]struct{} {
	if !gc.Option.IndexMeta.UsesSegmentSummaries() {
		return nil
	}
	buildIDs := make(map[int64]struct{})
	gc.Meta.Segments.RangeReused(func(segment *meta.SegmentInfo) bool {
		if ctx.Err() != nil {
			return false
		}
		for _, summary := range segment.GetIndexSummaries() {
			if summary.GetHasArtifact() {
				buildIDs[summary.GetBuildId()] = struct{}{}
			}
		}
		return true
	})
	return buildIDs
}

// referencedSummaryBuild fails closed on every manifest read/validation error.
func (gc *GarbageCollector) referencedSummaryBuild(ctx context.Context, segment *meta.SegmentInfo, buildID int64) (map[string]struct{}, bool) {
	if segment == nil {
		return nil, true
	}
	if !meta.IsSegmentHealthy(segment) {
		return nil, false
	}
	if !segment.GetManifestHasIndex() {
		return nil, true
	}
	entries, err := gc.Meta.ReadManifestIndexesWithIO(ctx, nil, segment.GetManifestPath(), meta.CreateStorageConfig())
	if err != nil {
		return nil, false
	}
	var files map[string]struct{}
	for _, entry := range entries {
		if entry.BuildID != buildID {
			continue
		}
		info, ok := meta.ManifestIndexFilePathInfoForSegment(gc.Option.Cli.RootPath(), segment.SegmentInfo, entry)
		if !ok {
			return nil, false
		}
		files = gc.getRetainedIndexFilesOfIndex(&model.SegmentIndex{
			SegmentID: segment.GetID(), CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(),
			BuildID: buildID, IndexVersion: entry.IndexVersion, IndexStorePathVersion: entry.IndexStorePathVersion,
			IndexFileKeys: entry.IndexFileKeys,
		})
		for _, file := range info.GetIndexFilePaths() {
			files[file] = struct{}{}
		}
	}
	if files != nil {
		return files, false
	}
	return nil, true
}
