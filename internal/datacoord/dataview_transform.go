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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// segmentTransformStart resolves legacy ordinary and committed-import metadata
// conservatively. A compacted base needs its own cursor: its first surviving
// row is not evidence of the Transform prefix covered by its parent revisions.
func segmentTransformStart(segment *SegmentInfo) uint64 {
	if start := segment.GetTransformStartAfterTimetick(); start != 0 {
		return start
	}
	if commit := segment.GetCommitTimestamp(); commit != 0 {
		return commit
	}
	if segment.GetIsImporting() || segment.GetCreatedByCompaction() || len(segment.GetCompactionFrom()) != 0 {
		return 0
	}
	return segment.GetStartPosition().GetTimestamp()
}

func minSegmentTransformStart(segments []*SegmentInfo) uint64 {
	var start uint64
	for i, segment := range segments {
		cursor := segmentTransformStart(segment)
		if i == 0 || cursor < start {
			start = cursor
		}
	}
	return start
}

// transformFrontierBounds captures K before any metadata projection. Including
// every healthy Segment (even already-published ones) is conservative and closes
// the window between durable output creation and DataView publication. No
// partition filter may be applied here.
func transformFrontierBounds(ctx context.Context, metadata *meta, imports ImportMeta, collectionID int64) (map[string]uint64, error) {
	collection := metadata.GetCollection(collectionID)
	if collection == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d metadata is not ready", collectionID)
	}
	bounds := make(map[string]uint64, len(collection.VChannelNames))
	for _, channel := range collection.VChannelNames {
		checkpoint := metadata.GetChannelCheckpoint(channel)
		if checkpoint == nil || checkpoint.GetTimestamp() == 0 || funcutil.IsDroppedChannelCheckpoint(checkpoint) {
			return nil, merr.WrapErrServiceNotReadyMsg("Transform checkpoint for %s is not ready", channel)
		}
		bounds[channel] = checkpoint.GetTimestamp()
	}
	// Read task pins before Segment metadata. A completed task has already
	// persisted all Segment cursors, so its removal cannot create a gap.
	if imports != nil {
		for _, job := range imports.GetJobBy(ctx, WithCollectionID(collectionID), WithoutJobStates(internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed)) {
			stored := job.(*importJob)
			for _, channel := range job.GetVchannels() {
				start := stored.GetTransformCommitTimeticks()[channel]
				if start == 0 {
					return nil, merr.WrapErrServiceNotReadyMsg("import job %d Transform fence for %s is not ready", job.GetJobID(), channel)
				}
				bounds[channel] = min(bounds[channel], start)
			}
		}
	}
	for _, segment := range metadata.SelectSegments(ctx, WithCollection(collectionID)) {
		if segment.GetLevel() == datapb.SegmentLevel_L0 || !isSegmentHealthy(segment) {
			continue
		}
		start := segmentTransformStart(segment)
		if start == 0 {
			// Before first-pack registration, K cannot pass the first Insert.
			// A V3 version-zero manifest is only an allocation placeholder;
			// row-count reports and sealing do not complete registration either.
			if !segment.GetIsImporting() &&
				(segment.GetState() == commonpb.SegmentState_Growing || segment.GetState() == commonpb.SegmentState_Sealed) &&
				segment.GetStartPosition() == nil && len(segment.GetBinlogs()) == 0 {
				committed, err := hasCommittedManifest(segment)
				if err != nil {
					return nil, err
				}
				if !committed {
					continue
				}
			}
			return nil, merr.WrapErrServiceNotReadyMsg("Segment %d Transform coverage is not ready", segment.GetID())
		}
		channel := segment.GetInsertChannel()
		bounds[channel] = min(bounds[channel], start)
	}
	return bounds, nil
}
