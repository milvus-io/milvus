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
)

// bump_defence DataView: the write-side truth the coordinator's readiness witness reads
// (layer 1 of the watermark check + segment-list pruning). These methods structurally
// satisfy coordinator.DataViewReader; mixCoord wires the Server in directly.

// isBackfillEligibleSegment reports whether a segment holds user data that a schema-change
// backfill must (eventually) cover. It is the shared filter of the schema-version
// consistency gate (GetCollectionStatistics) and the bump_defence DataView.
//
// Growing segments are excluded as a workaround until companion PR #48865 lands:
// streaming-created growing segments currently carry SchemaVersion=0 because the
// propagation chain in segment_alloc_worker.go and msg_handler_impl.go does not yet pass
// SchemaVersion through, so including them would permanently block under any write
// traffic. This is safe: growing segments are sealed/flushed by the schema change, at
// which point the backfill policy picks them up and updates their SchemaVersion.
// L0 segments only contain delete logs -- no user data to backfill.
//
// TODO: remove the Growing exclusion once #48865 lands.
func isBackfillEligibleSegment(si *SegmentInfo) bool {
	return isSegmentHealthy(si) &&
		!si.GetIsImporting() &&
		!si.GetIsInvisible() &&
		si.GetLevel() != datapb.SegmentLevel_L0 &&
		si.GetState() != commonpb.SegmentState_Growing
}

// StaleBackfillSegments returns the IDs of the collection's backfill-eligible segments
// whose schema_version is strictly below the watermark, i.e. the write side has not
// finished backfilling them yet. Empty means the DataView is clear for that watermark.
func (s *Server) StaleBackfillSegments(ctx context.Context, collectionID int64, watermark int32) []int64 {
	segments := s.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(si *SegmentInfo) bool {
		return isBackfillEligibleSegment(si) && si.GetSchemaVersion() < watermark
	}))
	out := make([]int64, 0, len(segments))
	for _, seg := range segments {
		out = append(out, seg.GetID())
	}
	return out
}

// BackfillSegmentDropped reports whether the segment is no longer a healthy datacoord
// segment (dropped by compaction/GC), so a segment-list round must stop waiting for it
// to ever report from a querynode.
func (s *Server) BackfillSegmentDropped(ctx context.Context, collectionID, segmentID int64) bool {
	return s.meta.GetHealthySegment(ctx, segmentID) == nil
}
