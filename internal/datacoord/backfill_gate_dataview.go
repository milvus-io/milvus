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
)

// bump_defence DataView: the write-side truth the coordinator's readiness witness reads
// (the watermark round's write-side check). Structurally satisfies
// coordinator.DataViewReader; mixCoord wires the Server in directly.

// StaleBackfillSegments returns the IDs of the collection's sealed data segments whose
// schema_version is strictly below the watermark, i.e. the write side has not finished
// backfilling them yet. Empty means the DataView is clear for that watermark.
//
// Growing segments are outside the sealed population (isSealedDataSegment) and thus not
// counted; besides being definitionally un-sealed, streaming-created growing segments
// currently carry SchemaVersion=0 (#48865), so counting them would block forever under
// write traffic. This is safe: the schema change seals them, and the backfill picks the
// flushed result up and advances its SchemaVersion.
func (s *Server) StaleBackfillSegments(ctx context.Context, collectionID int64, watermark int32) []int64 {
	segments := s.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(si *SegmentInfo) bool {
		return isSealedDataSegment(si) && si.GetSchemaVersion() < watermark
	}))
	out := make([]int64, 0, len(segments))
	for _, seg := range segments {
		out = append(out, seg.GetID())
	}
	return out
}

// ChannelCheckpointsAtLeast reports whether EVERY vchannel checkpoint of the collection
// has reached ts. Checkpoints only advance after all prior DML is durably persisted, so a
// checkpoint past the schema-change DDL timetick proves the DDL's fence-and-flush of the
// then-growing segments has fully landed -- no pre-V data still hides in growing state.
// An unknown collection or a missing checkpoint reads as not-reached (fail-closed).
func (s *Server) ChannelCheckpointsAtLeast(ctx context.Context, collectionID int64, ts uint64) bool {
	coll := s.meta.GetCollection(collectionID)
	if coll == nil || len(coll.VChannelNames) == 0 {
		return false
	}
	for _, vchannel := range coll.VChannelNames {
		cp := s.meta.GetChannelCheckpoint(vchannel)
		if cp == nil || cp.GetTimestamp() < ts {
			return false
		}
	}
	return true
}
