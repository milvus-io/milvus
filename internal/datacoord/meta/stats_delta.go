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

package meta

import (
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// addStatsDelta applies an in-place rewrite. File aggregates are additive, but
// the known size of a rewritten column is an absolute replacement. Unmentioned
// IDs remain accurate because this operation preserves the segment's row set.
func addStatsDelta(ctx context.Context, segment *SegmentInfo, result *datapb.CompactionSegment, schema *schemapb.CollectionSchema) *datapb.Statistics {
	out := addFileStatsDelta(ctx, segment.GetID(), segment.GetStats(), result.GetStats())
	if out == nil {
		return nil
	}
	if typeutil.IsExternalCollection(schema) {
		out.FieldLogicalSizes = nil
		return out
	}
	rows := segment.GetNumOfRows()
	if SegmentFieldLogicalSizes(segment.SegmentInfo) == nil {
		out.FieldLogicalSizes = nil
	}
	ids := result.GetFieldLogicalSizeUpdateIds()
	if len(ids) == 0 {
		// A legacy writer did not count its output. Its manifest delta or
		// the ChildFields of its freshly written groups can still identify
		// exactly which entries to invalidate. Group IDs are not field IDs.
		ids = legacyMaterializedFieldIDs(result)
		if len(ids) == 0 {
			out.FieldLogicalSizes = nil // opaque prebuilt materialization
		}
		for _, id := range ids {
			delete(out.GetFieldLogicalSizes().GetFields(), id)
		}
		for _, parent := range schema.GetStructArrayFields() {
			for _, child := range parent.GetFields() {
				if slices.Contains(ids, child.GetFieldID()) {
					delete(out.GetFieldLogicalSizes().GetFields(), parent.GetFieldID())
					break
				}
			}
		}
		return out
	}
	report := result.GetStats().GetFieldLogicalSizes()
	measured := result.GetNumOfRows() == rows && storage.FieldLogicalSizesCover(report, rows)
	for _, id := range ids {
		size := report.GetFields()[id]
		if !measured || size == nil {
			delete(out.GetFieldLogicalSizes().GetFields(), id)
			continue
		}
		if out.FieldLogicalSizes == nil {
			out.FieldLogicalSizes = &datapb.FieldLogicalSizes{Version: storage.FieldLogicalSizeVersion, RowCount: rows}
		}
		if out.FieldLogicalSizes.Fields == nil {
			out.FieldLogicalSizes.Fields = make(map[int64]*datapb.FieldLogicalSize)
		}
		out.FieldLogicalSizes.Fields[id] = proto.Clone(size).(*datapb.FieldLogicalSize)
	}
	return out
}

func legacyMaterializedFieldIDs(result *datapb.CompactionSegment) []int64 {
	var ids []int64
	if groups := result.GetManifestDelta().GetColumnGroups(); len(groups) > 0 {
		for _, group := range groups {
			columns := group.GetColumns()
			fields := droppedColumnFieldIDs(columns)
			if len(fields) == 0 || len(fields) != len(columns) {
				return nil
			}
			ids = append(ids, fields...)
		}
		return ids
	}
	for _, group := range result.GetInsertLogs() {
		if len(group.GetChildFields()) == 0 {
			return nil
		}
		ids = append(ids, group.GetChildFields()...)
	}
	return ids
}

// addFileStatsDelta folds an increment into a segment's existing Statistics and
// returns a fresh object; neither input is mutated, and the result never
// aliases base or delta — including when delta is nil, in which case an
// independent copy of base is returned rather than base itself — so a
// caller mutating the result cannot corrupt the segment's previous state.
//
// Only the additive fields participate: insert_binlog_size,
// insert_binlog_count, stats_binlog_size, and a union over null_counts and
// valid_element_counts.
// Everything else — delta_*, timestamp_from/to, delta_timestamp_from/to,
// timestamp_quantiles — is carried over from base untouched, so a malformed
// increment cannot overwrite aggregates it has no business changing.
//
// Producers of an increment are in-place compaction outputs, which only ever
// add columns, so every field is expected to be non-negative. Each result is
// clamped at zero anyway: nothing emits a negative increment today, and the
// clamp keeps a future producer that does from silently corrupting Stats.
func addFileStatsDelta(ctx context.Context, segmentID int64, base, delta *datapb.Statistics) *datapb.Statistics {
	if delta == nil {
		// Still hand back an independent copy: the caller may freely mutate
		// the result, and that must never reach back into the segment's
		// current Statistics.
		return copyStatistics(base)
	}

	out := copyStatistics(base)
	if out == nil {
		mlog.Warn(ctx, "applying stats delta onto a segment with no existing Statistics",
			mlog.FieldSegmentID(segmentID))
		out = &datapb.Statistics{}
	}

	out.InsertBinlogSize = clampStatsField(ctx, segmentID, "insert_binlog_size",
		out.InsertBinlogSize+delta.GetInsertBinlogSize())
	out.InsertBinlogCount = clampStatsField(ctx, segmentID, "insert_binlog_count",
		out.InsertBinlogCount+delta.GetInsertBinlogCount())
	out.StatsBinlogSize = clampStatsField(ctx, segmentID, "stats_binlog_size",
		out.StatsBinlogSize+delta.GetStatsBinlogSize())

	// valid_element_counts: an increment's columns are new to the segment, so
	// its count is the whole count. A field that already has data but no
	// count stays unknown; adding the increment would publish a partial sum.
	if vc := delta.GetValidElementCounts(); len(vc) > 0 {
		for f, n := range vc {
			_, counted := out.GetValidElementCounts()[f]
			_, hasData := base.GetNullCounts()[f]
			if !counted && hasData {
				continue
			}
			if out.ValidElementCounts == nil {
				out.ValidElementCounts = make(map[int64]int64, len(vc))
			}
			out.ValidElementCounts[f] = clampStatsField(ctx, segmentID, "valid_element_counts", out.ValidElementCounts[f]+n)
		}
	}

	if nc := delta.GetNullCounts(); len(nc) > 0 {
		if out.NullCounts == nil {
			out.NullCounts = make(map[int64]int64, len(nc))
		}
		for f, n := range nc {
			out.NullCounts[f] = clampStatsField(ctx, segmentID, "null_counts", out.NullCounts[f]+n)
		}
	}

	return out
}

// copyStatistics returns an independent copy of base — nil in, nil out —
// so neither the nil-delta pass-through nor the additive path in
// addStatsDelta ever hands the caller a pointer that aliases base.
//
// Uses proto.Clone rather than a field-by-field copy: the result is
// persisted, so a hand-rolled copy would silently drop any field added to
// the Statistics message in the future (no compile error, no test failure)
// and would drop unknown fields when talking to a mixed-version fleet.
// proto.Clone deep-copies every field, known and unknown, so it stays
// correct as the message evolves.
func copyStatistics(base *datapb.Statistics) *datapb.Statistics {
	if base == nil {
		return nil
	}
	return proto.Clone(base).(*datapb.Statistics)
}

func clampStatsField(ctx context.Context, segmentID int64, field string, v int64) int64 {
	if v < 0 {
		mlog.Warn(ctx, "stats delta drove aggregate below zero, clamping",
			mlog.FieldSegmentID(segmentID),
			mlog.String("field", field),
			mlog.Int64("value", v))
		return 0
	}
	return v
}
