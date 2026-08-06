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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// addStatsDelta folds an increment into a segment's existing Statistics and
// returns a fresh object; neither input is mutated, and the result never
// aliases base or delta — including when delta is nil, in which case an
// independent copy of base is returned rather than base itself — so a
// caller mutating the result cannot corrupt the segment's previous state.
//
// Only the additive fields participate: insert_binlog_size,
// insert_binlog_count, stats_binlog_size, and a union over null_counts.
// Everything else — delta_*, timestamp_from/to, delta_timestamp_from/to,
// timestamp_quantiles — is carried over from base untouched, so a malformed
// increment cannot overwrite aggregates it has no business changing.
//
// Producers of an increment are in-place compaction outputs, which only ever
// add columns, so every field is expected to be non-negative. Each result is
// clamped at zero anyway: nothing emits a negative increment today, and the
// clamp keeps a future producer that does from silently corrupting Stats.
func addStatsDelta(ctx context.Context, segmentID int64, base, delta *datapb.Statistics) *datapb.Statistics {
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
