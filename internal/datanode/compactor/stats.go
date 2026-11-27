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

package compactor

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// buildCompactionOutputStats produces the Statistics that ships on a
// CompactionSegment. The receiver (DataCoord) copies this verbatim onto
// SegmentInfo.Stats — it does NOT recompute — so this is the authoritative
// place to populate aggregate metrics for compaction outputs.
//
// The closed insert writer supplies both the stats-blob footprint (V3 embeds
// those files in the manifest, not in FieldBinlogs) and, on V3, the logical
// sizes of the actual rows written to this output. L0 has no insert writer.
func buildCompactionOutputStats(insertLogs, deltalogs []*datapb.FieldBinlog, writer storage.BinlogRecordWriter, rows int64) *datapb.Statistics {
	s := storage.BuildStatsFromFieldBinlogs(insertLogs, nil, nil, deltalogs)
	if writer != nil {
		s.StatsBinlogSize = writer.GetStatsBlobSize()
		s.FieldLogicalSizes = writerFieldLogicalSizes(writer, rows)
	}
	return s
}

// Partial column-group writers and full manifest writers expose the same
// optional snapshot. Never publish a field total covering only part of a segment.
func writerFieldLogicalSizes(writer any, rows int64) *datapb.FieldLogicalSizes {
	if provider, ok := writer.(interface {
		GetFieldLogicalSizes() *datapb.FieldLogicalSizes
	}); ok && rows > 0 {
		if sizes := provider.GetFieldLogicalSizes(); storage.FieldLogicalSizesCover(sizes, rows) {
			return sizes
		}
	}
	return nil
}

// Every physically written leaf is touched, even when its byte count is
// unknown. Writing a struct child also materializes its parent's shared row
// validity, but does not touch the other children used to read that shape.
func materializationLogicalFieldIDs(schema *schemapb.CollectionSchema, outputIDs []int64) []int64 {
	if len(schema.GetStructArrayFields()) == 0 {
		return outputIDs
	}
	selected := typeutil.NewSet(outputIDs...)
	for _, parent := range schema.GetStructArrayFields() {
		for _, child := range parent.GetFields() {
			if selected.Contain(child.GetFieldID()) {
				outputIDs = append(outputIDs, parent.GetFieldID())
				break
			}
		}
	}
	return outputIDs
}

// buildMaterializationStatsDelta produces the Statistics INCREMENT that ships
// on an in-place schema-bump materialization result. DataCoord adds it onto
// the segment's existing SegmentInfo.Stats rather than replacing it — see the
// contract on CompactionSegment.stats in data_coord.proto.
//
// Materialization appends function-output columns to an existing segment: it
// writes no rows, no deletes and no new timestamps, so only the additive
// fields are populated. Everything else is deliberately left zero, which is
// what tells the receiver to leave those aggregates alone. Logical field sizes
// are the exception: each measured output column covers the whole segment and
// replaces the old entry for its ID, rather than adding to it.
//
// memorySizes and nullCounts are keyed by output field ID, which equals
// ColumnGroup.GroupID for the one-field-per-group layout setupWriter builds.
// A group with no recorded memory size still counts as one binlog and still
// gets a null_counts entry: the field is physically present in the segment,
// and the presence contract in storage.BuildStatsFromFieldBinlogs requires an
// entry for every such field, zero included.
//
// validElementCounts carries the valid element counts of the VectorArray
// output fields; an output without an entry (not a VectorArray, or not
// countable) gets none.
func buildMaterializationStatsDelta(
	columnGroups []storagecommon.ColumnGroup,
	memorySizes map[int64]int,
	nullCounts map[int64]int64,
	validElementCounts map[int64]int64,
	statsBlobSize int64,
	sizes *datapb.FieldLogicalSizes,
) *datapb.Statistics {
	s := &datapb.Statistics{
		StatsBinlogSize:   statsBlobSize,
		FieldLogicalSizes: sizes,
		NullCounts:        make(map[int64]int64, len(columnGroups)),
	}
	for _, group := range columnGroups {
		s.InsertBinlogSize += int64(memorySizes[group.GroupID])
		s.InsertBinlogCount++
		s.NullCounts[group.GroupID] = nullCounts[group.GroupID]
		if n, ok := validElementCounts[group.GroupID]; ok {
			if s.ValidElementCounts == nil {
				s.ValidElementCounts = make(map[int64]int64)
			}
			s.ValidElementCounts[group.GroupID] = n
		}
	}
	return s
}
