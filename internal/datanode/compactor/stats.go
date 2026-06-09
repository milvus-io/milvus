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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// buildCompactionOutputStats produces the Statistics that ships on a
// CompactionSegment. The receiver (DataCoord) copies this verbatim onto
// SegmentInfo.Stats — it does NOT recompute — so this is the authoritative
// place to populate aggregate metrics for compaction outputs.
//
// statsBlobSize is the cumulative bloom-filter + BM25 blob memory size the
// writer observed while emitting stats. Both V2 and V3 writers track it on
// BinlogRecordWriter.GetStatsBlobSize: V2's value is the sum of
// statslog/bm25 FieldBinlog MemorySize fields; V3's value is the sum of
// raw blob lengths committed into the manifest. Passing it through this
// helper avoids the trap of trying to recompute from FieldBinlog arrays —
// V3 writers deliberately leave the statslog and bm25 FieldBinlogs nil
// because stats are embedded in the manifest, so an array-based recompute
// would silently report zero.
//
// deltalogs is normally nil for insert-side compactors (mix / sort /
// bump-schema produce fresh segments without deltas) and non-nil only for
// L0 compaction outputs, which carry only deltas and no inserts.
func buildCompactionOutputStats(insertLogs, deltalogs []*datapb.FieldBinlog, statsBlobSize int64) *datapb.Statistics {
	s := storage.BuildStatsFromFieldBinlogs(insertLogs, nil, nil, deltalogs)
	s.StatsBinlogSize = statsBlobSize
	return s
}
