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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// PreAllocateBinlogIDs pre-allocates binlog IDs based on the total number of binlogs from
// the segments for compaction, multiplied by an expansion factor.
// The schema is used to calculate a minimum ID count for V3 manifest segments where
// binlog metadata may be empty but stats output still requires IDs.
func PreAllocateBinlogIDs(allocator allocator.Allocator, segmentInfos []*SegmentInfo, schema *schemapb.CollectionSchema) (*datapb.IDRange, error) {
	binlogNum := int64(0)
	for _, s := range segmentInfos {
		// Insert / delta counts come from Statistics. Stats and BM25
		// stats arrays are still iterated because Statistics doesn't
		// carry a per-segment stat-file count today (V3 leaves these
		// arrays empty regardless; V2 sees the cumulative arrays here).
		stats := s.EnsureStats()
		binlogNum += stats.GetInsertBinlogCount() + stats.GetDeltaBinlogCount()
		for _, l := range s.GetStatslogs() {
			binlogNum += int64(len(l.GetBinlogs()))
		}
		for _, l := range s.GetBm25Statslogs() {
			binlogNum += int64(len(l.GetBinlogs()))
		}
	}
	// Compaction output always needs IDs for PK stats (1) and BM25 stats (per BM25 function).
	// For V3 manifest segments, binlog metadata may be empty since data is managed by manifest,
	// but stats output still requires IDs. Calculate the minimum from schema directly.
	minIDsFromSchema := int64(1) // 1 for PK stats
	if schema != nil {
		for _, fn := range schema.GetFunctions() {
			if fn.GetType() == schemapb.FunctionType_BM25 {
				minIDsFromSchema++
			}
		}
	}
	if binlogNum < minIDsFromSchema {
		binlogNum = minIDsFromSchema
	}
	n := binlogNum * int64(paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt())
	begin, end, err := allocator.AllocN(n)
	return &datapb.IDRange{Begin: begin, End: end}, err
}
