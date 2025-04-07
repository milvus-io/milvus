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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// PreAllocateBinlogIDs pre-allocates binlog IDs based on the total number of binlogs from
// the segments for compaction, multiplied by an expansion factor.
func PreAllocateBinlogIDs(allocator allocator.Allocator, segmentInfos []*SegmentInfo) (*datapb.IDRange, error) {
	binlogNum := 0
	for _, s := range segmentInfos {
		for _, l := range s.GetBinlogs() {
			binlogNum += len(l.GetBinlogs())
		}
		for _, l := range s.GetDeltalogs() {
			binlogNum += len(l.GetBinlogs())
		}
		for _, l := range s.GetStatslogs() {
			binlogNum += len(l.GetBinlogs())
		}
		for _, l := range s.GetBm25Statslogs() {
			binlogNum += len(l.GetBinlogs())
		}
	}
	n := binlogNum * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt()
	begin, end, err := allocator.AllocN(int64(n))
	return &datapb.IDRange{Begin: begin, End: end}, err
}

func GetCompactionExtraParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   paramtable.Get().CommonCfg.EnableStorageV2.Key,
			Value: paramtable.Get().CommonCfg.EnableStorageV2.GetValue(),
		},
		{
			Key:   paramtable.Get().DataNodeCfg.BinLogMaxSize.Key,
			Value: paramtable.Get().DataNodeCfg.BinLogMaxSize.GetValue(),
		},
		{
			Key:   paramtable.Get().DataNodeCfg.UseMergeSort.Key,
			Value: paramtable.Get().DataNodeCfg.UseMergeSort.GetValue(),
		},
		{
			Key:   paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.Key,
			Value: paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetValue(),
		},
	}
}
