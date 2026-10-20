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
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func UpdateCompactionSegmentSizeMetrics(segments []*datapb.CompactionSegment) {
	var totalSize int64
	for _, seg := range segments {
		totalSize += getCompactedSegmentSize(seg)
	}
	// observe size in bytes
	metrics.DataCoordCompactedSegmentSize.WithLabelValues().Observe(float64(totalSize))
}

func getCompactedSegmentSize(s *datapb.CompactionSegment) int64 {
	var segmentSize int64
	if s != nil {
		for _, binlogs := range s.GetInsertLogs() {
			for _, l := range binlogs.GetBinlogs() {
				segmentSize += l.GetMemorySize()
			}
		}

		for _, deltaLogs := range s.GetDeltalogs() {
			for _, l := range deltaLogs.GetBinlogs() {
				segmentSize += l.GetMemorySize()
			}
		}

		for _, statsLogs := range s.GetField2StatslogPaths() {
			for _, l := range statsLogs.GetBinlogs() {
				segmentSize += l.GetMemorySize()
			}
		}
	}

	return segmentSize
}

func calculateStatsTaskSlot(segmentSize int64) int64 {
	defaultSlots := Params.DataCoordCfg.StatsTaskSlotUsage.GetAsInt64()
	if segmentSize > 512*1024*1024 {
		taskSlot := max(segmentSize/512/1024/1024, 1) * defaultSlots
		return max(taskSlot, 1)
	} else if segmentSize > 100*1024*1024 {
		return max(defaultSlots/2, 1)
	} else if segmentSize > 10*1024*1024 {
		return max(defaultSlots/4, 1)
	}
	return max(defaultSlots/8, 1)
}
