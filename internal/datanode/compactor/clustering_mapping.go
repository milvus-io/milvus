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
	"context"
	"path/filepath"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The vector mapping phase owns sorting and final physical writing. There are
// no intermediate segments or later sort-compaction tasks. Groups are consumed
// sequentially so writer residency does not grow with the number of groups.
func (t *clusteringCompactionTask) mappingSortedVector(ctx context.Context) (segments []*datapb.CompactionSegment, stats *storage.PartitionStatsSnapshot, resultErr error) {
	if err := t.vectorLayout.Validate(); err != nil {
		return nil, nil, err
	}
	root := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), "clustering_spill")
	sorter, err := newClusteringMappingSorter(ctx, t.plan.Schema, root, uint64(max(int64(1), t.memoryLimit/4)))
	if err != nil {
		return nil, nil, err
	}
	defer func() { resultErr = merr.Combine(sorter.Close(), resultErr) }()
	groups := make([]uint32, len(t.vectorLayout.Counts))
	for ordinal, group := range t.vectorLayout.Groups {
		for _, offset := range group.Offsets {
			groups[offset] = uint32(ordinal)
		}
	}
	for ordinal, segment := range t.plan.SegmentBinlogs {
		err := t.mappingSegmentRows(ctx, segment, func(record storage.Record, row int, offset int64, assignment *clusteringpb.ClusteringCentroidIdMappingStats) error {
			cluster := assignment.CentroidIdMapping[offset]
			return sorter.Append(record, row, clusteringSortKey{group: groups[cluster], cluster: cluster, distance: assignment.DistanceToCentroid[offset], source: uint64(ordinal), row: uint64(offset)})
		})
		if err != nil {
			return nil, nil, err
		}
	}
	stats = &storage.PartitionStatsSnapshot{Version: t.plan.PlanID, SegmentStats: make(map[int64]storage.SegmentStats), ClusteringStats: &clusteringpb.PartitionClusteringStats{}}
	var writer *MultiSegmentWriter
	var group uint32
	var cluster uint32
	batch := storage.NewRecordBuilderWithFieldID(t.plan.Schema)
	defer batch.Release()
	flush := func() error {
		if batch.GetRowNum() == 0 {
			return nil
		}
		record := batch.Build()
		defer record.Release()
		if err := writer.Write(record); err != nil {
			return err
		}
		id := writer.currentSegmentID
		entry := stats.SegmentStats[id]
		rowOffset := int64(entry.NumRows)
		if len(entry.ClusteringRanges) > 0 && entry.ClusteringRanges[len(entry.ClusteringRanges)-1].ClusterOffset == cluster {
			entry.ClusteringRanges[len(entry.ClusteringRanges)-1].RowCount += int64(record.Len())
		} else {
			entry.ClusteringRanges = append(entry.ClusteringRanges, &clusteringpb.ClusteringRange{ClusterOffset: cluster, RowOffset: rowOffset, RowCount: int64(record.Len())})
		}
		entry.NumRows += record.Len()
		stats.SegmentStats[id] = entry
		return nil
	}
	closeGroup := func() error {
		if writer == nil {
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		var ids []int64
		for _, segment := range writer.GetCompactionSegments() {
			if int64(stats.SegmentStats[segment.SegmentID].NumRows) != segment.NumOfRows {
				return merr.WrapErrDataIntegrityMsg("sorted clustering writer row count mismatch")
			}
			ids = append(ids, segment.SegmentID)
			segments = append(segments, segment)
		}
		if len(ids) > 0 {
			stats.ClusteringStats.Groups = append(stats.ClusteringStats.Groups, &clusteringpb.ClusteringGroup{SegmentIds: ids})
		}
		writer = nil
		return nil
	}
	err = sorter.Finish(func(record storage.Record, row int, key clusteringSortKey) error {
		if writer == nil || key.group != group {
			if err := closeGroup(); err != nil {
				return err
			}
			group = key.group
			var err error
			writer, err = NewMultiSegmentWriter(ctx, t.binlogIO, NewCompactionAllocator(t.segIDAlloc, t.logIDAlloc), t.plan.MaxSize,
				t.plan.Schema, t.compactionParams, t.plan.MaxSegmentRows, t.partitionID, t.collectionID, t.plan.Channel, 100, t.getWriterOpts()...)
			if err != nil {
				return err
			}
			// Compact cleanup closes the active writer after failure. Earlier
			// groups are already closed; their handles hold no buffered rows.
			t.clusterBuffers = append(t.clusterBuffers, newClusterBuffer(int(group), writer, nil))
		}
		if key.cluster != cluster {
			if err := flush(); err != nil {
				return err
			}
			cluster = key.cluster
		}
		if err := batch.Append(record, row, row+1); err != nil {
			return err
		}
		if batch.GetMemorySize() >= uint64(max(int64(1), min(t.memoryLimit/16, int64(t.compactionParams.BinLogMaxSize)/2))) || batch.GetRowNum() >= 1024 {
			return flush()
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if err := closeGroup(); err != nil {
		return nil, nil, err
	}
	return segments, stats, nil
}
