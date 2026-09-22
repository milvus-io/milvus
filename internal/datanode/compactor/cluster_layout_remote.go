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
	"io"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// clusterRowBatcher owns all pending groups for ONE reader worker. The byte
// budget is shared by its groups, not multiplied by centroid_group_num.
type clusterRowBatcher struct {
	limit, bytes int64
	groups       map[int][]clusterLayoutSortRow
	write        func(int, []clusterLayoutSortRow) error
}

func (b *clusterRowBatcher) append(group int, row clusterLayoutSortRow) error {
	size := clusterRowBytes(row.value)
	if b.bytes > 0 && size > b.limit-b.bytes {
		if err := b.flush(); err != nil {
			return err
		}
	}
	// Oversize rows are submitted directly and never accumulated with others.
	if size >= b.limit {
		return b.write(group, []clusterLayoutSortRow{row})
	}
	if b.groups == nil {
		b.groups = make(map[int][]clusterLayoutSortRow)
	}
	b.groups[group] = append(b.groups[group], row)
	b.bytes += size
	if len(b.groups[group]) >= 1024 {
		return b.flush()
	}
	return nil
}

func (b *clusterRowBatcher) flush() error {
	ids := make([]int, 0, len(b.groups))
	for id := range b.groups {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		if err := b.write(id, b.groups[id]); err != nil {
			return err
		}
		delete(b.groups, id) // release row references, including slice backing arrays
	}
	b.bytes = 0
	return nil
}

func (t *clusteringCompactionTask) mappingClusterLayoutRemote(ctx context.Context) ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	writers := make([]*clusterStatsWriter, len(t.clusterBuffers))
	for id, buffer := range t.clusterBuffers {
		writers[id] = newClusterStatsWriter(buffer.writer, &datapb.ClusterStats{
			Version: clusterStatsVersion, ClusteringTaskId: t.GetPlanID(), FieldId: t.clusteringKeyField.GetFieldID(),
			GroupId: int64(id), CentroidIds: append([]uint32(nil), t.layoutPlan.CentroidGroups[id].Centroids...),
		})
		writers[id].keyLimit = int(max(int64(1), min(int64(clusterStatsBlockRows), t.memoryLimit/int64(max(1, len(writers)))/8/clusterLayoutSortKeySize)))
	}
	futures := make([]*conc.Future[any], 0, len(t.plan.SegmentBinlogs))
	budget := max(int64(1), min(int64(8<<20), t.memoryLimit/int64(max(1, t.getSpillPoolSize()))/8))
	for ordinal, segment := range t.plan.SegmentBinlogs {
		ordinal, segment := ordinal, segment
		futures = append(futures, t.spillPool.Submit(func() (any, error) {
			batcher := &clusterRowBatcher{limit: budget, write: func(group int, rows []clusterLayoutSortRow) error {
				if err := writers[group].WriteBatch(ctx, rows); err != nil {
					return err
				}
				t.writtenRowNum.Add(int64(len(rows)))
				return nil
			}}
			err := t.readClusterAssignments(ctx, ordinal, segment, batcher.append)
			if err == nil {
				err = batcher.flush()
			}
			if err != nil {
				cancel()
			}
			return nil, err
		}))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, nil, err
	}
	segments := make([]*datapb.CompactionSegment, 0)
	snapshot := &storage.PartitionStatsSnapshot{SegmentStats: make(map[int64]storage.SegmentStats)}
	for id, writer := range writers {
		outputs, err := writer.Close(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, segment := range outputs {
			snapshot.SegmentStats[segment.GetSegmentID()] = storage.SegmentStats{
				NumRows: int(segment.NumOfRows), FieldStats: []storage.FieldStats{t.clusterBuffers[id].clusteringKeyFieldStats.Clone()},
			}
		}
		segments = append(segments, outputs...)
	}
	return segments, snapshot, nil
}

func (t *clusteringCompactionTask) readClusterAssignments(ctx context.Context, ordinal int, segment *datapb.CompactionSegmentBinlogs, emit func(int, clusterLayoutSortRow) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delta, err := compaction.ComposeDeleteFromDeltalogs(ctx, t.primaryKeyField.DataType, segment,
		storage.WithDownloader(t.binlogIO.Download), storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		return err
	}
	filter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime, segment.GetCommitTimestamp())
	mappingPath, ok := t.segmentIDOffsetMapping[segment.GetSegmentID()]
	if !ok {
		return merr.WrapErrServiceInternalMsg("missing clustering assignment artifact for segment %d", segment.GetSegmentID())
	}
	blobs, err := t.binlogIO.Download(ctx, []string{mappingPath})
	if err != nil {
		return err
	}
	if len(blobs) != 1 {
		return merr.WrapErrServiceInternalMsg("expected one clustering assignment artifact")
	}
	mapping := &clusteringpb.ClusteringCentroidIdMappingStats{}
	if err = proto.Unmarshal(blobs[0], mapping); err != nil {
		return merr.Wrap(err, "decode clustering assignment artifact")
	}
	if err = clustercompaction.ValidateCentroidMappingStats(mapping, int64(len(mapping.CentroidIdMapping)), t.layoutPlan.CentroidCount); err != nil {
		return err
	}
	rr, existing, err := newCompactionSegmentRecordReader(ctx, segment, t.plan.Schema, t.compactionParams.StorageConfig,
		storage.WithDownloader(t.binlogIO.Download), storage.WithVersion(segment.StorageVersion), storage.WithBufferSize(t.bufferSize),
		storage.WithStorageConfig(t.compactionParams.StorageConfig), storage.WithCollectionID(t.collectionID))
	if err != nil {
		return err
	}
	materializer, err := NewRecordMaterializer(t.plan.Schema, t.plan.Schema.GetFunctions(), existing)
	if err != nil {
		rr.Close()
		return err
	}
	rr = wrapReaderWithTimestampOverwrite(newMaterializedRecordReader(rr, materializer), segment.GetCommitTimestamp())
	defer rr.Close()
	offset := 0
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		record, e := rr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		values := make([]*storage.Value, record.Len())
		if err = storage.ValueDeserializerWithSchema(record, values, t.plan.Schema, true); err != nil {
			return err
		}
		for _, value := range values {
			if offset >= len(mapping.CentroidIdMapping) {
				return merr.WrapErrServiceInternalMsg("assignment row count mismatch")
			}
			centroid := mapping.CentroidIdMapping[offset]
			key := clusterLayoutSortKey{centroidID: centroid, distance: mapping.DistanceToCentroid[offset], sourceSegmentOffset: uint64(ordinal), sourceRowOffset: uint64(offset)}
			offset++ // consume assignment for deleted/expired rows as well
			expire := int64(-1)
			if fields, ok := value.Value.(map[int64]interface{}); ok {
				if v, ok := fields[t.ttlFieldID].(int64); ok {
					expire = v
				}
			}
			if filter.Filtered(value.PK.GetValue(), uint64(value.Timestamp), expire) {
				continue
			}
			group, ok := t.centroidGroupIndex[centroid]
			if !ok {
				return merr.WrapErrServiceInternalMsg("centroid %d missing from layout", centroid)
			}
			if err = emit(group, clusterLayoutSortRow{value: value, key: key}); err != nil {
				return err
			}
		}
	}
	if offset != len(mapping.CentroidIdMapping) {
		return merr.WrapErrServiceInternalMsg("assignment row count mismatch")
	}
	return nil
}
