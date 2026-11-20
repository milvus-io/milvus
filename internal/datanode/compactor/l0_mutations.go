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
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (t *LevelZeroCompactionTask) dispatchMutations(ctx context.Context, pk *schemapb.FieldSchema, targets, inputs []*datapb.CompactionSegmentBinlogs, memoryLimit int64) ([]*datapb.CompactionSegment, error) {
	plugin, err := hookutil.GetCPluginContext(t.plan.GetPluginContext(), targets[0].GetCollectionID())
	if err != nil {
		return nil, err
	}
	results := make([]*datapb.CompactionSegment, len(targets))
	for i, target := range targets {
		results[i] = &datapb.CompactionSegment{SegmentID: target.GetSegmentID(), Channel: t.plan.GetChannel()}
	}
	source := compactionMutationSource(t.cm, t.BinlogIO)
	// Retain one decoded source file and its target projection at a time.
	// Its original WAL identity orders it against other files on replay.
	dispatch := func(rows []scopedMutation, size int64) error {
		orderMutations(rows)
		batchSize := getMaxBatchSize(float64(max(size, 1)), float64(memoryLimit))
		for begin := 0; begin < len(targets); begin += batchSize {
			batch := targets[begin:min(begin+batchSize, len(targets))]
			bfs, err := t.loadBF(ctx, batch)
			if err != nil {
				return err
			}
			for i, target := range batch {
				group, err := targetMutations(rows, target.GetPartitionID(), bfs[target.GetSegmentID()])
				if err != nil {
					return err
				}
				if group == nil {
					continue
				}
				file, err := t.writeTargetMutations(ctx, target, group, plugin)
				if err != nil {
					return err
				}
				out := results[begin+i]
				if len(out.Deltalogs) == 0 {
					out.Deltalogs = []*datapb.FieldBinlog{{}}
				}
				out.Deltalogs[0].Binlogs = append(out.Deltalogs[0].Binlogs, file)
			}
		}
		return nil
	}
	for _, input := range inputs {
		if err := readSegmentMutations(ctx, source, t.compactionParams.StorageConfig, t.plan, input, pk, memoryLimit, dispatch); err != nil {
			return nil, err
		}
	}
	for _, result := range results {
		result.Stats = buildCompactionOutputStats(nil, result.Deltalogs, 0)
	}
	return results, nil
}

func targetMutations(rows []scopedMutation, partition int64, bf *pkoracle.BloomFilterSet) (*storage.MutationDelta, error) {
	// Each decoded file has one physical layout and scope.
	layout := rows[0].layout
	if layout.PartitionID != common.AllPartitionsID && layout.PartitionID != partition {
		return nil, nil
	}
	layout.PartitionID = partition
	delta := &storage.MutationDelta{Layout: layout}
	for _, r := range rows {
		projected := r.Row
		if bf != nil {
			keys := make([]storage.PrimaryKey, typeutil.GetSizeOfIDs(r.PrimaryKeys))
			for i := range keys {
				key, err := storage.GenPrimaryKeyByRawData(typeutil.GetPK(r.PrimaryKeys, int64(i)), r.layout.PKType)
				if err != nil {
					return nil, err
				}
				keys[i] = key
			}
			hits := bf.BatchPkExist(storage.NewBatchLocationsCache(keys))
			projected.PrimaryKeys = &schemapb.IDs{}
			for i, hit := range hits {
				if hit {
					typeutil.AppendIDs(projected.PrimaryKeys, r.PrimaryKeys, i)
				}
			}
		}
		if typeutil.GetSizeOfIDs(projected.PrimaryKeys) == 0 {
			continue
		}
		delta.Rows = append(delta.Rows, projected)
	}
	if len(delta.Rows) == 0 {
		return nil, nil
	}
	return delta, nil
}

func (t *LevelZeroCompactionTask) writeTargetMutations(ctx context.Context, target *datapb.CompactionSegmentBinlogs, g *storage.MutationDelta, plugin *indexcgopb.StoragePluginContext) (*datapb.Binlog, error) {
	// Keep delete-only targets readable by older QueryNodes during upgrade.
	if !slices.ContainsFunc(g.Rows, func(r deltacodec.Row) bool { return r.Operation == deltacodec.Update }) {
		var keys []storage.PrimaryKey
		var ticks []uint64
		for _, r := range g.Rows {
			for i := range typeutil.GetSizeOfIDs(r.PrimaryKeys) {
				key, err := storage.GenPrimaryKeyByRawData(typeutil.GetPK(r.PrimaryKeys, int64(i)), g.Layout.PKType)
				if err != nil {
					return nil, err
				}
				keys = append(keys, key)
				ticks = append(ticks, r.TimeTick)
			}
		}
		return t.writeLegacyDeletes(ctx, target, g.Layout.PKType, keys, ticks)
	}
	id, err := t.allocator.AllocOne()
	if err != nil {
		return nil, err
	}
	path, err := t.deltaPath(target, id)
	if err != nil {
		return nil, err
	}
	cfg := t.compactionParams.StorageConfig
	bytes, err := storage.WriteMutationDelta(ctx, path, *g, cfg, plugin)
	if err != nil {
		return nil, err
	}
	file := &datapb.Binlog{LogID: id, LogPath: path, LogSize: bytes, EntriesNum: int64(len(g.Rows)), TimestampFrom: g.Rows[0].TimeTick, TimestampTo: g.Rows[len(g.Rows)-1].TimeTick, MutationCounts: &datapb.DeltaMutationCounts{}}
	for _, r := range g.Rows {
		n := int64(typeutil.GetSizeOfIDs(r.PrimaryKeys))
		if r.Operation == deltacodec.Delete {
			file.MutationCounts.DeleteKeys += n
		} else {
			file.MutationCounts.UpdateKeys += n
		}
		file.MemorySize += int64(13 + proto.Size(r.PrimaryKeys))
		for _, a := range r.Assignments {
			file.MemorySize += int64(proto.Size(a))
		}
	}
	return file, nil
}
