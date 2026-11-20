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
	"cmp"
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

func (t *LevelZeroCompactionTask) dispatchMutations(ctx context.Context, pk *schemapb.FieldSchema, targets, inputs []*datapb.CompactionSegmentBinlogs, batchSize int) ([]*datapb.CompactionSegment, error) {
	var rows []scopedMutation
	for _, input := range inputs {
		part, err := readSegmentMutations(ctx, compactionMutationSource(t.cm, t.BinlogIO), t.compactionParams.StorageConfig, t.plan, input, pk)
		if err != nil {
			return nil, err
		}
		rows = append(rows, part...)
	}
	orderMutations(rows)
	plugin, err := hookutil.GetCPluginContext(t.plan.GetPluginContext(), targets[0].GetCollectionID())
	if err != nil {
		return nil, err
	}
	var result []*datapb.CompactionSegment
	for batch := range slices.Chunk(targets, max(batchSize, 1)) {
		bfs, err := t.loadBF(ctx, batch)
		if err != nil {
			return nil, err
		}
		for _, target := range batch {
			groups, err := targetMutations(rows, target.GetPartitionID(), bfs[target.GetSegmentID()])
			if err != nil {
				return nil, err
			}
			out := &datapb.CompactionSegment{SegmentID: target.GetSegmentID(), Channel: t.plan.GetChannel()}
			for _, group := range groups {
				file, err := t.writeTargetMutations(ctx, target, group, plugin)
				if err != nil {
					return nil, err
				}
				if len(out.Deltalogs) == 0 {
					out.Deltalogs = []*datapb.FieldBinlog{{}}
				}
				out.Deltalogs[0].Binlogs = append(out.Deltalogs[0].Binlogs, file)
			}
			out.Stats = buildCompactionOutputStats(nil, out.Deltalogs, 0)
			result = append(result, out)
		}
	}
	return result, nil
}

func targetMutations(rows []scopedMutation, partition int64, bf *pkoracle.BloomFilterSet) (map[int32]*storage.MutationDelta, error) {
	groups := map[int32]*storage.MutationDelta{}
	for _, r := range rows {
		if r.layout.PartitionID != common.AllPartitionsID && r.layout.PartitionID != partition {
			continue
		}
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
		g := groups[r.layout.SchemaVersion]
		if g == nil {
			l := r.layout
			l.PartitionID = partition
			l.Fields = nil
			g = &storage.MutationDelta{Layout: l}
			groups[l.SchemaVersion] = g
		}
		for _, assignment := range r.Assignments {
			id := assignment.GetFieldId()
			found := slices.ContainsFunc(g.Layout.Fields, func(existing deltacodec.Field) bool { return existing.ID == id })
			if !found {
				for _, f := range r.layout.Fields {
					if f.ID == id {
						g.Layout.Fields = append(g.Layout.Fields, f)
						break
					}
				}
			}
		}
		g.Rows = append(g.Rows, projected)
	}
	return groups, nil
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
	slices.SortFunc(g.Layout.Fields, func(a, b deltacodec.Field) int { return cmp.Compare(a.ID, b.ID) })
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
