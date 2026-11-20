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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// Coalesce complete files across the round without keeping the whole
	// prefix resident. Their WAL identities order independently flushed groups.
	var rows []scopedMutation
	var size int64
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		orderMutations(rows)
		batchSize := getMaxBatchSize(float64(max(size, 1)), float64(memoryLimit))
		for begin := 0; begin < len(targets); begin += batchSize {
			batch := targets[begin:min(begin+batchSize, len(targets))]
			bfs, err := t.loadBF(ctx, batch)
			if err != nil {
				return err
			}
			for i, target := range batch {
				groups, err := targetMutations(rows, target.GetPartitionID(), bfs[target.GetSegmentID()])
				if err != nil {
					return err
				}
				for _, group := range groups {
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
		}
		rows = nil
		size = 0
		return nil
	}
	window := memoryLimit
	if maxFile := int64(t.compactionParams.BinLogMaxSize); maxFile > 0 {
		window = min(window, maxFile)
	}
	reserve := func(path string, needed int64) error {
		if needed > memoryLimit {
			return merr.Wrapf(merr.ErrServiceMemoryLimitExceeded, "L0 file %q needs %d bytes, budget %d", path, needed, memoryLimit)
		}
		if needed > window-size {
			return flush()
		}
		return nil
	}
	collect := func(fileRows []scopedMutation, fileSize int64) error {
		rows = append(rows, fileRows...)
		size += fileSize
		return nil
	}
	for _, input := range inputs {
		if err := readSegmentMutations(ctx, source, t.compactionParams.StorageConfig, t.plan, input, pk, reserve, collect); err != nil {
			return nil, err
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	for _, result := range results {
		result.Stats = buildCompactionOutputStats(nil, result.Deltalogs, 0)
	}
	return results, nil
}

func targetMutations(rows []scopedMutation, partition int64, bf *pkoracle.BloomFilterSet) ([]*storage.MutationDelta, error) {
	var groups []*storage.MutationDelta
	byVersion := make(map[int32]*storage.MutationDelta)
	fields := make(map[int32]map[int64]struct{})
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
		group := byVersion[r.layout.SchemaVersion]
		if group == nil {
			layout := r.layout
			layout.PartitionID = partition
			layout.Fields = nil
			group = &storage.MutationDelta{Layout: layout}
			byVersion[layout.SchemaVersion] = group
			groups = append(groups, group)
			fields[layout.SchemaVersion] = make(map[int64]struct{})
		}
		assigned := fields[r.layout.SchemaVersion]
		for _, a := range projected.Assignments {
			id := a.GetFieldId()
			if _, seen := assigned[id]; seen {
				continue
			}
			for _, f := range r.layout.Fields {
				if f.ID == id {
					assigned[id] = struct{}{}
					group.Layout.Fields = append(group.Layout.Fields, f)
					break
				}
			}
		}
		group.Rows = append(group.Rows, projected)
	}
	for _, group := range groups {
		slices.SortFunc(group.Layout.Fields, func(a, b deltacodec.Field) int { return cmp.Compare(a.ID, b.ID) })
	}
	return groups, nil
}

func (t *LevelZeroCompactionTask) writeTargetMutations(ctx context.Context, target *datapb.CompactionSegmentBinlogs, g *storage.MutationDelta, plugin *indexcgopb.StoragePluginContext) (*datapb.Binlog, error) {
	// The legacy decoder implies ordinal zero. Only down-convert when it
	// can reconstruct every delivered mutation's original identity.
	if !slices.ContainsFunc(g.Rows, func(r deltacodec.Row) bool { return r.Operation != deltacodec.Delete || r.SourceOrdinal != 0 }) {
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
