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
	"container/heap"
	"context"
	"io"
	"math"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const clusterRemoteMergeFanIn = 8

type clusterSortCompactionTask struct{ *sortCompactionTask }

func NewClusterSortCompactionTask(ctx context.Context, cm storage.ChunkManager, plan *datapb.CompactionPlan, params compaction.Params) Compactor {
	return &clusterSortCompactionTask{NewSortCompactionTask(ctx, cm, plan, params, nil)}
}

func (t *clusterSortCompactionTask) GetCompactionType() datapb.CompactionType {
	return datapb.CompactionType_ClusterSortCompaction
}

func (t *clusterSortCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	if err := t.ctx.Err(); err != nil {
		return nil, err
	}
	inputs := t.plan.GetSegmentBinlogs()
	if len(inputs) != 1 {
		return nil, merr.WrapErrServiceInternalMsg("cluster sort requires exactly one input segment")
	}
	allocated := t.plan.GetPreAllocatedSegmentIDs()
	if t.plan.Schema == nil || allocated.GetBegin() <= 0 || allocated.GetEnd()-allocated.GetBegin() != 1 || allocated.GetBegin() == inputs[0].GetSegmentID() {
		return nil, merr.WrapErrServiceInternalMsg("invalid cluster sort plan")
	}
	input := inputs[0]
	ref := input.GetClusterStats()
	if ref == nil || ref.Version != clusterStatsVersion || ref.Sorted || ref.ClusteringTaskId == 0 {
		return nil, merr.WrapErrServiceInternalMsg("cluster sort requires unsorted cluster_stats")
	}
	t.collectionID, t.partitionID = input.GetCollectionID(), input.GetPartitionID()
	if ref.NumRows <= 0 || len(ref.Files) == 0 || len(ref.CentroidIds) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid cluster sort input")
	}
	field := typeutil.GetField(t.plan.Schema, ref.FieldId)
	if field.GetDataType() != schemapb.DataType_FloatVector && field.GetDataType() != schemapb.DataType_Float16Vector && field.GetDataType() != schemapb.DataType_BFloat16Vector {
		return nil, merr.WrapErrServiceInternalMsg("cluster sort requires a supported dense vector field")
	}
	if len(compaction.GetTEXTFieldIDsFromSchema(t.plan.Schema)) > 0 {
		return nil, merr.WrapErrServiceInternalMsg("cluster sort does not support TEXT/LOB columns yet")
	}
	if err := binlog.DecompressCompactionBinlogsWithRootPath(t.compactionParams.StorageConfig.GetRootPath(), inputs); err != nil {
		return nil, err
	}
	pk, err := typeutil.GetPrimaryFieldSchema(t.plan.Schema)
	if err != nil {
		return nil, err
	}
	t.ttlFieldID = getTTLFieldID(t.plan.Schema)
	helper := &clusteringCompactionTask{plan: t.plan, binlogIO: t.binlogIO, compactionParams: t.compactionParams, collectionID: t.collectionID, partitionID: t.partitionID, writtenRowNum: atomic.NewInt64(0)}
	helper.memoryLimit = helper.getMemoryLimit()
	// Every active merge reader holds only ONE bounded run part, even when a
	// logical merged run spans many remote objects. Reserve room for native IO.
	budget := max(int64(1), min(int64(64<<20), helper.memoryLimit/(clusterRemoteMergeFanIn+4)/4))
	root := path.Join(t.compactionParams.StorageConfig.GetRootPath(), "cluster_sort_runs", strconv.FormatInt(t.GetPlanID(), 10), uuid.NewString())
	spillStorage := proto.Clone(t.compactionParams.StorageConfig).(*indexpb.StorageConfig)
	spillStorage.RootPath = root
	spillParams := t.compactionParams
	spillParams.StorageVersion, spillParams.StorageConfig = storage.StorageV1, spillStorage
	helper.bufferSize = min(budget, int64(t.compactionParams.BinLogMaxSize))
	spiller := &clusterLayoutSpiller{
		task: helper, cm: t.cm, binlogIO: t.binlogIO, root: root, spillParams: spillParams, spillStorage: spillStorage,
		segAlloc: allocator.NewLocalAllocator(1, math.MaxInt64), logAlloc: allocator.NewLocalAllocator(1, math.MaxInt64),
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.ctx), time.Minute)
		defer cancel()
		if e := t.cm.RemoveWithPrefix(ctx, root+"/"); e != nil {
			mlog.Warn(ctx, "failed to remove cluster sort temporary objects", mlog.String("prefix", root), mlog.Err(e))
		}
	}()
	// Stage 1 already applied the segment size limit. Sorting must not rotate
	// or coalesce segments, even if serialization changes their encoded size.
	finalWriter, err := NewMultiSegmentWriter(t.ctx, t.binlogIO, NewCompactionAllocator(
		allocator.NewLocalAllocator(allocated.GetBegin(), allocated.GetEnd()),
		allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())),
		math.MaxInt64, t.plan.Schema, t.compactionParams, ref.NumRows, t.partitionID, t.collectionID, t.plan.Channel, 100,
		storage.WithStorageConfig(t.compactionParams.StorageConfig), storage.WithBufferSize(min(budget, int64(t.compactionParams.BinLogMaxSize))),
		storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI), storage.WithWriterFormat(t.compactionParams.GetStorageFormat()))
	if err != nil {
		return nil, err
	}
	finalRef := proto.Clone(ref).(*datapb.ClusterStats)
	finalRef.Sorted = true
	out := newClusterStatsWriter(finalWriter, finalRef)
	partID := 0
	writePart := func(rows []clusterLayoutSortRow) (*clusterLayoutSpillRun, error) {
		partID++
		return spiller.writeRun(t.ctx, t.GetPlanID(), int(ref.GroupId), partID, rows)
	}
	var runs []clusterRemoteRun
	var rows []clusterLayoutSortRow
	var bytes int64
	spill := func() error {
		if len(rows) == 0 {
			return nil
		}
		part, e := writePart(rows)
		if e != nil {
			return e
		}
		runs = append(runs, clusterRemoteRun{part})
		rows = nil
		bytes = 0
		return nil
	}
	err = t.readStagingRows(input, pk.GetDataType(), func(row clusterLayoutSortRow) error {
		size := clusterRowBytes(row.value)
		if len(rows) > 0 && size > budget-bytes {
			if e := spill(); e != nil {
				return e
			}
		}
		rows = append(rows, row)
		bytes += size
		if bytes >= budget {
			return spill()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].key.less(rows[j].key) })
		if err = out.WriteBatch(t.ctx, rows); err != nil {
			return nil, err
		}
	} else {
		if err = spill(); err != nil {
			return nil, err
		}
		for len(runs) > clusterRemoteMergeFanIn {
			var next []clusterRemoteRun
			for start := 0; start < len(runs); start += clusterRemoteMergeFanIn {
				var merged clusterRemoteRun
				rows = nil
				bytes = 0
				flush := func() error {
					if len(rows) == 0 {
						return nil
					}
					part, e := writePart(rows)
					if e != nil {
						return e
					}
					merged = append(merged, part)
					rows = nil
					bytes = 0
					return nil
				}
				err = mergeClusterRemoteRuns(t.ctx, spiller, runs[start:min(start+clusterRemoteMergeFanIn, len(runs))], func(row clusterLayoutSortRow) error {
					size := clusterRowBytes(row.value)
					if len(rows) > 0 && size > budget-bytes {
						if e := flush(); e != nil {
							return e
						}
					}
					rows = append(rows, row)
					bytes += size
					if bytes >= budget {
						return flush()
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
				if err = flush(); err != nil {
					return nil, err
				}
				next = append(next, merged)
			}
			runs = next
		}
		batcher := &clusterRowBatcher{limit: min(budget, int64(1<<20)), write: func(_ int, batch []clusterLayoutSortRow) error { return out.WriteBatch(t.ctx, batch) }}
		if err = mergeClusterRemoteRuns(t.ctx, spiller, runs, func(row clusterLayoutSortRow) error { return batcher.append(0, row) }); err != nil {
			return nil, err
		}
		if err = batcher.flush(); err != nil {
			return nil, err
		}
	}
	segments, err := out.Close(t.ctx)
	if err != nil {
		return nil, err
	}
	if len(segments) > 1 || (len(segments) == 1 && segments[0].GetSegmentID() != allocated.GetBegin()) {
		return nil, merr.WrapErrServiceInternalMsg("cluster sort changed physical segment boundaries")
	}
	return &datapb.CompactionPlanResult{PlanID: t.GetPlanID(), State: datapb.CompactionTaskState_completed, Type: t.GetCompactionType(), Channel: t.plan.Channel, Segments: segments}, nil
}

// A logical run may have many parts. Its cursor closes each part before opening
// the next; multi-pass merge reduces logical fan-in, not just file count.
type (
	clusterRemoteRun    []*clusterLayoutSpillRun
	clusterRemoteCursor struct {
		parts   clusterRemoteRun
		reader  *clusterLayoutRunReader
		spiller *clusterLayoutSpiller
		ctx     context.Context
	}
)

func (c *clusterRemoteCursor) advance() (bool, error) {
	for {
		if c.reader != nil {
			ok, err := c.reader.advance()
			if err != nil || ok {
				return ok, err
			}
			c.reader.close()
			c.reader = nil
		}
		if len(c.parts) == 0 {
			return false, nil
		}
		r, err := newClusterLayoutRunReader(c.ctx, c.spiller, c.spiller.task, c.parts[0], 64<<10)
		if err != nil {
			return false, err
		}
		c.parts = c.parts[1:]
		c.reader = r
	}
}

type clusterRemoteHeap []*clusterRemoteCursor

func (h clusterRemoteHeap) Len() int { return len(h) }
func (h clusterRemoteHeap) Less(i, j int) bool {
	return h[i].reader.currentKey.less(h[j].reader.currentKey)
}
func (h clusterRemoteHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *clusterRemoteHeap) Push(v any)   { *h = append(*h, v.(*clusterRemoteCursor)) }
func (h *clusterRemoteHeap) Pop() any {
	old := *h
	v := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return v
}

func mergeClusterRemoteRuns(ctx context.Context, spiller *clusterLayoutSpiller, runs []clusterRemoteRun, emit func(clusterLayoutSortRow) error) error {
	if len(runs) > clusterRemoteMergeFanIn {
		return merr.WrapErrServiceInternalMsg("cluster merge fan-in exceeded")
	}
	h := &clusterRemoteHeap{}
	cursors := make([]*clusterRemoteCursor, 0, len(runs))
	defer func() {
		for _, c := range cursors {
			if c.reader != nil {
				c.reader.close()
			}
		}
	}()
	for _, run := range runs {
		c := &clusterRemoteCursor{parts: run, spiller: spiller, ctx: ctx}
		cursors = append(cursors, c)
		ok, err := c.advance()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, c)
		}
	}
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := heap.Pop(h).(*clusterRemoteCursor)
		if err := emit(clusterLayoutSortRow{value: c.reader.currentValue, key: c.reader.currentKey}); err != nil {
			return err
		}
		ok, err := c.advance()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, c)
		}
	}
	return nil
}

func (t *clusterSortCompactionTask) readStagingRows(input *datapb.CompactionSegmentBinlogs, pkType schemapb.DataType, emit func(clusterLayoutSortRow) error) error {
	ref := input.GetClusterStats()
	delta, err := compaction.ComposeDeleteFromDeltalogs(t.ctx, pkType, input, storage.WithDownloader(t.binlogIO.Download), storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		return err
	}
	filter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime, input.GetCommitTimestamp())
	rr, _, err := newCompactionSegmentRecordReader(t.ctx, input, t.plan.Schema, t.compactionParams.StorageConfig,
		storage.WithDownloader(t.binlogIO.Download), storage.WithVersion(input.GetStorageVersion()),
		storage.WithStorageConfig(t.compactionParams.StorageConfig), storage.WithBufferSize(1<<20), storage.WithCollectionID(t.collectionID))
	if err != nil {
		return err
	}
	defer rr.Close()
	file, index := 0, 0
	var keys []clusterLayoutSortKey
	var count int64
	allowed := make(map[uint32]bool, len(ref.CentroidIds))
	for _, id := range ref.CentroidIds {
		allowed[id] = true
	}
	nextKey := func() (clusterLayoutSortKey, error) {
		if index == len(keys) {
			if file >= len(ref.Files) {
				return clusterLayoutSortKey{}, merr.WrapErrServiceInternalMsg("cluster_stats has fewer keys than data rows")
			}
			p := ref.Files[file]
			size, e := t.cm.Size(t.ctx, p)
			if e != nil {
				return clusterLayoutSortKey{}, e
			}
			if size <= 16 || size > 16+clusterStatsBlockRows*clusterLayoutSortKeySize {
				return clusterLayoutSortKey{}, merr.WrapErrServiceInternalMsg("invalid cluster_stats block size")
			}
			blob, e := t.cm.Read(t.ctx, p)
			if e != nil {
				return clusterLayoutSortKey{}, e
			}
			keys, e = decodeClusterStatsBlock(blob)
			if e != nil {
				return clusterLayoutSortKey{}, e
			}
			index = 0
			file++
		}
		key := keys[index]
		index++
		count++
		if !allowed[key.centroidID] {
			return key, merr.WrapErrServiceInternalMsg("cluster_stats centroid is outside its group")
		}
		return key, nil
	}
	for {
		if err = t.ctx.Err(); err != nil {
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
			key, e := nextKey()
			if e != nil {
				return e
			}
			expire := int64(-1)
			if fields, ok := value.Value.(map[int64]interface{}); ok {
				if v, ok := fields[t.ttlFieldID].(int64); ok {
					expire = v
				}
			}
			if filter.Filtered(value.PK.GetValue(), uint64(value.Timestamp), expire) {
				continue
			}
			if err = emit(clusterLayoutSortRow{value: value, key: key}); err != nil {
				return err
			}
		}
	}
	if count != ref.NumRows || index != len(keys) || file != len(ref.Files) {
		return merr.WrapErrServiceInternalMsg("cluster_stats/data row count mismatch")
	}
	return nil
}
