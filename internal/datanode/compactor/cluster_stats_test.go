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
	"math"
	"path"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestClusterStatsBlockCodec(t *testing.T) {
	keys := []clusterLayoutSortKey{{centroidID: 10, distance: 1.25, sourceSegmentOffset: 5, sourceRowOffset: 7}, {centroidID: 2, distance: 0, sourceRowOffset: 9}}
	blob := encodeClusterStatsBlock(keys)
	got, err := decodeClusterStatsBlock(blob)
	require.NoError(t, err)
	require.Equal(t, keys, got)
	for _, bad := range [][]byte{nil, blob[:10], blob[:len(blob)-1], append(append([]byte(nil), blob...), 0), encodeClusterStatsBlock([]clusterLayoutSortKey{{distance: float32(math.NaN())}}), encodeClusterStatsBlock([]clusterLayoutSortKey{{distance: -1}})} {
		_, err = decodeClusterStatsBlock(bad)
		require.Error(t, err)
	}
}

func TestClusterRowBatcherSharedBudgetAndTail(t *testing.T) {
	row := clusterLayoutSortRow{value: &storage.Value{Value: map[int64]interface{}{100: make([]byte, 100)}}}
	count, calls := 0, 0
	b := &clusterRowBatcher{limit: clusterRowBytes(row.value) * 3, write: func(_ int, rows []clusterLayoutSortRow) error { count += len(rows); calls++; return nil }}
	for i := 0; i < 10; i++ {
		require.NoError(t, b.append(i%2, row))
		require.LessOrEqual(t, b.bytes, b.limit)
	}
	require.Less(t, count, 10)
	require.NoError(t, b.flush())
	require.Equal(t, 10, count)
	require.Less(t, calls, 10)
	require.Empty(t, b.groups)
	boom := errors.New("write failed")
	b = &clusterRowBatcher{limit: 1, write: func(_ int, _ []clusterLayoutSortRow) error { return boom }}
	require.ErrorIs(t, b.append(0, row), boom)
}

func clusterStatsTestWriter(t *testing.T, segmentSize int64) (*clusterStatsWriter, storage.ChunkManager, compaction.Params) {
	t.Helper()
	root := path.Join(t.TempDir(), "objects")
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	params := compaction.Params{StorageVersion: storage.StorageV1, StorageConfig: cfg, BinLogMaxSize: 1 << 20, StorageFormat: "parquet"}
	w, err := NewMultiSegmentWriter(context.Background(), flushio.NewBinlogIO(cm), NewCompactionAllocator(allocator.NewLocalAllocator(100, 200), allocator.NewLocalAllocator(1000, 10000)), segmentSize, genCollectionSchema(), params, 100, 20, 10, "test", 1, storage.WithStorageConfig(cfg), storage.WithBufferSize(1<<20))
	require.NoError(t, err)
	return newClusterStatsWriter(w, &datapb.ClusterStats{Version: 1, ClusteringTaskId: 99, FieldId: 103, GroupId: 0, CentroidIds: []uint32{0, 1}}), cm, params
}

func clusterStatsTestRow(id int64) clusterLayoutSortRow {
	row := genRow(id)
	return clusterLayoutSortRow{value: &storage.Value{ID: id, PK: storage.NewInt64PrimaryKey(id), Timestamp: row[1].(int64), Value: row}, key: clusterLayoutSortKey{centroidID: uint32(id % 2), distance: float32(id), sourceRowOffset: uint64(id)}}
}

func TestClusterStatsWriterRotationAndPoison(t *testing.T) {
	w, cm, params := clusterStatsTestWriter(t, 1)
	rows := []clusterLayoutSortRow{clusterStatsTestRow(0), clusterStatsTestRow(1), clusterStatsTestRow(2)}
	require.NoError(t, w.WriteBatch(context.Background(), rows))
	segments, err := w.Close(context.Background())
	require.NoError(t, err)
	require.Len(t, segments, 3)
	for i, segment := range segments {
		ref := segment.GetClusterStats()
		require.EqualValues(t, 1, ref.NumRows)
		require.False(t, ref.Sorted)
		require.Len(t, ref.Files, 1)
		blob, err := cm.Read(context.Background(), ref.Files[0])
		require.NoError(t, err)
		keys, err := decodeClusterStatsBlock(blob)
		require.NoError(t, err)
		require.Equal(t, rows[i].key, keys[0])
	}
	helper := &clusteringCompactionTask{plan: &datapb.CompactionPlan{Schema: genCollectionSchema()}, collectionID: 10}
	require.Equal(t, []int64{0, 1, 2}, readClusterLayoutOutputIDs(t, helper, flushio.NewBinlogIO(cm), params.StorageConfig, segments))
	w, _, _ = clusterStatsTestWriter(t, math.MaxInt64)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.WriteBatch(ctx, rows), context.Canceled)
	require.ErrorIs(t, w.WriteBatch(context.Background(), rows), context.Canceled)
	_, err = w.Close(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}

func TestClusterStatsWriterPartialFailureNotReplayed(t *testing.T) {
	w, _, _ := clusterStatsTestWriter(t, math.MaxInt64)
	good := clusterStatsTestRow(0)
	bad := clusterStatsTestRow(1)
	bad.value.Value.(map[int64]interface{})[103] = "not a vector"
	err := w.WriteBatch(context.Background(), []clusterLayoutSortRow{good, bad, clusterStatsTestRow(2)})
	require.Error(t, err)
	require.EqualValues(t, 1, w.refs[w.segmentID].NumRows)
	require.Equal(t, err, w.WriteBatch(context.Background(), []clusterLayoutSortRow{good}))
	require.EqualValues(t, 1, w.refs[w.segmentID].NumRows)
}

func TestClusterStatsWriterRejectsOtherGroupCentroid(t *testing.T) {
	w, _, _ := clusterStatsTestWriter(t, math.MaxInt64)
	row := clusterStatsTestRow(0)
	row.key.centroidID = 2 // This writer owns only centroids 0 and 1.
	err := w.WriteBatch(context.Background(), []clusterLayoutSortRow{row})
	require.ErrorContains(t, err, "outside centroid group")
	require.Empty(t, w.refs)
	_, closeErr := w.Close(context.Background())
	require.Equal(t, err, closeErr)
}

func TestClusterStatsWriterConcurrentBatches(t *testing.T) {
	w, cm, params := clusterStatsTestWriter(t, math.MaxInt64)
	w.keyLimit = 17
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for worker := int64(0); worker < 4; worker++ {
		wg.Add(1)
		go func(worker int64) {
			defer wg.Done()
			for batch := int64(0); batch < 8; batch++ {
				rows := make([]clusterLayoutSortRow, 4)
				for i := range rows {
					rows[i] = clusterStatsTestRow(worker*32 + batch*4 + int64(i))
				}
				if err := w.WriteBatch(context.Background(), rows); err != nil {
					errCh <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	segments, err := w.Close(context.Background())
	require.NoError(t, err)
	require.Len(t, segments, 1)
	helper := &clusteringCompactionTask{plan: &datapb.CompactionPlan{Schema: genCollectionSchema()}, collectionID: 10}
	ids := readClusterLayoutOutputIDs(t, helper, flushio.NewBinlogIO(cm), params.StorageConfig, segments)
	var keys []clusterLayoutSortKey
	for _, file := range segments[0].ClusterStats.Files {
		blob, err := cm.Read(context.Background(), file)
		require.NoError(t, err)
		part, err := decodeClusterStatsBlock(blob)
		require.NoError(t, err)
		keys = append(keys, part...)
	}
	require.Len(t, ids, 128)
	require.Len(t, keys, 128)
	seen := make(map[int64]bool)
	for i, id := range ids {
		require.False(t, seen[id])
		seen[id] = true
		require.Equal(t, clusterStatsTestRow(id).key, keys[i])
	}
	again, err := w.Close(context.Background())
	require.NoError(t, err)
	require.Same(t, segments[0], again[0])
	require.ErrorContains(t, w.WriteBatch(context.Background(), []clusterLayoutSortRow{clusterStatsTestRow(0)}), "closed")
}
