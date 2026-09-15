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
	"fmt"
	sio "io"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestClusterLayoutSortKeyStableOrderAndCodec(t *testing.T) {
	keys := []clusterLayoutSortKey{
		{centroidID: 1, distance: 0.1, sourceSegmentOffset: 0, sourceRowOffset: 0},
		{centroidID: 0, distance: 0.2, sourceSegmentOffset: 1, sourceRowOffset: 3},
		{centroidID: 0, distance: 0.2, sourceSegmentOffset: 0, sourceRowOffset: 9},
		{centroidID: 0, distance: 0.1, sourceSegmentOffset: 1, sourceRowOffset: 2},
	}

	mergeHeap := &clusterLayoutMergeHeap{}
	heap.Init(mergeHeap)
	for _, key := range keys {
		heap.Push(mergeHeap, &clusterLayoutRunReader{currentKey: key})
	}
	got := make([]clusterLayoutSortKey, 0, len(keys))
	for mergeHeap.Len() > 0 {
		got = append(got, heap.Pop(mergeHeap).(*clusterLayoutRunReader).currentKey)
	}
	require.Equal(t, []clusterLayoutSortKey{
		{centroidID: 0, distance: 0.1, sourceSegmentOffset: 1, sourceRowOffset: 2},
		{centroidID: 0, distance: 0.2, sourceSegmentOffset: 0, sourceRowOffset: 9},
		{centroidID: 0, distance: 0.2, sourceSegmentOffset: 1, sourceRowOffset: 3},
		{centroidID: 1, distance: 0.1, sourceSegmentOffset: 0, sourceRowOffset: 0},
	}, got)

	for _, key := range keys {
		encoded := make([]byte, clusterLayoutSortKeySize)
		encodeClusterLayoutSortKey(encoded, key)
		require.Equal(t, key, decodeClusterLayoutSortKey(encoded))
	}
}

type fakeClusterLayoutRangeWriter struct {
	segmentCapacity int
	segmentIDs      []int64
	segmentOffset   int
	written         int
}

func (w *fakeClusterLayoutRangeWriter) WriteValue(*storage.Value) error {
	if w.written >= w.segmentCapacity {
		w.segmentOffset++
		w.written = 0
	}
	w.written++
	return nil
}

func (w *fakeClusterLayoutRangeWriter) CurrentSegmentID() typeutil.UniqueID {
	return w.segmentIDs[w.segmentOffset]
}

func TestClusterLayoutRangeTrackerAndValidation(t *testing.T) {
	result := newClusterLayoutResult()
	writer := &fakeClusterLayoutRangeWriter{
		segmentCapacity: 4,
		segmentIDs:      []int64{100, 200},
	}
	tracker := newClusterLayoutRangeTracker(writer, result.CentroidRanges)
	for _, centroidID := range []uint32{0, 0, 1, 1, 1, 2, 2, 2} {
		require.NoError(t, tracker.write(&storage.Value{}, centroidID))
	}
	tracker.finish()

	require.Equal(t, []clusterLayoutRange{{SegmentID: 100, Offset: 0, Size: 2}}, result.CentroidRanges[0])
	require.Equal(t, []clusterLayoutRange{
		{SegmentID: 100, Offset: 2, Size: 2},
		{SegmentID: 200, Offset: 0, Size: 1},
	}, result.CentroidRanges[1])
	require.Equal(t, []clusterLayoutRange{{SegmentID: 200, Offset: 1, Size: 3}}, result.CentroidRanges[2])
	require.NoError(t, result.Validate([]*datapb.CompactionSegment{
		{SegmentID: 100, NumOfRows: 4},
		{SegmentID: 200, NumOfRows: 4},
	}))
}

func TestClusterLayoutResultRejectsInvalidRanges(t *testing.T) {
	segments := []*datapb.CompactionSegment{{SegmentID: 100, NumOfRows: 3}}
	for name, result := range map[string]*clusterLayoutResult{
		"gap": {
			CentroidRanges: map[uint32][]clusterLayoutRange{
				0: {{SegmentID: 100, Offset: 1, Size: 2}},
			},
		},
		"overlap": {
			CentroidRanges: map[uint32][]clusterLayoutRange{
				0: {{SegmentID: 100, Offset: 0, Size: 2}},
				1: {{SegmentID: 100, Offset: 1, Size: 2}},
			},
		},
		"multiple ranges for one centroid": {
			CentroidRanges: map[uint32][]clusterLayoutRange{
				0: {
					{SegmentID: 100, Offset: 0, Size: 1},
					{SegmentID: 100, Offset: 1, Size: 2},
				},
			},
		},
		"unknown segment": {
			CentroidRanges: map[uint32][]clusterLayoutRange{
				0: {{SegmentID: 200, Offset: 0, Size: 3}},
			},
		},
		"out of bounds": {
			CentroidRanges: map[uint32][]clusterLayoutRange{
				0: {{SegmentID: 100, Offset: 0, Size: 4}},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, result.Validate(segments))
		})
	}
}

func TestCalculateClusterLayoutSubRunRows(t *testing.T) {
	require.Equal(t, int64(8), calculateClusterLayoutSubRunRows(10_000, 2, 100))
	require.Equal(t, int64(1), calculateClusterLayoutSubRunRows(100, 8, 100))
	require.Equal(t, int64(1), calculateClusterLayoutSubRunRows(math.MaxInt64, 1, math.MaxInt))
}

func TestClusterLayoutSpillPoolSize(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key, "2"))
	defer params.Reset(params.DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key)
	require.NoError(t, params.Save(params.DataNodeCfg.ClusteringCompactionSpillPoolSize.Key, "0"))
	defer params.Reset(params.DataNodeCfg.ClusteringCompactionSpillPoolSize.Key)

	task := &clusteringCompactionTask{}
	require.Equal(t, 2, task.getSpillPoolSize())
	require.NoError(t, params.Save(params.DataNodeCfg.ClusteringCompactionSpillPoolSize.Key, "3"))
	require.Equal(t, 3, task.getSpillPoolSize())
}

func TestClusterLayoutSpillCleanupAndCancellation(t *testing.T) {
	root := path.Join(t.TempDir(), "spill")
	require.NoError(t, os.MkdirAll(root, 0o700))
	require.NoError(t, os.WriteFile(path.Join(root, "run"), []byte("temporary"), 0o600))
	spiller := &clusterLayoutSpiller{root: root}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	spiller.cleanup(ctx)
	_, err := os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)

	task := &clusteringCompactionTask{}
	_, _, err = task.mappingClusterLayoutSorted(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestClusterLayoutSortedEngineCleansSpillOnFailure(t *testing.T) {
	root := t.TempDir()
	localRoot := path.Join(root, "local")
	outputRoot := path.Join(root, "output")
	params := paramtable.Get()
	require.NoError(t, params.Save(params.LocalStorageCfg.Path.Key, localRoot))
	defer params.Reset(params.LocalStorageCfg.Path.Key)

	storageConfig := &indexpb.StorageConfig{StorageType: "local", RootPath: outputRoot}
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(outputRoot))
	task := &clusteringCompactionTask{
		binlogIO: flushio.NewBinlogIO(chunkManager),
		plan: &datapb.CompactionPlan{
			PlanID: 99,
			Schema: genCollectionSchema(),
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
				CollectionID: 10,
				PartitionID:  20,
				SegmentID:    30,
			}},
		},
		primaryKeyField:        genCollectionSchema().GetFields()[2],
		currentTime:            time.Now(),
		memoryLimit:            1 << 20,
		compactionParams:       compaction.Params{StorageConfig: storageConfig},
		writtenRowNum:          atomic.NewInt64(0),
		layoutPlan:             &clustercompaction.LayoutPlan{CentroidCount: 1},
		segmentIDOffsetMapping: map[int64]string{},
		spillPool:              conc.NewPool[any](1),
	}
	defer task.spillPool.Release()

	_, _, err := task.mappingClusterLayoutSorted(context.Background())
	require.ErrorContains(t, err, "missing clustering assignment artifact")
	_, err = os.Stat(path.Join(localRoot, "cluster_layout_compaction", "99"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestClusterLayoutSortedEngineMultipleInputsAndSubRuns(t *testing.T) {
	root := t.TempDir()
	localRoot := path.Join(root, "local")
	outputRoot := path.Join(root, "output")
	require.NoError(t, paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, localRoot))
	defer paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	require.NoError(t, os.MkdirAll(outputRoot, 0o700))

	schema := genCollectionSchema()
	outputStorage := &indexpb.StorageConfig{StorageType: "local", RootPath: outputRoot}
	params := compaction.Params{
		StorageVersion: storage.StorageV1,
		StorageFormat:  "parquet",
		BinLogMaxSize:  1 << 20,
		StorageConfig:  outputStorage,
	}
	outputCM := storage.NewLocalChunkManager(objectstorage.RootPath(outputRoot))
	outputIO := flushio.NewBinlogIO(outputCM)
	inputSegments := []*datapb.CompactionSegmentBinlogs{
		writeClusterLayoutInput(t, outputIO, outputStorage, params, schema, 10, []int64{0, 1, 2}),
		writeClusterLayoutInput(t, outputIO, outputStorage, params, schema, 20, []int64{3, 4, 5}),
	}
	writeAssignment := func(segmentID int64, stats *clusteringpb.ClusteringCentroidIdMappingStats) string {
		mappingPath := path.Join(outputRoot, "assignments", fmt.Sprintf("%d", segmentID))
		payload, err := proto.Marshal(stats)
		require.NoError(t, err)
		require.NoError(t, outputCM.Write(context.Background(), mappingPath, payload))
		return mappingPath
	}
	assignmentPaths := map[int64]string{
		10: writeAssignment(10, &clusteringpb.ClusteringCentroidIdMappingStats{
			CentroidIdMapping:  []uint32{1, 0, 0},
			NumInCentroid:      []int64{2, 1},
			DistanceToCentroid: []float32{0.5, 0.9, 0.2},
		}),
		20: writeAssignment(20, &clusteringpb.ClusteringCentroidIdMappingStats{
			CentroidIdMapping:  []uint32{0, 1, 0},
			NumInCentroid:      []int64{2, 1},
			DistanceToCentroid: []float32{0.3, 0.1, 0.2},
		}),
	}
	layoutPlan := &clustercompaction.LayoutPlan{
		Format:         clustercompaction.LayoutPlanFormat,
		RowCount:       6,
		CentroidCount:  2,
		CentroidCounts: []int64{4, 2},
		CentroidGroups: []clustercompaction.CentroidGroup{{
			CentroidGroupID: 0,
			Rows:            6,
			Centroids:       []uint32{0, 1},
		}},
	}
	require.NoError(t, layoutPlan.Validate())
	task := &clusteringCompactionTask{
		ctx:                    context.Background(),
		binlogIO:               outputIO,
		segIDAlloc:             allocator.NewLocalAllocator(100, 200),
		logIDAlloc:             allocator.NewLocalAllocator(1000, 2000),
		plan:                   &datapb.CompactionPlan{PlanID: 1, MaxSize: math.MaxInt64, MaxSegmentRows: 100, Schema: schema, Channel: "test", SegmentBinlogs: inputSegments},
		collectionID:           10,
		partitionID:            20,
		currentTime:            time.Now(),
		primaryKeyField:        schema.GetFields()[2],
		clusteringKeyField:     schema.GetFields()[5],
		isVectorClusteringKey:  true,
		ttlFieldID:             -1,
		bufferSize:             1 << 20,
		memoryLimit:            1,
		compactionParams:       params,
		writtenRowNum:          atomic.NewInt64(0),
		segmentIDOffsetMapping: assignmentPaths,
		layoutPlan:             layoutPlan,
		centroidGroupIndex:     map[uint32]int{0: 0, 1: 0},
		mappingPool:            conc.NewPool[any](1),
		spillPool:              conc.NewPool[any](2),
	}
	defer task.mappingPool.Release()
	defer task.spillPool.Release()

	fieldStats, err := storage.NewFieldStats(103, schema.GetFields()[5].GetDataType(), 0)
	require.NoError(t, err)
	outputWriter, err := NewMultiSegmentWriter(
		context.Background(), outputIO,
		NewCompactionAllocator(task.segIDAlloc, task.logIDAlloc),
		math.MaxInt64, schema, params, 100, 20, 10, "test", 100,
		storage.WithBufferSize(1<<20), storage.WithStorageConfig(outputStorage),
	)
	require.NoError(t, err)
	task.clusterBuffers = []*ClusterBuffer{newClusterBuffer(0, outputWriter, fieldStats)}

	segments, partitionStats, err := task.mappingClusterLayoutSorted(context.Background())
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Contains(t, partitionStats.SegmentStats, segments[0].GetSegmentID())
	require.NoError(t, task.layoutResult.Validate(segments))
	require.Equal(t, []clusterLayoutRange{{SegmentID: segments[0].GetSegmentID(), Offset: 0, Size: 4}}, task.layoutResult.CentroidRanges[0])
	require.Equal(t, []clusterLayoutRange{{SegmentID: segments[0].GetSegmentID(), Offset: 4, Size: 2}}, task.layoutResult.CentroidRanges[1])
	require.Equal(t, []int64{2, 5, 3, 1, 4, 0}, readClusterLayoutOutputIDs(t, task, outputIO, outputStorage, segments))
	_, err = os.Stat(path.Join(localRoot, "cluster_layout_compaction", "1"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func writeClusterLayoutInput(
	t *testing.T,
	binlogIO flushio.BinlogIO,
	storageConfig *indexpb.StorageConfig,
	params compaction.Params,
	schema *schemapb.CollectionSchema,
	segmentID int64,
	rowIDs []int64,
) *datapb.CompactionSegmentBinlogs {
	t.Helper()
	writer, err := NewMultiSegmentWriter(
		context.Background(), binlogIO,
		NewCompactionAllocator(
			allocator.NewLocalAllocator(segmentID, segmentID+1),
			allocator.NewLocalAllocator(segmentID*100, segmentID*100+100),
		),
		math.MaxInt64, schema, params, 100, 20, 10, "test", 100,
		storage.WithBufferSize(1<<20), storage.WithStorageConfig(storageConfig),
	)
	require.NoError(t, err)
	for _, rowID := range rowIDs {
		row := genRow(rowID)
		require.NoError(t, writer.WriteValue(&storage.Value{
			ID:        rowID,
			PK:        storage.NewInt64PrimaryKey(rowID),
			Timestamp: row[1].(int64),
			Value:     row,
		}))
	}
	require.NoError(t, writer.Close())
	segments := writer.GetCompactionSegments()
	require.Len(t, segments, 1)
	return &datapb.CompactionSegmentBinlogs{
		CollectionID:   10,
		PartitionID:    20,
		SegmentID:      segments[0].GetSegmentID(),
		FieldBinlogs:   segments[0].GetInsertLogs(),
		StorageVersion: segments[0].GetStorageVersion(),
		Manifest:       segments[0].GetManifest(),
	}
}

func readClusterLayoutOutputIDs(
	t *testing.T,
	task *clusteringCompactionTask,
	binlogIO flushio.BinlogIO,
	storageConfig *indexpb.StorageConfig,
	segments []*datapb.CompactionSegment,
) []int64 {
	t.Helper()
	ids := make([]int64, 0)
	for _, segment := range segments {
		reader, _, err := newCompactionSegmentRecordReader(
			context.Background(),
			&datapb.CompactionSegmentBinlogs{
				SegmentID:      segment.GetSegmentID(),
				FieldBinlogs:   segment.GetInsertLogs(),
				StorageVersion: segment.GetStorageVersion(),
				Manifest:       segment.GetManifest(),
			},
			task.plan.GetSchema(),
			storageConfig,
			storage.WithDownloader(binlogIO.Download),
			storage.WithCollectionID(task.collectionID),
			storage.WithVersion(segment.GetStorageVersion()),
			storage.WithBufferSize(1<<20),
			storage.WithStorageConfig(storageConfig),
		)
		require.NoError(t, err)
		for {
			record, err := reader.Next()
			if err == sio.EOF {
				break
			}
			require.NoError(t, err)
			values := make([]*storage.Value, record.Len())
			require.NoError(t, storage.ValueDeserializerWithSchema(record, values, task.plan.GetSchema(), true))
			for _, value := range values {
				ids = append(ids, value.ID)
			}
		}
		reader.Close()
	}
	return ids
}
