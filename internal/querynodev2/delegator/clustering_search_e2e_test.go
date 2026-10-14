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

package delegator

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clusteringindex"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Only the RPC transport is replaced by a proto round trip. Both centroid and
// segment indexes, worker SearchTask, CGo and reduction execute for real.
func TestClusteringSearchPipeline(t *testing.T) {
	for _, dtype := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		t.Run(dtype.String(), func(t *testing.T) {
			// Native chunk-manager and cache singletons belong to a server process.
			// Isolate each storage root/dtype from unrelated package tests.
			if os.Getenv("MILVUS_CLUSTER_SEARCH_CHILD") == "1" {
				testClusteringSearchPipeline(t, dtype)
				return
			}
			executable, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.Command(executable, "-test.run=^TestClusteringSearchPipeline$/^"+dtype.String()+"$", "-test.v", "-test.timeout=3m") // #nosec G204 -- Only this test executable and fixed dtype cases run.
			cmd.Env = append(os.Environ(), "MILVUS_CLUSTER_SEARCH_CHILD=1")
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

type clusteringSearchInput struct {
	topk      int64
	dtype     schemapb.DataType
	dim       int64
	vectors   [][]float32
	pks       [][]int64
	ranges    [][]*clusteringpb.ClusteringRange
	centroids [][]float32
}

type clusteringSearchFixture struct {
	topk     int64
	sd       *shardDelegator
	rows     map[int64]int64
	scope    []SnapshotItem
	bindings map[int64]*partitionStatsBinding
	dtype    schemapb.DataType
	dim      int64
}

func newClusteringSearchFixture(t testing.TB, input clusteringSearchInput) *clusteringSearchFixture {
	ctx := context.Background()
	if input.topk == 0 {
		input.topk = 4
	}
	paramtable.Init()
	params := paramtable.Get()
	root := t.TempDir()
	for key, value := range map[string]string{params.CommonCfg.StorageType.Key: "local", params.LocalStorageCfg.Path.Key: root, params.QueryNodeCfg.EnableSegmentPrune.Key: "true", params.QueryNodeCfg.DefaultSegmentFilterRatio.Key: "0.25"} {
		original := params.GetWithDefault(key, "")
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { params.Save(key, original) })
	}
	require.NoError(t, initcore.InitLocalChunkManager(root))
	require.NoError(t, initcore.InitRemoteChunkManager(params))
	require.NoError(t, initcore.InitRemoteArrowFileSystem(params))
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitTieredStorage(params)
	require.NoError(t, initcore.InitMmapManager(params, 1))
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	schema := &schemapb.CollectionSchema{Name: "clustering_search", Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: input.dtype, TypeParams: funcutil.Map2KeyValuePair(map[string]string{"dim": strconv.FormatInt(input.dim, 10)})},
	}}
	ip := funcutil.Map2KeyValuePair(map[string]string{"index_type": "HNSW", "metric_type": "L2", "M": "8", "efConstruction": "64", "ef": "128", "index_algo": "HYBRID", "graph_search_dim_threshold": "0", "graph_build_algo": "pipnn", "build_quant_type": "NONE", "search_quant_type": "NONE", "refine_quant_type": "NONE", "warmup": "sync", common.LoadPriorityKey: "HIGH", "dim": strconv.FormatInt(input.dim, 10)})
	require.NoError(t, function.GetManager().Alloc(1, delegatorFunctionRunnerKey("hint-ch_1v0"), schema))
	t.Cleanup(func() { function.GetManager().Release(1, delegatorFunctionRunnerKey("hint-ch_1v0")) })
	manager := segments.NewManager()
	require.NoError(t, manager.Collection.PutOrRef(1, schema, &segcorepb.CollectionIndexMeta{MaxIndexRowCount: 1 << 20, IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 101, IndexParams: ip}}}, &querypb.LoadMetaInfo{CollectionID: 1, PartitionIDs: []int64{2}, LoadType: querypb.LoadType_LoadCollection}))
	collection := manager.Collection.Get(1)
	t.Cleanup(func() { manager.Collection.Unref(1, 1) })
	stats := &storage.PartitionStatsSnapshot{Version: 100, SegmentStats: map[int64]storage.SegmentStats{}}
	all := []SnapshotItem{{NodeID: 1}}
	rows := map[int64]int64{}
	for segNo, vectors := range input.vectors {
		id := int64(10 + segNo)
		pk := input.pks[segNo]
		numRows := int64(len(pk))
		rows[id] = numRows
		ts := make([]int64, numRows)
		for i := range ts {
			ts[i] = 1
		}
		data := &storage.InsertData{Data: map[int64]storage.FieldData{0: &storage.Int64FieldData{Data: pk}, 1: &storage.Int64FieldData{Data: ts}, 100: &storage.Int64FieldData{Data: pk}, 101: &storage.FloatVectorFieldData{Data: vectors, Dim: int(input.dim)}}}
		if input.dtype != schemapb.DataType_FloatVector {
			raw, err := typeutil.ConvertFloat32ToFP16BF16Bytes(vectors, input.dtype)
			require.NoError(t, err)
			if input.dtype == schemapb.DataType_Float16Vector {
				data.Data[101] = &storage.Float16VectorFieldData{Data: raw, Dim: int(input.dim)}
			} else {
				data.Data[101] = &storage.BFloat16VectorFieldData{Data: raw, Dim: int(input.dim)}
			}
		}
		blobs, err := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: 1, Schema: schema}).Serialize(2, id, data)
		require.NoError(t, err)
		var logs []*datapb.FieldBinlog
		var inserts []string
		for _, blob := range blobs {
			field, err := strconv.ParseInt(blob.Key, 10, 64)
			require.NoError(t, err)
			file := filepath.Join(root, "insert_log", "1", "2", strconv.FormatInt(id, 10), blob.Key, "1")
			require.NoError(t, cm.Write(ctx, file, blob.Value))
			logs = append(logs, &datapb.FieldBinlog{FieldID: field, Binlogs: []*datapb.Binlog{{LogPath: file, EntriesNum: numRows, MemorySize: int64(len(blob.Value))}}})
			if field == 101 {
				inserts = append(inserts, file)
			}
		}
		idx, err := indexcgowrapper.CreateIndex(ctx, &indexcgopb.BuildIndexInfo{CollectionID: 1, PartitionID: 2, SegmentID: id, BuildID: id, IndexVersion: 1, FieldSchema: schema.Fields[3], Dim: input.dim, NumRows: numRows, CurrentIndexVersion: 10, IndexParams: ip, InsertFiles: inserts, StorageConfig: &indexcgopb.StorageConfig{StorageType: "local", RootPath: root}})
		require.NoError(t, err)
		uploaded, err := idx.UpLoad()
		require.NoError(t, err)
		require.NoError(t, idx.Delete())
		var files []string
		var size int64
		for _, f := range uploaded.GetSerializedIndexInfos() {
			files = append(files, f.GetFileName())
			size += f.GetFileSize()
		}
		load := &querypb.SegmentLoadInfo{CollectionID: 1, PartitionID: 2, SegmentID: id, InsertChannel: "hint-ch_1v0", NumOfRows: numRows, Level: datapb.SegmentLevel_L2, BinlogPaths: logs, IndexInfos: []*querypb.FieldIndexInfo{{FieldID: 101, IndexID: 1, BuildID: id, IndexVersion: 1, CurrentIndexVersion: 10, NumRows: numRows, EnableIndex: true, IndexParams: ip, IndexFilePaths: files, IndexSize: size}}}
		seg, err := segments.NewSegment(ctx, collection, manager.Segment, segments.SegmentTypeSealed, 1, load)
		require.NoError(t, err)
		local := seg.(*segments.LocalSegment)
		guard, err := local.StartLoadData()
		require.NoError(t, err)
		err = local.Load(ctx)
		guard.Done(err)
		require.NoError(t, err)
		manager.Segment.Put(ctx, segments.SegmentTypeSealed, seg)
		t.Cleanup(func() { seg.Release(ctx) })
		all[0].Segments = append(all[0].Segments, SegmentEntry{NodeID: 1, SegmentID: id, PartitionID: 2})
		stats.SegmentStats[id] = storage.SegmentStats{NumRows: int(numRows), ClusteringRanges: input.ranges[segNo]}
	}
	centers := &clusteringpb.ClusteringCentroidsStats{}
	for _, v := range input.centroids {
		centers.Centroids = append(centers.Centroids, &schemapb.VectorField{Dim: input.dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: v}}})
	}
	if input.dtype != schemapb.DataType_FloatVector {
		for _, c := range centers.Centroids {
			raw, err := typeutil.ConvertFloat32ToFP16BF16Bytes(c.GetFloatVector().Data, input.dtype)
			require.NoError(t, err)
			if input.dtype == schemapb.DataType_Float16Vector {
				c.Data = &schemapb.VectorField_Float16Vector{Float16Vector: raw}
			} else {
				c.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: raw}
			}
		}
	}
	metadata, err := clusteringindex.Build(ctx, &indexcgopb.BuildIndexInfo{CollectionID: 1, PartitionID: 2, IndexVersion: 100, FieldSchema: schema.Fields[3], Dim: input.dim, NumRows: int64(len(input.centroids)), CurrentIndexVersion: 10, IndexParams: funcutil.Map2KeyValuePair(map[string]string{"index_type": "FLAT", "metric_type": "L2"}), StorageConfig: &indexcgopb.StorageConfig{StorageType: "local", RootPath: root}}, centers, filepath.Join(root, "part_stats/1/2/hint-ch_1v0/100.clustering_index"))
	require.NoError(t, err)
	stats.ClusteringStats = &clusteringpb.PartitionClusteringStats{FormatVersion: 1, FieldId: 101, DataType: input.dtype, Dim: input.dim, NumClusters: uint32(len(input.centroids)), ClustersFile: "clusters", ClusteringIndex: metadata}
	var segmentIDs []int64
	for _, seg := range all[0].Segments {
		segmentIDs = append(segmentIDs, seg.SegmentID)
		stats.ClusteringStats.Groups = append(stats.ClusteringStats.Groups, &clusteringpb.ClusteringGroup{SegmentIds: []int64{seg.SegmentID}})
	}
	require.NoError(t, stats.ValidateClusteringStats())
	centroidIndex, err := clusteringindex.Load(ctx, 1, 2, "hint-ch_1v0", 100, schema.Fields[3], metadata, int64(len(input.centroids)))
	require.NoError(t, err)
	t.Cleanup(centroidIndex.Close)
	binding := &partitionStatsBinding{stats: stats, index: centroidIndex, locations: clusteringLocations(stats)}
	d := NewDistribution("hint-ch_1v0", NewChannelQueryView(nil, rows, []int64{2}, 1))
	d.AddDistributions(all[0].Segments...)
	d.SyncTargetVersion(&querypb.SyncAction{TargetVersion: 1, SealedInTarget: segmentIDs, SealedSegmentRowCount: rows}, []int64{2}, map[int64]*partitionStatsBinding{2: binding})
	_, _, pin := d.PinOnlineSegments()
	t.Cleanup(func() { d.Unpin(pin) })
	wm := &cluster.MockManager{}
	worker := &cluster.MockWorker{}
	wm.EXPECT().GetWorker(mock.Anything, int64(1)).Return(worker, nil)
	worker.EXPECT().SearchSegments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *querypb.SearchRequest) (*internalpb.SearchResults, error) {
		raw, err := proto.Marshal(request)
		require.NoError(t, err)
		copy := &querypb.SearchRequest{}
		require.NoError(t, proto.Unmarshal(raw, copy))
		task := tasks.NewSearchTask(ctx, collection, manager, copy, 1)
		err = task.Execute()
		if err != nil {
			return nil, err
		}
		return task.SearchResult(), nil
	})
	sd := &shardDelegator{collectionID: 1, vchannelName: "hint-ch_1v0", collection: collection, workerManager: wm, distribution: d}
	return &clusteringSearchFixture{sd: sd, rows: rows, scope: all, bindings: d.pinnedStatsBindings(pin), dtype: input.dtype, dim: input.dim, topk: input.topk}
}

func (f *clusteringSearchFixture) search(ctx context.Context, t testing.TB, algo string, queries [][]float32, scope []SnapshotItem, rows map[int64]int64, hints bool) *schemapb.SearchResultData {
	t.Helper()
	placeholderType := commonpb.PlaceholderType_FloatVector
	vectorType := planpb.VectorType_FloatVector
	if f.dtype == schemapb.DataType_Float16Vector {
		placeholderType = commonpb.PlaceholderType_Float16Vector
		vectorType = planpb.VectorType_Float16Vector
	}
	if f.dtype == schemapb.DataType_BFloat16Vector {
		placeholderType = commonpb.PlaceholderType_BFloat16Vector
		vectorType = planpb.VectorType_BFloat16Vector
	}
	values := make([][]byte, len(queries))
	for i, q := range queries {
		if f.dtype == schemapb.DataType_FloatVector {
			values[i] = make([]byte, len(q)*4)
			for j, v := range q {
				binary.LittleEndian.PutUint32(values[i][j*4:], math.Float32bits(v))
			}
		} else {
			var err error
			values[i], err = typeutil.ConvertFloat32ToFP16BF16Bytes(q, f.dtype)
			require.NoError(t, err)
		}
	}
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: placeholderType, Values: values}}})
	require.NoError(t, err)
	expr, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 101, VectorType: vectorType, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: f.topk, MetricType: "L2", SearchParams: `{"ef":128,"index_algo":"` + algo + `"}`, RoundDecimal: -1}}}})
	require.NoError(t, err)
	request := &querypb.SearchRequest{Req: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}, CollectionID: 1, PartitionIDs: []int64{2}, FieldId: 101, Nq: int64(len(queries)), Topk: f.topk, MetricType: "L2", MvccTimestamp: typeutil.MaxTimestamp, PlaceholderGroup: placeholder, SerializedExprPlan: expr}, DmlChannels: []string{"hint-ch_1v0"}}
	bindings := f.bindings
	if !hints {
		bindings = nil
	}
	results, err := f.sd.search(ctx, request, scope, nil, rows, bindings)
	require.NoError(t, err)
	require.Len(t, results, 1)
	decoded, err := segments.DecodeSearchResults(ctx, results)
	require.NoError(t, err)
	return decoded[0]
}

func testClusteringSearchPipeline(t *testing.T, dtype schemapb.DataType) {
	input := clusteringSearchInput{dtype: dtype, dim: 8}
	for seg := 0; seg < 2; seg++ {
		v, pk := make([]float32, 128*8), make([]int64, 128)
		for i := range pk {
			pk[i] = int64(seg*128 + i)
			v[i*8] = float32(seg*2+i/64) * 10
			v[i*8+1] = float32(i%64) / 64
		}
		input.vectors = append(input.vectors, v)
		input.pks = append(input.pks, pk)
		input.ranges = append(input.ranges, []*clusteringpb.ClusteringRange{{ClusterOffset: uint32(seg * 2), RowCount: 64}, {ClusterOffset: uint32(seg*2 + 1), RowOffset: 64, RowCount: 64}})
	}
	for i := 0; i < 4; i++ {
		v := make([]float32, 8)
		v[0] = float32(i) * 10
		input.centroids = append(input.centroids, v)
	}
	f := newClusteringSearchFixture(t, input)
	queries := [][]float32{make([]float32, 8), make([]float32, 8)}
	queries[1][0] = 20
	scope := []SnapshotItem{{NodeID: 1, Segments: f.scope[0].Segments[1:]}}
	for _, algo := range []string{"BF", "GRAPH"} {
		t.Run(algo, func(t *testing.T) {
			result := f.search(context.Background(), t, algo, queries, scope, f.rows, true)
			require.Equal(t, []int64{0, 4}, result.GetTopks())
			require.Equal(t, int64(128), result.GetIds().GetIntId().GetData()[0])
			badRows := map[int64]int64{10: 128, 11: 129}
			result = f.search(context.Background(), t, algo, queries, scope, badRows, true)
			require.Equal(t, []int64{4, 4}, result.GetTopks())
		})
	}
}
