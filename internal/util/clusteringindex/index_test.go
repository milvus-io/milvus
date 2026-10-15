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

package clusteringindex_test

import (
	"context"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/clusteringindex"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotIndexBuildLoadSearch(t *testing.T) {
	for _, dtype := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		t.Run(dtype.String(), func(t *testing.T) { testSnapshotIndex(t, dtype) })
	}
}

func testSnapshotIndex(t *testing.T, dtype schemapb.DataType) {
	ctx := context.Background()
	paramtable.Init()
	params := paramtable.Get()
	root := t.TempDir()
	require.NoError(t, params.Save(params.CommonCfg.StorageType.Key, "local"))
	require.NoError(t, params.Save(params.LocalStorageCfg.Path.Key, root))
	require.NoError(t, initcore.InitLocalChunkManager(root))
	require.NoError(t, initcore.InitRemoteChunkManager(params))
	require.NoError(t, initcore.InitRemoteArrowFileSystem(params))
	initcore.InitTieredStorage(params)
	field := &schemapb.FieldSchema{FieldID: 100, Name: "vector", DataType: dtype, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}}
	centroids := &clusteringpb.ClusteringCentroidsStats{}
	for i := 0; i < 256; i++ {
		centroids.Centroids = append(centroids.Centroids, &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{float32(i), float32(i % 17), 1, 2}}}})
	}
	query := make([]byte, 8*4)
	for j, id := range []int{11, 207} {
		for i, v := range centroids.Centroids[id].GetFloatVector().GetData() {
			binary.LittleEndian.PutUint32(query[(j*4+i)*4:], math.Float32bits(v))
		}
	}
	if dtype != schemapb.DataType_FloatVector {
		values := make([]float32, len(query)/4)
		for i := range values {
			values[i] = math.Float32frombits(binary.LittleEndian.Uint32(query[i*4:]))
		}
		var err error
		query, err = typeutil.ConvertFloat32ToFP16BF16Bytes(values, dtype)
		require.NoError(t, err)
		for _, c := range centroids.Centroids {
			values := c.GetFloatVector().Data
			raw, err := typeutil.ConvertFloat32ToFP16BF16Bytes(values, dtype)
			require.NoError(t, err)
			if dtype == schemapb.DataType_Float16Vector {
				c.Data = &schemapb.VectorField_Float16Vector{Float16Vector: raw}
			} else {
				c.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: raw}
			}
		}
	}
	for n, typ := range []string{"FLAT", "HNSW"} {
		t.Run(typ, func(t *testing.T) {
			version := int64(dtype)*1000 + int64(n)
			indexParams := []*commonpb.KeyValuePair{{Key: "index_type", Value: typ}, {Key: "metric_type", Value: "L2"}, {Key: "M", Value: "8"}, {Key: "efConstruction", Value: "64"}, {Key: "ef", Value: "256"}, {Key: "warmup", Value: "sync"}}
			if typ == "HNSW" {
				// Keep this a graph load/search test with exact distances. Cardinal's
				// defaults may select IVF for small dimensions and quantize vectors.
				indexParams = append(indexParams,
					&commonpb.KeyValuePair{Key: "mmap.enabled", Value: "true"},
					&commonpb.KeyValuePair{Key: "index_algo", Value: "HYBRID"},
					&commonpb.KeyValuePair{Key: "graph_search_dim_threshold", Value: "0"},
					&commonpb.KeyValuePair{Key: "graph_build_algo", Value: "pipnn"},
					&commonpb.KeyValuePair{Key: "pipnn_randseed", Value: "42"},
					&commonpb.KeyValuePair{Key: "build_quant_type", Value: "NONE"},
					&commonpb.KeyValuePair{Key: "search_quant_type", Value: "NONE"},
					&commonpb.KeyValuePair{Key: "refine_quant_type", Value: "NONE"},
				)
			}
			build := &indexcgopb.BuildIndexInfo{CollectionID: 1, PartitionID: 2, IndexVersion: version, FieldSchema: field, Dim: 4, NumRows: 256, CurrentIndexVersion: 10, IndexParams: indexParams, StorageConfig: &indexcgopb.StorageConfig{StorageType: "local", RootPath: root}}
			prefix := filepath.Join(root, "part_stats", "1", "2", "ch", typ+".clustering_index")
			metadata, err := clusteringindex.Build(ctx, build, centroids, prefix)
			require.NoError(t, err)
			require.Equal(t, int32(10), metadata.GetIndexEngineVersion())
			require.NotEmpty(t, metadata.GetIndexFilePaths())
			require.Positive(t, metadata.GetIndexSize())
			for _, file := range metadata.GetIndexFilePaths() {
				require.FileExists(t, file)
				require.Contains(t, file, prefix+"/")
			}
			first, err := clusteringindex.Load(ctx, 1, 2, "ch", version, field, metadata, 256)
			require.NoError(t, err)
			defer first.Close()
			second, err := clusteringindex.Load(ctx, 1, 2, "ch", version, field, metadata, 256)
			require.NoError(t, err)
			defer second.Close()
			first.Close()
			labels, distances, err := second.Search(ctx, query, 2, 1)
			require.NoError(t, err)
			require.Equal(t, []int64{11, 207}, labels)
			require.Equal(t, []float32{0, 0}, distances)
			_, _, err = second.Search(ctx, query[:4], 2, 1)
			require.Error(t, err)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, _, err = second.Search(canceled, query, 2, 1)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
