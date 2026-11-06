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

package loadresource

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestEstimateTantivyValidityBitmapBytesWordAligned(t *testing.T) {
	require.EqualValues(t, 0, estimateTantivyValidityBitmapBytes(-1))
	require.EqualValues(t, 0, estimateTantivyValidityBitmapBytes(0))
	require.EqualValues(t, 8, estimateTantivyValidityBitmapBytes(1))
	require.EqualValues(t, 8, estimateTantivyValidityBitmapBytes(64))
	require.EqualValues(t, 16, estimateTantivyValidityBitmapBytes(65))
	require.EqualValues(t, 24, estimateTantivyValidityBitmapBytes(129))
}

func TestEstimateSegmentFinalResourceUsesBinlogStatsAndTieredRatio(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_final_estimate",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
			{FieldID: 102, Name: "json", DataType: schemapb.DataType_JSON},
			{FieldID: 103, Name: "bm25", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    10,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: common.TimeStampField,
				Binlogs: []*datapb.Binlog{
					{MemorySize: 80, LogSize: 50, EntriesNum: 10},
				},
			},
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{LogSize: 160},
				},
			},
			{
				FieldID: 102,
				Binlogs: []*datapb.Binlog{
					{MemorySize: 200, LogSize: 120},
				},
			},
		},
		Statslogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{MemorySize: 30, LogSize: 20}}},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{MemorySize: 40, LogSize: 40}}},
		},
		Bm25Logs: []*datapb.FieldBinlog{
			{FieldID: 103, Binlogs: []*datapb.Binlog{{MemorySize: 40, LogSize: 20}}},
		},
		JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
			102: {MemorySize: 100, LogSize: 90},
		},
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			102: {MemorySize: 50, LogSize: 40},
		},
	}

	for _, test := range []struct {
		name           string
		tieredEnabled  bool
		expectedMemory uint64
		expectedDisk   uint64
	}{
		{name: "disabled uses full evictable resource", expectedMemory: 778, expectedDisk: 310},
		{name: "enabled applies cache ratios", tieredEnabled: true, expectedMemory: 484, expectedDisk: 78},
	} {
		t.Run(test.name, func(t *testing.T) {
			usage, err := EstimateSegmentFinalResource(context.Background(), schema, loadInfo, SegmentFinalEstimateOptions{
				MmapVectorField:                    true,
				MmapScalarField:                    false,
				MmapJSONStats:                      true,
				TieredEvictionEnabled:              test.tieredEnabled,
				TieredEvictableMemoryCacheRatio:    0.5,
				TieredEvictableDiskCacheRatio:      0.25,
				DeltaDataExpansionFactor:           2,
				JSONKeyStatsExpansionFactor:        1.5,
				TextIndexExpansionFactor:           2,
				PreferFieldDataWhenIndexHasRawData: false,
			}, nil)

			require.NoError(t, err)
			require.Equal(t, test.expectedMemory, usage.MemoryBytes)
			require.Equal(t, test.expectedDisk, usage.DiskBytes)
		})
	}
}

func TestEstimateSegmentLoadingResourceAccountsForTantivyValidityBitmap(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_text_loading_estimate",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		NumOfRows: 65,
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			101: {FieldID: 101, MemorySize: 100},
		},
	}

	for _, test := range []struct {
		name           string
		mmap           bool
		tiered         bool
		expectedMemory uint64
		expectedDisk   uint64
	}{
		{name: "non-mmap", expectedMemory: 116},
		{name: "mmap", mmap: true, expectedMemory: 16, expectedDisk: 100},
		{name: "tiered skips loading reservation", tiered: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{
				MmapScalarField:          test.mmap,
				TieredEvictionEnabled:    test.tiered,
				TextIndexExpansionFactor: 1,
			}, nil)

			require.NoError(t, err)
			require.Equal(t, test.expectedMemory, usage.MemoryBytes)
			require.Equal(t, test.expectedDisk, usage.DiskBytes)
		})
	}
}

func TestEstimateSegmentFinalResourceAllowsZeroTieredRatio(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_zero_tiered_ratio",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    10,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{MemorySize: 100, LogSize: 80},
				},
			},
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{MemorySize: 200, LogSize: 160},
				},
			},
		},
		Statslogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{MemorySize: 30, LogSize: 20}}},
		},
	}

	usage, err := EstimateSegmentFinalResource(context.Background(), schema, loadInfo, SegmentFinalEstimateOptions{
		MmapVectorField:                 true,
		TieredEvictionEnabled:           true,
		TieredEvictableMemoryCacheRatio: 0,
		TieredEvictableDiskCacheRatio:   0,
	}, nil)

	require.NoError(t, err)
	require.EqualValues(t, 30, usage.MemoryBytes)
	require.Zero(t, usage.DiskBytes)
}

func TestEstimateSegmentFinalResourceOnlyExpandsLegacyDelta(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test_delta_estimate"}

	for _, test := range []struct {
		name           string
		memorySize     int64
		logSize        int64
		expectedMemory uint64
	}{
		{
			name:           "storage v3 summary has no disk size",
			memorySize:     100,
			expectedMemory: 100,
		},
		{
			name:           "legacy compressed size uses expansion",
			memorySize:     100,
			logSize:        100,
			expectedMemory: 300,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			usage, err := EstimateSegmentFinalResource(context.Background(), schema, &querypb.SegmentLoadInfo{
				Deltalogs: []*datapb.FieldBinlog{{
					Binlogs: []*datapb.Binlog{{MemorySize: test.memorySize, LogSize: test.logSize}},
				}},
			}, SegmentFinalEstimateOptions{DeltaDataExpansionFactor: 3}, nil)

			require.NoError(t, err)
			require.Equal(t, test.expectedMemory, usage.MemoryBytes)
			require.Zero(t, usage.DiskBytes)
		})
	}
}

func TestEstimateSegmentFinalResourceSkipsDroppedFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_dropped_fields",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	t.Run("index", func(t *testing.T) {
		runnerCalled := false
		usage, err := EstimateSegmentFinalResource(context.Background(), schema, &querypb.SegmentLoadInfo{
			IndexInfos: []*querypb.FieldIndexInfo{
				{FieldID: 999, IndexFilePaths: []string{"index"}},
			},
			BinlogPaths: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 64}}},
			},
		}, SegmentFinalEstimateOptions{}, func(fn func() error) error {
			runnerCalled = true
			return fn()
		})

		require.NoError(t, err)
		require.False(t, runnerCalled)
		require.EqualValues(t, 64, usage.MemoryBytes)
		require.Zero(t, usage.DiskBytes)
	})

	t.Run("binlog group with only dropped fields", func(t *testing.T) {
		usage, err := EstimateSegmentFinalResource(context.Background(), schema, &querypb.SegmentLoadInfo{
			BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID:     200,
					ChildFields: []int64{999},
					Binlogs:     []*datapb.Binlog{{MemorySize: 128}},
				},
				{
					FieldID: 998,
					Binlogs: []*datapb.Binlog{{MemorySize: 128}},
				},
			},
		}, SegmentFinalEstimateOptions{}, nil)

		require.NoError(t, err)
		require.Zero(t, usage.MemoryBytes)
		require.Zero(t, usage.DiskBytes)
	})

	t.Run("binlog group with live and dropped fields", func(t *testing.T) {
		usage, err := EstimateSegmentFinalResource(context.Background(), schema, &querypb.SegmentLoadInfo{
			BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID:     200,
					ChildFields: []int64{999, 100},
					Binlogs:     []*datapb.Binlog{{MemorySize: 128}},
				},
			},
		}, SegmentFinalEstimateOptions{MmapVectorField: true}, nil)

		require.NoError(t, err)
		require.EqualValues(t, 128, usage.MemoryBytes)
		require.Zero(t, usage.DiskBytes)
	})
}

func TestEstimateSegmentLoadingResourceSkipsCAGRAGPUMemoryWhenAdaptForCPU(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_gpu_adapt_for_cpu",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    10,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:        101,
				IndexID:        10,
				BuildID:        11,
				IndexFilePaths: []string{"index"},
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "GPU_CAGRA"},
					{Key: common.MetricTypeKey, Value: metric.L2},
					{Key: "adapt_for_cpu", Value: "true"},
				},
			},
		},
	}

	usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{}, func(func() error) error {
		return nil
	})

	require.NoError(t, err)
	require.Empty(t, usage.FieldGPUMemoryBytes)
}

func TestEstimateSegmentLoadingResourceKeepsMilvusTableRealPKEager(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name:           "test_milvus_table_real_pk",
		ExternalSource: "s3://bucket/source",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:       100,
				Name:          "pk",
				DataType:      schemapb.DataType_Int64,
				IsPrimaryKey:  true,
				ExternalField: "pk",
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.WarmupKey, Value: common.WarmupDisable},
				},
			},
			{
				FieldID:       101,
				Name:          "vec",
				DataType:      schemapb.DataType_FloatVector,
				ExternalField: "vec",
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
					{Key: common.WarmupKey, Value: common.WarmupDisable},
				},
			},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    10,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID:     0,
				ChildFields: []int64{100, 101},
				Binlogs: []*datapb.Binlog{
					{MemorySize: 1000, LogSize: 1000, EntriesNum: 10},
				},
			},
		},
	}

	usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{
		ExternalRawDataFactor: 2,
	}, nil)

	require.NoError(t, err)
	require.EqualValues(t, 2000, usage.MemoryBytes)
	require.Zero(t, usage.DiskBytes)
	require.EqualValues(t, 100, loadInfo.GetEstimatedBytesPerRow())
}

func TestExternalLazyLoadIncludesStructOffsets(t *testing.T) {
	paramtable.Init()
	for _, policy := range []string{common.WarmupDisable, "sync", "async"} {
		t.Run(policy, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{
				ExternalSource: "s3://bucket/source", ExternalSpec: `{"format":"milvus-table"}`,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 200, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk", TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}}},
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{{Name: "items", FieldID: 101, Fields: []*schemapb.FieldSchema{
					{Name: "items[value]", FieldID: 102, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ExternalField: "items[value]", TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: policy}}},
				}}},
			}
			require.False(t, canSkipExternalRawDataLoading(schema, nil, nil, SegmentLoadingEstimateOptions{}))
			schema.Fields[0].Name = "pk"
			schema.Fields[0].ExternalField = "pk"
			require.False(t, canSkipExternalRawDataLoading(schema, nil, nil, SegmentLoadingEstimateOptions{}))
		})
	}
}

func TestEstimateSegmentLoadingResourceExternalVectorWarmup(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name             string
		indexFiles       []string
		omitIndex        bool
		fieldPolicy      string
		indexPolicy      string
		collectionPolicy string
		wantRawMemory    bool
	}{
		{name: "missing_index_uses_default_sync", omitIndex: true, fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupSync, wantRawMemory: true},
		{name: "fake_finished_index_uses_sync", fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupSync, wantRawMemory: true},
		{name: "fake_finished_index_uses_async", fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupAsync, wantRawMemory: true},
		{name: "fake_finished_index_disabled", fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupDisable},
		{name: "collection_disables_index_warmup", fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupSync, collectionPolicy: common.WarmupDisable},
		{name: "collection_enables_sync", fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupDisable, collectionPolicy: common.WarmupSync, wantRawMemory: true},
		{name: "collection_enables_async", fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupDisable, collectionPolicy: common.WarmupAsync, wantRawMemory: true},
		{name: "no_index_ignores_field_sync", fieldPolicy: common.WarmupSync, indexPolicy: common.WarmupDisable},
		{name: "usable_index_keeps_field_disabled", indexFiles: []string{"index"}, fieldPolicy: common.WarmupDisable, indexPolicy: common.WarmupSync},
		{name: "usable_index_keeps_field_sync", indexFiles: []string{"index"}, fieldPolicy: common.WarmupSync, indexPolicy: common.WarmupDisable, wantRawMemory: true},
		{name: "usable_index_uses_field_default", indexFiles: []string{"index"}, indexPolicy: common.WarmupSync},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := paramtable.Get()
			oldField := params.QueryNodeCfg.TieredWarmupVectorField.GetValue()
			oldIndex := params.QueryNodeCfg.TieredWarmupVectorIndex.GetValue()
			t.Cleanup(func() {
				require.NoError(t, params.Save(params.QueryNodeCfg.TieredWarmupVectorField.Key, oldField))
				require.NoError(t, params.Save(params.QueryNodeCfg.TieredWarmupVectorIndex.Key, oldIndex))
			})
			require.NoError(t, params.Save(params.QueryNodeCfg.TieredWarmupVectorField.Key, common.WarmupDisable))
			require.NoError(t, params.Save(params.QueryNodeCfg.TieredWarmupVectorIndex.Key, tc.indexPolicy))
			vector := &schemapb.FieldSchema{
				FieldID: 102, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				ExternalField: "embedding",
				TypeParams:    []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			}
			if tc.fieldPolicy != "" {
				vector.TypeParams = append(vector.TypeParams, &commonpb.KeyValuePair{Key: common.WarmupKey, Value: tc.fieldPolicy})
			}
			schema := &schemapb.CollectionSchema{
				ExternalSource: "s3://bucket/source", ExternalSpec: `{"format":"milvus-table"}`,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 200, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk", TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}}},
					vector,
				},
			}
			if tc.collectionPolicy != "" {
				schema.Properties = []*commonpb.KeyValuePair{{Key: common.WarmupVectorIndexKey, Value: tc.collectionPolicy}}
			}
			loadInfo := &querypb.SegmentLoadInfo{
				NumOfRows:   10,
				BinlogPaths: []*datapb.FieldBinlog{{FieldID: 0, ChildFields: []int64{100, 102, 200}, Binlogs: []*datapb.Binlog{{MemorySize: 1000, LogSize: 1000, EntriesNum: 10}}}},
			}
			if !tc.omitIndex {
				loadInfo.IndexInfos = []*querypb.FieldIndexInfo{{
					FieldID: 102, IndexFilePaths: tc.indexFiles,
					IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}, {Key: common.MetricTypeKey, Value: metric.COSINE}},
				}}
			}
			usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{
				ExternalRawDataFactor: 2,
			}, func(func() error) error { return nil })
			require.NoError(t, err)
			var wantMemory uint64
			if tc.wantRawMemory {
				wantMemory += 2000 // Raw bytes plus external load peak margin.
			}
			require.Equal(t, wantMemory, usage.MemoryBytes)
			require.Zero(t, usage.DiskBytes)
			require.EqualValues(t, 100, loadInfo.GetEstimatedBytesPerRow())
		})
	}
}

func TestEstimateSegmentLoadingResourceExternalMandatoryReads(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name          string
		format        string
		structChild   schemapb.DataType
		indexed       bool
		geometryCache bool
	}{
		{name: "struct_scalar_offsets", format: "milvus-table", structChild: schemapb.DataType_Array},
		{name: "struct_vector_offsets", format: "milvus-table", structChild: schemapb.DataType_ArrayOfVector},
		{name: "struct_vector_offsets_with_index", format: "milvus-table", structChild: schemapb.DataType_ArrayOfVector, indexed: true},
		{name: "milvus_table_geometry_cache", format: "milvus-table", geometryCache: true},
		{name: "milvus_table_geometry_cache_disabled", format: "milvus-table"},
		{name: "parquet_geometry_cache", format: "parquet", geometryCache: true},
		{name: "parquet_geometry_cache_disabled", format: "parquet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := paramtable.Get()
			oldCache := params.QueryNodeCfg.EnableGeometryCache.GetValue()
			t.Cleanup(func() {
				require.NoError(t, params.Save(params.QueryNodeCfg.EnableGeometryCache.Key, oldCache))
			})
			require.NoError(t, params.Save(params.QueryNodeCfg.EnableGeometryCache.Key, strconv.FormatBool(tc.geometryCache)))
			schema := &schemapb.CollectionSchema{
				ExternalSource: "s3://bucket/source", ExternalSpec: `{"format":"` + tc.format + `"}`,
				Properties: []*commonpb.KeyValuePair{{Key: common.WarmupVectorIndexKey, Value: common.WarmupDisable}},
				Fields: []*schemapb.FieldSchema{
					{FieldID: 200, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk", TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}}},
					{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, ExternalField: "vector", TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}, {Key: common.WarmupKey, Value: common.WarmupDisable}}},
				},
			}
			if tc.structChild != schemapb.DataType_None {
				child := &schemapb.FieldSchema{
					FieldID: 102, Name: "items[value]", DataType: tc.structChild, ElementType: schemapb.DataType_Int64, ExternalField: "items[value]",
					TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}, {Key: common.MaxCapacityKey, Value: "100"}},
				}
				if tc.structChild == schemapb.DataType_ArrayOfVector {
					child.ElementType = schemapb.DataType_FloatVector
					child.TypeParams = append(child.TypeParams, &commonpb.KeyValuePair{Key: common.DimKey, Value: "128"})
				}
				schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{Name: "items", FieldID: 103, Fields: []*schemapb.FieldSchema{child}}}
			} else {
				// No usable vector index: index warmup overrides this field-data setting.
				schema.Fields[2].TypeParams[1].Value = common.WarmupSync
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID: 102, Name: "shape", DataType: schemapb.DataType_Geometry, ExternalField: "shape",
					TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}},
				})
			}
			loadInfo := &querypb.SegmentLoadInfo{
				NumOfRows:   10,
				BinlogPaths: []*datapb.FieldBinlog{{FieldID: 0, ChildFields: []int64{100, 101, 102, 200}, Binlogs: []*datapb.Binlog{{MemorySize: 1000, LogSize: 1000, EntriesNum: 10}}}},
			}
			if tc.indexed {
				loadInfo.IndexInfos = []*querypb.FieldIndexInfo{{FieldID: 102, IndexFilePaths: []string{"index"}, IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "HNSW"}, {Key: common.MetricTypeKey, Value: metric.MaxSimCosine},
				}}}
			}
			for _, factor := range []float64{1, 2} {
				usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{
					ExternalRawDataFactor: factor,
				}, func(func() error) error { return nil })
				require.NoError(t, err)
				var wantMemory uint64
				if tc.structChild != schemapb.DataType_None || tc.geometryCache {
					wantMemory = uint64(1000 * factor)
				}
				if tc.structChild != schemapb.DataType_None {
					wantMemory += 440 // Struct offsets are additional to the raw-data reservation.
				}
				require.Equal(t, wantMemory, usage.MemoryBytes, "raw-data factor %v", factor)
				require.Zero(t, usage.DiskBytes)
				require.EqualValues(t, 100, loadInfo.GetEstimatedBytesPerRow())
			}
		})
	}
}

func TestEstimateSegmentLoadingResourceExternalInterimReads(t *testing.T) {
	paramtable.Init()
	for _, format := range []string{"milvus-table", "parquet"} {
		for _, tc := range []struct {
			name           string
			dataType       schemapb.DataType
			nullable       bool
			disableInterim bool
			growingMmap    bool
			indexFiles     []string
			wantMemory     uint64
		}{
			{name: "nullable_float", dataType: schemapb.DataType_FloatVector, nullable: true, wantMemory: 3150},
			{name: "nonnullable_float", dataType: schemapb.DataType_FloatVector, wantMemory: 3150},
			{name: "nullable_float16", dataType: schemapb.DataType_Float16Vector, nullable: true, wantMemory: 3150},
			{name: "nullable_bfloat16", dataType: schemapb.DataType_BFloat16Vector, nullable: true, wantMemory: 3150},
			{name: "sparse", dataType: schemapb.DataType_SparseFloatVector, wantMemory: 3150},
			{name: "interim_disabled", dataType: schemapb.DataType_FloatVector, nullable: true, disableInterim: true},
			{name: "growing_mmap_disables_interim", dataType: schemapb.DataType_FloatVector, nullable: true, growingMmap: true},
			{name: "unsupported_binary", dataType: schemapb.DataType_BinaryVector},
			{name: "unsupported_int8", dataType: schemapb.DataType_Int8Vector},
			// The mixed fake group still gets PART 2's interim margin, but no raw warmup.
			{name: "usable_index_keeps_field_disabled", dataType: schemapb.DataType_FloatVector, nullable: true, indexFiles: []string{"index"}, wantMemory: 1150},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				vector := &schemapb.FieldSchema{
					FieldID: 101, Name: "vector", DataType: tc.dataType, Nullable: tc.nullable, ExternalField: "vector",
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}, {Key: common.WarmupKey, Value: common.WarmupSync}},
				}
				if len(tc.indexFiles) > 0 {
					vector.TypeParams[1].Value = common.WarmupDisable
				}
				indexType, metricType := "HNSW", metric.COSINE
				if tc.dataType == schemapb.DataType_SparseFloatVector {
					vector.TypeParams = vector.TypeParams[1:]
					indexType, metricType = "SPARSE_INVERTED_INDEX", metric.IP
				}
				schema := &schemapb.CollectionSchema{
					ExternalSource: "s3://bucket/source", ExternalSpec: `{"format":"` + format + `"}`,
					Properties: []*commonpb.KeyValuePair{{Key: common.WarmupVectorIndexKey, Value: common.WarmupDisable}},
					Fields: []*schemapb.FieldSchema{
						{FieldID: 200, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
						{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk", TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}}},
						vector,
					},
				}
				loadInfo := &querypb.SegmentLoadInfo{
					NumOfRows:   1, // Nullable interim preparation reads even below the build threshold.
					BinlogPaths: []*datapb.FieldBinlog{{FieldID: 0, ChildFields: []int64{100, 101, 200}, Binlogs: []*datapb.Binlog{{MemorySize: 1000, LogSize: 1000, EntriesNum: 1}}}},
					IndexInfos: []*querypb.FieldIndexInfo{{FieldID: 101, IndexFilePaths: tc.indexFiles, IndexParams: []*commonpb.KeyValuePair{
						{Key: common.IndexTypeKey, Value: indexType}, {Key: common.MetricTypeKey, Value: metricType},
					}}},
				}
				usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{
					EnableInterimSegmentIndex: !tc.disableInterim,
					GrowingMmapEnabled:        tc.growingMmap,
					TempSegmentIndexFactor:    1.15,
					ExternalRawDataFactor:     2,
				}, func(func() error) error { return nil })
				require.NoError(t, err)
				require.Equal(t, tc.wantMemory, usage.MemoryBytes)
				require.Zero(t, usage.DiskBytes)
				require.EqualValues(t, 1000, loadInfo.GetEstimatedBytesPerRow())
			})
		}
	}
}

func TestEstimateSegmentLoadingResourceExternalTextMatchReads(t *testing.T) {
	paramtable.Init()
	for _, format := range []string{"milvus-table", "parquet"} {
		for _, tc := range []struct {
			name        string
			dataType    schemapb.DataType
			enableMatch bool
			hasStats    bool
		}{
			{name: "varchar_local", dataType: schemapb.DataType_VarChar, enableMatch: true},
			{name: "text_local", dataType: schemapb.DataType_Text, enableMatch: true},
			{name: "varchar_prebuilt", dataType: schemapb.DataType_VarChar, enableMatch: true, hasStats: true},
			{name: "text_prebuilt", dataType: schemapb.DataType_Text, enableMatch: true, hasStats: true},
			{name: "match_disabled", dataType: schemapb.DataType_VarChar},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				schema := &schemapb.CollectionSchema{
					ExternalSource: "s3://bucket/source", ExternalSpec: `{"format":"` + format + `"}`,
					Properties: []*commonpb.KeyValuePair{{Key: common.WarmupVectorIndexKey, Value: common.WarmupDisable}},
					Fields: []*schemapb.FieldSchema{
						{FieldID: 200, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
						{FieldID: 101, Name: "vector", ExternalField: "vector", DataType: schemapb.DataType_FloatVector,
							TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}, {Key: common.WarmupKey, Value: common.WarmupSync}}},
						{FieldID: 102, Name: "text", ExternalField: "text", DataType: tc.dataType,
							TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}, {Key: "enable_match", Value: strconv.FormatBool(tc.enableMatch)}, {Key: "enable_analyzer", Value: "true"}}},
					},
				}
				loadInfo := &querypb.SegmentLoadInfo{
					NumOfRows:   1,
					BinlogPaths: []*datapb.FieldBinlog{{FieldID: 0, ChildFields: []int64{101, 102, 200}, Binlogs: []*datapb.Binlog{{MemorySize: 1000, LogSize: 1000, EntriesNum: 1}}}},
				}
				if tc.hasStats {
					loadInfo.TextStatsLogs = map[int64]*datapb.TextIndexStats{102: {FieldID: 102}}
				}
				for _, factor := range []float64{1, 2} {
					usage, err := EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, SegmentLoadingEstimateOptions{ExternalRawDataFactor: factor}, nil)
					require.NoError(t, err)
					// VarChar retains the existing double-memory charge; Text does not.
					var raw uint64 = 1000
					if tc.dataType == schemapb.DataType_VarChar {
						raw *= 2
					}
					want := raw - 1000
					if tc.enableMatch && !tc.hasStats {
						want = raw + uint64(1000*(factor-1))
					}
					if tc.hasStats {
						want += 8
					} // One word of text-index validity.
					require.Equal(t, want, usage.MemoryBytes)
					require.Zero(t, usage.DiskBytes)
				}
			})
		}
	}
}
