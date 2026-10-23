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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// Exercise the public estimators with the same metadata the loader sees. In
// particular, index mmap must not replace the in-memory raw-data budget when a
// legacy floating index is ignored.
func TestEstimateSegmentLegacyFloatingIndexUsesRawBudget(t *testing.T) {
	paramtable.Init()
	cases := []struct {
		name        string
		dataType    schemapb.DataType
		elementType schemapb.DataType
		castType    string
	}{
		{"float", schemapb.DataType_Float, schemapb.DataType_None, ""},
		{"double", schemapb.DataType_Double, schemapb.DataType_None, ""},
		{"float_array", schemapb.DataType_Array, schemapb.DataType_Float, ""},
		{"double_array", schemapb.DataType_Array, schemapb.DataType_Double, ""},
		{"json_double", schemapb.DataType_JSON, schemapb.DataType_None, "DOUBLE"},
		{"json_array_double", schemapb.DataType_JSON, schemapb.DataType_None, "ARRAY_DOUBLE"},
		{"json_flat", schemapb.DataType_JSON, schemapb.DataType_None, "JSON"},
		{"json_missing_cast", schemapb.DataType_JSON, schemapb.DataType_None, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			field := &schemapb.FieldSchema{
				FieldID: 100, Name: "value", DataType: tc.dataType, ElementType: tc.elementType,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "false"}},
			}
			schema := &schemapb.CollectionSchema{Name: "legacy_index", Fields: []*schemapb.FieldSchema{field}}
			indexType := "INVERTED"
			if tc.dataType == schemapb.DataType_Double || tc.dataType == schemapb.DataType_Float {
				indexType = "STL_SORT"
			}
			index := &querypb.FieldIndexInfo{
				FieldID: 100, IndexID: 10, BuildID: 11, IndexSize: 1024, NumRows: 100,
				CurrentScalarIndexVersion: 5,
				IndexFilePaths:            []string{"/must/not/read/legacy-index"},
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: indexType},
					{Key: common.MmapEnabledKey, Value: "true"},
				},
			}
			if tc.castType != "" {
				index.IndexParams = append(index.IndexParams,
					&commonpb.KeyValuePair{Key: "json_cast_type", Value: tc.castType},
					&commonpb.KeyValuePair{Key: "json_path", Value: "value[\"x\"]"})
			}
			load := &querypb.SegmentLoadInfo{
				CollectionID: 1, PartitionID: 2, SegmentID: 3, NumOfRows: 100,
				BinlogPaths: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 800, LogSize: 400, EntriesNum: 100}}}},
			}
			calls := 0
			runner := func(fn func() error) error { calls++; return fn() }
			for _, tiered := range []bool{false, true} {
				for _, preferRaw := range []bool{false, true} {
					finalOptions := SegmentFinalEstimateOptions{
						TieredEvictionEnabled: tiered, TieredEvictableMemoryCacheRatio: 0.5,
						TieredEvictableDiskCacheRatio: 0.5, PreferFieldDataWhenIndexHasRawData: preferRaw,
					}
					loadingOptions := SegmentLoadingEstimateOptions{TieredEvictionEnabled: tiered, PreferFieldDataWhenIndexHasRawData: preferRaw}
					load.IndexInfos = nil
					wantFinal, err := EstimateSegmentFinalResource(context.Background(), schema, load, finalOptions, runner)
					require.NoError(t, err)
					wantLoading, err := EstimateSegmentLoadingResource(context.Background(), schema, load, loadingOptions, runner)
					require.NoError(t, err)
					for _, version := range []int32{0, 5} {
						index.CurrentScalarIndexVersion = version
						load.IndexInfos = []*querypb.FieldIndexInfo{index}
						gotFinal, err := EstimateSegmentFinalResource(context.Background(), schema, load, finalOptions, runner)
						require.NoError(t, err)
						gotLoading, err := EstimateSegmentLoadingResource(context.Background(), schema, load, loadingOptions, runner)
						require.NoError(t, err)
						require.Equal(t, wantFinal, gotFinal)
						require.Equal(t, wantLoading, gotLoading)
						require.Zero(t, calls, "ignored indexes must never reach the estimator runner")
						if !tiered {
							require.GreaterOrEqual(t, gotFinal.MemoryBytes, uint64(800))
							require.GreaterOrEqual(t, gotLoading.MemoryBytes, uint64(800))
							require.Zero(t, gotFinal.DiskBytes)
							require.Zero(t, gotLoading.DiskBytes)
						}
					}
				}
			}
		})
	}
}

func TestEstimateSegmentLegacyJsonCastRejectsBeforeEstimation(t *testing.T) {
	paramtable.Init()
	field := &schemapb.FieldSchema{FieldID: 100, Name: "value", DataType: schemapb.DataType_JSON}
	schema := &schemapb.CollectionSchema{Name: "legacy_cast", Fields: []*schemapb.FieldSchema{field}}
	index := &querypb.FieldIndexInfo{
		FieldID: 100, CurrentScalarIndexVersion: 5, IndexFilePaths: []string{"/legacy-cast"},
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "INVERTED"},
			{Key: "json_cast_type", Value: "DOUBLE"},
			{Key: "json_cast_function", Value: "STRING_TO_DOUBLE"},
		},
	}
	load := &querypb.SegmentLoadInfo{SegmentID: 3, IndexInfos: []*querypb.FieldIndexInfo{index}}
	calls := 0
	runner := func(fn func() error) error { calls++; return fn() }
	_, indexErr := EstimateIndexLoadResourceWithRunner(context.Background(), field, load, index, runner)
	_, finalErr := EstimateSegmentFinalResource(context.Background(), schema, load, SegmentFinalEstimateOptions{}, runner)
	_, loadingErr := EstimateSegmentLoadingResource(context.Background(), schema, load, SegmentLoadingEstimateOptions{}, runner)
	for _, err := range []error{indexErr, finalErr, loadingErr} {
		require.ErrorIs(t, err, merr.ErrSegcoreUnsupported)
		require.Contains(t, err.Error(), "must be rebuilt before loading")
		require.False(t, merr.IsRetryableErr(err))
		require.False(t, merr.Status(err).GetRetriable())
	}
	require.Zero(t, calls)
}
