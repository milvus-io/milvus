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
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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
		{name: "disabled uses full evictable resource", expectedMemory: 770, expectedDisk: 310},
		{name: "enabled applies cache ratios", tieredEnabled: true, expectedMemory: 480, expectedDisk: 78},
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
