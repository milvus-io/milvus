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

package autoscale

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestEstimateSegmentsLoadResourceReturnsErrorWithoutSchema(t *testing.T) {
	usage, err := EstimateSegmentsLoadResource(nil, nil, nil, EstimateOptions{})

	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Zero(t, usage)
}

func TestEstimateSegmentsLoadResourceUsesSchemaAndMmap(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{
		{
			ID:        1,
			NumOfRows: 1000,
			Binlogs:   testRawBinlogs(),
			Statslogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{MemorySize: 100, LogSize: 80}}},
			},
		},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		MmapVectorField: true,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(8100), usage.MemoryBytes)
	assert.Equal(t, int64(512000), usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceDoesNotRequireRows(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{
		{
			ID: 1,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 1, Binlogs: []*datapb.Binlog{{MemorySize: 100, LogSize: 80}}},
			},
		},
	}
	usage, err := EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{})

	require.NoError(t, err)
	assert.Equal(t, int64(100), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceSkipsRawVectorWhenIndexHasRawData(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{{ID: 1, NumOfRows: 1000, Binlogs: testRawBinlogs()}}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {testVectorIndex("HNSW", 10000)},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, indexes, EstimateOptions{})

	require.NoError(t, err)
	assert.Equal(t, int64(18000), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceKeepsRawVectorWhenPreferFieldData(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{{ID: 1, NumOfRows: 1000, Binlogs: testRawBinlogs()}}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {testVectorIndex("HNSW", 10000)},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, indexes, EstimateOptions{
		PreferFieldDataWhenIndexHasRawData: true,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(530000), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceEstimatesHNSWIndexWhenSizeMissing(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{{ID: 1, NumOfRows: 1000, Binlogs: testRawBinlogs()}}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {{
			FieldID: 100,
			NumRows: 1000,
			IndexFilePaths: []string{
				"/path/that/must/not/exist/index_file",
			},
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "HNSW"},
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.DimKey, Value: "128"},
				{Key: "M", Value: "8"},
			},
		}},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, indexes, EstimateOptions{})

	require.NoError(t, err)
	assert.Equal(t, int64(8000), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceUsesCIndexEstimate(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{{ID: 1, NumOfRows: 1000, Binlogs: testRawBinlogs()}}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {testVectorIndex("HNSW", 10000)},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, indexes, EstimateOptions{})

	require.NoError(t, err)
	assert.Equal(t, int64(18000), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceReturnsErrorWhenIndexTypeMissing(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{{ID: 1, NumOfRows: 1000, Binlogs: testRawBinlogs()}}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {{
			FieldID:        100,
			IndexSize:      10000,
			IndexFilePaths: []string{"/path/that/must/not/exist/index_file"},
		}},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, indexes, EstimateOptions{})

	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Zero(t, usage)
}

func TestEstimateSegmentsLoadResourceKeepsFlatRawFieldWhenIndexSizeMissing(t *testing.T) {
	schema := testSchema()
	segments := []*datapb.SegmentInfo{{ID: 1, NumOfRows: 1000, Binlogs: testRawBinlogs()}}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {{
			FieldID: 100,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "FLAT"},
			},
		}},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, indexes, EstimateOptions{
		MmapVectorField: true,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(8000), usage.MemoryBytes)
	assert.Equal(t, int64(512000), usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceIncludesJSONKeyStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	segments := []*datapb.SegmentInfo{
		{
			ID:        1,
			NumOfRows: 1000,
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				101: {MemorySize: 1000, LogSize: 800},
			},
		},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		JSONKeyStatsExpansionFactor: 1.5,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(1500), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)

	usage, err = EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		MmapJSONStats:               true,
		JSONKeyStatsExpansionFactor: 1.5,
	})

	require.NoError(t, err)
	assert.Zero(t, usage.MemoryBytes)
	assert.Equal(t, int64(1500), usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceIncludesTextStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	segments := []*datapb.SegmentInfo{
		{
			ID:        1,
			NumOfRows: 1000,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {MemorySize: 2000, LogSize: 1500},
			},
		},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		TextIndexExpansionFactor: 1.25,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(2500), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)

	usage, err = EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		MmapScalarField:          true,
		TextIndexExpansionFactor: 1.25,
	})

	require.NoError(t, err)
	assert.Zero(t, usage.MemoryBytes)
	assert.Equal(t, int64(2500), usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceAppliesTieredRatioToStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	segments := []*datapb.SegmentInfo{
		{
			ID:        1,
			NumOfRows: 1000,
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				101: {MemorySize: 101},
			},
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				102: {MemorySize: 201},
			},
		},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		MmapScalarField:        true,
		TieredEvictionEnabled:  true,
		TieredMemoryCacheRatio: 0.3,
		TieredDiskCacheRatio:   0.5,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(31), usage.MemoryBytes)
	assert.Equal(t, int64(101), usage.DiskBytes)
}

func TestEstimateSegmentsLoadResourceAllowsZeroTieredRatio(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}
	segments := []*datapb.SegmentInfo{
		{
			ID:        1,
			NumOfRows: 10,
			Binlogs: []*datapb.FieldBinlog{
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
		},
	}

	usage, err := EstimateSegmentsLoadResource(schema, segments, nil, EstimateOptions{
		MmapVectorField:        true,
		TieredEvictionEnabled:  true,
		TieredMemoryCacheRatio: 0,
		TieredDiskCacheRatio:   0,
	})

	require.NoError(t, err)
	assert.Equal(t, int64(30), usage.MemoryBytes)
	assert.Zero(t, usage.DiskBytes)
}

func TestFilterSegmentsByLoadFieldsKeepsChildFieldBinlogs(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{
			ID: 1,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 10, ChildFields: []int64{100, 101}},
				{FieldID: 11, ChildFields: []int64{102}},
				{FieldID: 200},
				{FieldID: 201},
			},
		},
	}

	filtered := FilterSegmentsByLoadFields(segments, []int64{100, 200})

	require.Len(t, filtered, 1)
	require.Len(t, filtered[0].GetBinlogs(), 2)
	assert.Equal(t, int64(10), filtered[0].GetBinlogs()[0].GetFieldID())
	assert.Equal(t, int64(200), filtered[0].GetBinlogs()[1].GetFieldID())
}

func TestFilterLoadFieldsKeepsSystemFields(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{
			ID: 1,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: common.RowIDField},
				{FieldID: common.TimeStampField},
				{FieldID: 100},
				{FieldID: 101},
			},
			Statslogs: []*datapb.FieldBinlog{
				{FieldID: common.RowIDField},
				{FieldID: 100},
				{FieldID: 101},
			},
		},
	}

	filteredSegments := FilterSegmentsByLoadFields(segments, []int64{100})
	require.Len(t, filteredSegments, 1)
	assert.ElementsMatch(t, []int64{common.RowIDField, common.TimeStampField, 100}, lo.Map(filteredSegments[0].GetBinlogs(), func(fieldBinlog *datapb.FieldBinlog, _ int) int64 {
		return fieldBinlog.GetFieldID()
	}))
	assert.ElementsMatch(t, []int64{common.RowIDField, 100}, lo.Map(filteredSegments[0].GetStatslogs(), func(fieldBinlog *datapb.FieldBinlog, _ int) int64 {
		return fieldBinlog.GetFieldID()
	}))
}

func TestEstimateSegmentsLoadResourceForLoadConfigKeepsFullSchemaForChildFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "loaded", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "sibling", DataType: schemapb.DataType_Int64},
		},
	}
	segments := []*datapb.SegmentInfo{
		{
			ID:        1,
			NumOfRows: 100,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     10,
					ChildFields: []int64{100, 101},
					Binlogs: []*datapb.Binlog{
						{MemorySize: 100, LogSize: 80},
					},
				},
			},
		},
	}

	usage, err := EstimateSegmentsLoadResourceForLoadConfig(schema, segments, nil, []int64{100}, nil, EstimateOptions{
		MmapScalarField: true,
	})

	require.NoError(t, err)
	assert.Zero(t, usage.MemoryBytes)
	assert.Equal(t, int64(100), usage.DiskBytes)
}

func testRawBinlogs() []*datapb.FieldBinlog {
	return []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{MemorySize: 8000, LogSize: 8000},
			},
		},
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{MemorySize: 512000, LogSize: 512000},
			},
		},
	}
}

func testVectorIndex(indexType string, indexSize int64) *querypb.FieldIndexInfo {
	return &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexSize: indexSize,
		NumRows:   1000,
		IndexFilePaths: []string{
			"/path/that/must/not/exist/index_file",
		},
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexType},
			{Key: common.MetricTypeKey, Value: "L2"},
			{Key: common.DimKey, Value: "128"},
		},
	}
}

func testSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  100,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}
