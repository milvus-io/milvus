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

package storagecommon

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRestoreColumnGroupLayoutRejectsInvalidMetadata(t *testing.T) {
	validManifest := []ColumnGroup{{Fields: []int64{100, 102}, Format: "parquet"}}
	validStats := []*datapb.ColumnGroupStatistics{{GroupId: 7, FieldIds: []int64{102, 100}}}
	for _, tc := range []struct {
		name     string
		manifest []ColumnGroup
		stats    []*datapb.ColumnGroupStatistics
	}{
		{"missing statistics", validManifest, nil},
		{"missing manifest group", nil, validStats},
		{"different membership", []ColumnGroup{{Fields: []int64{100}, Format: "parquet"}}, validStats},
		{"missing format", []ColumnGroup{{Fields: []int64{100, 102}}}, validStats},
		{"invalid field ID", []ColumnGroup{{Fields: []int64{-1}, Format: "parquet"}}, validStats},
		{"duplicate manifest group", append(append([]ColumnGroup{}, validManifest...), validManifest...), validStats},
		{"duplicate logical ID", validManifest, []*datapb.ColumnGroupStatistics{{GroupId: 7, FieldIds: []int64{100}}, {GroupId: 7, FieldIds: []int64{102}}}},
		{"overlapping logical groups", validManifest, []*datapb.ColumnGroupStatistics{{GroupId: 7, FieldIds: []int64{100}}, {GroupId: 8, FieldIds: []int64{100, 102}}}},
		{"duplicate field", validManifest, []*datapb.ColumnGroupStatistics{{GroupId: 7, FieldIds: []int64{100, 100}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RestoreColumnGroupLayout(tc.manifest, tc.stats)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
	groups, err := RestoreColumnGroupLayout(nil, nil)
	require.NoError(t, err)
	require.Empty(t, groups)
}

func TestDefaultPoliciesCanDriftAcrossBatches(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&params.CommonCfg.Stv2SplitSystemColumn, "true"},
		{&params.CommonCfg.Stv2SystemColumnIncludePK, "true"},
		{&params.CommonCfg.Stv2SplitByAvgSize, "true"},
		{&params.CommonCfg.Stv2SplitAvgSizeThreshold, "64"},
	} {
		key, previous := setting.item.Key, setting.item.GetValue()
		require.NoError(t, params.Save(key, setting.value))
		t.Cleanup(func() { _ = params.Save(key, previous) })
	}
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, DataType: schemapb.DataType_Int64},
		{FieldID: 1, DataType: schemapb.DataType_Int64},
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_VarChar},
		{FieldID: 102, DataType: schemapb.DataType_Int64},
		{FieldID: 103, DataType: schemapb.DataType_FloatVector},
	}
	short := SplitColumns(fields, map[int64]ColumnStats{101: {AvgSize: 22}}, DefaultPolicies()...)
	long := SplitColumns(fields, map[int64]ColumnStats{101: {AvgSize: 1041}}, DefaultPolicies()...)
	require.Equal(t, []ColumnGroup{
		{GroupID: 0, Fields: []int64{0, 1, 100}, Columns: []int{0, 1, 2}},
		{GroupID: 1, Fields: []int64{101, 102}, Columns: []int{3, 4}},
		{GroupID: 103, Fields: []int64{103}, Columns: []int{5}},
	}, short)
	require.Equal(t, []ColumnGroup{
		{GroupID: 0, Fields: []int64{0, 1, 100}, Columns: []int{0, 1, 2}},
		{GroupID: 1, Fields: []int64{102}, Columns: []int{4}},
		{GroupID: 101, Fields: []int64{101}, Columns: []int{3}},
		{GroupID: 103, Fields: []int64{103}, Columns: []int{5}},
	}, long)
	t.Logf("same schema/config, different batch statistics: %v -> %v", short, long)
}

func TestRestoreColumnGroupLayoutPreservesLogicalIDsAndManifestOrder(t *testing.T) {
	physical := []ColumnGroup{
		{Fields: []int64{102, 100}, Format: "vortex"},
		{Fields: []int64{101}, Format: "parquet"},
	}
	logical := []*datapb.ColumnGroupStatistics{
		{GroupId: 101, FieldIds: []int64{101}},
		{GroupId: 7, FieldIds: []int64{100, 102}},
	}
	groups, err := RestoreColumnGroupLayout(physical, logical)
	require.NoError(t, err)
	require.Equal(t, []ColumnGroup{
		{GroupID: 7, Fields: []int64{102, 100}, Format: "vortex"},
		{GroupID: 101, Fields: []int64{101}, Format: "parquet"},
	}, groups)
	groups[0].Fields[0] = 999
	require.Equal(t, []int64{102, 100}, physical[0].Fields)
	require.Equal(t, []int64{100, 102}, logical[1].GetFieldIds())
}

func TestRestoreColumnGroupAliasesAndNormalizeStatistics(t *testing.T) {
	physical := []ColumnGroup{
		{Fields: []int64{102}, Format: "vortex"},
		{Fields: []int64{101}, Format: "parquet"},
	}
	stats := &datapb.Statistics{
		InsertBinlogSize: 2082, InsertBinlogCount: 6, StatsBinlogSize: 42,
		DeltaBinlogSize: 8, DeleteNumRows: 1, TimestampFrom: 10, TimestampTo: 20,
		NullCounts: map[int64]int64{101: 1}, TimestampQuantiles: []int64{10, 20},
		Formats: []string{"parquet", "vortex"},
		LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
			{GroupId: 101, FieldIds: []int64{101}, MemorySize: 2000},
			{GroupId: 102, FieldIds: []int64{102}, MemorySize: 32},
			{GroupId: 1, FieldIds: []int64{101}, MemorySize: 50},
		}},
	}
	original := proto.Clone(stats).(*datapb.Statistics)
	for _, reverse := range []bool{false, true} {
		if reverse {
			groups := stats.LoadResource.ColumnGroups
			groups[0], groups[2] = groups[2], groups[0]
		}
		layout, err := RestoreColumnGroupLayout(physical, stats.GetLoadResource().GetColumnGroups())
		require.NoError(t, err)
		require.Equal(t, []ColumnGroup{
			{GroupID: 102, Fields: []int64{102}, Format: "vortex"},
			{GroupID: 1, Fields: []int64{101}, Format: "parquet"},
		}, layout, "manifest order must not be sorted by the chosen IDs")
		normalized, err := NormalizeColumnGroupStatistics(layout, stats)
		require.NoError(t, err)
		require.True(t, proto.Equal(&datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
			{GroupId: 1, FieldIds: []int64{101}, MemorySize: 2050},
			{GroupId: 102, FieldIds: []int64{102}, MemorySize: 32},
		}}, normalized.GetLoadResource()))
		expected := proto.Clone(original).(*datapb.Statistics)
		expected.LoadResource = proto.Clone(normalized.LoadResource).(*datapb.LoadResourceStatistics)
		require.True(t, proto.Equal(expected, normalized), "unrelated cumulative aggregates must be retained")
		recovered, err := RestoreColumnGroupLayout(physical, normalized.GetLoadResource().GetColumnGroups())
		require.NoError(t, err)
		require.Equal(t, layout, recovered)
		again, err := NormalizeColumnGroupStatistics(recovered, normalized)
		require.NoError(t, err)
		require.True(t, proto.Equal(normalized, again), "normalization must be idempotent")
	}
	// A complete snapshot can retain another valid alias as its write ID.
	preferred := []ColumnGroup{{GroupID: 101, Fields: []int64{101}}, {GroupID: 102, Fields: []int64{102}}}
	normalized, err := NormalizeColumnGroupStatistics(preferred, original)
	require.NoError(t, err)
	require.EqualValues(t, 101, normalized.LoadResource.ColumnGroups[0].GetGroupId())
	require.EqualValues(t, 2050, normalized.LoadResource.ColumnGroups[0].GetMemorySize())
	normalized.NullCounts[101] = 99
	normalized.LoadResource.ColumnGroups[0].FieldIds[0] = 999
	require.EqualValues(t, 1, original.NullCounts[101])
	require.EqualValues(t, 101, original.LoadResource.ColumnGroups[0].FieldIds[0])
}

func TestRestoreColumnGroupAliasesRejectAmbiguousOrInvalidSizes(t *testing.T) {
	for _, groups := range [][]*datapb.ColumnGroupStatistics{
		{{GroupId: 1, FieldIds: []int64{101}, MemorySize: -1}},
		{{GroupId: 1, FieldIds: []int64{101}, MemorySize: math.MaxInt64}, {GroupId: 101, FieldIds: []int64{101}, MemorySize: 1}},
		{{GroupId: 1, FieldIds: []int64{101, 102}}, {GroupId: 101, FieldIds: []int64{101}}},
		{{GroupId: 1, FieldIds: []int64{101}}, {GroupId: 1, FieldIds: []int64{101}}},
	} {
		_, err := RestoreColumnGroupLayout([]ColumnGroup{{Fields: []int64{101}, Format: "parquet"}}, groups)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
}

func TestNormalizeColumnGroupStatisticsValidatesLayout(t *testing.T) {
	stats := &datapb.Statistics{LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
		{GroupId: 1, FieldIds: []int64{101}, MemorySize: 10},
		{GroupId: 102, FieldIds: []int64{102}, MemorySize: 20},
	}}}
	for _, layout := range [][]ColumnGroup{
		{{GroupID: 1, Fields: []int64{101}}},
		{{GroupID: 1, Fields: []int64{101}}, {GroupID: 1, Fields: []int64{102}}},
		{{GroupID: -1, Fields: []int64{101}}, {GroupID: 102, Fields: []int64{102}}},
		{{GroupID: 1, Fields: []int64{101, 102}}},
	} {
		_, err := NormalizeColumnGroupStatistics(layout, stats)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
	normalized, err := NormalizeColumnGroupStatistics(nil, nil)
	require.NoError(t, err)
	require.Nil(t, normalized)
	empty := &datapb.Statistics{InsertBinlogCount: 1}
	normalized, err = NormalizeColumnGroupStatistics([]ColumnGroup{{GroupID: 1, Fields: []int64{101}}}, empty)
	require.NoError(t, err)
	require.True(t, proto.Equal(empty, normalized))
	require.NotSame(t, empty, normalized)
}
