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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
