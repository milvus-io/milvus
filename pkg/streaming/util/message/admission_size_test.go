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

package message

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
)

func TestSinglePartitionProjectionSizeMatchesSelectedRowAccounting(t *testing.T) {
	for _, varchar := range []bool{false, true} {
		primary := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64}
		keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 127, 128, -1}}}}
		if varchar {
			primary.DataType = schemapb.DataType_VarChar
			keys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"", "hello", "世界", "last"}}}}
		}
		source := &msgpb.InsertRequest{NumRows: 4, RowIDs: []int64{1 << 50, 1<<50 + 1, 1<<50 + 2, 1<<50 + 3}, Timestamps: []uint64{1, 1, 1, 1}, FieldsData: []*schemapb.FieldData{
			{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}}}}}},
			{FieldId: 102, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"", "a", "世界", "longer text"}}}}}},
		}}
		for _, selected := range [][]int{{0, 1, 2, 3}, {0, 2}, {1}} {
			selectedKeys := proto.Clone(keys).(*schemapb.IDs)
			ids := make([]int64, len(selected))
			if varchar {
				selectedKeys.GetStrId().Data = nil
			} else {
				selectedKeys.GetIntId().Data = nil
			}
			for i, row := range selected {
				ids[i] = source.RowIDs[row]
				if varchar {
					selectedKeys.GetStrId().Data = append(selectedKeys.GetStrId().Data, keys.GetStrId().Data[row])
				} else {
					selectedKeys.GetIntId().Data = append(selectedKeys.GetIntId().Data, keys.GetIntId().Data[row])
				}
			}
			rows := &InsertRows{PrimaryKeys: selectedKeys, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: ids}}, RequestRowMapping: &RequestRowMapping{}}
			encoder, err := fastpb.NewInsertMessageViewEncoder(&InsertMessageBody{Rows: rows}, source, selected)
			require.NoError(t, err)
			msg := NewInsertMessageBuilderV2().WithVChannel("v").WithHeader(&InsertMessageHeaderV2{CollectionId: 1, SchemaVersion: proto.Int32(1), Partitions: []*PartitionSegmentAssignment{{PartitionId: 2, Rows: uint64(len(selected))}}}).WithBodyEncoder(encoder).MustBuildMutable()
			projection, err := ReadInsertOrUpsertProjection(t.Context(), msg)
			require.NoError(t, err)
			body := MustAsMutableInsertMessageV2(msg).MustBody()
			fields := append(append([]*schemapb.FieldData(nil), body.Rows.Columns...), UpsertPrimaryKeyColumn(primary, selectedKeys))
			expected, err := fastpb.UpsertPartitionSizes(&msgpb.InsertRequest{NumRows: uint64(len(selected)), RowIDs: ids, Timestamps: make([]uint64, len(selected)), FieldsData: fields}, [][]int64{nil})
			require.NoError(t, err)
			actual, exact, err := projection.SinglePartitionBinarySize(primary)
			require.NoError(t, err)
			require.True(t, exact)
			require.Equal(t, expected[0], actual, "varchar=%t selection=%v", varchar, selected)
		}
	}
}

func TestSinglePartitionSparseProjectionRequiresDecodedSizing(t *testing.T) {
	rows := &InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1}}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 3, Contents: [][]byte{{2, 0, 0, 0, 0, 0, 128, 63}}}}}}}}}
	msg := NewInsertMessageBuilderV2().WithVChannel("v").WithHeader(&InsertMessageHeaderV2{CollectionId: 1, SchemaVersion: proto.Int32(1), Partitions: []*PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}}}).WithBody(&InsertMessageBody{Rows: rows}).MustBuildMutable()
	projection, err := ReadInsertOrUpsertProjection(t.Context(), msg)
	require.NoError(t, err)
	_, exact, err := projection.SinglePartitionBinarySize(&schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	require.False(t, exact, "sparse dimension normalization can change logical size without changing row values")
}

func TestNullableProjectionRequiresNormalizedSizing(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		vector := &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}
		column := &schemapb.FieldData{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: vector}}
		if legacy {
			column.ValidData = []bool{true, false}
		} else {
			vector.ValidData = []bool{true, false}
		}
		rows := &InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{3, 4}}}, Columns: []*schemapb.FieldData{column}}
		msg := NewInsertMessageBuilderV2().WithVChannel("v").WithHeader(&InsertMessageHeaderV2{CollectionId: 1, Partitions: []*PartitionSegmentAssignment{{PartitionId: 2, Rows: 2}}}).WithBody(&InsertMessageBody{Rows: rows}).MustBuildMutable()
		projection, err := ReadInsertOrUpsertProjection(t.Context(), msg)
		require.NoError(t, err)
		_, exact, err := projection.SinglePartitionBinarySize(&schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64})
		require.NoError(t, err)
		require.False(t, exact, "validity masks require logical normalization before accounting")
	}
}
