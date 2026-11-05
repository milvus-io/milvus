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

package fastpb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestUpsertViewEncoderDirectColumns(t *testing.T) {
	source := &msgpb.InsertRequest{NumRows: 3, RowIDs: []int64{10, 20, 30}, Timestamps: []uint64{1, 1, 1}, FieldsData: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}}}}}
	meta := &messagespb.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 9}}}}, RowIds: []int64{10, 30}, PartitionIndexes: []uint32{0, 1}, RequestRowMapping: &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{0, 2}}}
	encoder, err := NewUpsertViewEncoder(meta, source, []int{0, 2})
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	payload := make([]byte, size)
	n, err := encoder.MarshalTo(payload)
	require.NoError(t, err)
	require.Equal(t, size, n)
	decoded := &messagespb.UpsertMessageBody{}
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.Equal(t, []int64{10, 30}, decoded.RowIds)
	require.Equal(t, []float32{1, 2, 5, 6}, decoded.Columns[0].GetVectors().GetFloatVector().GetData())
	require.Equal(t, []uint32{0, 2}, decoded.RequestRowMapping.RowOffsets)
}

func BenchmarkUnifiedUpsertViewEncoding(b *testing.B) {
	const rows = 1024
	source := &msgpb.InsertRequest{NumRows: rows, RowIDs: make([]int64, rows), Timestamps: make([]uint64, rows), FieldsData: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, rows*128)}}}}}}}
	selected := make([]int, rows)
	for i := range selected {
		selected[i] = i
	}
	metadata := &messagespb.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, rows)}}}, RowIds: source.RowIDs}
	for _, outputs := range []bool{false, true} {
		name := "dense"
		if outputs {
			name = "dense_with_materialized_sparse"
			source.FieldsData = append(source.FieldsData, &schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: make([][]byte, rows)}}}}})
			for i := 0; i < rows; i++ {
				source.FieldsData[1].GetVectors().GetSparseFloatVector().Contents[i] = []byte{1, 0, 0, 0, 0, 0, 128, 63}
			}
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encoder, err := NewUpsertViewEncoder(metadata, source, selected)
				if err != nil {
					b.Fatal(err)
				}
				size, err := encoder.EncodedSize()
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(size), "payload_bytes")
				if _, err := encoder.MarshalTo(make([]byte, size)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestUpsertCursorSharesNullablePositionsAcrossSizeChunks(t *testing.T) {
	source := &msgpb.InsertRequest{NumRows: 5, RowIDs: []int64{10, 11, 12, 13, 14}, Timestamps: []uint64{1, 1, 1, 1, 1}, FieldsData: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, ValidData: []bool{true, false, true, false, true}, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}}}}}
	cursor, err := NewUpsertViewCursor(source)
	require.NoError(t, err)
	rows := []int{0, 1, 2, 3, 4}
	prefix := make([]int, 6)
	var got []float32
	var valid []bool
	chunks := 0
	for len(rows) > 0 {
		encoder, count, err := cursor.NextEncoder(rows, 40, prefix, 0)
		require.NoError(t, err)
		require.Positive(t, count)
		_, _, err = cursor.NextEncoder(rows, 40, prefix, 0)
		require.ErrorContains(t, err, "has not been marshaled")
		metadata := &messagespb.UpsertMessageBody{RowIds: source.RowIDs[rows[0] : rows[count-1]+1]}
		require.NoError(t, encoder.SetMetadata(metadata))
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		payload := make([]byte, size)
		_, err = encoder.MarshalTo(payload)
		require.NoError(t, err)
		body := &messagespb.UpsertMessageBody{}
		require.NoError(t, proto.Unmarshal(payload, body))
		got = append(got, body.Columns[0].GetVectors().GetFloatVector().Data...)
		valid = append(valid, body.Columns[0].ValidData...)
		rows = rows[count:]
		prefix = prefix[count:]
		chunks++
	}
	require.Greater(t, chunks, 1)
	require.Equal(t, []float32{1, 2, 3, 4, 5, 6}, got)
	require.Equal(t, []bool{true, false, true, false, true}, valid)
}
