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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestInsertRowsSizerMatchesIndependentViews(t *testing.T) {
	source := insertViewAllRepackTypesSource()
	before := proto.Clone(source)
	for _, selections := range [][][]int64{{nil}, {{0, 2}, {1, 3}}, {{3}, {1}, {0}, {2}}, {{0, 3}, {1, 2}}, {{1}, {3}}} {
		expected, err := referenceUpsertPartitionSizes(source, selections)
		require.NoError(t, err)
		sizer, err := NewInsertRowsSizer(&msgpb.InsertRequest{NumRows: source.NumRows, RowIDs: source.RowIDs, FieldsData: source.FieldsData})
		require.NoError(t, err)
		selected := make(map[int]int)
		for partition, rows := range selections {
			require.Equal(t, partition, sizer.AddPartition())
			if rows == nil {
				for row := range source.RowIDs {
					selected[row] = partition
				}
			} else {
				for _, row := range rows {
					selected[int(row)] = partition
				}
			}
		}
		for row := range source.RowIDs {
			if partition, ok := selected[row]; ok {
				require.NoError(t, sizer.AddRow(row, partition))
			}
		}
		actual, err := sizer.Sizes()
		require.NoError(t, err)
		require.Equal(t, expected, actual, "selections %v", selections)
	}
	require.True(t, proto.Equal(before, source), "sizing must not mutate source or materialize selected columns")
}

func TestInsertRowsSizerPackedLengthBoundariesAndLogicalRowIDs(t *testing.T) {
	for _, count := range []int{1, 127, 128, 129, 16383, 16384} {
		source := &msgpb.InsertRequest{NumRows: uint64(count), RowIDs: make([]int64, count), Timestamps: make([]uint64, count), FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: make([]string, count)}}}}}}}
		selections := make([][]int64, 2)
		for row := range source.RowIDs {
			source.RowIDs[row] = 1<<50 + int64(row)
			if row%3 == 0 {
				source.RowIDs[row] = -source.RowIDs[row]
			}
			source.FieldsData[0].GetScalars().GetStringData().Data[row] = "some payload"
			selections[row%2] = append(selections[row%2], int64(row))
		}
		if count == 1 {
			selections = selections[:1]
		}
		expected, err := referenceUpsertPartitionSizes(source, selections)
		require.NoError(t, err)
		sizer, err := NewInsertRowsSizer(source)
		require.NoError(t, err)
		for range selections {
			sizer.AddPartition()
		}
		for row := range source.RowIDs {
			require.NoError(t, sizer.AddRow(row, row%len(selections)))
		}
		actual, err := sizer.Sizes()
		require.NoError(t, err)
		require.Equal(t, expected, actual, "rows %d", count)
	}
}

func BenchmarkInsertRowsPartitionAccounting(b *testing.B) {
	const rows = 4096
	source := &msgpb.InsertRequest{NumRows: rows, RowIDs: make([]int64, rows), Timestamps: make([]uint64, rows), FieldsData: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, rows*128)}}}}}}}
	for row := range source.RowIDs {
		source.RowIDs[row] = 1<<50 + int64(row)
	}
	for _, partitions := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("partitions=%d/batch", partitions), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				selections := make([][]int64, partitions)
				for row := range source.RowIDs {
					selections[row%partitions] = append(selections[row%partitions], int64(row))
				}
				if _, err := UpsertPartitionSizes(source, selections); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("partitions=%d/incremental", partitions), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				sizer, err := NewInsertRowsSizer(source)
				if err != nil {
					b.Fatal(err)
				}
				for range partitions {
					sizer.AddPartition()
				}
				for row := range source.RowIDs {
					if err := sizer.AddRow(row, row%partitions); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := sizer.Sizes(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
