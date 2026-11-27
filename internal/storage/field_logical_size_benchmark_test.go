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

package storage

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
)

// Compare the master writer's existing group-size accounting with the V3
// writer's combined group and per-field accounting on the same projected batch.
// This isolates the in-memory work from Arrow construction and object I/O.
func BenchmarkV3FieldLogicalSizes(b *testing.B) {
	const rows, dim = 4096, 128
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "name", DataType: schemapb.DataType_VarChar, Nullable: true},
		{FieldID: 102, Name: "embedding", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
	}}
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(b, err)
	groups := []storagecommon.ColumnGroup{{GroupID: 1, Columns: []int{0, 1}}, {GroupID: 2, Columns: []int{2}}}
	for _, test := range []struct {
		name      string
		nullEvery int
	}{{"all_valid", 0}, {"one_in_eight_strings_null", 8}} {
		b.Run(test.name, func(b *testing.B) {
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer builder.Release()
			vector := make([]byte, dim*4)
			for row := 0; row < rows; row++ {
				builder.Field(0).(*array.Int64Builder).Append(int64(row))
				if test.nullEvery != 0 && row%test.nullEvery == 0 {
					builder.Field(1).AppendNull()
				} else {
					builder.Field(1).(*array.StringBuilder).Append("field-size benchmark")
				}
				builder.Field(2).(*array.FixedSizeBinaryBuilder).Append(vector)
			}
			record := builder.NewRecord()
			defer record.Release()
			b.Run("group_sizes", func(b *testing.B) {
				stats := v3BatchStats{columnGroupUncompressed: make(map[int64]uint64)}
				b.ReportAllocs()
				for b.Loop() {
					stats.rowNum += record.NumRows()
					for i, a := range record.Columns() {
						n := calculateActualDataSize(a)
						stats.writtenUncompressed += n
						for _, group := range groups {
							if lo.Contains(group.Columns, i) {
								stats.columnGroupUncompressed[group.GroupID] += n
								break
							}
						}
					}
				}
				require.Positive(b, stats.writtenUncompressed)
			})
			b.Run("group_and_field_sizes", func(b *testing.B) {
				stats := v3BatchStats{columnGroupUncompressed: make(map[int64]uint64)}
				b.ReportAllocs()
				for b.Loop() {
					if err := stats.writeBatch(record, schema, groups, func(uint64) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
				require.Equal(b, uint64(stats.rowNum)*dim*4, stats.fieldLogicalSizes.Fields[102].PayloadBytes)
			})
		})
	}
}
