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

package arrowconv

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// recordWithMetadata builds a one-column record carrying the two schema-level
// metadata keys the retrieve export stamps. The column itself is irrelevant to
// ReconcileValidData -- it reads only the metadata -- so it stays minimal.
func recordWithMetadata(t *testing.T, kv map[string]string) arrow.Record {
	t.Helper()
	md := arrow.MetadataFrom(kv)
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "c0", Type: arrow.PrimitiveTypes.Int64}}, &md)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	return bldr.NewRecord()
}

func int64Col(id int64, valid []bool) *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:    schemapb.DataType_Int64,
		FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		}},
	}
	if valid != nil {
		fd.ValidData = valid
	}
	return fd
}

// TestReconcileValidData covers the mechanism that keeps the two transports
// mergeable, and which no other test in the tree exercises: both of its
// corrective branches are unreachable from the e2e fixtures (the "synthesize"
// branch needs the ORDER BY pipeline, which is excluded from Arrow routing, and
// every nullable fixture column has real nulls).
//
// The authority is the EXPORT's record, not schema nullability: the C++ side
// lists in milvus.valid_data_fields exactly the ids whose source DataArray
// carried a bitmap. Reconcile must make the rebuilt columns agree with that
// list in BOTH directions, or a reduce that mixes results from the two
// transports produces a ValidData shorter than the row count.
func TestReconcileValidData(t *testing.T) {
	const numRows = 3

	t.Run("synthesizes an all-true bitmap when the source had one", func(t *testing.T) {
		// Listed in valid_data_fields, but the rebuilt column has none because
		// the Arrow column had no nulls. This is the ORDER BY shape.
		rec := recordWithMetadata(t, map[string]string{
			fieldOrderMetadataKey:      "100",
			validDataFieldsMetadataKey: "100",
		})
		defer rec.Release()

		cols := []*schemapb.FieldData{int64Col(100, nil)}
		require.NoError(t, ReconcileValidData(rec, cols, numRows))
		assert.Equal(t, []bool{true, true, true}, typeutil.GetFieldDataValidData(cols[0]),
			"a column the source gave a bitmap must come back with one, or "+
				"AppendFieldData appends no validity bit for it and the merged "+
				"ValidData ends up shorter than the row count")
	})

	t.Run("strips a bitmap the source did not have", func(t *testing.T) {
		// Not listed, yet the rebuilt column carries one -- schema nullability
		// won over the export's own record.
		rec := recordWithMetadata(t, map[string]string{
			fieldOrderMetadataKey:      "100",
			validDataFieldsMetadataKey: "",
		})
		defer rec.Release()

		cols := []*schemapb.FieldData{int64Col(100, []bool{true, false, true})}
		require.NoError(t, ReconcileValidData(rec, cols, numRows))
		assert.Empty(t, typeutil.GetFieldDataValidData(cols[0]),
			"the export is the authority in both directions")
	})

	t.Run("leaves an agreeing column untouched", func(t *testing.T) {
		rec := recordWithMetadata(t, map[string]string{
			fieldOrderMetadataKey:      "100,101",
			validDataFieldsMetadataKey: "101",
		})
		defer rec.Release()

		cols := []*schemapb.FieldData{
			int64Col(100, nil),
			int64Col(101, []bool{true, false, true}),
		}
		require.NoError(t, ReconcileValidData(rec, cols, numRows))
		assert.Empty(t, typeutil.GetFieldDataValidData(cols[0]))
		assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(cols[1]),
			"an existing bitmap must be preserved, not overwritten with all-true")
	})

	t.Run("errors when the valid_data_fields key is absent", func(t *testing.T) {
		// A missing key must NOT read as "no column had one": that would
		// silently strip every bitmap in the result.
		rec := recordWithMetadata(t, map[string]string{
			fieldOrderMetadataKey: "100",
		})
		defer rec.Release()

		cols := []*schemapb.FieldData{int64Col(100, []bool{true, false, true})}
		err := ReconcileValidData(rec, cols, numRows)
		require.Error(t, err)
		assert.Contains(t, err.Error(), validDataFieldsMetadataKey)
		assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(cols[0]),
			"the column must be left alone when reconciliation cannot be decided")
	})

	t.Run("errors on malformed valid_data_fields", func(t *testing.T) {
		rec := recordWithMetadata(t, map[string]string{
			fieldOrderMetadataKey:      "100",
			validDataFieldsMetadataKey: "100,not-an-id",
		})
		defer rec.Release()

		require.Error(t, ReconcileValidData(rec,
			[]*schemapb.FieldData{int64Col(100, nil)}, numRows))
	})
}

// TestFieldOrder pins the other metadata key, including that a missing one is an
// error rather than an empty order.
func TestFieldOrder(t *testing.T) {
	t.Run("parses ascending and non-ascending lists", func(t *testing.T) {
		rec := recordWithMetadata(t, map[string]string{
			fieldOrderMetadataKey:      "109,101,104,1",
			validDataFieldsMetadataKey: "",
		})
		defer rec.Release()
		order, err := FieldOrder(rec)
		require.NoError(t, err)
		assert.Equal(t, []int64{109, 101, 104, 1}, order,
			"order must be preserved verbatim -- it is the positional contract "+
				"between the export and the reassembly")
	})

	t.Run("errors when the key is absent", func(t *testing.T) {
		md := arrow.MetadataFrom(map[string]string{})
		schema := arrow.NewSchema(
			[]arrow.Field{{Name: "c0", Type: arrow.PrimitiveTypes.Int64}}, &md)
		bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer bldr.Release()
		bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
		rec := bldr.NewRecord()
		defer rec.Release()

		_, err := FieldOrder(rec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fieldOrderMetadataKey)
	})
}

// TestUserColumnIDs records which ids the materializer treats as user columns:
// the field order minus whatever the protobuf header retained.
func TestUserColumnIDs(t *testing.T) {
	order := []int64{109, 101, 1, 110}
	header := []*schemapb.FieldData{int64Col(1, nil), int64Col(110, nil)}
	assert.Equal(t, []int64{109, 101}, UserColumnIDs(order, header),
		"ids present in the header stayed on the protobuf path and must not be "+
			"matched against Arrow columns")
}
