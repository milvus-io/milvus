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

package queryutil

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeRecord builds a 2-column record (int64 pk, float32 score) carrying the
// same schema metadata the real export attaches.
func makeRecord(t *testing.T, mem memory.Allocator, pks []int64, scores []float32) arrow.Record {
	t.Helper()
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{"109,104", ""})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, &md)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(pks, nil)
	b.Field(1).(*array.Float32Builder).AppendValues(scores, nil)
	return b.NewRecord()
}

func TestBuildMergedArrowColumns(t *testing.T) {
	mem := memory.NewGoAllocator()

	r0 := makeRecord(t, mem, []int64{10, 11, 12}, []float32{1.0, 1.1, 1.2})
	r1 := makeRecord(t, mem, []int64{20, 21}, []float32{2.0, 2.1})
	defer r0.Release()
	defer r1.Release()

	// A nil slot must still occupy an index, or resultIdx stops meaning anything.
	records := []arrow.Record{r0, nil, r1}

	// Interleave across records and go backwards within one, so a bug that
	// assumes sorted or contiguous refs shows up.
	selected := []rowRef{
		{resultIdx: 2, rowIdx: 1}, // 21
		{resultIdx: 0, rowIdx: 2}, // 12
		{resultIdx: 0, rowIdx: 0}, // 10
		{resultIdx: 2, rowIdx: 0}, // 20
	}

	merged, err := buildMergedArrowColumns(records, selected, []int{0, 1})
	require.NoError(t, err)
	require.NotNil(t, merged)
	defer merged.Release()

	require.EqualValues(t, 4, merged.NumRows())
	require.EqualValues(t, 2, merged.NumCols())

	gotPK := merged.Column(0).(*array.Int64)
	gotScore := merged.Column(1).(*array.Float32)
	wantPK := []int64{21, 12, 10, 20}
	wantScore := []float32{2.1, 1.2, 1.0, 2.0}
	for i := range wantPK {
		assert.Equal(t, wantPK[i], gotPK.Value(i), "pk row %d", i)
		assert.InDelta(t, wantScore[i], gotScore.Value(i), 1e-6, "score row %d", i)
	}

	// Schema metadata must survive: the receiving side reassembles columns from
	// milvus.field_order, so losing it is silent corruption rather than an error.
	md := merged.Schema().Metadata()
	idx := md.FindKey("milvus.field_order")
	require.GreaterOrEqual(t, idx, 0, "field_order metadata lost")
	assert.Equal(t, "109,104", md.Values()[idx])
}

func TestBuildMergedArrowColumnsEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()
	r := makeRecord(t, mem, []int64{1, 2}, []float32{1, 2})
	defer r.Release()

	t.Run("no selected rows", func(t *testing.T) {
		out, err := buildMergedArrowColumns([]arrow.Record{r}, nil, []int{0, 1})
		require.NoError(t, err)
		assert.Nil(t, out)
	})

	t.Run("all records nil", func(t *testing.T) {
		out, err := buildMergedArrowColumns([]arrow.Record{nil, nil},
			[]rowRef{{resultIdx: 0, rowIdx: 0}}, []int{0, 1})
		require.NoError(t, err)
		assert.Nil(t, out)
	})

	t.Run("row out of range is an error, not a silent drop", func(t *testing.T) {
		_, err := buildMergedArrowColumns([]arrow.Record{r},
			[]rowRef{{resultIdx: 0, rowIdx: 99}}, []int{0, 1})
		require.Error(t, err)
	})

	t.Run("result index out of range is an error", func(t *testing.T) {
		_, err := buildMergedArrowColumns([]arrow.Record{r},
			[]rowRef{{resultIdx: 7, rowIdx: 0}}, []int{0, 1})
		require.Error(t, err)
	})

	t.Run("column count mismatch is an error", func(t *testing.T) {
		md := arrow.NewMetadata(nil, nil)
		schema1 := arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		}, &md)
		b := array.NewRecordBuilder(mem, schema1)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{5}, nil)
		narrow := b.NewRecord()
		defer narrow.Release()

		_, err := buildMergedArrowColumns([]arrow.Record{r, narrow},
			[]rowRef{{resultIdx: 0, rowIdx: 0}}, []int{0, 1})
		require.Error(t, err)
	})

	t.Run("single record gathers without merging", func(t *testing.T) {
		out, err := buildMergedArrowColumns([]arrow.Record{r},
			[]rowRef{{resultIdx: 0, rowIdx: 1}}, []int{0, 1})
		require.NoError(t, err)
		require.NotNil(t, out)
		defer out.Release()
		assert.EqualValues(t, 1, out.NumRows())
		assert.EqualValues(t, int64(2), out.Column(0).(*array.Int64).Value(0))
	})
}
