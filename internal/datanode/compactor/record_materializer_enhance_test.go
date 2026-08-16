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

package compactor

// RecordMaterializer white-box unit tests — UT_ENHANCEMENT_PLAN Part A gaps.
// The pre-existing record_materializer_test.go already pins BM25/MinHash output
// bytes (mock-runner oracle) and selection lifecycle; this file closes the gaps
// the design flagged as uncovered at unit level:
//   A1  absent nullable field: bit-by-bit null bitmap + preserved-column byte identity
//       (existing test only checked Len()/NullN() counts — S0).
//   A1b absent field WITH default: per-row default value (existing had none).
//   A7  stringInputsFromRecord null-cell branch (RM-15b) and missing-column error
//       (RM-15a) — S0/S2.
// A2/A3/A4 (inputView immutability + selection×function composition) run through
// the REAL function runner, which unit tests deliberately avoid (runner needs
// tokenizer init); they are covered end-to-end by the bump integration read-back
// tests (TestAdditiveBM25Output_ReadBack / TestAdditiveMinHashOutput_ReadBack).

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

// TestMaterializerAbsentNullableBitmapExact — A1/S0. Every row of a synthesized
// absent nullable field must be NULL bit-by-bit, and the preserved column must be
// byte-identical. Count-only (Len/NullN) checks cannot catch a corrupt bitmap.
func TestMaterializerAbsentNullableBitmapExact(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 103, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	m, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)
	defer m.Close()

	pk := newInt64Array(t, []int64{7, 8, 9})
	defer pk.Release()
	rec := &materializerTestRecord{len: 3, columns: map[storage.FieldID]arrow.Array{100: pk}}

	wrapped, err := m.Wrap(rec)
	require.NoError(t, err)
	defer cleanupMaterializedRecord(wrapped)

	col := wrapped.Column(103)
	require.NotNil(t, col)
	require.Equal(t, 3, col.Len())
	for i := 0; i < 3; i++ {
		require.True(t, col.IsNull(i), "row %d of absent nullable field must be null (bitmap bit)", i)
	}
	pkOut, ok := wrapped.Column(100).(*array.Int64)
	require.True(t, ok)
	require.Equal(t, []int64{7, 8, 9}, []int64{pkOut.Value(0), pkOut.Value(1), pkOut.Value(2)}, "preserved pk must be byte-identical")
}

// TestMaterializerAbsentDefaultValueExact — A1b/S0. Absent field WITH a default
// must materialize the default value on every row (non-null), not null.
func TestMaterializerAbsentDefaultValueExact(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 103, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}},
	}}
	m, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)
	defer m.Close()

	pk := newInt64Array(t, []int64{1, 2})
	defer pk.Release()
	rec := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: pk}}

	wrapped, err := m.Wrap(rec)
	require.NoError(t, err)
	defer cleanupMaterializedRecord(wrapped)

	col, ok := wrapped.Column(103).(*array.Int64)
	require.True(t, ok)
	require.Equal(t, 2, col.Len())
	for i := 0; i < 2; i++ {
		require.True(t, col.IsValid(i), "default row %d must be non-null", i)
		require.Equal(t, int64(42), col.Value(i), "default row %d value", i)
	}
}

// TestStringInputsFromRecordNullAndMissing — A7/S0+S2. A null text cell must
// become an empty string (not panic, not garbage); a missing input column must
// error deterministically.
func TestStringInputsFromRecordNullAndMissing(t *testing.T) {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.Append("a")
	b.AppendNull()
	b.Append("c")
	arr := b.NewArray()
	defer arr.Release()

	rec := &materializerTestRecord{len: 3, columns: map[storage.FieldID]arrow.Array{100: arr}}
	got, err := stringInputsFromRecord(rec, 100)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "", "c"}, got, "null cell must map to empty string")

	_, err = stringInputsFromRecord(&materializerTestRecord{len: 1}, 999)
	require.Error(t, err, "missing input column must error, not panic")
}
