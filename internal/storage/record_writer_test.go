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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// TestToWriterRecordProjectsOverWideRecord pins the schema-bump additive
// regression: the reader hands the writer a record carrying the row anchor in
// addition to the appended field, while the writer's schema is only the
// appended field. toWriterRecord must narrow the record to the writer's column
// set instead of passing the over-wide record straight to the FFI writer
// (which crashed loon_writer_write on the column-count mismatch).
func TestToWriterRecordProjectsOverWideRecord(t *testing.T) {
	// Writer schema: only the appended field 103.
	writerSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 103, Name: "added", DataType: schemapb.DataType_VarChar},
	}}
	writerArrow, err := ConvertToArrowSchema(writerSchema, false)
	require.NoError(t, err)

	// Over-wide reader record: anchor field 0 (int64) + appended field 103.
	anchorB := array.NewInt64Builder(memory.DefaultAllocator)
	anchorB.AppendValues([]int64{10, 11, 12}, nil)
	anchor := anchorB.NewArray()
	anchorB.Release()
	defer anchor.Release()

	addedB := array.NewStringBuilder(memory.DefaultAllocator)
	addedB.AppendValues([]string{"a", "b", "c"}, nil)
	added := addedB.NewArray()
	addedB.Release()
	defer added.Release()

	wideArrow := arrow.NewSchema([]arrow.Field{
		{Name: "0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "103", Type: arrow.BinaryTypes.String},
	}, nil)
	wideRec := array.NewRecord(wideArrow, []arrow.Array{anchor, added}, 3)
	defer wideRec.Release()
	overWide := NewSimpleArrowRecord(wideRec, map[FieldID]int{0: 0, 103: 1})

	rec, release := toWriterRecord(overWide, writerSchema, writerArrow)
	defer release()

	require.EqualValues(t, 1, rec.NumCols(), "record must be narrowed to the writer's single column")
	require.Equal(t, "added", rec.Schema().Field(0).Name)
	col, ok := rec.Column(0).(*array.String)
	require.True(t, ok)
	require.Equal(t, 3, col.Len())
	require.Equal(t, []string{"a", "b", "c"}, []string{col.Value(0), col.Value(1), col.Value(2)})
}

// TestToWriterRecordFastPathOnMatchingSchema verifies the optimization is kept:
// a simpleArrowRecord whose schema already equals the writer's is returned
// as-is with a no-op release.
func TestToWriterRecordFastPathOnMatchingSchema(t *testing.T) {
	writerSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 103, Name: "added", DataType: schemapb.DataType_VarChar},
	}}
	writerArrow, err := ConvertToArrowSchema(writerSchema, false)
	require.NoError(t, err)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues([]string{"x", "y"}, nil)
	col := b.NewArray()
	b.Release()
	defer col.Release()
	matchRec := array.NewRecord(writerArrow, []arrow.Array{col}, 2)
	defer matchRec.Release()
	sar := NewSimpleArrowRecord(matchRec, map[FieldID]int{103: 0})

	rec, release := toWriterRecord(sar, writerSchema, writerArrow)
	defer release()

	require.Same(t, matchRec, rec, "matching schema must take the fast path and return the backing record")
}
