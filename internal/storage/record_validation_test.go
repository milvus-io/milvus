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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A Record can violate its logical row contract even when each Arrow array is
// individually well-formed (e.g. independently decoded column groups).
type validationRecord struct {
	rows    int
	columns map[FieldID]arrow.Array
}

func (r *validationRecord) Len() int                      { return r.rows }
func (r *validationRecord) Column(id FieldID) arrow.Array { return r.columns[id] }
func (r *validationRecord) Retain()                       {}
func (r *validationRecord) Release()                      {}

type structureCountingArray struct {
	arrow.Array
	typeChecks   int
	lengthChecks int
}

func (a *structureCountingArray) DataType() arrow.DataType {
	a.typeChecks++
	return a.Array.DataType()
}

func (a *structureCountingArray) Len() int {
	a.lengthChecks++
	return a.Array.Len()
}

func TestRecordValidatorChecksStructureOnlyOnRead(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "value", DataType: schemapb.DataType_Int64},
	}}
	input := mergeSortTestRec(t, map[FieldID][]int64{100: {1, 2}}, nil)
	defer input.Release()
	builder := NewRecordBuilder(schema)
	defer builder.Release()
	require.NoError(t, builder.Append(input, 0, input.Len()))
	output := builder.Build()
	defer output.Release()
	column := &structureCountingArray{Array: output.Column(100)}
	record := &validationRecord{rows: output.Len(), columns: map[FieldID]arrow.Array{100: column}}
	validator, err := NewRecordValidator(schema)
	require.NoError(t, err)

	require.NoError(t, validator.ValidateForRead(record))
	require.Positive(t, column.typeChecks)
	require.Positive(t, column.lengthChecks)
	column.typeChecks, column.lengthChecks = 0, 0
	require.NoError(t, validator.Validate(record))
	require.Zero(t, column.typeChecks, "builder output must not repeat input type checks")
	require.Zero(t, column.lengthChecks, "builder output must not repeat input length checks")
}

func requireIntegrityFailure(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	status := merr.Status(err)
	require.EqualValues(t, 1009, status.GetCode())
	require.False(t, status.GetRetriable())
	require.NotEqual(t, "true", status.GetExtraInfo()["is_input_error"])
}

func TestSortPreservesDefaultValues(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, path := range []string{"sort", "merge", "append"} {
			t.Run(fmt.Sprintf("%s/nullable=%t", path, nullable), func(t *testing.T) {
				schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
					{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64, Nullable: nullable,
						DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 10}}},
				}}
				keys := mergeSortTestRec(t, map[FieldID][]int64{100: {1, 2}}, nil)
				defer keys.Release()
				b := array.NewInt64Builder(memory.DefaultAllocator)
				b.AppendNull()
				b.Append(20)
				values := b.NewArray()
				b.Release()
				defer values.Release()
				input := &validationRecord{rows: 2, columns: map[FieldID]arrow.Array{100: keys.Column(100), 101: values}}
				var got []int64
				writer := &MockRecordWriter{writefn: func(r Record) error {
					out := r.Column(101).(*array.Int64)
					require.Zero(t, out.NullN())
					got = append(got, out.Int64Values()...)
					return nil
				}}
				readers := []RecordReader{&oneShotRecordReader{rec: input}}
				validator, err := NewRecordValidator(schema)
				require.NoError(t, err)
				readers[0] = validator.WrapReader(readers[0])
				output := validator.WrapWriter(writer)
				switch path {
				case "sort":
					_, _, err = Sort(1024, schema, readers, output, func(Record, int, int) bool { return true }, []int64{100})
				case "merge":
					_, err = MergeSort(1024, schema, readers, output, func(Record, int, int) bool { return true }, []int64{100})
				case "append":
					rb := NewRecordBuilder(schema)
					defer rb.Release()
					err = rb.Append(input, 0, input.Len())
					if err == nil {
						out := rb.Build()
						defer out.Release()
						err = writer.Write(out)
					}
				}
				require.NoError(t, err)
				require.Equal(t, []int64{10, 20}, got)
			})
		}
	}
}

func TestSortValidatesOnlySurvivingOutputBeforeWriter(t *testing.T) {
	for _, malformed := range []string{"null", "vector width"} {
		for _, merge := range []bool{false, true} {
			for _, filtered := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/merge=%t/filtered=%t", malformed, merge, filtered), func(t *testing.T) {
					field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64}
					var column arrow.Array
					if malformed == "null" {
						column = array.MakeArrayOfNull(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, 1)
					} else {
						field.DataType, field.Nullable = schemapb.DataType_FloatVector, true
						field.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}
						b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
						b.Append([]byte{1, 2}) // two float32 values require eight bytes
						column = b.NewArray()
						b.Release()
					}
					defer column.Release()
					schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64}, field,
					}}
					keys := mergeSortTestRec(t, map[FieldID][]int64{100: {1}}, nil)
					defer keys.Release()
					input := &validationRecord{rows: 1, columns: map[FieldID]arrow.Array{100: keys.Column(100), 101: column}}
					writes := 0
					writer := &MockRecordWriter{writefn: func(Record) error { writes++; return nil }}
					readers := []RecordReader{&oneShotRecordReader{rec: input}}
					predicate := func(Record, int, int) bool { return !filtered }
					validator, err := NewRecordValidator(schema)
					require.NoError(t, err)
					readers[0] = validator.WrapReader(readers[0])
					output := validator.WrapWriter(writer)
					if merge {
						_, err = MergeSort(1024, schema, readers, output, predicate, []int64{100})
					} else {
						_, _, err = Sort(1024, schema, readers, output, predicate, []int64{100})
					}
					if filtered {
						require.NoError(t, err)
					} else {
						requireIntegrityFailure(t, err)
					}
					require.Zero(t, writes, "invalid or filtered output must not reach writer statistics or native IO")
				})
			}
		}
	}
}

func TestRecordValidatorRejectsMalformedInputStructure(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	physical, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	validator, err := NewRecordValidator(schema)
	require.NoError(t, err)
	valid := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 8})
	valid.Append(make([]byte, 8))
	column := valid.NewArray()
	valid.Release()
	defer column.Release()
	nulls := array.MakeArrayOfNull(memory.DefaultAllocator, physical.Field(0).Type, 1)
	defer nulls.Release()
	wrong := array.MakeArrayOfNull(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, 1)
	defer wrong.Release()
	for name, input := range map[string]Record{
		"nil record":     nil,
		"missing column": &validationRecord{rows: 1},
		"short column":   &validationRecord{rows: 2, columns: map[FieldID]arrow.Array{101: column}},
		"wrong type":     &validationRecord{rows: 1, columns: map[FieldID]arrow.Array{101: wrong}},
	} {
		t.Run(name, func(t *testing.T) {
			if input != nil {
				defer input.Release()
			}
			require.NotPanics(t, func() {
				requireIntegrityFailure(t, validator.ValidateForRead(input))
			})
		})
	}
	t.Run("nonnullable null is checked only on output", func(t *testing.T) {
		record := NewSimpleArrowRecord(array.NewRecord(physical, []arrow.Array{nulls}, 1), map[FieldID]int{101: 0})
		defer record.Release()
		require.NoError(t, validator.ValidateForRead(record))
		requireIntegrityFailure(t, validator.Validate(record))
	})
}

func TestSortRejectsMissingFieldBeforePredicate(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "missing", DataType: schemapb.DataType_Int64},
	}}
	validator, err := NewRecordValidator(schema)
	require.NoError(t, err)
	input := mergeSortTestRec(t, map[FieldID][]int64{100: {1}}, nil)
	defer input.Release()
	for _, merge := range []bool{false, true} {
		calls := 0
		predicate := func(r Record, _, _ int) bool { calls++; r.Column(101); return true }
		require.NotPanics(t, func() {
			var err error
			readers := []RecordReader{validator.WrapReader(&oneShotRecordReader{rec: input})}
			if merge {
				_, err = MergeSort(1024, schema, readers, nil, predicate, []int64{100})
			} else {
				_, _, err = Sort(1024, schema, readers, nil, predicate, []int64{100})
			}
			requireIntegrityFailure(t, err)
		})
		require.Zero(t, calls)
	}
}

func TestSortPreservesVectorElementValidation(t *testing.T) {
	for _, elementNullable := range []bool{false, true} {
		field := &schemapb.FieldSchema{
			FieldID: 101, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: elementNullable,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
		}
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
		physical, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		validator, err := NewRecordValidator(schema)
		require.NoError(t, err)
		for _, scenario := range []string{"visible null", "null parent", "sliced parent", "wrong width", "filtered null"} {
			if scenario == "wrong width" && !elementNullable {
				continue
			}
			t.Run(scenario+"/"+physical.Field(0).Type.String(), func(t *testing.T) {
				b := array.NewBuilder(memory.DefaultAllocator, physical.Field(0).Type).(*array.ListBuilder)
				b.Append(scenario != "null parent")
				child := b.ValueBuilder()
				if scenario == "wrong width" {
					child.(*array.BinaryBuilder).Append([]byte{1})
				} else {
					child.AppendNull()
				}
				b.Append(true)
				switch values := child.(type) {
				case *array.BinaryBuilder:
					values.Append(make([]byte, 8))
				case *array.FixedSizeBinaryBuilder:
					values.Append(make([]byte, 8))
				}
				column := b.NewArray()
				b.Release()
				defer column.Release()
				if scenario == "sliced parent" {
					column = array.NewSlice(column, 1, 2)
					defer column.Release()
				}
				input := NewSimpleArrowRecord(array.NewRecord(physical, []arrow.Array{column}, int64(column.Len())), map[FieldID]int{101: 0})
				defer input.Release()
				for _, merge := range []bool{false, true} {
					t.Run(fmt.Sprintf("merge=%t", merge), func(t *testing.T) {
						writes := 0
						writer := validator.WrapWriter(&MockRecordWriter{writefn: func(Record) error { writes++; return nil }})
						readers := []RecordReader{validator.WrapReader(&oneShotRecordReader{rec: input})}
						predicate := func(Record, int, int) bool { return scenario != "filtered null" }
						var err error
						if merge {
							_, err = MergeSort(1024, schema, readers, writer, predicate, nil)
						} else {
							_, _, err = Sort(1024, schema, readers, writer, predicate, nil)
						}
						invalid := scenario == "wrong width" || (scenario == "visible null" && !elementNullable)
						if invalid {
							requireIntegrityFailure(t, err)
						} else {
							require.NoError(t, err)
						}
						if invalid || scenario == "filtered null" {
							require.Zero(t, writes)
						} else {
							require.Positive(t, writes)
						}
					})
				}
			})
		}
	}
}
