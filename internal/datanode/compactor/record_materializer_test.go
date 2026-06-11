package compactor

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type materializerTestRecord struct {
	columns      map[storage.FieldID]arrow.Array
	len          int
	releaseCount int
	retainCount  int
}

func (r *materializerTestRecord) Column(fieldID storage.FieldID) arrow.Array {
	return r.columns[fieldID]
}

func (r *materializerTestRecord) Len() int {
	return r.len
}

func (r *materializerTestRecord) Release() {
	r.releaseCount++
}

func (r *materializerTestRecord) Retain() {
	r.retainCount++
}

type materializerTestReader struct {
	records []*materializerTestRecord
	idx     int
	closed  bool
}

func (r *materializerTestReader) Next() (storage.Record, error) {
	if r.idx >= len(r.records) {
		return nil, errors.New("no more records")
	}
	record := r.records[r.idx]
	r.idx++
	return record, nil
}

func (r *materializerTestReader) Close() error {
	r.closed = true
	return nil
}

type selectedColumnMaterializer struct {
	fieldID int64
}

func (m selectedColumnMaterializer) Materialize(rec storage.Record) (map[int64]arrow.Array, error) {
	rec.Column(m.fieldID)
	return nil, nil
}

func (m selectedColumnMaterializer) Close() {}

type materializerTestFunctionRunner struct {
	schema       *schemapb.FunctionSchema
	inputFields  []*schemapb.FieldSchema
	outputFields []*schemapb.FieldSchema
	outputs      []any
	err          error
	closed       bool
	inputs       []any
}

func (r *materializerTestFunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	r.inputs = inputs
	return r.outputs, r.err
}

func (r *materializerTestFunctionRunner) GetSchema() *schemapb.FunctionSchema {
	return r.schema
}

func (r *materializerTestFunctionRunner) GetOutputFields() []*schemapb.FieldSchema {
	return r.outputFields
}

func (r *materializerTestFunctionRunner) GetInputFields() []*schemapb.FieldSchema {
	return r.inputFields
}

func (r *materializerTestFunctionRunner) Close() {
	r.closed = true
}

var _ function.FunctionRunner = (*materializerTestFunctionRunner)(nil)

func TestRecordMaterializerWrapNoOpWhenAllFieldsExist(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}, 101: {}})
	require.NoError(t, err)
	defer materializer.Close()

	record := &materializerTestRecord{len: 2}
	wrapped, err := materializer.Wrap(record)
	require.NoError(t, err)
	require.Same(t, record, wrapped)
}

func TestRecordMaterializerWrapFillsNullableMissingFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)
	defer materializer.Close()

	record := &materializerTestRecord{len: 3}
	wrapped, err := materializer.Wrap(record)
	require.NoError(t, err)
	require.NotSame(t, record, wrapped)

	column := wrapped.Column(101)
	require.NotNil(t, column)
	require.Equal(t, 3, column.Len())
	require.Equal(t, 3, column.NullN())
	wrapped.Release()
}

func TestRecordMaterializerWrapWithSelectionMaterializesKeptRowsOnly(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)
	defer materializer.Close()

	input := newStringArray(t, []string{"drop-0", "keep-1", "drop-2", "keep-3"})
	defer input.Release()
	record := &materializerTestRecord{len: 4, columns: map[storage.FieldID]arrow.Array{100: input}}
	selection := &recordSelection{ranges: []rowRange{{start: 1, end: 2}, {start: 3, end: 4}}, length: 2}

	wrapped, err := materializer.WrapWithSelection(record, selection)
	require.NoError(t, err)
	require.Equal(t, 2, wrapped.Len())

	textColumn := wrapped.Column(100).(*array.String)
	require.Equal(t, []string{"keep-1", "keep-3"}, []string{textColumn.Value(0), textColumn.Value(1)})
	addedColumn := wrapped.Column(101)
	require.NotNil(t, addedColumn)
	require.Equal(t, 2, addedColumn.Len())
	require.Equal(t, 2, addedColumn.NullN())

	wrapped.Release()
	require.Equal(t, 1, record.releaseCount)
}

func TestRecordMaterializerWrapWithSelectionReturnsLazyColumnError(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
	}}
	materializer := &RecordMaterializer{
		schema:        schema,
		materializers: []FunctionMaterializer{selectedColumnMaterializer{fieldID: 100}},
	}

	input := newInt64Array(t, []int64{1, 2})
	defer input.Release()
	record := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: input}}
	selection := &recordSelection{ranges: []rowRange{{start: 0, end: 1}}, length: 1}

	wrapped, err := materializer.WrapWithSelection(record, selection)
	require.Nil(t, wrapped)
	require.ErrorContains(t, err, "failed to append value")
}

func TestRecordMaterializerWrapSkipsMissingSystemFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)
	defer materializer.Close()

	record := &materializerTestRecord{len: 3}
	wrapped, err := materializer.Wrap(record)
	require.NoError(t, err)
	require.Same(t, record, wrapped)
}

func TestRecordMaterializerWrapFailsForMissingNonNullableField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "required", DataType: schemapb.DataType_Int64},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)
	defer materializer.Close()

	_, err = materializer.Wrap(&materializerTestRecord{len: 3})
	require.Error(t, err)
	require.ErrorContains(t, err, "missing field data required")
}

func TestBM25FunctionMaterializerMaterializesSparseOutput(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerBM25Schema()
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
		outputs: []any{&schemapb.SparseFloatArray{
			Dim: 16,
			Contents: [][]byte{
				typeutil.CreateSparseFloatRow([]uint32{1}, []float32{0.5}),
				typeutil.CreateSparseFloatRow([]uint32{2}, []float32{0.8}),
			},
		}},
	}
	materializer, err := newBM25FunctionMaterializer(schema, runner, []int{0}, false)
	require.NoError(t, err)
	defer materializer.Close()

	input := newStringArray(t, []string{"hello", "world"})
	defer input.Release()
	record := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: input}}

	arrays, err := materializer.Materialize(record)
	require.NoError(t, err)
	defer releaseArrowArrays(arrays)
	require.Contains(t, arrays, int64(101))
	require.Equal(t, 2, arrays[101].Len())
	require.Equal(t, []any{[]string{"hello", "world"}}, runner.inputs)
}

func TestBM25FunctionMaterializerRejectsBinaryTextInputWithoutLOBDecoding(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerBM25Schema()
	inputField.DataType = schemapb.DataType_Text
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
		outputs: []any{&schemapb.SparseFloatArray{
			Dim:      16,
			Contents: [][]byte{typeutil.CreateSparseFloatRow([]uint32{1}, []float32{0.5})},
		}},
	}
	materializer, err := newBM25FunctionMaterializer(schema, runner, []int{0}, false)
	require.NoError(t, err)
	defer materializer.Close()

	input := newBinaryArray(t, [][]byte{[]byte("encoded-lob-ref")})
	defer input.Release()
	_, err = materializer.Materialize(&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{100: input}})
	require.ErrorContains(t, err, "cannot materialize bm25 from text binary values without lob decoding")
	require.Nil(t, runner.inputs)
}

func TestBM25FunctionMaterializerMaterializesNullableInputAsNonNullableOutput(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerBM25Schema()
	inputField.Nullable = true
	firstRow := typeutil.CreateSparseFloatRow([]uint32{1}, []float32{0.5})
	emptyRow := typeutil.CreateSparseFloatRow(nil, nil)
	thirdRow := typeutil.CreateSparseFloatRow([]uint32{2}, []float32{0.8})
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
		outputs: []any{&schemapb.SparseFloatArray{
			Dim:      16,
			Contents: [][]byte{firstRow, emptyRow, thirdRow},
		}},
	}
	materializer, err := newBM25FunctionMaterializer(schema, runner, []int{0}, false)
	require.NoError(t, err)
	defer materializer.Close()

	input := newNullableStringArray(t, []string{"hello", "", "world"}, []bool{true, false, true})
	defer input.Release()
	record := &materializerTestRecord{len: 3, columns: map[storage.FieldID]arrow.Array{100: input}}

	arrays, err := materializer.Materialize(record)
	require.NoError(t, err)
	defer releaseArrowArrays(arrays)
	output := arrays[101]
	require.Equal(t, 3, output.Len())
	require.Zero(t, output.NullN())
	require.True(t, output.IsValid(0))
	require.True(t, output.IsValid(1))
	require.True(t, output.IsValid(2))
	binaryOutput := output.(*array.Binary)
	require.Equal(t, firstRow, binaryOutput.Value(0))
	require.Equal(t, emptyRow, binaryOutput.Value(1))
	require.Equal(t, thirdRow, binaryOutput.Value(2))
	require.Equal(t, []any{[]string{"hello", "", "world"}}, runner.inputs)
}

func TestBM25FunctionMaterializerRejectsNullableOutput(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerBM25Schema()
	outputField.Nullable = true
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
	}

	_, err := newBM25FunctionMaterializer(schema, runner, []int{0}, false)
	require.ErrorContains(t, err, "function output field cannot be nullable")
}

func TestBM25FunctionMaterializerRejectsBadRunnerOutput(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerBM25Schema()

	newMaterializer := func(outputs []any) *bm25FunctionMaterializer {
		runner := &materializerTestFunctionRunner{
			schema:       functionSchema,
			inputFields:  []*schemapb.FieldSchema{inputField},
			outputFields: []*schemapb.FieldSchema{outputField},
			outputs:      outputs,
		}
		materializer, err := newBM25FunctionMaterializer(schema, runner, []int{0}, false)
		require.NoError(t, err)
		return materializer
	}

	t.Run("output count mismatch", func(t *testing.T) {
		input := newStringArray(t, []string{"hello"})
		defer input.Release()
		_, err := newMaterializer(nil).Materialize(&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{100: input}})
		require.ErrorContains(t, err, "expects 1 outputs")
	})

	t.Run("output type not sparse float array", func(t *testing.T) {
		input := newStringArray(t, []string{"hello"})
		defer input.Release()
		_, err := newMaterializer([]any{&schemapb.FloatArray{Data: []float32{1}}}).Materialize(&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{100: input}})
		require.ErrorContains(t, err, "unexpected output type")
	})

	t.Run("output row count mismatch", func(t *testing.T) {
		input := newStringArray(t, []string{"hello", "world"})
		defer input.Release()
		_, err := newMaterializer([]any{&schemapb.SparseFloatArray{
			Dim:      16,
			Contents: [][]byte{typeutil.CreateSparseFloatRow([]uint32{1}, []float32{0.5})},
		}}).Materialize(&materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: input}})
		require.ErrorContains(t, err, "bm25 function output row count mismatch")
	})

	t.Run("input field column missing", func(t *testing.T) {
		_, err := newMaterializer([]any{}).Materialize(&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{}})
		require.ErrorContains(t, err, "input field 100 not found")
	})

	t.Run("input column not string or binary", func(t *testing.T) {
		input := newInt64Array(t, []int64{1})
		defer input.Release()
		_, err := newMaterializer([]any{}).Materialize(&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{100: input}})
		require.ErrorContains(t, err, "data type must be varchar or text")
	})
}

func TestFunctionOutputIndexesToMaterializePartialStateReturnsAllOutputs(t *testing.T) {
	functionSchema := &schemapb.FunctionSchema{OutputFieldIds: []int64{101, 102}}
	require.Nil(t, functionOutputIndexesToMaterialize(functionSchema, map[int64]struct{}{101: {}, 102: {}}))
	require.Equal(t, []int{0, 1}, functionOutputIndexesToMaterialize(functionSchema, map[int64]struct{}{101: {}}))
}

func TestMaterializedRecordReaderReleasesPreviousRecordOnNextAndClose(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)

	first := &materializerTestRecord{len: 1}
	second := &materializerTestRecord{len: 1}
	base := &materializerTestReader{records: []*materializerTestRecord{first, second}}
	reader := newMaterializedRecordReader(base, materializer)

	_, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, 0, first.releaseCount)

	_, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, 1, first.releaseCount)
	require.Equal(t, 0, second.releaseCount)

	require.NoError(t, reader.Close())
	require.Equal(t, 1, second.releaseCount)
	require.True(t, base.closed)
}

func materializerBM25Schema() (*schemapb.CollectionSchema, *schemapb.FunctionSchema, *schemapb.FieldSchema, *schemapb.FieldSchema) {
	inputField := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar}
	outputField := &schemapb.FieldSchema{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}
	functionSchema := &schemapb.FunctionSchema{
		Name:           "bm25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{100},
		OutputFieldIds: []int64{101},
	}
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{inputField, outputField}, Functions: []*schemapb.FunctionSchema{functionSchema}}, functionSchema, inputField, outputField
}

func materializerMinHashSchema() (*schemapb.CollectionSchema, *schemapb.FunctionSchema, *schemapb.FieldSchema, *schemapb.FieldSchema) {
	inputField := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
	}
	outputField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "minhash",
		DataType: schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "32"},
		},
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:           "minhash_func",
		Type:           schemapb.FunctionType_MinHash,
		InputFieldIds:  []int64{inputField.GetFieldID()},
		OutputFieldIds: []int64{outputField.GetFieldID()},
	}
	schema := &schemapb.CollectionSchema{
		Fields:    []*schemapb.FieldSchema{inputField, outputField},
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}
	return schema, functionSchema, inputField, outputField
}

func TestMinHashFunctionMaterializerMaterializesBinaryOutput(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerMinHashSchema()
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
		outputs: []any{&schemapb.FieldData{
			Type:    schemapb.DataType_BinaryVector,
			FieldId: outputField.GetFieldID(),
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 32,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: []byte{
							0x01, 0x02, 0x03, 0x04,
							0x05, 0x06, 0x07, 0x08,
						},
					},
				},
			},
		}},
	}
	materializer, err := newFunctionMaterializer(schema, runner, []int{0}, false)
	require.NoError(t, err)
	defer materializer.Close()

	input := newStringArray(t, []string{"hello", "world"})
	defer input.Release()
	record := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: input}}

	arrays, err := materializer.Materialize(record)
	require.NoError(t, err)
	require.Len(t, arrays, 1)
	defer releaseArrowArrays(arrays)

	output := arrays[101]
	require.NotNil(t, output)
	require.Equal(t, 2, output.Len())
	binary, ok := output.(*array.FixedSizeBinary)
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, binary.Value(0))
	require.Equal(t, []byte{0x05, 0x06, 0x07, 0x08}, binary.Value(1))
	require.Len(t, runner.inputs, 1)
	require.Equal(t, []string{"hello", "world"}, runner.inputs[0])
}

func TestMinHashFunctionMaterializerRejectsNonBinaryOutputField(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerMinHashSchema()
	outputField.DataType = schemapb.DataType_FloatVector
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
	}

	_, err := newFunctionMaterializer(schema, runner, []int{0}, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "output field data type must be binary vector for minhash function materialization")
}

func TestMinHashFunctionMaterializerRejectsRowCountMismatch(t *testing.T) {
	schema, functionSchema, inputField, outputField := materializerMinHashSchema()
	runner := &materializerTestFunctionRunner{
		schema:       functionSchema,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{outputField},
		outputs: []any{&schemapb.FieldData{
			Type:    schemapb.DataType_BinaryVector,
			FieldId: outputField.GetFieldID(),
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 32,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: []byte{0x01, 0x02, 0x03, 0x04},
					},
				},
			},
		}},
	}
	materializer, err := newFunctionMaterializer(schema, runner, []int{0}, false)
	require.NoError(t, err)
	defer materializer.Close()

	input := newStringArray(t, []string{"hello", "world"})
	defer input.Release()
	record := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: input}}

	arrays, err := materializer.Materialize(record)
	require.Nil(t, arrays)
	require.Error(t, err)
	require.ErrorContains(t, err, "minhash function output row count mismatch")
}

func TestRecordMaterializerMaterializesBM25AndMinHashOutputs(t *testing.T) {
	bm25Schema, bm25Function, inputField, bm25Output := materializerBM25Schema()
	minHashOutput := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "minhash",
		DataType: schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "32"},
		},
	}
	minHashFunction := &schemapb.FunctionSchema{
		Name:           "minhash_func",
		Type:           schemapb.FunctionType_MinHash,
		InputFieldIds:  []int64{inputField.GetFieldID()},
		OutputFieldIds: []int64{minHashOutput.GetFieldID()},
	}
	bm25Schema.Fields = append(bm25Schema.Fields, minHashOutput)
	bm25Schema.Functions = []*schemapb.FunctionSchema{bm25Function, minHashFunction}

	input := newStringArray(t, []string{"hello", "world"})
	defer input.Release()
	record := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: input}}

	bm25Runner := &materializerTestFunctionRunner{
		schema:       bm25Function,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{bm25Output},
		outputs: []any{&schemapb.SparseFloatArray{
			Dim: 16,
			Contents: [][]byte{
				typeutil.CreateSparseFloatRow([]uint32{1}, []float32{0.5}),
				typeutil.CreateSparseFloatRow([]uint32{2}, []float32{0.8}),
			},
		}},
	}
	minHashRunner := &materializerTestFunctionRunner{
		schema:       minHashFunction,
		inputFields:  []*schemapb.FieldSchema{inputField},
		outputFields: []*schemapb.FieldSchema{minHashOutput},
		outputs: []any{&schemapb.FieldData{
			Type:    schemapb.DataType_BinaryVector,
			FieldId: minHashOutput.GetFieldID(),
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim:  32,
					Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
				},
			},
		}},
	}
	bm25Materializer, err := newFunctionMaterializer(bm25Schema, bm25Runner, []int{0}, false)
	require.NoError(t, err)
	minHashMaterializer, err := newFunctionMaterializer(bm25Schema, minHashRunner, []int{0}, false)
	require.NoError(t, err)
	materializer := &RecordMaterializer{
		schema:        bm25Schema,
		materializers: []FunctionMaterializer{bm25Materializer, minHashMaterializer},
	}
	defer materializer.Close()

	wrapped, err := materializer.Wrap(record)
	require.NoError(t, err)
	defer wrapped.Release()

	require.NotNil(t, wrapped.Column(bm25Output.GetFieldID()))
	require.NotNil(t, wrapped.Column(minHashOutput.GetFieldID()))
	require.Equal(t, 2, wrapped.Len())
}

func newStringArray(t *testing.T, values []string) *array.String {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	t.Cleanup(builder.Release)
	builder.AppendValues(values, nil)
	return builder.NewStringArray()
}

func newNullableStringArray(t *testing.T, values []string, valid []bool) *array.String {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	t.Cleanup(builder.Release)
	builder.AppendValues(values, valid)
	return builder.NewStringArray()
}

func newBinaryArray(t *testing.T, values [][]byte) *array.Binary {
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	t.Cleanup(builder.Release)
	builder.AppendValues(values, nil)
	return builder.NewBinaryArray()
}

func newInt64Array(t *testing.T, values []int64) *array.Int64 {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	t.Cleanup(builder.Release)
	builder.AppendValues(values, nil)
	return builder.NewInt64Array()
}
