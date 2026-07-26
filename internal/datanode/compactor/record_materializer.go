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

import (
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type FunctionMaterializer interface {
	Materialize(rec storage.Record) (map[int64]arrow.Array, error)
	Close()
}

type rowRange struct {
	start int
	end   int
}

type recordSelection struct {
	ranges []rowRange
	length int
}

func (s *recordSelection) Len() int {
	if s == nil {
		return 0
	}
	return s.length
}

type RecordMaterializer struct {
	materializers []FunctionMaterializer
	missingFields []*schemapb.FieldSchema
	schema        *schemapb.CollectionSchema
	// existingFields is the set of fields physically present in the source
	// records. Selection slicing is gated on it: probing a record for a field
	// it does not carry is not an option, since V2/V3 records panic on Column
	// for an unknown field instead of returning nil.
	existingFields map[int64]struct{}
}

// NewRecordMaterializer builds a full materializer: it computes the absent
// function outputs and prefills every other schema field existingFields does
// not carry, so the wrapped record covers the whole schema (full rewrites,
// sort/mix/clustering, which persist every column).
func NewRecordMaterializer(schema *schemapb.CollectionSchema, functions []*schemapb.FunctionSchema, existingFields map[int64]struct{}) (*RecordMaterializer, error) {
	materializer, materializedFields, _, err := newRecordMaterializerBase(schema, functions, existingFields)
	if err != nil {
		return nil, err
	}
	materializer.missingFields = missingNonMaterializedSchemaFields(schema, existingFields, materializedFields)
	return materializer, nil
}

// NewFunctionBackfillMaterializer scopes materialization to backfilling the
// given functions' outputs: it computes those outputs and prefills only their
// inputs absent from existingFields. Every other absent schema field stays
// untouched — an added field is nullable by DDL mandate and thus synthesizable
// at read time, so a version bump never requires materializing it physically
// (the bump-only path stamps metadata without touching data at all); rewriting
// compactions materialize such fields only as a byproduct of writing the full
// schema. existingFields must be the segment's physical storage truth.
func NewFunctionBackfillMaterializer(schema *schemapb.CollectionSchema, missingFunctions []*schemapb.FunctionSchema, existingFields map[int64]struct{}) (*RecordMaterializer, error) {
	materializer, _, activeInputFields, err := newRecordMaterializerBase(schema, missingFunctions, existingFields)
	if err != nil {
		return nil, err
	}
	materializer.missingFields = absentInputFields(activeInputFields, existingFields)
	return materializer, nil
}

// newRecordMaterializerBase sets up the function runners for every function
// with at least one output to materialize, leaving missingFields (the prefill
// set) to the exported constructors. The returned input fields are the
// runner-declared reads of those functions — the runner, not the function
// schema, is the authority on what gets read: it resolves implicit inputs
// (e.g. the multi-analyzer by_field column) that InputFieldIds never lists.
func newRecordMaterializerBase(schema *schemapb.CollectionSchema, functions []*schemapb.FunctionSchema, existingFields map[int64]struct{}) (*RecordMaterializer, map[int64]struct{}, []*schemapb.FieldSchema, error) {
	materializer := &RecordMaterializer{schema: schema, existingFields: existingFields}
	materializedFields := make(map[int64]struct{})
	var activeInputFields []*schemapb.FieldSchema
	for _, functionSchema := range functions {
		outputIndexes := functionOutputIndexesToMaterialize(functionSchema, existingFields)
		if len(outputIndexes) == 0 {
			continue
		}
		for _, outputIndex := range outputIndexes {
			materializedFields[functionSchema.GetOutputFieldIds()[outputIndex]] = struct{}{}
		}

		runner, err := function.NewFunctionRunner(schema, functionSchema)
		if err != nil {
			materializer.Close()
			return nil, nil, nil, err
		}
		if runner == nil {
			materializer.Close()
			return nil, nil, nil, merr.WrapErrFunctionFailedMsg("failed to set up function runner for %s", functionSchema.GetName())
		}
		functionMaterializer, err := newFunctionMaterializer(schema, runner, outputIndexes, true)
		if err != nil {
			runner.Close()
			materializer.Close()
			return nil, nil, nil, err
		}
		materializer.materializers = append(materializer.materializers, functionMaterializer)
		activeInputFields = append(activeInputFields, runner.GetInputFields()...)
	}
	return materializer, materializedFields, activeInputFields, nil
}

// absentInputFields returns the input fields existingFields does not carry —
// the exact set a backfill materializer must prefill before running the
// functions.
func absentInputFields(inputFields []*schemapb.FieldSchema, existingFields map[int64]struct{}) []*schemapb.FieldSchema {
	seen := make(map[int64]struct{})
	missing := make([]*schemapb.FieldSchema, 0)
	for _, field := range inputFields {
		fieldID := field.GetFieldID()
		if _, ok := existingFields[fieldID]; ok {
			continue
		}
		if _, dup := seen[fieldID]; dup {
			continue
		}
		seen[fieldID] = struct{}{}
		missing = append(missing, field)
	}
	return missing
}

func (m *RecordMaterializer) Wrap(rec storage.Record) (storage.Record, error) {
	return m.WrapWithSelection(rec, nil)
}

// WrapWithSelection wraps rec — optionally filtered to selection — filling absent
// function outputs and missing schema fields. rec stays borrowed from its reader
// and is valid until the reader's next Next/Close; the caller must clean up only
// the derived arrays owned by the returned record (cleanupMaterializedRecord),
// never the input record itself. Callers that keep the returned record across a
// reader advance must Retain/Release it explicitly (see storage.Sort).
func (m *RecordMaterializer) WrapWithSelection(rec storage.Record, selection *recordSelection) (storage.Record, error) {
	base := rec
	if selection != nil {
		selected, err := newSelectedRecord(rec, m.schema, m.existingFields, selection)
		if err != nil {
			return nil, err
		}
		base = selected
	}
	if !m.hasMaterialization() {
		return base, nil
	}

	computed := make(map[int64]arrow.Array)
	view := &materializedRecord{base: base, computed: computed}

	// Fill ordinary missing fields before running functions. Function inputs may
	// themselves be fields added by an earlier schema evolution, so runners must
	// read through this overlay instead of probing the physical source record.
	for _, field := range m.missingFields {
		fieldID := field.GetFieldID()
		arr, err := storage.GenerateEmptyArrayFromSchema(field, base.Len())
		if err != nil {
			releaseArrowArrays(computed)
			cleanupMaterializedRecord(base)
			return nil, err
		}
		computed[fieldID] = arr
	}
	for _, materializer := range m.materializers {
		arrays, err := materializer.Materialize(view)
		if err != nil {
			releaseArrowArrays(computed)
			cleanupMaterializedRecord(base)
			return nil, err
		}
		for fieldID, arr := range arrays {
			computed[fieldID] = arr
		}
	}
	if len(computed) == 0 {
		return base, nil
	}
	return view, nil
}

func (m *RecordMaterializer) Close() {
	if m == nil {
		return
	}
	for _, materializer := range m.materializers {
		materializer.Close()
	}
}

func (m *RecordMaterializer) hasMaterialization() bool {
	return m != nil && (len(m.materializers) > 0 || len(m.missingFields) > 0)
}

type materializedRecord struct {
	base        storage.Record
	computed    map[int64]arrow.Array
	cleanupOnce sync.Once
}

var _ storage.Record = (*materializedRecord)(nil)

func (r *materializedRecord) Column(fieldID storage.FieldID) arrow.Array {
	if col, ok := r.computed[fieldID]; ok {
		return col
	}
	return r.base.Column(fieldID)
}

func (r *materializedRecord) Len() int {
	return r.base.Len()
}

func (r *materializedRecord) Retain() {
	r.base.Retain()
	for _, col := range r.computed {
		col.Retain()
	}
}

func (r *materializedRecord) Release() {
	r.base.Release()
	for _, col := range r.computed {
		col.Release()
	}
}

func (r *materializedRecord) cleanupDerived() {
	r.cleanupOnce.Do(func() {
		releaseArrowArrays(r.computed)
		cleanupMaterializedRecord(r.base)
	})
}

type selectedRecord struct {
	base        storage.Record
	selection   *recordSelection
	columns     map[int64]arrow.Array
	cleanupOnce sync.Once
}

var _ storage.Record = (*selectedRecord)(nil)

// newSelectedRecord eagerly slices every schema field physically present in
// base down to the selection ranges. The column set must be fixed for the
// record's lifetime: a column created lazily after a wrapper Retain-snapshot
// (e.g. timestampOverwriteRecord) would escape the snapshot and be released
// once more than it was retained. Presence is decided by existingFields, never
// by probing base.Column: V2/V3 records panic on Column for absent fields.
func newSelectedRecord(base storage.Record, schema *schemapb.CollectionSchema, existingFields map[int64]struct{}, selection *recordSelection) (*selectedRecord, error) {
	columns := make(map[int64]arrow.Array)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fieldID := field.GetFieldID()
		if _, ok := existingFields[fieldID]; !ok {
			continue
		}
		col, err := buildSelectedColumn(base, field, selection)
		if err != nil {
			releaseArrowArrays(columns)
			return nil, err
		}
		columns[fieldID] = col
	}
	return &selectedRecord{
		base:      base,
		selection: selection,
		columns:   columns,
	}, nil
}

func buildSelectedColumn(base storage.Record, field *schemapb.FieldSchema, selection *recordSelection) (arrow.Array, error) {
	builder := storage.NewRecordBuilder(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	defer builder.Release()
	for _, rowRange := range selection.ranges {
		if err := builder.Append(base, rowRange.start, rowRange.end); err != nil {
			return nil, err
		}
	}
	built := builder.Build()
	defer built.Release()
	col := built.Column(field.GetFieldID())
	if col == nil {
		return nil, merr.WrapErrServiceInternalMsg("selected record field %d not found", field.GetFieldID())
	}
	col.Retain()
	return col, nil
}

func (r *selectedRecord) Column(fieldID storage.FieldID) arrow.Array {
	return r.columns[fieldID]
}

func (r *selectedRecord) Len() int {
	return r.selection.Len()
}

func (r *selectedRecord) Retain() {
	r.base.Retain()
	for _, col := range r.columns {
		col.Retain()
	}
}

func (r *selectedRecord) Release() {
	r.base.Release()
	for _, col := range r.columns {
		col.Release()
	}
}

func (r *selectedRecord) cleanupDerived() {
	r.cleanupOnce.Do(func() {
		releaseArrowArrays(r.columns)
	})
}

type materializedRecordReader struct {
	base         storage.RecordReader
	materializer *RecordMaterializer
	current      storage.Record
}

var _ storage.RecordReader = (*materializedRecordReader)(nil)

func newMaterializedRecordReader(base storage.RecordReader, materializer *RecordMaterializer) storage.RecordReader {
	if !materializer.hasMaterialization() {
		return base
	}
	return &materializedRecordReader{base: base, materializer: materializer}
}

func (r *materializedRecordReader) Next() (storage.Record, error) {
	if r.current != nil {
		cleanupMaterializedRecord(r.current)
		r.current = nil
	}
	rec, err := r.base.Next()
	if err != nil {
		return nil, err
	}
	wrapped, err := r.materializer.Wrap(rec)
	if err != nil {
		// rec stays owned by the base reader; it is released on its next
		// Next/Close, never here.
		return nil, err
	}
	r.current = wrapped
	return wrapped, nil
}

func (r *materializedRecordReader) Close() error {
	if r.current != nil {
		cleanupMaterializedRecord(r.current)
		r.current = nil
	}
	r.materializer.Close()
	return r.base.Close()
}

type bm25FunctionMaterializer struct {
	runner               function.FunctionRunner
	inputFieldIDs        []int64
	outputFieldIDs       []int64
	missingOutputIndexes []int
	outputFields         map[int64]*schemapb.FieldSchema
	ownRunner            bool
}

type minHashFunctionMaterializer struct {
	runner               function.FunctionRunner
	inputFieldIDs        []int64
	outputFieldIDs       []int64
	missingOutputIndexes []int
	outputFields         map[int64]*schemapb.FieldSchema
	outputDims           map[int64]int64
	ownRunner            bool
}

var (
	_ FunctionMaterializer = (*bm25FunctionMaterializer)(nil)
	_ FunctionMaterializer = (*minHashFunctionMaterializer)(nil)
)

func newFunctionMaterializer(schema *schemapb.CollectionSchema, runner function.FunctionRunner, missingOutputIndexes []int, ownRunner bool) (FunctionMaterializer, error) {
	functionSchema := runner.GetSchema()
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		return newBM25FunctionMaterializer(schema, runner, missingOutputIndexes, ownRunner)
	case schemapb.FunctionType_MinHash:
		return newMinHashFunctionMaterializer(schema, runner, missingOutputIndexes, ownRunner)
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported function type %s", functionSchema.GetType().String())
	}
}

func newMinHashFunctionMaterializer(schema *schemapb.CollectionSchema, runner function.FunctionRunner, missingOutputIndexes []int, ownRunner bool) (*minHashFunctionMaterializer, error) {
	functionSchema := runner.GetSchema()
	inputFields := runner.GetInputFields()
	if len(inputFields) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("minhash function should have input fields")
	}
	inputFieldIDs := make([]int64, 0, len(inputFields))
	for _, inputField := range inputFields {
		if inputField == nil || typeutil.GetField(schema, inputField.GetFieldID()) == nil {
			return nil, merr.WrapErrFunctionFailedMsg("input field not found in schema")
		}
		if inputField.GetDataType() != schemapb.DataType_VarChar && inputField.GetDataType() != schemapb.DataType_Text {
			return nil, merr.WrapErrFunctionFailedMsg("input field data type must be varchar or text for minhash function materialization")
		}
		inputFieldIDs = append(inputFieldIDs, inputField.GetFieldID())
	}

	outputFieldIDs := functionSchema.GetOutputFieldIds()
	if len(outputFieldIDs) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("minhash function should have output fields")
	}

	outputFields := make(map[int64]*schemapb.FieldSchema, len(outputFieldIDs))
	outputDims := make(map[int64]int64, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		outputField := typeutil.GetField(schema, outputFieldID)
		if outputField == nil {
			return nil, merr.WrapErrFunctionFailedMsg("output field not found in schema")
		}
		if outputField.GetDataType() != schemapb.DataType_BinaryVector {
			return nil, merr.WrapErrFunctionFailedMsg("output field data type must be binary vector for minhash function materialization")
		}
		if outputField.GetNullable() {
			return nil, merr.WrapErrFunctionFailedMsg("function output field cannot be nullable: function %s, field %s", functionSchema.GetName(), outputField.GetName())
		}
		outputDim, err := typeutil.GetDim(outputField)
		if err != nil {
			return nil, merr.WrapErrFunctionFailed(err, "failed to read MinHash output field %s dimension", outputField.GetName())
		}
		if outputDim <= 0 || outputDim%32 != 0 {
			return nil, merr.WrapErrFunctionFailedMsg("invalid MinHash output field %s dimension %d", outputField.GetName(), outputDim)
		}
		outputFields[outputFieldID] = outputField
		outputDims[outputFieldID] = outputDim
	}

	return &minHashFunctionMaterializer{
		runner:               runner,
		inputFieldIDs:        inputFieldIDs,
		outputFieldIDs:       outputFieldIDs,
		missingOutputIndexes: missingOutputIndexes,
		outputFields:         outputFields,
		outputDims:           outputDims,
		ownRunner:            ownRunner,
	}, nil
}

func newBM25FunctionMaterializer(schema *schemapb.CollectionSchema, runner function.FunctionRunner, missingOutputIndexes []int, ownRunner bool) (*bm25FunctionMaterializer, error) {
	functionSchema := runner.GetSchema()
	inputFields := runner.GetInputFields()
	if len(inputFields) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function should have input fields")
	}
	inputFieldIDs := make([]int64, 0, len(inputFields))
	for _, inputField := range inputFields {
		if inputField == nil || typeutil.GetField(schema, inputField.GetFieldID()) == nil {
			return nil, merr.WrapErrParameterInvalidMsg("input field not found in schema")
		}
		if inputField.GetDataType() != schemapb.DataType_VarChar && inputField.GetDataType() != schemapb.DataType_Text {
			return nil, merr.WrapErrParameterInvalidMsg("input field data type must be varchar or text for bm25 function materialization")
		}
		inputFieldIDs = append(inputFieldIDs, inputField.GetFieldID())
	}

	outputFieldIDs := functionSchema.GetOutputFieldIds()
	if len(outputFieldIDs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function should have output fields")
	}

	outputFields := make(map[int64]*schemapb.FieldSchema, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		outputField := typeutil.GetField(schema, outputFieldID)
		if outputField == nil {
			return nil, merr.WrapErrParameterInvalidMsg("output field not found in schema")
		}
		if outputField.GetDataType() != schemapb.DataType_SparseFloatVector {
			return nil, merr.WrapErrParameterInvalidMsg("output field data type must be sparse float vector for bm25 function materialization")
		}
		if outputField.GetNullable() {
			return nil, merr.WrapErrParameterInvalidMsg("function output field cannot be nullable: function %s, field %s", functionSchema.GetName(), outputField.GetName())
		}
		outputFields[outputFieldID] = outputField
	}

	return &bm25FunctionMaterializer{
		runner:               runner,
		inputFieldIDs:        inputFieldIDs,
		outputFieldIDs:       outputFieldIDs,
		missingOutputIndexes: missingOutputIndexes,
		outputFields:         outputFields,
		ownRunner:            ownRunner,
	}, nil
}

func (m *bm25FunctionMaterializer) Materialize(rec storage.Record) (map[int64]arrow.Array, error) {
	inputs := make([]any, 0, len(m.inputFieldIDs))
	for _, inputFieldID := range m.inputFieldIDs {
		input, err := stringInputsFromRecord(rec, inputFieldID)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
	}
	outputs, err := m.runner.BatchRun(inputs...)
	if err != nil {
		return nil, err
	}
	if len(outputs) != len(m.outputFieldIDs) {
		return nil, merr.WrapErrFunctionFailedMsg("bm25 function materialization expects %d outputs, got %d", len(m.outputFieldIDs), len(outputs))
	}

	result := make(map[int64]arrow.Array, len(m.missingOutputIndexes))
	for _, outputIndex := range m.missingOutputIndexes {
		outputFieldID := m.outputFieldIDs[outputIndex]
		outputSparseArray, ok := outputs[outputIndex].(*schemapb.SparseFloatArray)
		if !ok {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("unexpected output type from BM25 function runner, expected SparseFloatArray, got %T", outputs[outputIndex])
		}
		arr, err := buildSparseFloatVectorArrowArray(m.outputFields[outputFieldID], outputSparseArray, rec.Len())
		if err != nil {
			releaseArrowArrays(result)
			return nil, err
		}
		result[outputFieldID] = arr
	}
	return result, nil
}

func (m *bm25FunctionMaterializer) Close() {
	if m.ownRunner && m.runner != nil {
		m.runner.Close()
	}
}

func (m *minHashFunctionMaterializer) Materialize(rec storage.Record) (map[int64]arrow.Array, error) {
	inputs := make([]any, 0, len(m.inputFieldIDs))
	for _, inputFieldID := range m.inputFieldIDs {
		input, err := stringInputsFromRecord(rec, inputFieldID)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
	}
	outputs, err := m.runner.BatchRun(inputs...)
	if err != nil {
		return nil, err
	}
	if len(outputs) != len(m.outputFieldIDs) {
		return nil, merr.WrapErrFunctionFailedMsg("minhash function materialization expects %d outputs, got %d", len(m.outputFieldIDs), len(outputs))
	}

	result := make(map[int64]arrow.Array, len(m.missingOutputIndexes))
	for _, outputIndex := range m.missingOutputIndexes {
		outputFieldID := m.outputFieldIDs[outputIndex]
		outputFieldData, ok := outputs[outputIndex].(*schemapb.FieldData)
		if !ok {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("unexpected output type from MinHash function runner, expected FieldData, got %T", outputs[outputIndex])
		}
		vectorField := outputFieldData.GetVectors()
		if vectorField == nil {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("unexpected output from MinHash function runner, expected binary vector field data")
		}
		binaryVector, ok := vectorField.GetData().(*schemapb.VectorField_BinaryVector)
		if !ok || binaryVector == nil {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("unexpected output from MinHash function runner, expected binary vector field data")
		}
		outputDim := vectorField.GetDim()
		if outputDim <= 0 || outputDim%32 != 0 {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("invalid MinHash output dim %d for field %d", outputDim, outputFieldID)
		}
		expectedDim := m.outputDims[outputFieldID]
		if outputDim != expectedDim {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("minhash function output dim mismatch for field %d, expected %d, got %d", outputFieldID, expectedDim, outputDim)
		}
		expectedBytes := int64(rec.Len()) * outputDim / 8
		if int64(len(binaryVector.BinaryVector)) != expectedBytes {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg(
				"minhash function output payload length mismatch for field %d, expected %d bytes, got %d",
				outputFieldID, expectedBytes, len(binaryVector.BinaryVector))
		}
		fieldData := &storage.BinaryVectorFieldData{
			Data: binaryVector.BinaryVector,
			Dim:  int(outputDim),
		}
		if fieldData.RowNum() != rec.Len() {
			releaseArrowArrays(result)
			return nil, merr.WrapErrFunctionFailedMsg("minhash function output row count mismatch, expected %d, got %d", rec.Len(), fieldData.RowNum())
		}
		arr, err := buildArrowArrayFromFieldData(m.outputFields[outputFieldID], fieldData, rec.Len())
		if err != nil {
			releaseArrowArrays(result)
			return nil, err
		}
		result[outputFieldID] = arr
	}
	return result, nil
}

func (m *minHashFunctionMaterializer) Close() {
	if m.ownRunner && m.runner != nil {
		m.runner.Close()
	}
}

func functionOutputIndexesToMaterialize(functionSchema *schemapb.FunctionSchema, existingFields map[int64]struct{}) []int {
	outputFieldIDs := functionSchema.GetOutputFieldIds()
	indexes := make([]int, 0, len(outputFieldIDs))
	hasMissingOutput := false
	for idx, outputFieldID := range outputFieldIDs {
		indexes = append(indexes, idx)
		if _, ok := existingFields[outputFieldID]; !ok {
			hasMissingOutput = true
		}
	}
	if !hasMissingOutput {
		return nil
	}
	return indexes
}

func missingNonMaterializedSchemaFields(schema *schemapb.CollectionSchema, existingFields map[int64]struct{}, materializedFields map[int64]struct{}) []*schemapb.FieldSchema {
	missing := make([]*schemapb.FieldSchema, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fieldID := field.GetFieldID()
		if common.IsSystemField(fieldID) {
			continue
		}
		if _, ok := existingFields[fieldID]; ok {
			continue
		}
		if _, ok := materializedFields[fieldID]; ok {
			continue
		}
		missing = append(missing, field)
	}
	return missing
}

func stringInputsFromRecord(rec storage.Record, fieldID int64) ([]string, error) {
	col := rec.Column(fieldID)
	if col == nil {
		return nil, merr.WrapErrFunctionFailedMsg("input field %d not found in record", fieldID)
	}
	inputs := make([]string, rec.Len())
	switch values := col.(type) {
	case *array.String:
		for i := 0; i < rec.Len(); i++ {
			if values.IsValid(i) {
				inputs[i] = values.Value(i)
			}
		}
	case *array.Binary:
		return nil, merr.WrapErrFunctionFailedMsg("cannot materialize bm25 from text binary values without lob decoding")
	default:
		return nil, merr.WrapErrFunctionFailedMsg("input field %d data type must be varchar or text for bm25 function materialization, got %T", fieldID, col)
	}
	return inputs, nil
}

func buildSparseFloatVectorArrowArray(field *schemapb.FieldSchema, outputSparseArray *schemapb.SparseFloatArray, rowCount int) (arrow.Array, error) {
	if len(outputSparseArray.GetContents()) != rowCount {
		return nil, merr.WrapErrFunctionFailedMsg("bm25 function output row count mismatch, expected %d, got %d", rowCount, len(outputSparseArray.GetContents()))
	}

	fieldData := &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Contents: outputSparseArray.GetContents(),
			Dim:      outputSparseArray.GetDim(),
		},
	}

	return buildArrowArrayFromFieldData(field, fieldData, rowCount)
}

func buildArrowArrayFromFieldData(field *schemapb.FieldSchema, fieldData storage.FieldData, rowCount int) (arrow.Array, error) {
	if rowCount == 0 {
		outputSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
		arrowSchema, err := storage.ConvertToArrowSchema(outputSchema, true)
		if err != nil {
			return nil, err
		}
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()
		record := builder.NewRecord()
		defer record.Release()
		col := record.Column(0)
		col.Retain()
		return col, nil
	}
	if fieldData.RowNum() != rowCount {
		return nil, merr.WrapErrFunctionFailedMsg("function output row count mismatch for field %d, expected %d, got %d", field.GetFieldID(), rowCount, fieldData.RowNum())
	}

	outputSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	arrowSchema, err := storage.ConvertToArrowSchema(outputSchema, true)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	insertData := &storage.InsertData{Data: map[int64]storage.FieldData{
		field.GetFieldID(): fieldData,
	}}
	if err := storage.BuildRecord(builder, insertData, outputSchema); err != nil {
		return nil, err
	}
	record := builder.NewRecord()
	defer record.Release()

	col := record.Column(0)
	col.Retain()
	return col, nil
}

func releaseArrowArrays(arrays map[int64]arrow.Array) {
	for _, arr := range arrays {
		arr.Release()
	}
}

type derivedRecord interface {
	cleanupDerived()
}

// cleanupMaterializedRecord releases only the arrays created by materialization
// or selection. The base record stays borrowed from and owned by its reader.
func cleanupMaterializedRecord(record storage.Record) {
	if derived, ok := record.(derivedRecord); ok {
		derived.cleanupDerived()
	}
}
