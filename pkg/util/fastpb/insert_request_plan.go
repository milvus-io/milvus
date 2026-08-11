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
	"math"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type insertFieldPlanClass uint8

const (
	insertFieldPlanNone insertFieldPlanClass = iota
	insertFieldPlanScalar
	insertFieldPlanVector
)

type insertFieldValuePlan uint8

const (
	insertFieldValueNone insertFieldValuePlan = iota
	insertFieldValueScalarEmpty
	insertFieldValueScalarPacked
	insertFieldValueScalarRepeated
	insertFieldValueScalarArray
	insertFieldValueVectorEmpty
	insertFieldValueVectorDenseBytes
	insertFieldValueVectorFloat
	insertFieldValueVectorSparse
	insertFieldValueVectorArray
)

// insertFieldSizeState keeps aggregate protobuf payload sizes and the ordinal
// of an ARRAY field in the request-level row-major token arena. It never owns
// row data. Aggregate sizes are flattened into the replay plan; ARRAY tokens
// remain cursor scratch that InsertRequestViewEncoder borrows during MarshalTo.
type insertFieldSizeState struct {
	field            *schemapb.FieldData
	class            insertFieldPlanClass
	valuePlan        insertFieldValuePlan
	valueFieldNumber protowire.Number
	metadataSize     int
	payloadSize      int
	selectedRows     int
	sparseDim        int64
	stride           int
	compactVector    bool
	arrayOrdinal     int
}

type insertFieldRowDelta struct {
	payloadSize      int
	selectedRows     int
	sparseDim        int64
	arrayCellPayload int
}

type insertFieldComputedSize struct {
	fieldSize    int
	nestedSize   int
	payloadSize  int
	selectedRows int
	sparseDim    int
}

type insertRequestSizeState struct {
	template          *msgpb.InsertRequest
	source            *msgpb.InsertRequest
	fixedSize         int
	rowCount          int
	timestampPayload  int
	rowIDPayload      int
	encodedSize       int
	fields            []insertFieldSizeState
	rowDeltas         []insertFieldRowDelta
	arrayCellPayloads []int
	arrayFieldCount   int
}

func (s *insertRequestSizeState) reset(template, source *msgpb.InsertRequest) error {
	if template == nil {
		return insertViewInternal("nil output template")
	}
	if source == nil {
		return insertViewInternal("nil source request")
	}
	if len(source.GetRowData()) != 0 {
		return insertViewInternal("row-based InsertRequest is not supported")
	}

	fixedSize, err := insertRequestFixedSize(template)
	if err != nil {
		return err
	}
	arrayCellPayloads := s.arrayCellPayloads[:0]
	s.arrayFieldCount = 0
	fieldCount := len(source.GetFieldsData())
	if cap(s.fields) < fieldCount {
		s.fields = make([]insertFieldSizeState, fieldCount)
	} else {
		s.fields = s.fields[:fieldCount]
	}
	if cap(s.rowDeltas) < fieldCount {
		s.rowDeltas = make([]insertFieldRowDelta, fieldCount)
	} else {
		s.rowDeltas = s.rowDeltas[:fieldCount]
		clear(s.rowDeltas)
	}
	for i, field := range source.GetFieldsData() {
		if err := s.fields[i].reset(field); err != nil {
			return err
		}
		if s.fields[i].valuePlan == insertFieldValueScalarArray {
			s.fields[i].arrayOrdinal = s.arrayFieldCount
			s.arrayFieldCount++
		}
	}

	s.template = template
	s.source = source
	s.fixedSize = fixedSize
	s.rowCount = 0
	s.timestampPayload = 0
	s.rowIDPayload = 0
	s.encodedSize = 0
	s.arrayCellPayloads = arrayCellPayloads
	return nil
}

func (s *insertRequestSizeState) previewRow(row int, fieldDataIndices []int64) (int, error) {
	if err := s.validateRow(row); err != nil {
		return 0, err
	}
	if len(fieldDataIndices) != len(s.fields) {
		return 0, insertViewInternal("field data index count %d does not match FieldsData count %d", len(fieldDataIndices), len(s.fields))
	}

	clear(s.rowDeltas)
	for i := range s.fields {
		dataIndex := fieldDataIndices[i]
		if dataIndex < 0 || uint64(dataIndex) > uint64(math.MaxInt) {
			return 0, insertViewInternal("field data index %d for field %d is invalid", dataIndex, i)
		}
		if err := s.fields[i].previewRow(row, int(dataIndex), &s.rowDeltas[i]); err != nil {
			return 0, err
		}
	}
	return s.previewSize(row, s.rowDeltas)
}

func (s *insertRequestSizeState) previewCachedRow(row int, deltas []insertFieldRowDelta) (int, error) {
	if err := s.validateRow(row); err != nil {
		return 0, err
	}
	if len(deltas) != len(s.fields) {
		return 0, insertViewInternal("cached row delta count %d does not match FieldsData count %d", len(deltas), len(s.fields))
	}
	copy(s.rowDeltas, deltas)
	return s.previewSize(row, s.rowDeltas)
}

func (s *insertRequestSizeState) validateRow(row int) error {
	if row < 0 {
		return insertViewInternal("row offset is negative: %d", row)
	}
	if row >= len(s.source.GetRowIDs()) {
		return insertViewInternal("row offset %d exceeds RowIDs length %d", row, len(s.source.GetRowIDs()))
	}
	if row >= len(s.source.GetTimestamps()) {
		return insertViewInternal("row offset %d exceeds Timestamps length %d", row, len(s.source.GetTimestamps()))
	}
	if s.source.GetNumRows() != 0 && uint64(row) >= s.source.GetNumRows() {
		return insertViewInternal("row offset %d exceeds source NumRows %d", row, s.source.GetNumRows())
	}
	return nil
}

func (s *insertRequestSizeState) previewSize(row int, deltas []insertFieldRowDelta) (int, error) {
	newRowCount := s.rowCount + 1
	timestampPayload, err := checkedAddSize(s.timestampPayload, protowire.SizeVarint(s.source.GetTimestamps()[row]), "timestamp payload")
	if err != nil {
		return 0, err
	}
	rowIDPayload, err := checkedAddSize(s.rowIDPayload, protowire.SizeVarint(uint64(s.source.GetRowIDs()[row])), "row ID payload")
	if err != nil {
		return 0, err
	}

	total := s.fixedSize
	timestampSize, err := insertBytesFieldSize(10, timestampPayload)
	if err != nil {
		return 0, err
	}
	total, err = checkedAddSize(total, timestampSize, "insert request size")
	if err != nil {
		return 0, err
	}
	rowIDSize, err := insertBytesFieldSize(11, rowIDPayload)
	if err != nil {
		return 0, err
	}
	total, err = checkedAddSize(total, rowIDSize, "insert request size")
	if err != nil {
		return 0, err
	}

	for i := range s.fields {
		computed, err := s.fields[i].computedSize(&deltas[i], newRowCount)
		if err != nil {
			return 0, err
		}
		fieldWireSize, err := insertBytesFieldSize(13, computed.fieldSize)
		if err != nil {
			return 0, err
		}
		total, err = checkedAddSize(total, fieldWireSize, "insert request size")
		if err != nil {
			return 0, err
		}
	}

	total, err = checkedAddSize(total, insertProto3VarintFieldSize(14, uint64(newRowCount)), "insert request size")
	if err != nil {
		return 0, err
	}
	return total, nil
}

func (s *insertRequestSizeState) commitRow(row, encodedSize int) {
	s.rowCount++
	s.timestampPayload += protowire.SizeVarint(s.source.GetTimestamps()[row])
	s.rowIDPayload += protowire.SizeVarint(uint64(s.source.GetRowIDs()[row]))
	for i := range s.fields {
		s.fields[i].commit(s.rowDeltas[i])
		if s.fields[i].arrayOrdinal >= 0 {
			s.arrayCellPayloads = append(s.arrayCellPayloads, s.rowDeltas[i].arrayCellPayload)
		}
	}
	s.encodedSize = encodedSize
}

func (s *insertRequestSizeState) buildPlan(plan []int) ([]int, int, error) {
	if s.rowCount == 0 {
		return nil, 0, insertViewInternal("cannot finalize an empty insert request view")
	}
	expectedArrayCellPayloads, err := checkedProduct(s.rowCount, s.arrayFieldCount, "array cell payload plan")
	if err != nil {
		return nil, 0, err
	}
	if len(s.arrayCellPayloads) != expectedArrayCellPayloads {
		return nil, 0, insertViewInternal("array cell payload plan has %d entries, expected %d", len(s.arrayCellPayloads), expectedArrayCellPayloads)
	}
	plan = prepareSizePlan(plan, len(s.fields))
	plan = append(plan, s.timestampPayload, s.rowIDPayload)

	total := s.fixedSize
	timestampSize, err := insertBytesFieldSize(10, s.timestampPayload)
	if err != nil {
		return nil, 0, err
	}
	total, err = checkedAddSize(total, timestampSize, "insert request size")
	if err != nil {
		return nil, 0, err
	}
	rowIDSize, err := insertBytesFieldSize(11, s.rowIDPayload)
	if err != nil {
		return nil, 0, err
	}
	total, err = checkedAddSize(total, rowIDSize, "insert request size")
	if err != nil {
		return nil, 0, err
	}

	for i := range s.fields {
		computed, err := s.fields[i].computedSize(nil, s.rowCount)
		if err != nil {
			return nil, 0, err
		}
		plan = s.fields[i].appendPlan(plan, computed, s.rowCount)
		fieldWireSize, err := insertBytesFieldSize(13, computed.fieldSize)
		if err != nil {
			return nil, 0, err
		}
		total, err = checkedAddSize(total, fieldWireSize, "insert request size")
		if err != nil {
			return nil, 0, err
		}
	}
	total, err = checkedAddSize(total, insertProto3VarintFieldSize(14, uint64(s.rowCount)), "insert request size")
	if err != nil {
		return nil, 0, err
	}
	if total != s.encodedSize {
		return nil, 0, insertViewInternal("incremental encoded size changed during finalization: previewed %d, finalized %d", s.encodedSize, total)
	}
	return plan, total, nil
}

func (s *insertFieldSizeState) reset(field *schemapb.FieldData) error {
	*s = insertFieldSizeState{
		field:        field,
		arrayOrdinal: -1,
	}
	if field == nil {
		return insertViewInternal("source FieldsData contains nil field")
	}
	if field.GetStructArrays() != nil {
		return insertViewInternal("source field %q (%d) still contains StructArrays; proxy must flatten struct fields before repacking", field.GetFieldName(), field.GetFieldId())
	}

	metadataSize, err := insertFieldMetadataSize(field)
	if err != nil {
		return err
	}
	s.metadataSize = metadataSize
	switch value := field.Field.(type) {
	case nil:
		s.class = insertFieldPlanNone
		return nil
	case *schemapb.FieldData_Scalars:
		if value.Scalars == nil {
			return insertViewInternal("scalar field %q (%d) has a nil ScalarField", field.GetFieldName(), field.GetFieldId())
		}
		s.class = insertFieldPlanScalar
		return s.resetScalar(value.Scalars)
	case *schemapb.FieldData_Vectors:
		if value.Vectors == nil {
			return insertViewInternal("vector field %q (%d) has a nil VectorField", field.GetFieldName(), field.GetFieldId())
		}
		s.class = insertFieldPlanVector
		if err := s.resetVector(value.Vectors); err != nil {
			return err
		}
		s.compactVector = len(field.GetValidData()) > 0 && (s.valuePlan == insertFieldValueVectorDenseBytes ||
			s.valuePlan == insertFieldValueVectorFloat || s.valuePlan == insertFieldValueVectorSparse)
		return nil
	case *schemapb.FieldData_StructArrays:
		return insertViewInternal("source field %q (%d) still contains StructArrays; proxy must flatten struct fields before repacking", field.GetFieldName(), field.GetFieldId())
	default:
		return insertViewInternal("source field %q (%d) has unsupported FieldData oneof %T", field.GetFieldName(), field.GetFieldId(), value)
	}
}

func (s *insertFieldSizeState) resetScalar(scalar *schemapb.ScalarField) error {
	switch value := scalar.Data.(type) {
	case nil:
		s.valuePlan = insertFieldValueScalarEmpty
	case *schemapb.ScalarField_BoolData:
		if value.BoolData == nil {
			return insertViewInternal("nil bool scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 1
	case *schemapb.ScalarField_IntData:
		if value.IntData == nil {
			return insertViewInternal("nil int scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 2
	case *schemapb.ScalarField_LongData:
		if value.LongData == nil {
			return insertViewInternal("nil long scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 3
	case *schemapb.ScalarField_FloatData:
		if value.FloatData == nil {
			return insertViewInternal("nil float scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 4
	case *schemapb.ScalarField_DoubleData:
		if value.DoubleData == nil {
			return insertViewInternal("nil double scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 5
	case *schemapb.ScalarField_StringData:
		if value.StringData == nil {
			return insertViewInternal("nil string scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 6
	case *schemapb.ScalarField_BytesData:
		if value.BytesData == nil {
			return insertViewInternal("nil bytes scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 7
	case *schemapb.ScalarField_ArrayData:
		if value.ArrayData == nil {
			return insertViewInternal("nil array scalar payload")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarArray, 8
		s.payloadSize = insertProto3VarintFieldSize(2, uint64(value.ArrayData.GetElementType()))
	case *schemapb.ScalarField_JsonData:
		if value.JsonData == nil {
			return insertViewInternal("nil JSON scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 9
	case *schemapb.ScalarField_GeometryData:
		if value.GeometryData == nil {
			return insertViewInternal("nil geometry scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 10
	case *schemapb.ScalarField_TimestamptzData:
		if value.TimestamptzData == nil {
			return insertViewInternal("nil timestamptz scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 11
	case *schemapb.ScalarField_GeometryWktData:
		if value.GeometryWktData == nil {
			return insertViewInternal("nil geometry WKT scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 12
	case *schemapb.ScalarField_MolData:
		if value.MolData == nil {
			return insertViewInternal("nil molecular scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 13
	case *schemapb.ScalarField_MolSmilesData:
		if value.MolSmilesData == nil {
			return insertViewInternal("nil molecular SMILES scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarRepeated, 14
	case *schemapb.ScalarField_DateData:
		if value.DateData == nil {
			return insertViewInternal("nil date scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 15
	case *schemapb.ScalarField_TimeData:
		if value.TimeData == nil {
			return insertViewInternal("nil time scalar array")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueScalarPacked, 16
	default:
		return insertViewInternal("unsupported ScalarField oneof %T", value)
	}
	return nil
}

func (s *insertFieldSizeState) resetVector(vector *schemapb.VectorField) error {
	switch value := vector.Data.(type) {
	case nil:
		s.valuePlan = insertFieldValueVectorEmpty
	case *schemapb.VectorField_BinaryVector:
		stride, err := denseVectorStride(vector.GetDim(), 8, "binary vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride = insertFieldValueVectorDenseBytes, 3, stride
	case *schemapb.VectorField_FloatVector:
		if value.FloatVector == nil {
			return insertViewInternal("field %q (%d) has a nil float vector payload", s.field.GetFieldName(), s.field.GetFieldId())
		}
		stride, err := nonNegativeDim(vector.GetDim(), "float vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride = insertFieldValueVectorFloat, 2, stride
	case *schemapb.VectorField_Float16Vector:
		stride, err := denseVectorStride(vector.GetDim(), 2, "float16 vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride = insertFieldValueVectorDenseBytes, 4, stride
	case *schemapb.VectorField_Bfloat16Vector:
		stride, err := denseVectorStride(vector.GetDim(), 2, "bfloat16 vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride = insertFieldValueVectorDenseBytes, 5, stride
	case *schemapb.VectorField_SparseFloatVector:
		if value.SparseFloatVector == nil {
			return insertViewInternal("field %q (%d) has a nil sparse vector payload", s.field.GetFieldName(), s.field.GetFieldId())
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueVectorSparse, 6
	case *schemapb.VectorField_Int8Vector:
		stride, err := denseVectorStride(vector.GetDim(), 1, "int8 vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride = insertFieldValueVectorDenseBytes, 7, stride
	case *schemapb.VectorField_VectorArray:
		if value.VectorArray == nil {
			return insertViewInternal("nil ArrayOfVector payload")
		}
		s.valuePlan, s.valueFieldNumber = insertFieldValueVectorArray, 8
		s.payloadSize = insertProto3VarintFieldSize(1, uint64(value.VectorArray.GetDim())) +
			insertProto3VarintFieldSize(3, uint64(value.VectorArray.GetElementType()))
	default:
		return insertViewInternal("field %q (%d) has unsupported VectorField oneof %T", s.field.GetFieldName(), s.field.GetFieldId(), value)
	}
	return nil
}

func (s *insertFieldSizeState) previewRow(row, dataIndex int, delta *insertFieldRowDelta) error {
	*delta = insertFieldRowDelta{}
	valid := s.field.GetValidData()
	if len(valid) > 0 && row >= len(valid) {
		return insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), s.field.GetFieldName(), s.field.GetFieldId())
	}

	switch value := s.field.Field.(type) {
	case nil:
		return nil
	case *schemapb.FieldData_Scalars:
		return s.previewScalarRow(value.Scalars, row, delta)
	case *schemapb.FieldData_Vectors:
		if s.compactVector && !valid[row] {
			return nil
		}
		return s.previewVectorRow(value.Vectors, row, dataIndex, delta)
	default:
		return insertViewInternal("source field %q (%d) has unsupported FieldData oneof %T", s.field.GetFieldName(), s.field.GetFieldId(), value)
	}
}

func (s *insertFieldSizeState) previewScalarRow(scalar *schemapb.ScalarField, row int, delta *insertFieldRowDelta) error {
	switch value := scalar.Data.(type) {
	case nil:
		return nil
	case *schemapb.ScalarField_BoolData:
		if row >= len(value.BoolData.GetData()) {
			return insertViewInternal("row offset %d exceeds bool scalar length %d", row, len(value.BoolData.GetData()))
		}
		delta.payloadSize = 1
	case *schemapb.ScalarField_IntData:
		if row >= len(value.IntData.GetData()) {
			return insertViewInternal("row offset %d exceeds int scalar length %d", row, len(value.IntData.GetData()))
		}
		delta.payloadSize = protowire.SizeVarint(uint64(value.IntData.GetData()[row]))
	case *schemapb.ScalarField_LongData:
		if row >= len(value.LongData.GetData()) {
			return insertViewInternal("row offset %d exceeds long scalar length %d", row, len(value.LongData.GetData()))
		}
		delta.payloadSize = protowire.SizeVarint(uint64(value.LongData.GetData()[row]))
	case *schemapb.ScalarField_FloatData:
		if row >= len(value.FloatData.GetData()) {
			return insertViewInternal("row offset %d exceeds float scalar length %d", row, len(value.FloatData.GetData()))
		}
		delta.payloadSize = 4
	case *schemapb.ScalarField_DoubleData:
		if row >= len(value.DoubleData.GetData()) {
			return insertViewInternal("row offset %d exceeds double scalar length %d", row, len(value.DoubleData.GetData()))
		}
		delta.payloadSize = 8
	case *schemapb.ScalarField_StringData:
		return previewStringRow(value.StringData.GetData(), row, 1, "string scalar", delta)
	case *schemapb.ScalarField_BytesData:
		return previewBytesRow(value.BytesData.GetData(), row, 1, "bytes scalar", delta)
	case *schemapb.ScalarField_ArrayData:
		if row >= len(value.ArrayData.GetData()) {
			return insertViewInternal("row offset %d exceeds array scalar length %d", row, len(value.ArrayData.GetData()))
		}
		itemSize, cellPayload := scalarCellWirePlan(value.ArrayData.GetData()[row])
		wireSize, err := insertBytesFieldSize(1, itemSize)
		if err != nil {
			return err
		}
		delta.payloadSize = wireSize
		delta.arrayCellPayload = cellPayload
	case *schemapb.ScalarField_JsonData:
		return previewBytesRow(value.JsonData.GetData(), row, 1, "JSON scalar", delta)
	case *schemapb.ScalarField_GeometryData:
		return previewBytesRow(value.GeometryData.GetData(), row, 1, "geometry scalar", delta)
	case *schemapb.ScalarField_TimestamptzData:
		return previewInt64Row(value.TimestamptzData.GetData(), row, "timestamptz scalar", delta)
	case *schemapb.ScalarField_GeometryWktData:
		return previewStringRow(value.GeometryWktData.GetData(), row, 1, "geometry WKT scalar", delta)
	case *schemapb.ScalarField_MolData:
		return previewBytesRow(value.MolData.GetData(), row, 1, "molecular scalar", delta)
	case *schemapb.ScalarField_MolSmilesData:
		return previewStringRow(value.MolSmilesData.GetData(), row, 1, "molecular SMILES scalar", delta)
	case *schemapb.ScalarField_DateData:
		if row >= len(value.DateData.GetData()) {
			return insertViewInternal("row offset %d exceeds date scalar length %d", row, len(value.DateData.GetData()))
		}
		delta.payloadSize = protowire.SizeVarint(uint64(value.DateData.GetData()[row]))
	case *schemapb.ScalarField_TimeData:
		return previewInt64Row(value.TimeData.GetData(), row, "time scalar", delta)
	default:
		return insertViewInternal("unsupported ScalarField oneof %T", value)
	}
	return nil
}

func (s *insertFieldSizeState) previewVectorRow(vector *schemapb.VectorField, row, dataIndex int, delta *insertFieldRowDelta) error {
	switch value := vector.Data.(type) {
	case nil:
		return nil
	case *schemapb.VectorField_BinaryVector:
		if _, _, err := vectorBounds(dataIndex, s.stride, len(value.BinaryVector), s.field, "binary vector"); err != nil {
			return err
		}
		delta.payloadSize, delta.selectedRows = s.stride, 1
	case *schemapb.VectorField_FloatVector:
		if _, _, err := vectorBounds(dataIndex, s.stride, len(value.FloatVector.GetData()), s.field, "float vector"); err != nil {
			return err
		}
		payloadSize, err := checkedProduct(s.stride, 4, "float vector row payload")
		if err != nil {
			return err
		}
		delta.payloadSize, delta.selectedRows = payloadSize, 1
	case *schemapb.VectorField_Float16Vector:
		if _, _, err := vectorBounds(dataIndex, s.stride, len(value.Float16Vector), s.field, "float16 vector"); err != nil {
			return err
		}
		delta.payloadSize, delta.selectedRows = s.stride, 1
	case *schemapb.VectorField_Bfloat16Vector:
		if _, _, err := vectorBounds(dataIndex, s.stride, len(value.Bfloat16Vector), s.field, "bfloat16 vector"); err != nil {
			return err
		}
		delta.payloadSize, delta.selectedRows = s.stride, 1
	case *schemapb.VectorField_SparseFloatVector:
		if dataIndex < 0 || dataIndex >= len(value.SparseFloatVector.GetContents()) {
			return insertViewInternal("compact sparse vector index %d exceeds payload rows %d for field %q (%d)", dataIndex, len(value.SparseFloatVector.GetContents()), s.field.GetFieldName(), s.field.GetFieldId())
		}
		contents := value.SparseFloatVector.GetContents()[dataIndex]
		rowDim, err := sparseRowDim(contents)
		if err != nil {
			return err
		}
		wireSize, err := insertBytesFieldSize(1, len(contents))
		if err != nil {
			return err
		}
		delta.payloadSize, delta.selectedRows, delta.sparseDim = wireSize, 1, rowDim
	case *schemapb.VectorField_Int8Vector:
		if _, _, err := vectorBounds(dataIndex, s.stride, len(value.Int8Vector), s.field, "int8 vector"); err != nil {
			return err
		}
		delta.payloadSize, delta.selectedRows = s.stride, 1
	case *schemapb.VectorField_VectorArray:
		if row >= len(value.VectorArray.GetData()) {
			return insertViewInternal("row offset %d exceeds ArrayOfVector length %d", row, len(value.VectorArray.GetData()))
		}
		itemSize := nullableProtoSize(value.VectorArray.GetData()[row], false)
		wireSize, err := insertBytesFieldSize(2, itemSize)
		if err != nil {
			return err
		}
		delta.payloadSize = wireSize
	default:
		return insertViewInternal("field %q (%d) has unsupported VectorField oneof %T", s.field.GetFieldName(), s.field.GetFieldId(), value)
	}
	return nil
}

func (s *insertFieldSizeState) computedSize(delta *insertFieldRowDelta, rowCount int) (insertFieldComputedSize, error) {
	payloadSize := s.payloadSize
	selectedRows := s.selectedRows
	sparseDim := s.sparseDim
	if delta != nil {
		var err error
		payloadSize, err = checkedAddSize(payloadSize, delta.payloadSize, "field payload size")
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		selectedRows, err = checkedAddSize(selectedRows, delta.selectedRows, "selected vector row count")
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		if delta.sparseDim > sparseDim {
			sparseDim = delta.sparseDim
		}
	}

	computed := insertFieldComputedSize{
		payloadSize:  payloadSize,
		selectedRows: selectedRows,
	}
	if sparseDim < 0 || uint64(sparseDim) > uint64(math.MaxInt) {
		return insertFieldComputedSize{}, insertViewInternal("selected sparse vector dimension %d exceeds addressable memory", sparseDim)
	}
	computed.sparseDim = int(sparseDim)

	fieldSize := s.metadataSize
	switch s.class {
	case insertFieldPlanNone:
	case insertFieldPlanScalar:
		scalarSize, err := s.scalarSize(payloadSize, rowCount)
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		computed.nestedSize = scalarSize
		scalarWireSize, err := insertBytesFieldSize(3, scalarSize)
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		fieldSize, err = checkedAddSize(fieldSize, scalarWireSize, "FieldData size")
		if err != nil {
			return insertFieldComputedSize{}, err
		}
	case insertFieldPlanVector:
		vectorSize, err := s.vectorSize(payloadSize, selectedRows, sparseDim)
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		computed.nestedSize = vectorSize
		vectorWireSize, err := insertBytesFieldSize(4, vectorSize)
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		fieldSize, err = checkedAddSize(fieldSize, vectorWireSize, "FieldData size")
		if err != nil {
			return insertFieldComputedSize{}, err
		}
	}
	if len(s.field.GetValidData()) > 0 && rowCount > 0 {
		validWireSize, err := insertBytesFieldSize(7, rowCount)
		if err != nil {
			return insertFieldComputedSize{}, err
		}
		fieldSize, err = checkedAddSize(fieldSize, validWireSize, "FieldData size")
		if err != nil {
			return insertFieldComputedSize{}, err
		}
	}
	computed.fieldSize = fieldSize
	return computed, nil
}

func (s *insertFieldSizeState) scalarSize(payloadSize, rowCount int) (int, error) {
	switch s.valuePlan {
	case insertFieldValueScalarEmpty:
		return 0, nil
	case insertFieldValueScalarPacked:
		arraySize := 0
		if rowCount > 0 {
			var err error
			arraySize, err = insertBytesFieldSize(1, payloadSize)
			if err != nil {
				return 0, err
			}
		}
		return insertBytesFieldSize(s.valueFieldNumber, arraySize)
	case insertFieldValueScalarRepeated, insertFieldValueScalarArray:
		return insertBytesFieldSize(s.valueFieldNumber, payloadSize)
	default:
		return 0, insertViewInternal("unexpected scalar size plan %d", s.valuePlan)
	}
}

func (s *insertFieldSizeState) vectorSize(payloadSize, selectedRows int, sparseDim int64) (int, error) {
	vector := s.field.GetVectors()
	outerDim := vector.GetDim()
	if s.valuePlan == insertFieldValueVectorSparse && selectedRows > 0 {
		outerDim = vector.GetSparseFloatVector().GetDim()
	}
	size := insertProto3VarintFieldSize(1, uint64(outerDim))

	switch s.valuePlan {
	case insertFieldValueVectorEmpty:
		return size, nil
	case insertFieldValueVectorDenseBytes:
		if selectedRows == 0 {
			return size, nil
		}
		wireSize, err := insertBytesFieldSize(s.valueFieldNumber, payloadSize)
		if err != nil {
			return 0, err
		}
		return checkedAddSize(size, wireSize, "VectorField size")
	case insertFieldValueVectorFloat:
		if selectedRows == 0 {
			return size, nil
		}
		floatArraySize := 0
		if payloadSize > 0 {
			var err error
			floatArraySize, err = insertBytesFieldSize(1, payloadSize)
			if err != nil {
				return 0, err
			}
		}
		wireSize, err := insertBytesFieldSize(2, floatArraySize)
		if err != nil {
			return 0, err
		}
		return checkedAddSize(size, wireSize, "VectorField size")
	case insertFieldValueVectorSparse:
		if selectedRows == 0 {
			return size, nil
		}
		sparseSize := payloadSize
		sparseSize, err := checkedAddSize(sparseSize, insertProto3VarintFieldSize(2, uint64(sparseDim)), "sparse vector size")
		if err != nil {
			return 0, err
		}
		wireSize, err := insertBytesFieldSize(6, sparseSize)
		if err != nil {
			return 0, err
		}
		return checkedAddSize(size, wireSize, "VectorField size")
	case insertFieldValueVectorArray:
		wireSize, err := insertBytesFieldSize(8, payloadSize)
		if err != nil {
			return 0, err
		}
		return checkedAddSize(size, wireSize, "VectorField size")
	default:
		return 0, insertViewInternal("unexpected vector size plan %d", s.valuePlan)
	}
}

func (s *insertFieldSizeState) appendPlan(plan []int, computed insertFieldComputedSize, rowCount int) []int {
	plan = append(plan, computed.fieldSize)
	switch s.class {
	case insertFieldPlanScalar:
		plan = append(plan, computed.nestedSize)
		if s.valuePlan != insertFieldValueScalarEmpty {
			plan = append(plan, computed.payloadSize)
		}
	case insertFieldPlanVector:
		plan = append(plan, computed.nestedSize)
		switch s.valuePlan {
		case insertFieldValueVectorDenseBytes, insertFieldValueVectorFloat:
			plan = append(plan, computed.selectedRows)
		case insertFieldValueVectorSparse:
			plan = append(plan, computed.selectedRows, computed.payloadSize, computed.sparseDim)
		case insertFieldValueVectorArray:
			plan = append(plan, computed.payloadSize)
		}
	}
	if len(s.field.GetValidData()) > 0 && rowCount > 0 {
		plan = append(plan, rowCount)
	}
	return plan
}

func (s *insertFieldSizeState) commit(delta insertFieldRowDelta) {
	s.payloadSize += delta.payloadSize
	s.selectedRows += delta.selectedRows
	if delta.sparseDim > s.sparseDim {
		s.sparseDim = delta.sparseDim
	}
}

func insertRequestFixedSize(template *msgpb.InsertRequest) (int, error) {
	size := 0
	if template.GetBase() != nil {
		baseSize := nullableProtoSize(template.GetBase(), false)
		wireSize, err := insertBytesFieldSize(1, baseSize)
		if err != nil {
			return 0, err
		}
		size = wireSize
	}
	var err error
	for _, field := range []struct {
		number protowire.Number
		value  string
		force  bool
	}{
		{2, template.GetShardName(), false},
		{3, template.GetDbName(), false},
		{4, template.GetCollectionName(), false},
		{5, template.GetPartitionName(), false},
	} {
		wireSize, wireErr := insertStringFieldSize(field.number, field.value, field.force)
		if wireErr != nil {
			return 0, wireErr
		}
		size, err = checkedAddSize(size, wireSize, "insert request metadata size")
		if err != nil {
			return 0, err
		}
	}
	for _, field := range []struct {
		number protowire.Number
		value  uint64
	}{
		{6, uint64(template.GetDbID())},
		{7, uint64(template.GetCollectionID())},
		{8, uint64(template.GetPartitionID())},
		{9, uint64(template.GetSegmentID())},
		{15, uint64(template.GetVersion())},
	} {
		size, err = checkedAddSize(size, insertProto3VarintFieldSize(field.number, field.value), "insert request metadata size")
		if err != nil {
			return 0, err
		}
	}
	if template.Namespace != nil {
		wireSize, wireErr := insertStringFieldSize(16, *template.Namespace, true)
		if wireErr != nil {
			return 0, wireErr
		}
		size, err = checkedAddSize(size, wireSize, "insert request metadata size")
		if err != nil {
			return 0, err
		}
	}
	return checkedAddSize(size, len(template.ProtoReflect().GetUnknown()), "insert request metadata size")
}

func insertFieldMetadataSize(field *schemapb.FieldData) (int, error) {
	size := insertProto3VarintFieldSize(1, uint64(field.GetType()))
	nameSize, err := insertStringFieldSize(2, field.GetFieldName(), false)
	if err != nil {
		return 0, err
	}
	size, err = checkedAddSize(size, nameSize, "FieldData metadata size")
	if err != nil {
		return 0, err
	}
	size, err = checkedAddSize(size, insertProto3VarintFieldSize(5, uint64(field.GetFieldId())), "FieldData metadata size")
	if err != nil {
		return 0, err
	}
	if field.GetIsDynamic() {
		size, err = checkedAddSize(size, protowire.SizeTag(6)+1, "FieldData metadata size")
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func previewStringRow(values []string, row int, fieldNumber protowire.Number, label string, delta *insertFieldRowDelta) error {
	if row >= len(values) {
		return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
	}
	wireSize, err := insertBytesFieldSize(fieldNumber, len(values[row]))
	if err != nil {
		return err
	}
	delta.payloadSize = wireSize
	return nil
}

func previewBytesRow(values [][]byte, row int, fieldNumber protowire.Number, label string, delta *insertFieldRowDelta) error {
	if row >= len(values) {
		return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
	}
	wireSize, err := insertBytesFieldSize(fieldNumber, len(values[row]))
	if err != nil {
		return err
	}
	delta.payloadSize = wireSize
	return nil
}

func previewInt64Row(values []int64, row int, label string, delta *insertFieldRowDelta) error {
	if row >= len(values) {
		return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
	}
	delta.payloadSize = protowire.SizeVarint(uint64(values[row]))
	return nil
}

func insertProto3VarintFieldSize(number protowire.Number, value uint64) int {
	if value == 0 {
		return 0
	}
	return protowire.SizeTag(number) + protowire.SizeVarint(value)
}

func insertStringFieldSize(number protowire.Number, value string, force bool) (int, error) {
	if !force && value == "" {
		return 0, nil
	}
	return insertBytesFieldSize(number, len(value))
}

func insertBytesFieldSize(number protowire.Number, payloadSize int) (int, error) {
	if payloadSize < 0 {
		return 0, insertViewInternal("negative protobuf payload size %d", payloadSize)
	}
	prefixSize, err := checkedAddSize(protowire.SizeTag(number), protowire.SizeVarint(uint64(payloadSize)), "protobuf length prefix")
	if err != nil {
		return 0, err
	}
	return checkedAddSize(prefixSize, payloadSize, "protobuf bytes field")
}

// checkedAddSize is the single largest flat cost on the O(rows x fields) sizing
// path (~12% of a wide-table encode). It does not inline: splitting the error
// construction into a //go:noinline helper brings it to cost 81 against a
// budget of 80, and rewriting the overflow test to use wraparound makes it
// worse (84). Getting under the budget needs the label parameter gone, which
// costs the diagnostics on every call site, so it is left alone until the
// sizing loop itself is restructured.
func checkedAddSize(a, b int, label string) (int, error) {
	if a < 0 || b < 0 || a > math.MaxInt-b {
		return 0, insertViewInternal("%s exceeds addressable memory", label)
	}
	return a + b, nil
}
