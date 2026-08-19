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
// of an ARRAY/ArrayOfVector field in the request-level row-major token arena.
// It never owns row data. Aggregate sizes are flattened into the replay plan;
// nested-cell tokens remain cursor scratch borrowed during MarshalTo.
type insertFieldSizeState struct {
	field            *schemapb.FieldData
	class            insertFieldPlanClass
	valuePlan        insertFieldValuePlan
	valueFieldNumber protowire.Number
	metadataSize     int
	payloadSize      int
	wireSize         int
	previewWireSize  int
	selectedRows     int
	sparseDim        int64
	stride           int
	payloadStride    int
	compactVector    bool
	arrayOrdinal     int
	validData        []bool
}

// insertFieldValidData resolves the row-validity mask with the same precedence
// typeutil.GetFieldDataValidData uses: the legacy FieldData location first,
// else the field-specific ScalarField/VectorField location #52203 moved it to.
func insertFieldValidData(field *schemapb.FieldData) []bool {
	if legacy := field.GetValidData(); len(legacy) > 0 {
		return legacy
	}
	if scalars := field.GetScalars(); scalars != nil {
		return scalars.GetValidData()
	}
	return field.GetVectors().GetValidData()
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

type vectorArrayCellPlanKind uint8

const (
	vectorArrayCellPlanEmpty vectorArrayCellPlanKind = iota
	vectorArrayCellPlanFloat
	vectorArrayCellProtoFallback = -1
)

type vectorArrayCellPlan struct {
	kind           vectorArrayCellPlanKind
	payloadSize    int
	floatArraySize int
	cellSize       int
}

type insertRequestSizeState struct {
	template                 *msgpb.InsertRequest
	source                   *msgpb.InsertRequest
	fixedSize                int
	rowCount                 int
	timestampPayload         int
	timestampWireSize        int
	previewTimestampWireSize int
	rowIDPayload             int
	rowIDWireSize            int
	previewRowIDWireSize     int
	numRowsWireSize          int
	previewNumRowsWireSize   int
	encodedSize              int
	fields                   []insertFieldSizeState
	rowDeltas                []insertFieldRowDelta
	aggregateDeltas          []insertFieldRowDelta
	aggregateCheckpoint      []insertFieldRowDelta
	arrayCellPayloads        []int
	arrayFieldCount          int
}

func (s *insertRequestSizeState) reset(template, source *msgpb.InsertRequest, selectedRowCapacity int) error {
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
	if cap(s.aggregateDeltas) < fieldCount {
		s.aggregateDeltas = make([]insertFieldRowDelta, fieldCount)
		s.aggregateCheckpoint = make([]insertFieldRowDelta, fieldCount)
	} else {
		s.aggregateDeltas = s.aggregateDeltas[:fieldCount]
		s.aggregateCheckpoint = s.aggregateCheckpoint[:fieldCount]
		clear(s.aggregateDeltas)
		clear(s.aggregateCheckpoint)
	}
	for i, field := range source.GetFieldsData() {
		if err := s.fields[i].reset(field); err != nil {
			return err
		}
		if s.fields[i].valuePlan == insertFieldValueScalarArray || s.fields[i].valuePlan == insertFieldValueVectorArray {
			s.fields[i].arrayOrdinal = s.arrayFieldCount
			s.arrayFieldCount++
		}
	}
	if s.arrayFieldCount > 0 {
		expectedPayloads, err := checkedProduct(selectedRowCapacity, s.arrayFieldCount, "nested cell payload scratch")
		if err != nil {
			return err
		}
		const maxPreallocatedCellPayloads = 1 << 20
		if expectedPayloads <= maxPreallocatedCellPayloads && cap(arrayCellPayloads) < expectedPayloads {
			arrayCellPayloads = make([]int, 0, expectedPayloads)
		}
	}

	s.template = template
	s.source = source
	s.fixedSize = fixedSize
	s.rowCount = 0
	s.timestampPayload = 0
	s.timestampWireSize = 0
	s.previewTimestampWireSize = 0
	s.rowIDPayload = 0
	s.rowIDWireSize = 0
	s.previewRowIDWireSize = 0
	s.numRowsWireSize = 0
	s.previewNumRowsWireSize = 0
	s.encodedSize = fixedSize
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

// aggregateSimpleRows commits a simple prefix using either a fixed-width upper
// bound or exact block sizing for variable-width payloads. The remaining
// boundary rows still use exact incremental sizing.
func (s *insertRequestSizeState) aggregateSimpleRows(rows []int, previous, sizeLimit int) (int, error) {
	boundedVariableWidth := false
	for i := range s.fields {
		field := &s.fields[i]
		if sizeLimit > 0 && (field.valuePlan == insertFieldValueScalarRepeated ||
			field.valuePlan == insertFieldValueVectorSparse) {
			boundedVariableWidth = true
		}
		if field.class == insertFieldPlanNone {
			continue
		}
		if field.class == insertFieldPlanScalar &&
			(field.valuePlan == insertFieldValueScalarEmpty || field.valuePlan == insertFieldValueScalarPacked ||
				field.valuePlan == insertFieldValueScalarRepeated) {
			continue
		}
		if field.class == insertFieldPlanVector && !field.compactVector &&
			(field.valuePlan == insertFieldValueVectorEmpty || field.valuePlan == insertFieldValueVectorDenseBytes ||
				field.valuePlan == insertFieldValueVectorFloat || field.valuePlan == insertFieldValueVectorSparse) {
			continue
		}
		return 0, nil
	}

	prefixCount := len(rows)
	preparedVariableWidth := false
	preparedTimestampPayload := 0
	preparedRowIDPayload := 0
	if sizeLimit > 0 {
		if boundedVariableWidth {
			var err error
			prefixCount, preparedTimestampPayload, preparedRowIDPayload, err = s.boundedVariableSimplePrefix(rows, previous, sizeLimit)
			if err != nil {
				return 0, err
			}
			preparedVariableWidth = true
		} else {
			low, high := 0, len(rows)
			for low < high {
				middle := low + (high-low+1)/2
				upperBound, err := s.simpleRowsUpperBound(middle)
				if err != nil {
					return 0, err
				}
				if upperBound < sizeLimit {
					low = middle
				} else {
					high = middle - 1
				}
			}
			prefixCount = low
		}
	}
	if prefixCount == 0 {
		return 0, nil
	}
	selected := rows[:prefixCount]

	for i, row := range selected {
		if row < 0 {
			return 0, insertViewInternal("row offset at selection index %d is negative: %d", i, row)
		}
		if row <= previous {
			return 0, insertViewInternal("cursor row offsets must be globally increasing: rows[%d]=%d after %d", i, row, previous)
		}
		if err := s.validateRow(row); err != nil {
			return 0, err
		}
		previous = row
	}

	rowCount := len(selected)
	if _, err := checkedProduct(rowCount, 10, "timestamp payload upper bound"); err != nil {
		return 0, err
	}
	timestampPayload := preparedTimestampPayload
	rowIDPayload := preparedRowIDPayload
	if !preparedVariableWidth {
		for _, row := range selected {
			timestampPayload += protowire.SizeVarint(s.source.GetTimestamps()[row])
			rowIDPayload += protowire.SizeVarint(uint64(s.source.GetRowIDs()[row]))
		}
	}

	total := s.fixedSize
	timestampWireSize, err := insertBytesFieldSize(10, timestampPayload)
	if err != nil {
		return 0, err
	}
	total, err = checkedAddSize(total, timestampWireSize, "insert request size")
	if err != nil {
		return 0, err
	}
	rowIDWireSize, err := insertBytesFieldSize(11, rowIDPayload)
	if err != nil {
		return 0, err
	}
	total, err = checkedAddSize(total, rowIDWireSize, "insert request size")
	if err != nil {
		return 0, err
	}

	for i := range s.fields {
		field := &s.fields[i]
		payloadSize, selectedRows, sparseDim := 0, 0, int64(0)
		if preparedVariableWidth {
			payloadSize = s.aggregateDeltas[i].payloadSize
			selectedRows = s.aggregateDeltas[i].selectedRows
			sparseDim = s.aggregateDeltas[i].sparseDim
		} else {
			var err error
			payloadSize, selectedRows, sparseDim, err = field.aggregateSimplePayload(selected)
			if err != nil {
				return 0, err
			}
		}
		field.payloadSize = payloadSize
		field.selectedRows = selectedRows
		field.sparseDim = sparseDim
		computed, err := field.computedSize(nil, rowCount)
		if err != nil {
			return 0, err
		}
		fieldWireSize, err := insertBytesFieldSize(13, computed.fieldSize)
		if err != nil {
			return 0, err
		}
		field.wireSize = fieldWireSize
		field.previewWireSize = fieldWireSize
		total, err = checkedAddSize(total, fieldWireSize, "insert request size")
		if err != nil {
			return 0, err
		}
	}

	numRowsWireSize := insertProto3VarintFieldSize(14, uint64(rowCount))
	total, err = checkedAddSize(total, numRowsWireSize, "insert request size")
	if err != nil {
		return 0, err
	}

	s.rowCount = rowCount
	s.timestampPayload = timestampPayload
	s.timestampWireSize = timestampWireSize
	s.previewTimestampWireSize = timestampWireSize
	s.rowIDPayload = rowIDPayload
	s.rowIDWireSize = rowIDWireSize
	s.previewRowIDWireSize = rowIDWireSize
	s.numRowsWireSize = numRowsWireSize
	s.previewNumRowsWireSize = numRowsWireSize
	s.encodedSize = total
	return prefixCount, nil
}

// boundedVariableSimplePrefix finds the exact bounded prefix in a single
// forward pass. It checks size once per block and only replays the block that
// crosses the limit, so repeated scalar and sparse multi-packet splitting is
// O(N) instead of rescanning every remaining suffix.
func (s *insertRequestSizeState) boundedVariableSimplePrefix(rows []int, previous, sizeLimit int) (int, int, int, error) {
	const blockRows = 64
	clear(s.aggregateDeltas)
	timestampPayload := 0
	rowIDPayload := 0

	for blockStart := 0; blockStart < len(rows); blockStart += blockRows {
		blockEnd := min(blockStart+blockRows, len(rows))
		copy(s.aggregateCheckpoint, s.aggregateDeltas)
		checkpointTimestampPayload := timestampPayload
		checkpointRowIDPayload := rowIDPayload
		checkpointPrevious := previous

		for rowIndex := blockStart; rowIndex < blockEnd; rowIndex++ {
			var err error
			timestampPayload, rowIDPayload, previous, err = s.accumulateSimpleRow(
				rows[rowIndex], rowIndex, previous, timestampPayload, rowIDPayload, s.aggregateDeltas,
			)
			if err != nil {
				return 0, 0, 0, err
			}
		}
		encodedSize, err := s.simpleAggregateSize(blockEnd, timestampPayload, rowIDPayload, s.aggregateDeltas)
		if err != nil {
			return 0, 0, 0, err
		}
		if encodedSize < sizeLimit {
			continue
		}

		copy(s.aggregateDeltas, s.aggregateCheckpoint)
		timestampPayload = checkpointTimestampPayload
		rowIDPayload = checkpointRowIDPayload
		previous = checkpointPrevious
		for rowIndex := blockStart; rowIndex < blockEnd; rowIndex++ {
			copy(s.aggregateCheckpoint, s.aggregateDeltas)
			checkpointTimestampPayload = timestampPayload
			checkpointRowIDPayload = rowIDPayload
			timestampPayload, rowIDPayload, previous, err = s.accumulateSimpleRow(
				rows[rowIndex], rowIndex, previous, timestampPayload, rowIDPayload, s.aggregateDeltas,
			)
			if err != nil {
				return 0, 0, 0, err
			}
			encodedSize, err = s.simpleAggregateSize(rowIndex+1, timestampPayload, rowIDPayload, s.aggregateDeltas)
			if err != nil {
				return 0, 0, 0, err
			}
			if encodedSize >= sizeLimit {
				if rowIndex == 0 {
					return 1, timestampPayload, rowIDPayload, nil
				}
				copy(s.aggregateDeltas, s.aggregateCheckpoint)
				return rowIndex, checkpointTimestampPayload, checkpointRowIDPayload, nil
			}
		}
	}
	return len(rows), timestampPayload, rowIDPayload, nil
}

func (s *insertRequestSizeState) accumulateSimpleRow(
	row, selectionIndex, previous, timestampPayload, rowIDPayload int,
	deltas []insertFieldRowDelta,
) (int, int, int, error) {
	if row < 0 {
		return 0, 0, previous, insertViewInternal("row offset at selection index %d is negative: %d", selectionIndex, row)
	}
	if row <= previous {
		return 0, 0, previous, insertViewInternal("cursor row offsets must be globally increasing: rows[%d]=%d after %d", selectionIndex, row, previous)
	}
	if err := s.validateRow(row); err != nil {
		return 0, 0, previous, err
	}

	var err error
	timestampPayload, err = checkedAddSize(timestampPayload, protowire.SizeVarint(s.source.GetTimestamps()[row]), "timestamp payload")
	if err != nil {
		return 0, 0, previous, err
	}
	rowIDPayload, err = checkedAddSize(rowIDPayload, protowire.SizeVarint(uint64(s.source.GetRowIDs()[row])), "row ID payload")
	if err != nil {
		return 0, 0, previous, err
	}

	clear(s.rowDeltas)
	for i := range s.fields {
		if err := s.fields[i].previewRow(row, row, &s.rowDeltas[i]); err != nil {
			return 0, 0, previous, err
		}
		deltas[i].payloadSize, err = checkedAddSize(deltas[i].payloadSize, s.rowDeltas[i].payloadSize, "simple field payload")
		if err != nil {
			return 0, 0, previous, err
		}
		deltas[i].selectedRows, err = checkedAddSize(deltas[i].selectedRows, s.rowDeltas[i].selectedRows, "simple field selected rows")
		if err != nil {
			return 0, 0, previous, err
		}
		if s.rowDeltas[i].sparseDim > deltas[i].sparseDim {
			deltas[i].sparseDim = s.rowDeltas[i].sparseDim
		}
	}
	return timestampPayload, rowIDPayload, row, nil
}

func (s *insertRequestSizeState) simpleAggregateSize(
	rowCount, timestampPayload, rowIDPayload int,
	deltas []insertFieldRowDelta,
) (int, error) {
	total := s.fixedSize
	for _, wireSize := range []uint64{
		directBytesFieldSize(10, uint64(timestampPayload)),
		directBytesFieldSize(11, uint64(rowIDPayload)),
	} {
		if wireSize > uint64(math.MaxInt) {
			return 0, insertViewInternal("insert request packed column exceeds addressable memory")
		}
		var err error
		total, err = checkedAddSize(total, int(wireSize), "insert request size")
		if err != nil {
			return 0, err
		}
	}
	for i := range s.fields {
		wireSize, direct, err := s.fields[i].directWireSize(&deltas[i], rowCount)
		if err != nil {
			return 0, err
		}
		if !direct {
			return 0, insertViewInternal("field %q (%d) lost simple direct sizing", s.fields[i].field.GetFieldName(), s.fields[i].field.GetFieldId())
		}
		total, err = checkedAddSize(total, wireSize, "insert request size")
		if err != nil {
			return 0, err
		}
	}
	numRowsWireSize := directProto3VarintFieldSize(14, uint64(rowCount))
	if numRowsWireSize > uint64(math.MaxInt) {
		return 0, insertViewInternal("NumRows field exceeds addressable memory")
	}
	return checkedAddSize(total, int(numRowsWireSize), "insert request size")
}

func (s *insertRequestSizeState) simpleRowsUpperBound(rowCount int) (int, error) {
	packedPayload, err := checkedProduct(rowCount, 10, "packed request column upper bound")
	if err != nil {
		return 0, err
	}
	total := s.fixedSize
	for _, number := range []protowire.Number{10, 11} {
		wireSize, err := insertBytesFieldSize(number, packedPayload)
		if err != nil {
			return 0, err
		}
		total, err = checkedAddSize(total, wireSize, "insert request size upper bound")
		if err != nil {
			return 0, err
		}
	}
	for i := range s.fields {
		field := &s.fields[i]
		maxPayload, selectedRows, sparseDim, err := field.simplePayloadUpperBound(rowCount)
		if err != nil {
			return 0, err
		}
		computed, err := field.computedSize(&insertFieldRowDelta{payloadSize: maxPayload, selectedRows: selectedRows, sparseDim: sparseDim}, rowCount)
		if err != nil {
			return 0, err
		}
		wireSize, err := insertBytesFieldSize(13, computed.fieldSize)
		if err != nil {
			return 0, err
		}
		total, err = checkedAddSize(total, wireSize, "insert request size upper bound")
		if err != nil {
			return 0, err
		}
	}
	return checkedAddSize(total, insertProto3VarintFieldSize(14, uint64(rowCount)), "insert request size upper bound")
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

	timestampWireSize, err := insertBytesFieldSize(10, timestampPayload)
	if err != nil {
		return 0, err
	}
	if timestampWireSize < s.timestampWireSize {
		return 0, insertViewInternal("timestamp wire size shrank from %d to %d", s.timestampWireSize, timestampWireSize)
	}
	wireDelta := uint64(timestampWireSize - s.timestampWireSize)
	rowIDWireSize, err := insertBytesFieldSize(11, rowIDPayload)
	if err != nil {
		return 0, err
	}
	if rowIDWireSize < s.rowIDWireSize {
		return 0, insertViewInternal("row ID wire size shrank from %d to %d", s.rowIDWireSize, rowIDWireSize)
	}
	wireDelta += uint64(rowIDWireSize - s.rowIDWireSize)

	for i := range s.fields {
		fieldWireSize, direct, err := s.fields[i].directWireSize(&deltas[i], newRowCount)
		if err != nil {
			return 0, err
		}
		if !direct {
			computed, err := s.fields[i].computedSize(&deltas[i], newRowCount)
			if err != nil {
				return 0, err
			}
			fieldWireSize, err = insertBytesFieldSize(13, computed.fieldSize)
			if err != nil {
				return 0, err
			}
		}
		s.fields[i].previewWireSize = fieldWireSize
		if fieldWireSize < s.fields[i].wireSize {
			return 0, insertViewInternal("field %d wire size shrank from %d to %d", i, s.fields[i].wireSize, fieldWireSize)
		}
		fieldDelta := uint64(fieldWireSize - s.fields[i].wireSize)
		if wireDelta > math.MaxUint64-fieldDelta {
			return 0, insertViewInternal("insert request wire-size delta exceeds addressable memory")
		}
		wireDelta += fieldDelta
	}

	numRowsWireSize := insertProto3VarintFieldSize(14, uint64(newRowCount))
	if numRowsWireSize < s.numRowsWireSize {
		return 0, insertViewInternal("NumRows wire size shrank from %d to %d", s.numRowsWireSize, numRowsWireSize)
	}
	numRowsDelta := uint64(numRowsWireSize - s.numRowsWireSize)
	if wireDelta > math.MaxUint64-numRowsDelta || wireDelta+numRowsDelta > uint64(math.MaxInt-s.encodedSize) {
		return 0, insertViewInternal("insert request size exceeds addressable memory")
	}
	total := s.encodedSize + int(wireDelta+numRowsDelta)
	s.previewTimestampWireSize = timestampWireSize
	s.previewRowIDWireSize = rowIDWireSize
	s.previewNumRowsWireSize = numRowsWireSize
	return total, nil
}

func (s *insertRequestSizeState) commitRow(row, encodedSize int) {
	s.rowCount++
	s.timestampPayload += protowire.SizeVarint(s.source.GetTimestamps()[row])
	s.timestampWireSize = s.previewTimestampWireSize
	s.rowIDPayload += protowire.SizeVarint(uint64(s.source.GetRowIDs()[row]))
	s.rowIDWireSize = s.previewRowIDWireSize
	s.numRowsWireSize = s.previewNumRowsWireSize
	for i := range s.fields {
		s.fields[i].commit(s.rowDeltas[i], s.fields[i].previewWireSize)
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
	s.validData = insertFieldValidData(field)
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
		s.compactVector = len(s.validData) > 0 && (s.valuePlan == insertFieldValueVectorDenseBytes ||
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
		s.valuePlan, s.valueFieldNumber, s.stride, s.payloadStride = insertFieldValueVectorDenseBytes, 3, stride, stride
	case *schemapb.VectorField_FloatVector:
		if value.FloatVector == nil {
			return insertViewInternal("field %q (%d) has a nil float vector payload", s.field.GetFieldName(), s.field.GetFieldId())
		}
		stride, err := nonNegativeDim(vector.GetDim(), "float vector")
		if err != nil {
			return err
		}
		payloadStride, err := checkedProduct(stride, 4, "float vector row payload")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride, s.payloadStride = insertFieldValueVectorFloat, 2, stride, payloadStride
	case *schemapb.VectorField_Float16Vector:
		stride, err := denseVectorStride(vector.GetDim(), 2, "float16 vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride, s.payloadStride = insertFieldValueVectorDenseBytes, 4, stride, stride
	case *schemapb.VectorField_Bfloat16Vector:
		stride, err := denseVectorStride(vector.GetDim(), 2, "bfloat16 vector")
		if err != nil {
			return err
		}
		s.valuePlan, s.valueFieldNumber, s.stride, s.payloadStride = insertFieldValueVectorDenseBytes, 5, stride, stride
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
		s.valuePlan, s.valueFieldNumber, s.stride, s.payloadStride = insertFieldValueVectorDenseBytes, 7, stride, stride
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
	valid := s.validData
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

func (s *insertFieldSizeState) packedScalarPayloadUpperBound(rowCount int) (int, error) {
	width := 0
	switch s.valuePlan {
	case insertFieldValueNone, insertFieldValueScalarEmpty:
	case insertFieldValueScalarPacked:
		switch s.valueFieldNumber {
		case 1:
			width = 1
		case 4:
			width = 4
		case 5:
			width = 8
		case 2, 3, 11, 15, 16:
			width = 10
		default:
			return 0, insertViewInternal("unexpected packed scalar field number %d", s.valueFieldNumber)
		}
	default:
		return 0, insertViewInternal("unexpected packed scalar size plan %d", s.valuePlan)
	}
	return checkedProduct(rowCount, width, "packed scalar payload upper bound")
}

func (s *insertFieldSizeState) simplePayloadUpperBound(rowCount int) (int, int, int64, error) {
	switch s.class {
	case insertFieldPlanNone:
		return 0, 0, 0, nil
	case insertFieldPlanScalar:
		switch s.valuePlan {
		case insertFieldValueScalarEmpty, insertFieldValueScalarPacked:
			payloadSize, err := s.packedScalarPayloadUpperBound(rowCount)
			return payloadSize, 0, 0, err
		}
	case insertFieldPlanVector:
		switch s.valuePlan {
		case insertFieldValueVectorEmpty:
			return 0, 0, 0, nil
		case insertFieldValueVectorDenseBytes, insertFieldValueVectorFloat:
			payloadSize, err := checkedProduct(rowCount, s.payloadStride, "dense vector payload upper bound")
			return payloadSize, rowCount, 0, err
		}
	}
	return 0, 0, 0, insertViewInternal("field %q (%d) is not eligible for simple aggregate sizing", s.field.GetFieldName(), s.field.GetFieldId())
}

func (s *insertFieldSizeState) aggregateSimplePayload(rows []int) (int, int, int64, error) {
	if err := s.validateSelectedValidData(rows); err != nil {
		return 0, 0, 0, err
	}
	switch s.class {
	case insertFieldPlanNone:
		return 0, 0, 0, nil
	case insertFieldPlanScalar:
		switch s.valuePlan {
		case insertFieldValueScalarEmpty, insertFieldValueScalarPacked:
			payloadSize, err := s.aggregatePackedScalarPayload(rows)
			return payloadSize, 0, 0, err
		case insertFieldValueScalarRepeated:
			payloadSize, err := s.aggregateRepeatedScalarPayload(rows)
			return payloadSize, 0, 0, err
		}
	case insertFieldPlanVector:
		switch s.valuePlan {
		case insertFieldValueVectorEmpty:
			return 0, 0, 0, nil
		case insertFieldValueVectorDenseBytes, insertFieldValueVectorFloat:
			if err := s.validateDenseVectorRows(rows); err != nil {
				return 0, 0, 0, err
			}
			payloadSize, err := checkedProduct(len(rows), s.payloadStride, "dense vector selected payload")
			return payloadSize, len(rows), 0, err
		case insertFieldValueVectorSparse:
			values := s.field.GetVectors().GetSparseFloatVector().GetContents()
			payloadSize := uint64(0)
			maxDim := int64(0)
			for _, row := range rows {
				if row < 0 || row >= len(values) {
					return 0, 0, 0, insertViewInternal("sparse vector row %d exceeds payload rows %d for field %q (%d)", row, len(values), s.field.GetFieldName(), s.field.GetFieldId())
				}
				rowDim, err := sparseRowDim(values[row])
				if err != nil {
					return 0, 0, 0, err
				}
				wireSize := directBytesFieldSize(1, uint64(len(values[row])))
				if payloadSize > math.MaxUint64-wireSize {
					return 0, 0, 0, insertViewInternal("sparse vector selected payload exceeds addressable memory")
				}
				payloadSize += wireSize
				if rowDim > maxDim {
					maxDim = rowDim
				}
			}
			if payloadSize > uint64(math.MaxInt) {
				return 0, 0, 0, insertViewInternal("sparse vector selected payload exceeds addressable memory")
			}
			return int(payloadSize), len(rows), maxDim, nil
		}
	}
	return 0, 0, 0, insertViewInternal("field %q (%d) is not eligible for simple aggregate sizing", s.field.GetFieldName(), s.field.GetFieldId())
}

func (s *insertFieldSizeState) validateSelectedValidData(rows []int) error {
	valid := s.validData
	if len(valid) == 0 {
		return nil
	}
	for _, row := range rows {
		if row < 0 || row >= len(valid) {
			return insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), s.field.GetFieldName(), s.field.GetFieldId())
		}
	}
	return nil
}

func (s *insertFieldSizeState) validateDenseVectorRows(rows []int) error {
	if len(rows) == 0 {
		return nil
	}
	lastRow := rows[len(rows)-1]
	vector := s.field.GetVectors()
	switch value := vector.Data.(type) {
	case *schemapb.VectorField_BinaryVector:
		_, _, err := vectorBounds(lastRow, s.stride, len(value.BinaryVector), s.field, "binary vector")
		return err
	case *schemapb.VectorField_FloatVector:
		_, _, err := vectorBounds(lastRow, s.stride, len(value.FloatVector.GetData()), s.field, "float vector")
		return err
	case *schemapb.VectorField_Float16Vector:
		_, _, err := vectorBounds(lastRow, s.stride, len(value.Float16Vector), s.field, "float16 vector")
		return err
	case *schemapb.VectorField_Bfloat16Vector:
		_, _, err := vectorBounds(lastRow, s.stride, len(value.Bfloat16Vector), s.field, "bfloat16 vector")
		return err
	case *schemapb.VectorField_Int8Vector:
		_, _, err := vectorBounds(lastRow, s.stride, len(value.Int8Vector), s.field, "int8 vector")
		return err
	default:
		return insertViewInternal("field %q (%d) has unexpected simple vector payload %T", s.field.GetFieldName(), s.field.GetFieldId(), value)
	}
}

func (s *insertFieldSizeState) aggregateRepeatedScalarPayload(rows []int) (int, error) {
	payloadSize, _, err := s.repeatedScalarWireStats(rows)
	return payloadSize, err
}

func (s *insertFieldSizeState) repeatedScalarWireStats(rows []int) (int, int, error) {
	valid := s.validData
	for _, row := range rows {
		if len(valid) > 0 && (row < 0 || row >= len(valid)) {
			return 0, 0, insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), s.field.GetFieldName(), s.field.GetFieldId())
		}
	}
	scalar := s.field.GetScalars()
	switch value := scalar.Data.(type) {
	case *schemapb.ScalarField_StringData:
		return repeatedStringWireStats(value.StringData.GetData(), rows, "string scalar")
	case *schemapb.ScalarField_BytesData:
		return repeatedBytesWireStats(value.BytesData.GetData(), rows, "bytes scalar")
	case *schemapb.ScalarField_JsonData:
		return repeatedBytesWireStats(value.JsonData.GetData(), rows, "JSON scalar")
	case *schemapb.ScalarField_GeometryData:
		return repeatedBytesWireStats(value.GeometryData.GetData(), rows, "geometry scalar")
	case *schemapb.ScalarField_GeometryWktData:
		return repeatedStringWireStats(value.GeometryWktData.GetData(), rows, "geometry WKT scalar")
	case *schemapb.ScalarField_MolData:
		return repeatedBytesWireStats(value.MolData.GetData(), rows, "molecular scalar")
	case *schemapb.ScalarField_MolSmilesData:
		return repeatedStringWireStats(value.MolSmilesData.GetData(), rows, "molecular SMILES scalar")
	default:
		return 0, 0, insertViewInternal("unexpected repeated ScalarField oneof %T", value)
	}
}

func repeatedStringWireStats(values []string, rows []int, label string) (int, int, error) {
	payloadSize := uint64(0)
	maxWireSize := uint64(0)
	for _, row := range rows {
		if row < 0 || row >= len(values) {
			return 0, 0, insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		wireSize := directBytesFieldSize(1, uint64(len(values[row])))
		if payloadSize > math.MaxUint64-wireSize {
			return 0, 0, insertViewInternal("%s payload exceeds addressable memory", label)
		}
		payloadSize += wireSize
		if wireSize > maxWireSize {
			maxWireSize = wireSize
		}
	}
	if payloadSize > uint64(math.MaxInt) || maxWireSize > uint64(math.MaxInt) {
		return 0, 0, insertViewInternal("%s payload exceeds addressable memory", label)
	}
	return int(payloadSize), int(maxWireSize), nil
}

func repeatedBytesWireStats(values [][]byte, rows []int, label string) (int, int, error) {
	payloadSize := uint64(0)
	maxWireSize := uint64(0)
	for _, row := range rows {
		if row < 0 || row >= len(values) {
			return 0, 0, insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		wireSize := directBytesFieldSize(1, uint64(len(values[row])))
		if payloadSize > math.MaxUint64-wireSize {
			return 0, 0, insertViewInternal("%s payload exceeds addressable memory", label)
		}
		payloadSize += wireSize
		if wireSize > maxWireSize {
			maxWireSize = wireSize
		}
	}
	if payloadSize > uint64(math.MaxInt) || maxWireSize > uint64(math.MaxInt) {
		return 0, 0, insertViewInternal("%s payload exceeds addressable memory", label)
	}
	return int(payloadSize), int(maxWireSize), nil
}

func (s *insertFieldSizeState) aggregatePackedScalarPayload(rows []int) (int, error) {
	if _, err := s.packedScalarPayloadUpperBound(len(rows)); err != nil {
		return 0, err
	}
	valid := s.validData
	if len(valid) > 0 && len(rows) > 0 && rows[len(rows)-1] >= len(valid) {
		for _, row := range rows {
			if row >= len(valid) {
				return 0, insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), s.field.GetFieldName(), s.field.GetFieldId())
			}
		}
	}

	if s.class == insertFieldPlanNone || s.valuePlan == insertFieldValueScalarEmpty {
		return 0, nil
	}
	scalar := s.field.GetScalars()
	switch value := scalar.Data.(type) {
	case *schemapb.ScalarField_BoolData:
		values := value.BoolData.GetData()
		if err := validateSelectedRows(rows, len(values), "bool scalar"); err != nil {
			return 0, err
		}
		return len(rows), nil
	case *schemapb.ScalarField_IntData:
		values := value.IntData.GetData()
		if err := validateSelectedRows(rows, len(values), "int scalar"); err != nil {
			return 0, err
		}
		size := 0
		for _, row := range rows {
			size += protowire.SizeVarint(uint64(values[row]))
		}
		return size, nil
	case *schemapb.ScalarField_LongData:
		values := value.LongData.GetData()
		if err := validateSelectedRows(rows, len(values), "long scalar"); err != nil {
			return 0, err
		}
		size := 0
		for _, row := range rows {
			size += protowire.SizeVarint(uint64(values[row]))
		}
		return size, nil
	case *schemapb.ScalarField_FloatData:
		values := value.FloatData.GetData()
		if err := validateSelectedRows(rows, len(values), "float scalar"); err != nil {
			return 0, err
		}
		return checkedProduct(len(rows), 4, "float scalar payload")
	case *schemapb.ScalarField_DoubleData:
		values := value.DoubleData.GetData()
		if err := validateSelectedRows(rows, len(values), "double scalar"); err != nil {
			return 0, err
		}
		return checkedProduct(len(rows), 8, "double scalar payload")
	case *schemapb.ScalarField_TimestamptzData:
		return aggregateSelectedInt64(value.TimestamptzData.GetData(), rows, "timestamptz scalar")
	case *schemapb.ScalarField_DateData:
		values := value.DateData.GetData()
		if err := validateSelectedRows(rows, len(values), "date scalar"); err != nil {
			return 0, err
		}
		size := 0
		for _, row := range rows {
			size += protowire.SizeVarint(uint64(values[row]))
		}
		return size, nil
	case *schemapb.ScalarField_TimeData:
		return aggregateSelectedInt64(value.TimeData.GetData(), rows, "time scalar")
	default:
		return 0, insertViewInternal("unexpected packed ScalarField oneof %T", value)
	}
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
		delta.payloadSize, delta.selectedRows = s.payloadStride, 1
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
		itemSize, cellPayload, err := vectorArrayCellWirePlan(value.VectorArray.GetData()[row])
		if err != nil {
			return err
		}
		wireSize, err := insertBytesFieldSize(2, itemSize)
		if err != nil {
			return err
		}
		delta.payloadSize = wireSize
		delta.arrayCellPayload = cellPayload
	default:
		return insertViewInternal("field %q (%d) has unsupported VectorField oneof %T", s.field.GetFieldName(), s.field.GetFieldId(), value)
	}
	return nil
}

func vectorArrayCellWirePlan(cell *schemapb.VectorField) (int, int, error) {
	plan, ok, err := classifyVectorArrayCell(cell)
	if err != nil {
		return 0, 0, err
	}
	if ok {
		return plan.cellSize, plan.payloadSize, nil
	}
	return nullableProtoSize(cell, false), vectorArrayCellProtoFallback, nil
}

func classifyVectorArrayCell(cell *schemapb.VectorField) (vectorArrayCellPlan, bool, error) {
	if isNilProto(cell) {
		return vectorArrayCellPlan{}, true, nil
	}
	// ValidData (field 9) has no producer anywhere in this repo yet, but proto
	// reflection recognizes it now, so GetUnknown() no longer catches it: a cell
	// carrying it would silently lose that data through the arithmetic path.
	// Fall back to the protobuf path until this one earns explicit handling.
	if len(cell.GetValidData()) != 0 {
		return vectorArrayCellPlan{}, false, nil
	}
	if len(cell.ProtoReflect().GetUnknown()) != 0 {
		return vectorArrayCellPlan{}, false, nil
	}
	plan := vectorArrayCellPlan{
		kind:     vectorArrayCellPlanEmpty,
		cellSize: insertProto3VarintFieldSize(1, uint64(cell.GetDim())),
	}
	switch value := cell.Data.(type) {
	case nil:
		return plan, true, nil
	case *schemapb.VectorField_FloatVector:
		if value.FloatVector == nil || len(value.FloatVector.ProtoReflect().GetUnknown()) != 0 {
			return vectorArrayCellPlan{}, false, nil
		}
		payloadSize, err := checkedProduct(len(value.FloatVector.GetData()), 4, "ArrayOfVector float cell payload")
		if err != nil {
			return vectorArrayCellPlan{}, false, err
		}
		floatArraySize := 0
		if payloadSize > 0 {
			floatArraySize, err = insertBytesFieldSize(1, payloadSize)
			if err != nil {
				return vectorArrayCellPlan{}, false, err
			}
		}
		floatWireSize, err := insertBytesFieldSize(2, floatArraySize)
		if err != nil {
			return vectorArrayCellPlan{}, false, err
		}
		cellSize, err := checkedAddSize(plan.cellSize, floatWireSize, "ArrayOfVector cell size")
		if err != nil {
			return vectorArrayCellPlan{}, false, err
		}
		plan.kind = vectorArrayCellPlanFloat
		plan.payloadSize = payloadSize
		plan.floatArraySize = floatArraySize
		plan.cellSize = cellSize
		return plan, true, nil
	default:
		return vectorArrayCellPlan{}, false, nil
	}
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
		if len(s.validData) > 0 && rowCount > 0 {
			validWireSize, err := insertBytesFieldSize(17, rowCount)
			if err != nil {
				return insertFieldComputedSize{}, err
			}
			scalarSize, err = checkedAddSize(scalarSize, validWireSize, "ScalarField size")
			if err != nil {
				return insertFieldComputedSize{}, err
			}
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
		if len(s.validData) > 0 && rowCount > 0 {
			validWireSize, err := insertBytesFieldSize(9, rowCount)
			if err != nil {
				return insertFieldComputedSize{}, err
			}
			vectorSize, err = checkedAddSize(vectorSize, validWireSize, "VectorField size")
			if err != nil {
				return insertFieldComputedSize{}, err
			}
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
	computed.fieldSize = fieldSize
	return computed, nil
}

// directWireSize mirrors computedSize for the arithmetic-only field plans, but
// keeps the hot split loop in uint64 and validates overflow once at the edge.
func (s *insertFieldSizeState) directWireSize(delta *insertFieldRowDelta, rowCount int) (int, bool, error) {
	payloadSize := uint64(s.payloadSize)
	selectedRows := uint64(s.selectedRows)
	sparseDim := s.sparseDim
	if delta != nil {
		payloadSize += uint64(delta.payloadSize)
		selectedRows += uint64(delta.selectedRows)
		if delta.sparseDim > sparseDim {
			sparseDim = delta.sparseDim
		}
	}
	if payloadSize > uint64(math.MaxInt) || selectedRows > uint64(math.MaxInt) {
		return 0, true, insertViewInternal("direct field size exceeds addressable memory")
	}

	fieldSize := uint64(s.metadataSize)
	switch s.class {
	case insertFieldPlanNone:
	case insertFieldPlanScalar:
		scalarSize := uint64(0)
		switch s.valuePlan {
		case insertFieldValueScalarEmpty:
		case insertFieldValueScalarPacked:
			arraySize := uint64(0)
			if rowCount > 0 {
				arraySize = directBytesFieldSize(1, payloadSize)
			}
			scalarSize = directBytesFieldSize(s.valueFieldNumber, arraySize)
		case insertFieldValueScalarRepeated:
			scalarSize = directBytesFieldSize(s.valueFieldNumber, payloadSize)
		default:
			return 0, false, nil
		}
		if len(s.validData) > 0 && rowCount > 0 {
			scalarSize += directBytesFieldSize(17, uint64(rowCount))
		}
		fieldSize += directBytesFieldSize(3, scalarSize)
	case insertFieldPlanVector:
		vector := s.field.GetVectors()
		outerDim := vector.GetDim()
		if s.valuePlan == insertFieldValueVectorSparse && selectedRows > 0 {
			outerDim = vector.GetSparseFloatVector().GetDim()
		}
		vectorSize := directProto3VarintFieldSize(1, uint64(outerDim))
		switch s.valuePlan {
		case insertFieldValueVectorEmpty:
		case insertFieldValueVectorDenseBytes:
			if selectedRows > 0 {
				vectorSize += directBytesFieldSize(s.valueFieldNumber, payloadSize)
			}
		case insertFieldValueVectorFloat:
			if selectedRows > 0 {
				floatArraySize := uint64(0)
				if payloadSize > 0 {
					floatArraySize = directBytesFieldSize(1, payloadSize)
				}
				vectorSize += directBytesFieldSize(2, floatArraySize)
			}
		case insertFieldValueVectorSparse:
			if selectedRows > 0 {
				if sparseDim < 0 {
					return 0, true, insertViewInternal("selected sparse vector dimension %d is negative", sparseDim)
				}
				sparseSize := payloadSize + directProto3VarintFieldSize(2, uint64(sparseDim))
				vectorSize += directBytesFieldSize(6, sparseSize)
			}
		case insertFieldValueVectorArray:
			vectorSize += directBytesFieldSize(8, payloadSize)
		default:
			return 0, false, nil
		}
		if len(s.validData) > 0 && rowCount > 0 {
			vectorSize += directBytesFieldSize(9, uint64(rowCount))
		}
		fieldSize += directBytesFieldSize(4, vectorSize)
	default:
		return 0, false, nil
	}
	fieldWireSize := directBytesFieldSize(13, fieldSize)
	if fieldWireSize > uint64(math.MaxInt) {
		return 0, true, insertViewInternal("direct FieldData wire size exceeds addressable memory")
	}
	return int(fieldWireSize), true, nil
}

func directProto3VarintFieldSize(number protowire.Number, value uint64) uint64 {
	if value == 0 {
		return 0
	}
	return uint64(protowire.SizeTag(number) + protowire.SizeVarint(value))
}

func directBytesFieldSize(number protowire.Number, payloadSize uint64) uint64 {
	return uint64(protowire.SizeTag(number)+protowire.SizeVarint(payloadSize)) + payloadSize
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
	// Validity now lives inside the nested ScalarField/VectorField message
	// (#52203), so the slot only exists when the writer enters that message.
	if (s.class == insertFieldPlanScalar || s.class == insertFieldPlanVector) && len(s.validData) > 0 && rowCount > 0 {
		plan = append(plan, rowCount)
	}
	return plan
}

func (s *insertFieldSizeState) commit(delta insertFieldRowDelta, wireSize int) {
	s.payloadSize += delta.payloadSize
	s.wireSize = wireSize
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

func validateSelectedRows(rows []int, valueCount int, label string) error {
	if len(rows) == 0 || rows[len(rows)-1] < valueCount {
		return nil
	}
	for _, row := range rows {
		if row >= valueCount {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, valueCount)
		}
	}
	return nil
}

func aggregateSelectedInt64(values []int64, rows []int, label string) (int, error) {
	if err := validateSelectedRows(rows, len(values), label); err != nil {
		return 0, err
	}
	size := 0
	for _, row := range rows {
		size += protowire.SizeVarint(uint64(values[row]))
	}
	return size, nil
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
