// Copyright 2024 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RecordToInsertData converts a Record batch into *InsertData using the provided
// collection schema. Columns are matched by Milvus field id through the Record
// abstraction.
//
// Fields present in the schema but absent from the Record are left empty
// in the returned InsertData (intended for function-output fields that will be
// filled in later by the function executor). requiredFields must be present and
// have the same row count as the Record.
//
// Fields are deserialised via serdeMap row-by-row, so nullable ValidData is
// populated correctly through each FieldData's AppendRow implementation.
func RecordToInsertData(
	rec Record,
	schema *schemapb.CollectionSchema,
	requiredFields typeutil.Set[int64],
) (*InsertData, error) {
	insertData, err := NewInsertDataWithFunctionOutputField(schema)
	if err != nil {
		return nil, err
	}
	if rec == nil || rec.Len() == 0 {
		return insertData, nil
	}

	numRows := rec.Len()

	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fieldID := field.GetFieldID()
		col, ok := recordColumn(rec, fieldID)
		if !ok {
			if requiredFields != nil && requiredFields.Contain(fieldID) {
				return nil, merr.WrapErrParameterInvalidMsg("required field %s (ID=%d) not found in record",
					field.GetName(), fieldID)
			}
			continue
		}
		if col.Len() != numRows {
			return nil, merr.WrapErrParameterInvalidMsg("field %s (ID=%d) row count mismatch: %d != %d",
				field.GetName(), fieldID, col.Len(), numRows)
		}
		dt := field.GetDataType()

		dim := 0
		if typeutil.IsVectorType(dt) {
			d, _ := typeutil.GetDim(field)
			dim = int(d)
		}
		elementType := schemapb.DataType_None
		if dt == schemapb.DataType_ArrayOfVector {
			elementType = field.GetElementType()
		}

		fd, ok := insertData.Data[fieldID]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("field %s (ID=%d) not initialized in InsertData",
				field.GetName(), fieldID)
		}

		entry, ok := serdeMap[dt]
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("unsupported data type %s for field %s", dt, field.GetName())
		}

		for i := 0; i < numRows; i++ {
			val, err := entry.deserialize(col, i, elementType, dim, true /* shouldCopy */, field.GetElementNullable())
			if err != nil {
				return nil, merr.Wrapf(err, "deserialize field %s row %d",
					field.GetName(), i)
			}
			if err := fd.AppendRow(val); err != nil {
				return nil, merr.Wrapf(err, "append field %s row %d",
					field.GetName(), i)
			}
		}
	}

	for fieldID := range requiredFields {
		fd, ok := insertData.Data[fieldID]
		rowNum := 0
		if fd != nil {
			rowNum = fd.RowNum()
		}
		if !ok || rowNum != numRows {
			return nil, merr.WrapErrParameterInvalidMsg("required field ID=%d has %d rows, expected %d",
				fieldID, rowNum, numRows)
		}
	}

	return insertData, nil
}

func recordColumn(rec Record, fieldID FieldID) (col arrow.Array, ok bool) {
	switch r := rec.(type) {
	case *simpleArrowRecord:
		colIdx, ok := r.field2Col[fieldID]
		if !ok {
			return nil, false
		}
		if colIdx < 0 || colIdx >= int(r.r.NumCols()) {
			return nil, false
		}
		return r.r.Column(colIdx), true
	case *compositeRecord:
		col := r.Column(fieldID)
		return col, col != nil
	}

	col = rec.Column(fieldID)
	return col, col != nil
}
