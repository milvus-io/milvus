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
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ArrowRecordToInsertData converts an arrow.Record into *InsertData using the
// provided collection schema. Columns are matched between the arrow record and
// the schema by name.
//
// Fields present in the schema but absent from the arrow record are left empty
// in the returned InsertData (intended for function-output fields that will be
// filled in later by the function executor).
//
// Fields are deserialised via serdeMap row-by-row, so nullable ValidData is
// populated correctly through each FieldData's AppendRow implementation.
func ArrowRecordToInsertData(
	rec arrow.Record,
	schema *schemapb.CollectionSchema,
) (*InsertData, error) {
	insertData, err := NewInsertDataWithFunctionOutputField(schema)
	if err != nil {
		return nil, err
	}
	if rec == nil || rec.NumRows() == 0 {
		return insertData, nil
	}

	arrowSchema := rec.Schema()
	numRows := int(rec.NumRows())

	for _, field := range schema.GetFields() {
		// External input columns are named by ExternalField in the arrow
		// record (the reader uses the source-table column name, not the
		// milvus field name). Internal columns use the field name.
		columnName := field.GetName()
		if ext := field.GetExternalField(); ext != "" {
			columnName = ext
		}
		indices := arrowSchema.FieldIndices(columnName)
		if len(indices) == 0 {
			// Field not present in arrow record (e.g. function output, system field).
			continue
		}
		col := rec.Column(indices[0])
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

		fd, ok := insertData.Data[field.GetFieldID()]
		if !ok {
			return nil, fmt.Errorf("field %s (ID=%d) not initialized in InsertData",
				field.GetName(), field.GetFieldID())
		}

		entry, ok := serdeMap[dt]
		if !ok {
			return nil, fmt.Errorf("unsupported data type %s for field %s", dt, field.GetName())
		}

		for i := 0; i < numRows; i++ {
			val, err := entry.deserialize(col, i, elementType, dim, true /* shouldCopy */)
			if err != nil {
				return nil, fmt.Errorf("deserialize field %s row %d: %w",
					field.GetName(), i, err)
			}
			if err := fd.AppendRow(val); err != nil {
				return nil, fmt.Errorf("append field %s row %d: %w",
					field.GetName(), i, err)
			}
		}
	}

	return insertData, nil
}
