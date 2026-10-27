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
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RecordValidator checks the Milvus record contract before consumers access
// columns or cross the native writer boundary. Construct once per immutable
// schema, not per row. Violations describe the observed data, not its origin:
// a writer-side violation is still DataIntegrity, not proof of a writer bug.
type RecordValidator struct {
	physical  *arrow.Schema
	fields    []recordFieldContract
	field2Col map[FieldID]int
	reader    bool
}

type recordFieldContract struct {
	field       *schemapb.FieldSchema
	expected    arrow.DataType
	vectorWidth int
}

// NewRecordValidator accepts an explicit physical schema for writers (including
// binary TEXT/LOB references). With nil physical, readers accept either TEXT
// representation; they may legitimately preserve refs instead of decoding them.
// Callers must omit pending function outputs from schema until materialized.
func NewRecordValidator(schema *schemapb.CollectionSchema, physical *arrow.Schema) (*RecordValidator, error) {
	if schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("record validator requires a schema")
	}
	reader := physical == nil
	if physical == nil {
		var err error
		physical, err = ConvertToArrowSchema(schema, true)
		if err != nil {
			return nil, err
		}
	}
	fields := typeutil.GetAllFieldSchemas(schema)
	if len(fields) != physical.NumFields() {
		return nil, merr.WrapErrServiceInternalMsg("record validator schema has %d fields, physical schema has %d", len(fields), physical.NumFields())
	}
	v := &RecordValidator{physical: physical, reader: reader, field2Col: make(map[FieldID]int, len(fields))}
	for i, field := range fields {
		v.field2Col[field.GetFieldID()] = i
		contract := recordFieldContract{field: field, expected: physical.Field(i).Type}
		vectorType := field.GetDataType()
		if vectorType == schemapb.DataType_ArrayOfVector {
			vectorType = field.GetElementType()
		}
		if typeutil.IsVectorType(vectorType) && vectorType != schemapb.DataType_SparseFloatVector {
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			contract.vectorWidth, err = getArrayOfVectorElementByteWidth(vectorType, int(dim))
			if err != nil {
				return nil, err
			}
		}
		v.fields = append(v.fields, contract)
	}
	return v, nil
}

// Validate inspects only logical rows. Arrow permits backing columns longer
// than NumRows; readers can borrow those arrays, but writers must slice them
// before FFI. Scalar Array payloads are protobuf Binary, not Arrow List: their
// semantic decoding remains the responsibility of the deserializer.
func (v *RecordValidator) Validate(rec Record) error {
	return v.validate(rec, true)
}

// ValidateForRead protects column access before predicates and sorting. Value
// checks belong to the existing row-copy loop or the final writer boundary,
// not a separate scan of nested payloads that filtering may discard.
func (v *RecordValidator) ValidateForRead(rec Record) error {
	return v.validate(rec, false)
}

func recordRows(rec Record) (int, error) {
	if rec == nil {
		return 0, merr.WrapErrDataIntegrityMsg("nil record")
	}
	switch rec := rec.(type) {
	case *simpleArrowRecord:
		if rec == nil || rec.r == nil {
			return 0, merr.WrapErrDataIntegrityMsg("nil Arrow record")
		}
	case *compositeRecord:
		if rec == nil || len(rec.recs) == 0 || rec.recs[0] == nil {
			return 0, merr.WrapErrDataIntegrityMsg("missing composite record row anchor")
		}
	}
	rows := rec.Len()
	if rows < 0 {
		return 0, merr.WrapErrDataIntegrityMsg("negative record rows: %d", rows)
	}
	return rows, nil
}

func (v *RecordValidator) validate(rec Record, full bool) error {
	rows, err := recordRows(rec)
	if err != nil {
		return err
	}
	for _, contract := range v.fields {
		col, ok := TryRecordColumn(rec, contract.field.GetFieldID())
		if !ok || col == nil {
			return merr.WrapErrDataIntegrityMsg("field %s (ID=%d): missing column, record rows=%d",
				contract.field.GetName(), contract.field.GetFieldID(), rows)
		}
		if err := v.validateColumn(contract, col, rows, full); err != nil {
			return merr.Wrapf(err, "field %s (ID=%d), record rows=%d, expected type=%s",
				contract.field.GetName(), contract.field.GetFieldID(), rows, contract.expected)
		}
	}
	return nil
}

func (v *RecordValidator) validateColumn(c recordFieldContract, col arrow.Array, rows int, full bool) error {
	if col.Len() < rows {
		return merr.WrapErrDataIntegrityMsg("short column: length=%d", col.Len())
	}
	if col.Len() > rows {
		col = array.NewSlice(col, 0, int64(rows))
		defer col.Release()
	}
	if err := validateColumnType(c, col, v.reader); err != nil {
		return err
	}
	if !c.field.GetNullable() && col.NullN() != 0 {
		return merr.WrapErrDataIntegrityMsg("non-nullable column contains %d nulls", col.NullN())
	}
	if !full {
		return nil
	}
	if c.field.GetDataType() == schemapb.DataType_ArrayOfVector {
		list := col.(*array.List) // physical type was checked above
		values := list.ListValues()
		_, fixedWidth := values.(*array.FixedSizeBinary)
		skipChildren := fixedWidth && values.NullN() == 0
		for row := 0; row < rows; row++ {
			// A null parent masks its children. List slices can retain children
			// outside their visible range; neither is a data-integrity failure.
			if list.IsNull(row) {
				continue
			}
			start, end := list.ValueOffsets(row)
			if err := validateElementRange(start, end, values.Len()); err != nil {
				return merr.Wrapf(err, "row %d", row)
			}
			if skipChildren {
				continue
			}
			for i := int(start); i < int(end); i++ {
				if values.IsNull(i) {
					// Arrow ListOf marks its child nullable regardless of the
					// Milvus ElementNullable setting. Enforce the latter here.
					if !c.field.GetElementNullable() {
						return merr.WrapErrDataIntegrityMsg("row %d: non-nullable element %d is null", row, int64(i)-start)
					}
					continue
				}
				if binary, ok := values.(*array.Binary); ok && binary.ValueLen(i) != c.vectorWidth {
					return merr.WrapErrDataIntegrityMsg("row %d element %d: vector width=%d, expected=%d", row, int64(i)-start, binary.ValueLen(i), c.vectorWidth)
				}
			}
		}
	} else if binary, ok := col.(*array.Binary); ok && c.vectorWidth > 0 {
		for row := 0; row < rows; row++ {
			if !binary.IsNull(row) && binary.ValueLen(row) != c.vectorWidth {
				return merr.WrapErrDataIntegrityMsg("row %d: vector width=%d, expected=%d", row, binary.ValueLen(row), c.vectorWidth)
			}
		}
	}
	return nil
}

func validateColumnType(c recordFieldContract, col arrow.Array, reader bool) error {
	if arrow.TypeEqual(col.DataType(), c.expected) {
		return nil
	}
	// Readers preserve either TEXT representation. Writers may normalize an
	// all-null column, which has no payload or LOB reference to interpret.
	if c.field.GetDataType() == schemapb.DataType_Text &&
		isTextArrowType(col.DataType()) && isTextArrowType(c.expected) &&
		(reader || col.NullN() == col.Len()) {
		return nil
	}
	return merr.WrapErrDataIntegrityMsg("column type mismatch: got %s, expected %s", col.DataType(), c.expected)
}

func validateElementRange(start, end int64, length int) error {
	if start < 0 || end < start || end > int64(length) {
		return merr.WrapErrDataIntegrityMsg("invalid element range [%d,%d), child length=%d", start, end, length)
	}
	return nil
}

func isTextArrowType(dt arrow.DataType) bool {
	return dt.ID() == arrow.STRING || dt.ID() == arrow.BINARY
}

// validatedWriterRecord is private to the synchronous outer-writer -> packed
// writer call. The outer writer has projected, sliced and validated it before
// updating statistics. Reusing that proof avoids a second nested-value scan.
// The record and both schemas must remain immutable for this call.
type validatedWriterRecord struct {
	*simpleArrowRecord
	schema *schemapb.CollectionSchema
}

func prepareWriterRecord(r Record, schema *schemapb.CollectionSchema, physical *arrow.Schema, validator **RecordValidator) (arrow.Record, func(), error) {
	if checked, ok := r.(*validatedWriterRecord); ok && checked.schema == schema && checked.r.Schema().Equal(physical) {
		return checked.r, func() {}, nil
	}
	if *validator == nil {
		var err error
		*validator, err = NewRecordValidator(schema, physical)
		if err != nil {
			return nil, nil, err
		}
	}
	return (*validator).toWriterRecord(r)
}
