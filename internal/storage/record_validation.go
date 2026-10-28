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

// RecordValidator guards compaction's sorting inputs and outputs. It is opt-in:
// shared sort, record builders and writers retain their existing contracts.
// Construct once per immutable schema, after choosing the materialized fields.
type RecordValidator struct {
	fields []recordFieldContract
}

type recordFieldContract struct {
	field       *schemapb.FieldSchema
	expected    arrow.DataType
	vectorWidth int
}

// NewRecordValidator accepts both decoded TEXT and binary LOB references.
// Physical projection remains the writer's responsibility.
func NewRecordValidator(schema *schemapb.CollectionSchema) (*RecordValidator, error) {
	if schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("record validator requires a schema")
	}
	physical, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, err
	}
	fields := typeutil.GetAllFieldSchemas(schema)
	v := &RecordValidator{}
	for i, field := range fields {
		contract := recordFieldContract{field: field, expected: physical.Field(i).Type}
		vectorType := field.GetDataType()
		if field.GetNullable() && typeutil.IsVectorType(vectorType) &&
			vectorType != schemapb.DataType_SparseFloatVector && vectorType != schemapb.DataType_ArrayOfVector {
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

// Validate checks only nullability and vector width after filtering and default
// filling, before writer statistics or native IO. The record must be produced by
// RecordBuilder using this validator's schema, so its structure is guaranteed by
// construction. ArrayOfVector child constraints are enforced by appendValueAt;
// scalar Array payload decoding remains the deserializer's responsibility.
func (v *RecordValidator) Validate(rec Record) error {
	rows := rec.Len()
	for _, contract := range v.fields {
		if contract.field.GetNullable() && contract.vectorWidth == 0 {
			continue
		}
		col := rec.Column(contract.field.GetFieldID())
		if err := validateColumnValues(contract, col, rows); err != nil {
			return merr.Wrapf(err, "field %s (ID=%d), record rows=%d",
				contract.field.GetName(), contract.field.GetFieldID(), rows)
		}
	}
	return nil
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

// ValidateForRead checks structure before predicates and sorting. It does not
// reject null values: row copying may fill defaults, and filtering may discard
// them. Final value constraints are checked by Validate on the output.
func (v *RecordValidator) ValidateForRead(rec Record) error {
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
		if col.Len() < rows {
			return merr.WrapErrDataIntegrityMsg("field %s (ID=%d): short column, length=%d, record rows=%d",
				contract.field.GetName(), contract.field.GetFieldID(), col.Len(), rows)
		}
		if err := validateColumnType(contract, col); err != nil {
			return merr.Wrapf(err, "field %s (ID=%d), record rows=%d, expected type=%s",
				contract.field.GetName(), contract.field.GetFieldID(), rows, contract.expected)
		}
	}
	return nil
}

func validateColumnValues(c recordFieldContract, col arrow.Array, rows int) error {
	if !c.field.GetNullable() && col.NullN() != 0 {
		return merr.WrapErrDataIntegrityMsg("non-nullable column contains %d nulls", col.NullN())
	}
	if binary, ok := col.(*array.Binary); ok && c.vectorWidth > 0 {
		for row := 0; row < rows; row++ {
			if !binary.IsNull(row) && binary.ValueLen(row) != c.vectorWidth {
				return merr.WrapErrDataIntegrityMsg("row %d: vector width=%d, expected=%d", row, binary.ValueLen(row), c.vectorWidth)
			}
		}
	}
	return nil
}

func validateColumnType(c recordFieldContract, col arrow.Array) error {
	if arrow.TypeEqual(col.DataType(), c.expected) {
		return nil
	}
	// Compaction can preserve binary TEXT references or copy decoded UTF8.
	if c.field.GetDataType() == schemapb.DataType_Text &&
		isTextArrowType(col.DataType()) && isTextArrowType(c.expected) {
		return nil
	}
	return merr.WrapErrDataIntegrityMsg("column type mismatch: got %s, expected %s", col.DataType(), c.expected)
}

func isTextArrowType(dt arrow.DataType) bool {
	return dt.ID() == arrow.STRING || dt.ID() == arrow.BINARY
}

// WrapReader checks each borrowed batch once without changing its ownership.
func (v *RecordValidator) WrapReader(reader RecordReader) RecordReader {
	return &validatingRecordReader{RecordReader: reader, validator: v}
}

type validatingRecordReader struct {
	RecordReader
	validator *RecordValidator
}

func (r *validatingRecordReader) Next() (Record, error) {
	rec, err := r.RecordReader.Next()
	if err != nil {
		return nil, err
	}
	if err := r.validator.ValidateForRead(rec); err != nil {
		return nil, merr.Wrap(err, "compaction input validation")
	}
	return rec, nil
}

// WrapWriter checks values in completed RecordBuilder batches before the delegate
// can update statistics or invoke native IO. Close and error handling are unchanged.
func (v *RecordValidator) WrapWriter(writer RecordWriter) RecordWriter {
	return &validatingRecordWriter{RecordWriter: writer, validator: v}
}

type validatingRecordWriter struct {
	RecordWriter
	validator *RecordValidator
}

func (w *validatingRecordWriter) Write(rec Record) error {
	if err := w.validator.Validate(rec); err != nil {
		return merr.Wrap(err, "compaction output validation")
	}
	return w.RecordWriter.Write(rec)
}
