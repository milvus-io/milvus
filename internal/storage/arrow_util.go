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
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func isNullableDenseVectorArrowType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector:
		return true
	default:
		return false
	}
}

type appendValueDefault struct {
	value       *schemapb.ValueField
	geometryWKB []byte
}

func newAppendValueDefault(field *schemapb.FieldSchema) (appendValueDefault, error) {
	defaultValue := field.GetDefaultValue()
	ret := appendValueDefault{value: defaultValue}
	if defaultValue != nil && field.GetDataType() == schemapb.DataType_Geometry {
		val, err := common.ConvertWKTToWKB(defaultValue.GetStringData())
		if err != nil {
			return ret, merr.WrapErrServiceInternalErr(err, "invalid default value for geometry field %s", field.GetName())
		}
		ret.geometryWKB = val
	}
	return ret, nil
}

func appendValueAt(builder array.Builder, a arrow.Array, idx int, field *schemapb.FieldSchema, appendDefault appendValueDefault) (uint64, error) {
	// a could never be nil here
	defaultValue := appendDefault.value
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		ba, ok := a.(*array.Boolean)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ba.IsNull(idx) {
			if defaultValue != nil {
				b.Append(defaultValue.GetBoolData())
				return 1, nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ba.Value(idx))
			return 1, nil
		}
	case *array.Int8Builder:
		ia, ok := a.(*array.Int8)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			if defaultValue != nil {
				b.Append(int8(defaultValue.GetIntData()))
				return 1, nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 1, nil
		}
	case *array.Int16Builder:
		ia, ok := a.(*array.Int16)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			if defaultValue != nil {
				b.Append(int16(defaultValue.GetIntData()))
				return 2, nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 2, nil
		}
	case *array.Int32Builder:
		ia, ok := a.(*array.Int32)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			if defaultValue != nil {
				b.Append(defaultValue.GetIntData())
				return 4, nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 4, nil
		}
	case *array.Int64Builder:
		ia, ok := a.(*array.Int64)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ia.IsNull(idx) {
			if defaultValue != nil {
				b.Append(defaultValue.GetLongData())
				return 8, nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(ia.Value(idx))
			return 8, nil
		}
	case *array.Float32Builder:
		fa, ok := a.(*array.Float32)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if fa.IsNull(idx) {
			if defaultValue != nil {
				b.Append(defaultValue.GetFloatData())
				return 4, nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(fa.Value(idx))
			return 4, nil
		}
	case *array.Float64Builder:
		if a == nil {
			if defaultValue != nil {
				b.Append(defaultValue.GetDoubleData())
				return 8, nil
			} else {
				b.AppendNull()
				return 0, nil
			}
		}
		fa, ok := a.(*array.Float64)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if fa.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			b.Append(fa.Value(idx))
			return 8, nil
		}
	case *array.StringBuilder:
		sa, ok := a.(*array.String)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if sa.IsNull(idx) {
			if defaultValue != nil {
				val := defaultValue.GetStringData()
				b.Append(val)
				return uint64(len(val)), nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			val := sa.Value(idx)
			b.Append(val)
			return uint64(len(val)), nil
		}
	case *array.BinaryBuilder:
		ba, ok := a.(*array.Binary)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ba.IsNull(idx) {
			// could be internal $meta json
			if defaultValue != nil {
				if field.GetDataType() == schemapb.DataType_Geometry {
					b.Append(appendDefault.geometryWKB)
					return uint64(len(appendDefault.geometryWKB)), nil
				}
				val := defaultValue.GetBytesData()
				b.Append(val)
				return uint64(len(val)), nil
			}
			b.AppendNull()
			return 0, nil
		} else {
			val := ba.Value(idx)
			b.Append(val)
			return uint64(len(val)), nil
		}
	case *array.FixedSizeBinaryBuilder:
		ba, ok := a.(*array.FixedSizeBinary)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if ba.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		} else {
			val := ba.Value(idx)
			b.Append(val)
			return uint64(len(val)), nil
		}
	case *array.ListBuilder:
		// Handle ListBuilder for ArrayOfVector type
		la, ok := a.(*array.List)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		if la.IsNull(idx) {
			b.AppendNull()
			return 0, nil
		}

		start, end := la.ValueOffsets(idx)
		b.Append(true)

		valuesArray := la.ListValues()
		var totalSize uint64 = 0
		valueBuilder := b.ValueBuilder()
		switch vb := valueBuilder.(type) {
		case *array.FixedSizeBinaryBuilder:
			fixedArray, ok := valuesArray.(*array.FixedSizeBinary)
			if !ok {
				return 0, merr.WrapErrServiceInternalMsg("invalid value type %T, expect %T", valuesArray.DataType(), vb.Type())
			}
			for i := start; i < end; i++ {
				val := fixedArray.Value(int(i))
				vb.Append(val)
				totalSize += uint64(len(val))
			}
		default:
			return 0, merr.WrapErrServiceInternalMsg("unsupported value builder type in ListBuilder: %T", valueBuilder)
		}

		return totalSize, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported builder type: %T", builder)
	}
}

// GenerateEmptyArrayFromSchema generate empty array from schema
// If schema has default value, the array will bef filled with it.
// Otherwise, null will be used instead.
// If input schema is not nullable, an error will be returned.
func GenerateEmptyArrayFromSchema(schema *schemapb.FieldSchema, numRows int) (arrow.Array, error) {
	// if not nullable, return error
	if !schema.GetNullable() {
		return nil, merr.WrapErrServiceInternalMsg("missing field data %s", schema.Name)
	}
	dim, _ := typeutil.GetDim(schema)

	elementType := schemapb.DataType_None
	if schema.GetDataType() == schemapb.DataType_ArrayOfVector {
		elementType = schema.GetElementType()
	}
	arrowType := serdeMap[schema.GetDataType()].arrowType(int(dim), elementType)
	if schema.GetDataType() == schemapb.DataType_Text {
		arrowType = arrow.BinaryTypes.Binary
	} else if schema.GetNullable() && isNullableDenseVectorArrowType(schema.GetDataType()) {
		arrowType = arrow.BinaryTypes.Binary
	}
	builder := array.NewBuilder(memory.DefaultAllocator, arrowType)
	if schema.GetDefaultValue() != nil {
		switch schema.GetDataType() {
		case schemapb.DataType_Bool:
			bd := builder.(*array.BooleanBuilder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) bool { return schema.GetDefaultValue().GetBoolData() }),
				nil)
		case schemapb.DataType_Int8:
			bd := builder.(*array.Int8Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int8 { return int8(schema.GetDefaultValue().GetIntData()) }),
				nil)
		case schemapb.DataType_Int16:
			bd := builder.(*array.Int16Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int16 { return int16(schema.GetDefaultValue().GetIntData()) }),
				nil)
		case schemapb.DataType_Int32:
			bd := builder.(*array.Int32Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int32 { return schema.GetDefaultValue().GetIntData() }),
				nil)
		case schemapb.DataType_Int64:
			bd := builder.(*array.Int64Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int64 { return schema.GetDefaultValue().GetLongData() }),
				nil)
		case schemapb.DataType_Float:
			bd := builder.(*array.Float32Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) float32 { return schema.GetDefaultValue().GetFloatData() }),
				nil)
		case schemapb.DataType_Double:
			bd := builder.(*array.Float64Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) float64 { return schema.GetDefaultValue().GetDoubleData() }),
				nil)

		case schemapb.DataType_Timestamptz:
			bd := builder.(*array.Int64Builder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) int64 { return schema.GetDefaultValue().GetTimestamptzData() }),
				nil)

		case schemapb.DataType_VarChar, schemapb.DataType_String:
			bd := builder.(*array.StringBuilder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) string { return schema.GetDefaultValue().GetStringData() }),
				nil)
		case schemapb.DataType_JSON:
			bd := builder.(*array.BinaryBuilder)
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) []byte { return schema.GetDefaultValue().GetBytesData() }),
				nil)
		case schemapb.DataType_Geometry:
			bd := builder.(*array.BinaryBuilder)
			defaultValue, err := common.ConvertWKTToWKB(schema.GetDefaultValue().GetStringData())
			if err != nil {
				return nil, merr.WrapErrServiceInternalErr(err, "invalid default value for geometry field %s", schema.GetName())
			}
			bd.AppendValues(
				lo.RepeatBy(numRows, func(_ int) []byte { return defaultValue }),
				nil)
		default:
			return nil, merr.WrapErrServiceInternalMsg("Unexpected default value type: %s", schema.GetDataType().String())
		}
	} else {
		builder.AppendNulls(numRows)
	}

	return builder.NewArray(), nil
}

// RecordBuilder is a helper to build arrow record.
// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
// small batch size will cause write performance degradation. To work around this issue, we accumulate
// records and write them in batches. This requires additional memory copy.
type RecordBuilder struct {
	fields      []*schemapb.FieldSchema
	arrowFields []arrow.Field
	arrowSchema *arrow.Schema
	builders    []array.Builder
	defaults    []appendValueDefault

	nRows int
	size  uint64
}

// preparedValueAppender binds a destination builder to one source Arrow array.
// MergeSort keeps one of these per output field while a reader's current record
// is live. The type checks and default handling are performed when the record
// is installed, rather than once for every emitted (row, field) pair. Keeping
// the builder and source pointers in this typed union avoids both per-row
// interface assertions and per-record closure allocations.
type preparedValueAppender struct {
	appendFn         preparedAppendFunc
	defaultValue     *schemapb.ValueField
	geometryWKB      []byte
	boolBuilder      *array.BooleanBuilder
	boolSource       *array.Boolean
	int8Builder      *array.Int8Builder
	int8Source       *array.Int8
	int16Builder     *array.Int16Builder
	int16Source      *array.Int16
	int32Builder     *array.Int32Builder
	int32Source      *array.Int32
	int64Builder     *array.Int64Builder
	int64Source      *array.Int64
	float32Builder   *array.Float32Builder
	float32Source    *array.Float32
	float64Builder   *array.Float64Builder
	float64Source    *array.Float64
	stringBuilder    *array.StringBuilder
	stringSource     *array.String
	binaryBuilder    *array.BinaryBuilder
	binarySource     *array.Binary
	fixedBuilder     *array.FixedSizeBinaryBuilder
	fixedSource      *array.FixedSizeBinary
	listBuilder      *array.ListBuilder
	listSource       *array.List
	listValues       *array.FixedSizeBinary
	listValueBuilder *array.FixedSizeBinaryBuilder
}

type preparedAppendFunc func(*preparedValueAppender, int) (uint64, error)

type preparedRecordAppender struct {
	fields []preparedValueAppender
}

const directForwardMinRows = 128

func prepareValueAppender(builder array.Builder, a arrow.Array, appendDefault appendValueDefault) (preparedValueAppender, error) {
	app := preparedValueAppender{defaultValue: appendDefault.value, geometryWKB: appendDefault.geometryWKB}
	invalid := func() error {
		return merr.WrapErrServiceInternalMsg("invalid source value type %T for builder %T", a, builder)
	}
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		sa, ok := a.(*array.Boolean)
		if !ok {
			return app, invalid()
		}
		app.boolBuilder, app.boolSource = b, sa
		app.appendFn = appendPreparedBool
	case *array.Int8Builder:
		sa, ok := a.(*array.Int8)
		if !ok {
			return app, invalid()
		}
		app.int8Builder, app.int8Source = b, sa
		app.appendFn = appendPreparedInt8
	case *array.Int16Builder:
		sa, ok := a.(*array.Int16)
		if !ok {
			return app, invalid()
		}
		app.int16Builder, app.int16Source = b, sa
		app.appendFn = appendPreparedInt16
	case *array.Int32Builder:
		sa, ok := a.(*array.Int32)
		if !ok {
			return app, invalid()
		}
		app.int32Builder, app.int32Source = b, sa
		app.appendFn = appendPreparedInt32
	case *array.Int64Builder:
		sa, ok := a.(*array.Int64)
		if !ok {
			return app, invalid()
		}
		app.int64Builder, app.int64Source = b, sa
		app.appendFn = appendPreparedInt64
	case *array.Float32Builder:
		sa, ok := a.(*array.Float32)
		if !ok {
			return app, invalid()
		}
		app.float32Builder, app.float32Source = b, sa
		app.appendFn = appendPreparedFloat32
	case *array.Float64Builder:
		if a == nil {
			app.float64Builder = b
			app.appendFn = appendPreparedFloat64
			return app, nil
		}
		sa, ok := a.(*array.Float64)
		if !ok {
			return app, invalid()
		}
		app.float64Builder, app.float64Source = b, sa
		app.appendFn = appendPreparedFloat64
	case *array.StringBuilder:
		sa, ok := a.(*array.String)
		if !ok {
			return app, invalid()
		}
		app.stringBuilder, app.stringSource = b, sa
		app.appendFn = appendPreparedString
	case *array.BinaryBuilder:
		sa, ok := a.(*array.Binary)
		if !ok {
			return app, invalid()
		}
		app.binaryBuilder, app.binarySource = b, sa
		app.appendFn = appendPreparedBinary
	case *array.FixedSizeBinaryBuilder:
		sa, ok := a.(*array.FixedSizeBinary)
		if !ok {
			return app, invalid()
		}
		app.fixedBuilder, app.fixedSource = b, sa
		app.appendFn = appendPreparedFixedBinary
	case *array.ListBuilder:
		sa, ok := a.(*array.List)
		if !ok {
			return app, invalid()
		}
		app.listBuilder, app.listSource = b, sa
		switch valueBuilder := b.ValueBuilder().(type) {
		case *array.FixedSizeBinaryBuilder:
			values, ok := sa.ListValues().(*array.FixedSizeBinary)
			if !ok {
				return app, merr.WrapErrServiceInternalMsg("unsupported list source value type %T", sa.ListValues())
			}
			app.listValues, app.listValueBuilder = values, valueBuilder
			app.appendFn = appendPreparedListFixedBinary
		default:
			return app, merr.WrapErrServiceInternalMsg("unsupported list value builder type %T", b.ValueBuilder())
		}
	default:
		return app, merr.WrapErrServiceInternalMsg("unsupported builder type: %T", builder)
	}
	return app, nil
}

func (a *preparedValueAppender) appendAt(i int) (uint64, error) {
	if a.appendFn == nil {
		return 0, merr.WrapErrServiceInternalMsg("prepared appender has no dispatch function")
	}
	return a.appendFn(a, i)
}

func appendPreparedBool(a *preparedValueAppender, i int) (uint64, error) {
	if a.boolSource.IsNull(i) {
		if a.defaultValue != nil {
			a.boolBuilder.Append(a.defaultValue.GetBoolData())
			return 1, nil
		}
		a.boolBuilder.AppendNull()
		return 0, nil
	}
	a.boolBuilder.Append(a.boolSource.Value(i))
	return 1, nil
}

func appendPreparedInt8(a *preparedValueAppender, i int) (uint64, error) {
	if a.int8Source.IsNull(i) {
		if a.defaultValue != nil {
			a.int8Builder.Append(int8(a.defaultValue.GetIntData()))
			return 1, nil
		}
		a.int8Builder.AppendNull()
		return 0, nil
	}
	a.int8Builder.Append(a.int8Source.Value(i))
	return 1, nil
}

func appendPreparedInt16(a *preparedValueAppender, i int) (uint64, error) {
	if a.int16Source.IsNull(i) {
		if a.defaultValue != nil {
			a.int16Builder.Append(int16(a.defaultValue.GetIntData()))
			return 2, nil
		}
		a.int16Builder.AppendNull()
		return 0, nil
	}
	a.int16Builder.Append(a.int16Source.Value(i))
	return 2, nil
}

func appendPreparedInt32(a *preparedValueAppender, i int) (uint64, error) {
	if a.int32Source.IsNull(i) {
		if a.defaultValue != nil {
			a.int32Builder.Append(a.defaultValue.GetIntData())
			return 4, nil
		}
		a.int32Builder.AppendNull()
		return 0, nil
	}
	a.int32Builder.Append(a.int32Source.Value(i))
	return 4, nil
}

func appendPreparedInt64(a *preparedValueAppender, i int) (uint64, error) {
	if a.int64Source.IsNull(i) {
		if a.defaultValue != nil {
			a.int64Builder.Append(a.defaultValue.GetLongData())
			return 8, nil
		}
		a.int64Builder.AppendNull()
		return 0, nil
	}
	a.int64Builder.Append(a.int64Source.Value(i))
	return 8, nil
}

func appendPreparedFloat32(a *preparedValueAppender, i int) (uint64, error) {
	if a.float32Source.IsNull(i) {
		if a.defaultValue != nil {
			a.float32Builder.Append(a.defaultValue.GetFloatData())
			return 4, nil
		}
		a.float32Builder.AppendNull()
		return 0, nil
	}
	a.float32Builder.Append(a.float32Source.Value(i))
	return 4, nil
}

func appendPreparedFloat64(a *preparedValueAppender, i int) (uint64, error) {
	if a.float64Source == nil {
		if a.defaultValue != nil {
			a.float64Builder.Append(a.defaultValue.GetDoubleData())
			return 8, nil
		}
		a.float64Builder.AppendNull()
		return 0, nil
	}
	if a.float64Source.IsNull(i) {
		a.float64Builder.AppendNull()
		return 0, nil
	}
	a.float64Builder.Append(a.float64Source.Value(i))
	return 8, nil
}

func appendPreparedString(a *preparedValueAppender, i int) (uint64, error) {
	if a.stringSource.IsNull(i) {
		if a.defaultValue != nil {
			v := a.defaultValue.GetStringData()
			a.stringBuilder.Append(v)
			return uint64(len(v)), nil
		}
		a.stringBuilder.AppendNull()
		return 0, nil
	}
	v := a.stringSource.Value(i)
	a.stringBuilder.Append(v)
	return uint64(len(v)), nil
}

func appendPreparedBinary(a *preparedValueAppender, i int) (uint64, error) {
	if a.binarySource.IsNull(i) {
		if a.defaultValue != nil {
			v := a.defaultValue.GetBytesData()
			if len(a.geometryWKB) > 0 {
				v = a.geometryWKB
			}
			a.binaryBuilder.Append(v)
			return uint64(len(v)), nil
		}
		a.binaryBuilder.AppendNull()
		return 0, nil
	}
	v := a.binarySource.Value(i)
	a.binaryBuilder.Append(v)
	return uint64(len(v)), nil
}

func appendPreparedFixedBinary(a *preparedValueAppender, i int) (uint64, error) {
	if a.fixedSource.IsNull(i) {
		a.fixedBuilder.AppendNull()
		return 0, nil
	}
	v := a.fixedSource.Value(i)
	a.fixedBuilder.Append(v)
	return uint64(len(v)), nil
}

func appendPreparedListFixedBinary(a *preparedValueAppender, i int) (uint64, error) {
	if a.listSource.IsNull(i) {
		a.listBuilder.AppendNull()
		return 0, nil
	}
	start, end := a.listSource.ValueOffsets(i)
	a.listBuilder.Append(true)
	var size uint64
	for j := start; j < end; j++ {
		v := a.listValues.Value(int(j))
		a.listValueBuilder.Append(v)
		size += uint64(len(v))
	}
	return size, nil
}

// prepareRecord binds every destination builder to the corresponding source
// column once for the lifetime of rec. Callers invoke appendPreparedRow for
// each emitted row until the reader advances.
func (b *RecordBuilder) prepareRecord(rec Record, prepared *preparedRecordAppender) error {
	if err := b.prepareAppendDefaults(); err != nil {
		return err
	}
	if len(prepared.fields) != len(b.builders) {
		prepared.fields = make([]preparedValueAppender, len(b.builders))
	}
	for i, builder := range b.builders {
		col := rec.Column(b.fields[i].FieldID)
		appender, err := prepareValueAppender(builder, col, b.defaults[i])
		if err != nil {
			return merr.Wrapf(err, "failed to append value for field %s", b.fields[i].GetName())
		}
		prepared.fields[i] = appender
	}
	return nil
}

// appendPreparedRow appends one row through already-bound field appenders.
func (b *RecordBuilder) appendPreparedRow(prepared *preparedRecordAppender, idx int) error {
	for i := range prepared.fields {
		size, err := prepared.fields[i].appendAt(idx)
		if err != nil {
			return merr.Wrapf(err, "failed to append value at offset %d for field %s", idx, b.fields[i].GetName())
		}
		b.size += size
	}
	b.nRows++
	return nil
}

// reservePrepared reserves only a bounded amount of near-term row capacity.
// Reserving the whole input segment defeats the reader-count memory bound and
// is particularly harmful for wide vector schemas.
func (b *RecordBuilder) reservePrepared(rows int) {
	if rows <= 0 {
		return
	}
	if rows > 4096 {
		rows = 4096
	}
	for _, builder := range b.builders {
		reserveBuilderRows(builder, rows)
	}
}

func reserveBuilderRows(builder array.Builder, rows int) {
	switch b := builder.(type) {
	case *array.ListBuilder:
		b.Reserve(rows)
		b.ValueBuilder().Reserve(rows)
	default:
		builder.Reserve(rows)
	}
}

// canDirectForwardRecord reports whether rec is already in the exact Arrow
// shape and logical value state the builder would produce. It is deliberately
// strict: an unknown or wrapped record falls back to prepared rebuilding.
func (b *RecordBuilder) canDirectForwardRecord(rec Record) bool {
	sar, ok := rec.(*simpleArrowRecord)
	if !ok {
		return false
	}
	if err := b.prepareAppendDefaults(); err != nil {
		return false
	}
	// Whole-record forwarding hands the source schema directly to V2/V3 writers.
	// Require exact field names, nullability, nested metadata, endianness, and
	// schema metadata; physical type equality alone is not writer compatibility.
	if !sar.r.Schema().Equal(b.arrowSchema) ||
		!sar.r.Schema().Metadata().Equal(b.arrowSchema.Metadata()) {
		return false
	}
	for i, field := range b.fields {
		colIdx, exists := sar.field2Col[field.GetFieldID()]
		// A writer-compatible simple Arrow record must use the exact writer field
		// order. This also proves every source column is used once, without a
		// temporary seen-columns allocation on each run eligibility check.
		if !exists || colIdx != i || colIdx >= int(sar.r.NumCols()) ||
			sar.r.Column(colIdx).DataType().Fingerprint() != b.arrowFields[i].Type.Fingerprint() {
			return false
		}
		col := sar.r.Column(colIdx)
		if !b.arrowFields[i].Nullable && col.NullN() > 0 {
			return false
		}
		// RecordBuilder replaces nulls with schema defaults. Forwarding a column
		// that contains nulls would bypass that logical rewrite even though its
		// physical Arrow type matches.
		if b.defaults[i].value != nil && col.NullN() > 0 {
			return false
		}
	}
	return true
}

// recordBuilderValueSize returns the same logical payload byte count that
// appendPreparedRow adds to RecordBuilder.size. Arrow validity and offset
// buffers are intentionally excluded: batch flush/segment rotation must be
// identical whether rows are rebuilt or forwarded zero-copy. The bool is false
// for an unsupported physical layout so direct forwarding can fail closed.
func recordBuilderValueSize(a arrow.Array, start, end int) (uint64, bool) {
	validCount := func() int {
		if a.NullN() == 0 {
			return end - start
		}
		count := 0
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				count++
			}
		}
		return count
	}
	switch values := a.(type) {
	case *array.Boolean, *array.Int8:
		return uint64(validCount()), true
	case *array.Int16:
		return uint64(validCount() * 2), true
	case *array.Int32, *array.Float32:
		return uint64(validCount() * 4), true
	case *array.Int64, *array.Float64:
		return uint64(validCount() * 8), true
	case *array.String:
		if values.NullN() == 0 {
			offsets := values.ValueOffsets()
			return uint64(offsets[end] - offsets[start]), true
		}
		var size uint64
		for i := start; i < end; i++ {
			if values.IsValid(i) {
				size += uint64(len(values.Value(i)))
			}
		}
		return size, true
	case *array.Binary:
		if values.NullN() == 0 {
			offsets := values.ValueOffsets()
			return uint64(offsets[end] - offsets[start]), true
		}
		var size uint64
		for i := start; i < end; i++ {
			if values.IsValid(i) {
				size += uint64(len(values.Value(i)))
			}
		}
		return size, true
	case *array.FixedSizeBinary:
		return uint64(validCount() * values.DataType().(*arrow.FixedSizeBinaryType).ByteWidth), true
	case *array.List:
		listValues, ok := values.ListValues().(*array.FixedSizeBinary)
		if !ok {
			return 0, false
		}
		var size uint64
		for i := start; i < end; i++ {
			if values.IsNull(i) {
				continue
			}
			valueStart, valueEnd := values.ValueOffsets(i)
			size += uint64(valueEnd-valueStart) * uint64(listValues.DataType().(*arrow.FixedSizeBinaryType).ByteWidth)
		}
		return size, true
	default:
		return 0, false
	}
}

// directForwardRecord builds a writer-schema Arrow record over zero-copy source
// slices. compatible must be the result of canDirectForwardRecord for this same
// synchronously held record; false and all invalid bounds fail closed.
func (b *RecordBuilder) directForwardRecord(rec Record, start, end int, maxSize uint64, compatible bool) (*simpleArrowRecord, uint64, int, bool) {
	if end-start < directForwardMinRows {
		return nil, 0, start, false
	}
	sar, ok := rec.(*simpleArrowRecord)
	if !ok || !compatible || start < 0 || end > sar.Len() || start >= end {
		return nil, 0, start, false
	}
	forwardEnd := end
	var size uint64
	for _, field := range b.fields {
		colIdx := sar.field2Col[field.GetFieldID()]
		columnSize, ok := recordBuilderValueSize(sar.r.Column(colIdx), start, end)
		if !ok {
			return nil, 0, start, false
		}
		size += columnSize
	}
	if size > maxSize {
		size = 0
		for row := start; row < end; row++ {
			for _, field := range b.fields {
				colIdx := sar.field2Col[field.GetFieldID()]
				columnSize, ok := recordBuilderValueSize(sar.r.Column(colIdx), row, row+1)
				if !ok {
					return nil, 0, start, false
				}
				size += columnSize
			}
			// RecordBuilder flushes after the first row that reaches the limit.
			// End the forwarded slice at the same row, even when that row makes the
			// logical batch slightly larger than the target.
			if size >= maxSize {
				forwardEnd = row + 1
				break
			}
		}
	}
	if forwardEnd-start < directForwardMinRows {
		return nil, 0, start, false
	}
	if start == 0 && forwardEnd == sar.Len() {
		sar.Retain()
		return sar, size, forwardEnd, true
	}
	arrays := make([]arrow.Array, len(b.fields))
	for i, field := range b.fields {
		colIdx := sar.field2Col[field.GetFieldID()]
		col := sar.r.Column(colIdx)
		arrays[i] = array.NewSlice(col, int64(start), int64(forwardEnd))
	}
	recSchema := arrow.NewSchema(b.arrowFields, nil)
	out := array.NewRecord(recSchema, arrays, int64(forwardEnd-start))
	for _, col := range arrays {
		col.Release()
	}
	return NewSimpleArrowRecord(out, sar.field2Col), size, forwardEnd, true
}

func (b *RecordBuilder) prepareAppendDefaults() error {
	if b.defaults != nil {
		return nil
	}
	defaults := make([]appendValueDefault, len(b.fields))
	for i, field := range b.fields {
		appendDefault, err := newAppendValueDefault(field)
		if err != nil {
			return err
		}
		defaults[i] = appendDefault
	}
	b.defaults = defaults
	return nil
}

func (b *RecordBuilder) Append(rec Record, start, end int) error {
	if err := b.prepareAppendDefaults(); err != nil {
		return err
	}
	for offset := start; offset < end; offset++ {
		for i, builder := range b.builders {
			f := b.fields[i]
			col := rec.Column(f.FieldID)
			size, err := appendValueAt(builder, col, offset, f, b.defaults[i])
			if err != nil {
				return merr.Wrapf(err, "failed to append value at offset %d for field %s", offset, f.GetName())
			}
			b.size += size
		}
	}
	b.nRows += (end - start)
	return nil
}

func (b *RecordBuilder) GetRowNum() int {
	return b.nRows
}

func (b *RecordBuilder) GetSize() uint64 {
	return b.size
}

func (b *RecordBuilder) Release() {
	for _, builder := range b.builders {
		builder.Release()
	}
}

func (b *RecordBuilder) Build() Record {
	arrays := make([]arrow.Array, len(b.builders))
	fields := make([]arrow.Field, len(b.builders))
	field2Col := make(map[FieldID]int, len(b.builders))
	for c, builder := range b.builders {
		arrays[c] = builder.NewArray()
		f := b.fields[c]
		fid := f.FieldID
		fields[c] = b.arrowFields[c]
		fields[c].Type = arrays[c].DataType()
		field2Col[fid] = c
	}

	rec := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(b.nRows)), field2Col)
	// NewRecord retained every column; drop the builder-side creator refs so the
	// record is the sole owner and columns can actually reach refcount zero.
	for _, arr := range arrays {
		arr.Release()
	}
	b.nRows = 0
	b.size = 0
	return rec
}

func NewRecordBuilder(schema *schemapb.CollectionSchema) *RecordBuilder {
	// assumes 5 sub fields per StructArrayField
	fields := make([]*schemapb.FieldSchema, 0, len(schema.Fields)+len(schema.StructArrayFields)*5)
	fields = append(fields, schema.Fields...)
	for _, sf := range schema.StructArrayFields {
		fields = append(fields, sf.Fields...)
	}

	builders := make([]array.Builder, len(fields))
	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		dim, _ := typeutil.GetDim(field)

		elementType := schemapb.DataType_None
		if field.DataType == schemapb.DataType_ArrayOfVector {
			elementType = field.GetElementType()
		}
		if field.GetNullable() && isNullableDenseVectorArrowType(field.DataType) {
			builders[i] = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		} else if field.DataType == schemapb.DataType_Text {
			// TEXT fields are stored as binary (LOB references) in manifest storage,
			// so the builder must use binary type to match what the reader returns.
			builders[i] = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		} else {
			arrowType := serdeMap[field.DataType].arrowType(int(dim), elementType)
			builders[i] = array.NewBuilder(memory.DefaultAllocator, arrowType)
		}
		arrowFields[i] = newRecordBuilderArrowField(field, builders[i].Type(), dim, elementType)
	}

	return &RecordBuilder{
		fields:      fields,
		arrowFields: arrowFields,
		arrowSchema: arrow.NewSchema(arrowFields, nil),
		builders:    builders,
	}
}

func newRecordBuilderArrowField(field *schemapb.FieldSchema, arrowType arrow.DataType, dim int64, elementType schemapb.DataType) arrow.Field {
	keys := []string{packed.ArrowFieldIdMetadataKey}
	values := []string{strconv.Itoa(int(field.GetFieldID()))}

	if field.GetNullable() && isNullableDenseVectorArrowType(field.GetDataType()) {
		keys = append(keys, "dim")
		values = append(values, strconv.Itoa(int(dim)))
	}

	if field.GetDataType() == schemapb.DataType_ArrayOfVector {
		keys = append(keys, "elementType", "dim")
		values = append(values, strconv.Itoa(int(elementType)), strconv.Itoa(int(dim)))
	}

	return arrow.Field{
		Name:     field.GetName(),
		Type:     arrowType,
		Nullable: field.GetNullable(),
		Metadata: arrow.NewMetadata(keys, values),
	}
}
