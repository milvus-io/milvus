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

package milvusclient

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/typeutil"
	"github.com/milvus-io/milvus/client/v3/row"
)

type InsertOption interface {
	InsertRequest(coll *entity.Collection) (*milvuspb.InsertRequest, error)
	CollectionName() string
	WriteBackPKs(schema *entity.Schema, pks column.Column) error
}

type UpsertOption interface {
	UpsertRequest(coll *entity.Collection) (*milvuspb.UpsertRequest, error)
	CollectionName() string
}

var (
	_ UpsertOption = (*columnBasedDataOption)(nil)
	_ InsertOption = (*columnBasedDataOption)(nil)
)

type columnBasedDataOption struct {
	collName      string
	partitionName string
	namespace     *string
	columns       []column.Column
	partialUpdate bool

	// deferredErr captures construction-time errors from builder helpers (e.g. WithStructArrayColumn)
	// so they surface on InsertRequest/UpsertRequest rather than panicking in the chain.
	deferredErr error

	// partialOps carries per-field FieldPartialUpdateOp directives. Keyed by
	// field name. Entries with REPLACE (or nil) are treated as no-ops and are
	// not serialized onto the wire.
	partialOps map[string]*schemapb.FieldPartialUpdateOp
}

func (opt *columnBasedDataOption) WriteBackPKs(_ *entity.Schema, _ column.Column) error {
	// column based data option need not write back pk
	return nil
}

func (opt *columnBasedDataOption) processInsertColumns(colSchema *entity.Schema, columns ...column.Column) ([]*schemapb.FieldData, int, error) {
	// setup dynamic related var
	isDynamic := colSchema.EnableDynamicField

	inputDynamicColumn := lo.FindOrElse(columns, nil, func(col column.Column) bool {
		return col.FieldData().GetIsDynamic()
	})

	// check columns and field matches
	var rowSize int
	mNameField := make(map[string]*entity.Field)
	for _, field := range colSchema.Fields {
		mNameField[field.Name] = field
	}
	mNameColumn := make(map[string]column.Column)
	var dynamicColumns []column.Column
	for _, col := range columns {
		_, dup := mNameColumn[col.Name()]
		if dup {
			return nil, 0, fmt.Errorf("duplicated column %s found", col.Name())
		}
		l := col.Len()
		if rowSize == 0 {
			rowSize = l
		} else if rowSize != l {
			return nil, 0, errors.New("column size not match")
		}
		field, has := mNameField[col.Name()]
		if !has {
			if !isDynamic {
				return nil, 0, fmt.Errorf("field %s does not exist in collection %s", col.Name(), colSchema.CollectionName)
			}
			if inputDynamicColumn != nil {
				if col == inputDynamicColumn {
					continue
				}
				return nil, 0, errors.New("cannot pass pre-composed dynamic json column with other dynamic columns")
			}
			// add to dynamic column list for further processing
			dynamicColumns = append(dynamicColumns, col)
			continue
		}
		// make non-nullable created column fit nullable field definition
		if field.Nullable {
			col.SetNullable(true)
		}

		mNameColumn[col.Name()] = col
		if col.Type() != field.DataType {
			return nil, 0, fmt.Errorf("param column %s has type %s but collection field definition is %s", col.Name(), col.Type().Name(), field.DataType.Name())
		}
		if field.DataType == entity.FieldTypeFloatVector || field.DataType == entity.FieldTypeBinaryVector ||
			field.DataType == entity.FieldTypeFloat16Vector || field.DataType == entity.FieldTypeBFloat16Vector ||
			field.DataType == entity.FieldTypeInt8Vector {
			dim := 0
			switch column := col.(type) {
			case *column.ColumnFloatVector:
				dim = column.Dim()
			case *column.ColumnBinaryVector:
				dim = column.Dim()
			case *column.ColumnFloat16Vector:
				dim = column.Dim()
			case *column.ColumnBFloat16Vector:
				dim = column.Dim()
			case *column.ColumnInt8Vector:
				dim = column.Dim()
			}
			if fmt.Sprintf("%d", dim) != field.TypeParams[entity.TypeParamDim] {
				return nil, 0, fmt.Errorf("params column %s vector dim %d not match collection definition, which has dim of %s", field.Name, dim, field.TypeParams[entity.TypeParamDim])
			}
		}
	}

	// missing field shall be checked in server side
	// // check all fixed field pass value
	// for _, field := range colSchema.Fields {
	// 	_, has := mNameColumn[field.Name]
	// 	if !has &&
	// 		!field.AutoID && !field.IsDynamic {
	// 		return nil, 0, fmt.Errorf("field %s not passed", field.Name)
	// 	}
	// }

	fieldsData := make([]*schemapb.FieldData, 0, len(mNameColumn)+1)
	for _, fixedColumn := range mNameColumn {
		// make sure the field data in compact mode
		fixedColumn.CompactNullableValues()
		fieldsData = append(fieldsData, fixedColumn.FieldData())
	}
	if inputDynamicColumn != nil {
		fieldsData = append(fieldsData, inputDynamicColumn.FieldData())
	}
	if len(dynamicColumns) > 0 {
		// use empty column name here
		col, err := opt.mergeDynamicColumns("", rowSize, dynamicColumns)
		if err != nil {
			return nil, 0, err
		}
		fieldsData = append(fieldsData, col)
	}

	return fieldsData, rowSize, nil
}

func (opt *columnBasedDataOption) mergeDynamicColumns(dynamicName string, rowSize int, columns []column.Column) (*schemapb.FieldData, error) {
	values := make([][]byte, 0, rowSize)
	for i := 0; i < rowSize; i++ {
		m := make(map[string]interface{})
		for _, column := range columns {
			// range guaranteed
			m[column.Name()], _ = column.Get(i)
		}
		bs, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		values = append(values, bs)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: dynamicName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: values,
					},
				},
			},
		},
		IsDynamic: true,
	}, nil
}

func (opt *columnBasedDataOption) WithColumns(columns ...column.Column) *columnBasedDataOption {
	opt.columns = append(opt.columns, columns...)
	return opt
}

func (opt *columnBasedDataOption) WithBoolColumn(colName string, data []bool) *columnBasedDataOption {
	column := column.NewColumnBool(colName, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithInt8Column(colName string, data []int8) *columnBasedDataOption {
	column := column.NewColumnInt8(colName, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithInt16Column(colName string, data []int16) *columnBasedDataOption {
	column := column.NewColumnInt16(colName, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithInt32Column(colName string, data []int32) *columnBasedDataOption {
	column := column.NewColumnInt32(colName, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithInt64Column(colName string, data []int64) *columnBasedDataOption {
	column := column.NewColumnInt64(colName, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithVarcharColumn(colName string, data []string) *columnBasedDataOption {
	column := column.NewColumnVarChar(colName, data)
	return opt.WithColumns(column)
}

// WithTextColumn appends a native TEXT column to the write request.
func (opt *columnBasedDataOption) WithTextColumn(colName string, data []string) *columnBasedDataOption {
	column := column.NewColumnText(colName, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithFloatVectorColumn(colName string, dim int, data [][]float32) *columnBasedDataOption {
	column := column.NewColumnFloatVector(colName, dim, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithFloat16VectorColumn(colName string, dim int, data [][]float32) *columnBasedDataOption {
	f16v := make([][]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		f16v = append(f16v, typeutil.Float32ArrayToFloat16Bytes(data[i]))
	}
	column := column.NewColumnFloat16Vector(colName, dim, f16v)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithBFloat16VectorColumn(colName string, dim int, data [][]float32) *columnBasedDataOption {
	bf16v := make([][]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		bf16v = append(bf16v, typeutil.Float32ArrayToBFloat16Bytes(data[i]))
	}
	column := column.NewColumnBFloat16Vector(colName, dim, bf16v)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithBinaryVectorColumn(colName string, dim int, data [][]byte) *columnBasedDataOption {
	column := column.NewColumnBinaryVector(colName, dim, data)
	return opt.WithColumns(column)
}

func (opt *columnBasedDataOption) WithInt8VectorColumn(colName string, dim int, data [][]int8) *columnBasedDataOption {
	column := column.NewColumnInt8Vector(colName, dim, data)
	return opt.WithColumns(column)
}

// WithStructArrayColumn appends a struct-array column built from a row-based representation,
// inferring the per-sub-field array type from the corresponding field in `structSchema`.
//
// `rows` is a per-collection-row list; a nil entry represents a null StructArray row. Each
// non-null entry is a map keyed by sub-field name. The value for a scalar sub-field must be
// `[]<T>` (e.g. []int32, []string); the value for a vector sub-field must be
// `[][]float32` / `[][]byte` / `[][]int8` matching the vector type.
//
// Example:
//
//	structSchema := entity.NewStructSchema().
//	    WithField(entity.NewField().WithName("clip_str").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256)).
//	    WithField(entity.NewField().WithName("clip_emb").WithDataType(entity.FieldTypeFloatVector).WithDim(8))
//	rows := []map[string]any{
//	    {"clip_str": []string{"a", "b"}, "clip_emb": [][]float32{v1, v2}},
//	    {"clip_str": []string{"c"},      "clip_emb": [][]float32{v3}},
//	}
//	opt.WithStructArrayColumn("clips", structSchema, rows)
func (opt *columnBasedDataOption) WithStructArrayColumn(colName string, structSchema *entity.StructSchema, rows []map[string]any) *columnBasedDataOption {
	col, err := buildStructArrayColumn(colName, structSchema, rows)
	if err != nil {
		// Defer error reporting to InsertRequest/UpsertRequest so the builder chain stays valid.
		if opt.deferredErr == nil {
			opt.deferredErr = errors.Wrapf(err, "WithStructArrayColumn(%q)", colName)
		}
		return opt
	}
	return opt.WithColumns(col)
}

func buildStructArrayColumn(colName string, structSchema *entity.StructSchema, rows []map[string]any) (column.Column, error) {
	structCol, err := column.NewColumnStructArrayFromSchema(colName, structSchema)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row == nil {
			structCol.SetNullable(true)
			break
		}
	}
	for i, row := range rows {
		if err := structCol.AppendValue(row); err != nil {
			return nil, errors.Wrapf(err, "row %d", i)
		}
	}
	return structCol, nil
}

func (opt *columnBasedDataOption) WithPartition(partitionName string) *columnBasedDataOption {
	opt.partitionName = partitionName
	return opt
}

// WithNamespace scopes the write to a collection namespace. Primary keys are
// still collection-scoped for delete/upsert tombstones, so callers must keep
// primary keys unique across namespaces in the same collection.
func (opt *columnBasedDataOption) WithNamespace(namespace string) *columnBasedDataOption {
	opt.namespace = &namespace
	return opt
}

func (opt *columnBasedDataOption) WithPartialUpdate(partialUpdate bool) *columnBasedDataOption {
	opt.partialUpdate = partialUpdate
	return opt
}

// WithArrayAppend declares that the Array field `fieldName` should be merged
// with ARRAY_APPEND semantics during an Upsert. The server implicitly enables
// partial_update when any non-REPLACE op is present, so callers do not need
// to also invoke WithPartialUpdate(true).
func (opt *columnBasedDataOption) WithArrayAppend(fieldName string) *columnBasedDataOption {
	return opt.WithFieldPartialOp(fieldName, schemapb.FieldPartialUpdateOp_ARRAY_APPEND)
}

// WithArrayRemove declares that the Array field `fieldName` should be merged
// with ARRAY_REMOVE semantics during an Upsert. See WithArrayAppend for the
// implicit partial_update promotion.
func (opt *columnBasedDataOption) WithArrayRemove(fieldName string) *columnBasedDataOption {
	return opt.WithFieldPartialOp(fieldName, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE)
}

// WithPathReplace replaces one existing value selected by a request-wide
// relative path such as "[1]" or "[1][age]".
func (opt *columnBasedDataOption) WithPathReplace(fieldName, path string) *columnBasedDataOption {
	opt.setFieldPartialUpdateOp(&schemapb.FieldPartialUpdateOp{
		FieldName: fieldName,
		Op:        schemapb.FieldPartialUpdateOp_PATH_REPLACE,
		Path:      path,
	})
	return opt
}

// WithFieldPartialOp attaches an explicit FieldPartialUpdateOp to the field
// with name `fieldName`. Intended for advanced callers; typical users should
// prefer the op-specific helpers (WithArrayAppend, WithArrayRemove).
func (opt *columnBasedDataOption) WithFieldPartialOp(fieldName string, op schemapb.FieldPartialUpdateOp_OpType) *columnBasedDataOption {
	opt.setFieldPartialUpdateOp(&schemapb.FieldPartialUpdateOp{FieldName: fieldName, Op: op})
	return opt
}

func (opt *columnBasedDataOption) setFieldPartialUpdateOp(op *schemapb.FieldPartialUpdateOp) {
	if opt.partialOps == nil {
		opt.partialOps = make(map[string]*schemapb.FieldPartialUpdateOp)
	}
	// Builder calls configure the final request; they are not themselves wire
	// operations. Preserve the existing last-write-wins behavior here and let
	// Proxy reject requests that actually contain duplicate field_ops entries.
	fieldName := op.GetFieldName()
	if op.GetOp() == schemapb.FieldPartialUpdateOp_REPLACE {
		delete(opt.partialOps, fieldName)
		return
	}
	opt.partialOps[fieldName] = op
}

// buildFieldOps materializes the recorded FieldPartialUpdateOp directives
// into a proto-ready slice. Only non-REPLACE ops are emitted — REPLACE is
// the on-wire default and emitting it would waste bytes on every upsert.
//
// The returned slice is independent of the input fieldsData; a field
// referenced by an op that was not in fieldsData is still emitted so the
// server can surface a validation error rather than silently drop the
// op. Client-side filtering would hide user typos.
func (opt *columnBasedDataOption) buildFieldOps() []*schemapb.FieldPartialUpdateOp {
	if len(opt.partialOps) == 0 {
		return nil
	}
	out := make([]*schemapb.FieldPartialUpdateOp, 0, len(opt.partialOps))
	for _, op := range opt.partialOps {
		if op.GetOp() == schemapb.FieldPartialUpdateOp_REPLACE {
			continue
		}
		out = append(out, op)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (opt *columnBasedDataOption) CollectionName() string {
	return opt.collName
}

func (opt *columnBasedDataOption) InsertRequest(coll *entity.Collection) (*milvuspb.InsertRequest, error) {
	if opt.deferredErr != nil {
		return nil, opt.deferredErr
	}
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	return &milvuspb.InsertRequest{
		CollectionName:  opt.collName,
		PartitionName:   opt.partitionName,
		Namespace:       opt.namespace,
		FieldsData:      fieldsData,
		NumRows:         uint32(rowNum),
		SchemaTimestamp: coll.UpdateTimestamp,
	}, nil
}

func (opt *columnBasedDataOption) UpsertRequest(coll *entity.Collection) (*milvuspb.UpsertRequest, error) {
	if opt.deferredErr != nil {
		return nil, opt.deferredErr
	}
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	// Materialize any WithArrayAppend/WithArrayRemove/WithFieldPartialOp
	// directives into UpsertRequest.field_ops. Auto-promote partial_update
	// when any non-REPLACE op is present.
	fieldOps := opt.buildFieldOps()
	partialUpdate := opt.partialUpdate
	if len(fieldOps) > 0 {
		partialUpdate = true
	}
	return &milvuspb.UpsertRequest{
		CollectionName:  opt.collName,
		PartitionName:   opt.partitionName,
		Namespace:       opt.namespace,
		FieldsData:      fieldsData,
		NumRows:         uint32(rowNum),
		SchemaTimestamp: coll.UpdateTimestamp,
		PartialUpdate:   partialUpdate,
		FieldOps:        fieldOps,
	}, nil
}

func NewColumnBasedInsertOption(collName string, columns ...column.Column) *columnBasedDataOption {
	return &columnBasedDataOption{
		columns:  columns,
		collName: collName,
		// leave partition name empty, using default partition
	}
}

type rowBasedDataOption struct {
	*columnBasedDataOption
	rows         []any
	keepAutoIDPk bool // keep user passed auto id pk field
}

func NewRowBasedInsertOption(collName string, rows ...any) *rowBasedDataOption {
	return &rowBasedDataOption{
		columnBasedDataOption: &columnBasedDataOption{
			collName: collName,
		},
		rows:         rows,
		keepAutoIDPk: false,
	}
}

func (opt *rowBasedDataOption) WithPartition(partitionName string) *rowBasedDataOption {
	opt.columnBasedDataOption.WithPartition(partitionName)
	return opt
}

func (opt *rowBasedDataOption) WithNamespace(namespace string) *rowBasedDataOption {
	opt.columnBasedDataOption.WithNamespace(namespace)
	return opt
}

func (opt *rowBasedDataOption) WithPartialUpdate(partialUpdate bool) *rowBasedDataOption {
	opt.columnBasedDataOption.WithPartialUpdate(partialUpdate)
	return opt
}

func (opt *rowBasedDataOption) WithArrayAppend(fieldName string) *rowBasedDataOption {
	opt.columnBasedDataOption.WithArrayAppend(fieldName)
	return opt
}

// WithPathReplace replaces one existing value selected by a request-wide
// relative path such as "[1]" or "[1][age]".
func (opt *rowBasedDataOption) WithPathReplace(fieldName, path string) *rowBasedDataOption {
	opt.columnBasedDataOption.WithPathReplace(fieldName, path)
	return opt
}

func (opt *rowBasedDataOption) WithArrayRemove(fieldName string) *rowBasedDataOption {
	opt.columnBasedDataOption.WithArrayRemove(fieldName)
	return opt
}

func (opt *rowBasedDataOption) WithFieldPartialOp(fieldName string, op schemapb.FieldPartialUpdateOp_OpType) *rowBasedDataOption {
	opt.columnBasedDataOption.WithFieldPartialOp(fieldName, op)
	return opt
}

func (opt *rowBasedDataOption) InsertRequest(coll *entity.Collection) (*milvuspb.InsertRequest, error) {
	columns, err := row.AnyToColumns(opt.rows, opt.keepAutoIDPk, coll.Schema)
	if err != nil {
		return nil, err
	}
	opt.columnBasedDataOption.columns = columns
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	return &milvuspb.InsertRequest{
		CollectionName: opt.collName,
		PartitionName:  opt.partitionName,
		Namespace:      opt.namespace,
		FieldsData:     fieldsData,
		NumRows:        uint32(rowNum),
	}, nil
}

func (opt *rowBasedDataOption) UpsertRequest(coll *entity.Collection) (*milvuspb.UpsertRequest, error) {
	if opt.deferredErr != nil {
		return nil, opt.deferredErr
	}
	conversionSchema, err := opt.pathReplaceRowSchema(coll.Schema)
	if err != nil {
		return nil, err
	}
	columns, err := row.AnyToColumns(opt.rows, opt.keepAutoIDPk, conversionSchema)
	if err != nil {
		return nil, err
	}
	opt.columnBasedDataOption.columns = columns
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	fieldOps := opt.buildFieldOps()
	partialUpdate := opt.partialUpdate
	if len(fieldOps) > 0 {
		partialUpdate = true
	}
	return &milvuspb.UpsertRequest{
		CollectionName: opt.collName,
		PartitionName:  opt.partitionName,
		Namespace:      opt.namespace,
		FieldsData:     fieldsData,
		NumRows:        uint32(rowNum),
		PartialUpdate:  partialUpdate,
		FieldOps:       fieldOps,
	}, nil
}

func (opt *rowBasedDataOption) pathReplaceRowSchema(schema *entity.Schema) (*entity.Schema, error) {
	conversionSchema := schema
	for fieldIndex, field := range schema.Fields {
		op := opt.partialOps[field.Name]
		// Unknown fields and non-Struct Array fields stay server-authoritative.
		if op.GetOp() != schemapb.FieldPartialUpdateOp_PATH_REPLACE ||
			field.DataType != entity.FieldTypeArray || field.ElementType != entity.FieldTypeStruct {
			continue
		}
		mask, err := pathReplaceStructFieldMask(opt.rows, field.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "PATH_REPLACE field %q", field.Name)
		}
		structSchema, err := selectPathReplaceStructSchema(field, mask)
		if err != nil {
			return nil, err
		}
		if conversionSchema == schema {
			cloned := *schema
			cloned.Fields = slices.Clone(schema.Fields)
			conversionSchema = &cloned
		}
		clonedField := *field
		clonedField.StructSchema = structSchema
		conversionSchema.Fields[fieldIndex] = &clonedField
	}
	return conversionSchema, nil
}

func selectPathReplaceStructSchema(field *entity.Field, mask []string) (*entity.StructSchema, error) {
	if field.StructSchema == nil {
		return nil, errors.Newf("struct array field %q has no child schema", field.Name)
	}
	selected := make(map[string]struct{}, len(mask))
	for _, name := range mask {
		selected[name] = struct{}{}
	}
	result := entity.NewStructSchema()
	for _, child := range field.StructSchema.Fields {
		if _, ok := selected[child.Name]; ok {
			result.WithField(child)
			delete(selected, child.Name)
		}
	}
	for name := range selected {
		return nil, errors.Newf("struct array field %q has no child %q", field.Name, name)
	}
	return result, nil
}

func pathReplaceStructFieldMask(rows []interface{}, fieldName string) ([]string, error) {
	var expected []string
	for rowIndex, inputRow := range rows {
		value, found, err := pathReplaceRowField(reflect.ValueOf(inputRow), fieldName)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.Newf("row %d is missing struct array field %q", rowIndex, fieldName)
		}
		for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Ptr) {
			if value.IsNil() {
				return nil, errors.Newf(
					"row %d struct array field %q must not be null for PATH_REPLACE", rowIndex, fieldName)
			}
			value = value.Elem()
		}
		if !value.IsValid() || value.Kind() != reflect.Map || value.Type().Key().Kind() != reflect.String {
			return nil, errors.Newf(
				"row %d struct array field %q must be map[string]any, got %s", rowIndex, fieldName, value.Kind())
		}
		mask := make([]string, 0, value.Len())
		iter := value.MapRange()
		for iter.Next() {
			mask = append(mask, iter.Key().String())
		}
		sort.Strings(mask)
		if len(mask) == 0 {
			return nil, errors.Newf("row %d struct array field %q child mask must not be empty", rowIndex, fieldName)
		}
		if rowIndex == 0 {
			expected = mask
			continue
		}
		if !slices.Equal(expected, mask) {
			return nil, errors.Newf(
				"row %d struct array field %q child mask %v does not match request mask %v",
				rowIndex, fieldName, mask, expected)
		}
	}
	return expected, nil
}

func pathReplaceRowField(value reflect.Value, fieldName string) (reflect.Value, bool, error) {
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			break
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return reflect.Value{}, false, errors.New("unsupported nil row")
	}
	switch value.Kind() {
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, false, errors.Newf("unsupported row map key type: %s", value.Type().Key())
		}
		iter := value.MapRange()
		for iter.Next() {
			if iter.Key().String() == fieldName {
				return iter.Value(), true, nil
			}
		}
		return reflect.Value{}, false, nil
	case reflect.Struct:
		var result reflect.Value
		found := false
		for i := 0; i < value.NumField(); i++ {
			fieldType := value.Type().Field(i)
			if fieldType.Anonymous && fieldType.Type.Kind() == reflect.Struct {
				embedded, embeddedFound, err := pathReplaceRowField(value.Field(i), fieldName)
				if err != nil {
					return reflect.Value{}, false, err
				}
				if embeddedFound {
					if found {
						return reflect.Value{}, false, errors.Newf(
							"column has duplicated name: %s when parsing field: %s", fieldName, fieldType.Name)
					}
					result, found = embedded, true
				}
				continue
			}
			name := fieldType.Name
			if tag, ok := fieldType.Tag.Lookup(row.MilvusTag); ok {
				if tag == row.MilvusSkipTagValue {
					continue
				}
				if taggedName, ok := row.ParseTagSetting(tag, row.MilvusTagSep)[row.MilvusTagName]; ok {
					name = taggedName
				}
			}
			if name != fieldName {
				continue
			}
			if found {
				return reflect.Value{}, false, errors.Newf(
					"column has duplicated name: %s when parsing field: %s", name, fieldType.Name)
			}
			result, found = value.Field(i), true
		}
		return result, found, nil
	default:
		return reflect.Value{}, false, errors.Newf("unsupported row type: %s", value.Kind())
	}
}

func (opt *rowBasedDataOption) WriteBackPKs(sch *entity.Schema, pks column.Column) error {
	pkField := sch.PKField()
	// not auto id, return
	if pkField == nil || !pkField.AutoID {
		return nil
	}
	if len(opt.rows) != pks.Len() {
		return errors.New("input row count is not equal to result pk length")
	}

	for i, r := range opt.rows {
		// index range checked
		v, _ := pks.Get(i)
		err := row.SetField(r, pkField.Name, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (opt *rowBasedDataOption) WithKeepAutoIDPk(keepPk bool) *rowBasedDataOption {
	opt.keepAutoIDPk = keepPk
	return opt
}

type DeleteOption interface {
	Request() (*milvuspb.DeleteRequest, error)
}

type deleteOption struct {
	collectionName string
	partitionName  string
	namespace      *string
	expr           string
	templateParams map[string]any
}

func (opt *deleteOption) Request() (*milvuspb.DeleteRequest, error) {
	req := &milvuspb.DeleteRequest{
		CollectionName: opt.collectionName,
		PartitionName:  opt.partitionName,
		Namespace:      opt.namespace,
		Expr:           opt.expr,
	}
	req.ExprTemplateValues = make(map[string]*schemapb.TemplateValue, len(opt.templateParams))
	for key, value := range opt.templateParams {
		tmplVal, err := any2TmplValue(value)
		if err != nil {
			return req, errors.Wrapf(err, "invalid delete expression template parameter %q", key)
		}
		req.ExprTemplateValues[key] = tmplVal
	}
	return req, nil
}

func (opt *deleteOption) WithExpr(expr string) *deleteOption {
	opt.expr = expr
	return opt
}

// WithTemplateParam binds an expression-template value for delete. Slice and
// blob values are not copied; do not mutate them until Client.Delete returns.
func (opt *deleteOption) WithTemplateParam(key string, val any) *deleteOption {
	if opt.templateParams == nil {
		opt.templateParams = make(map[string]any)
	}
	opt.templateParams[key] = val
	return opt
}

func (opt *deleteOption) WithInt64IDs(fieldName string, ids []int64) *deleteOption {
	opt.expr = fmt.Sprintf("%s in %s", fieldName, strings.Join(strings.Fields(fmt.Sprint(ids)), ","))
	return opt
}

func (opt *deleteOption) WithStringIDs(fieldName string, ids []string) *deleteOption {
	opt.expr = fmt.Sprintf("%s in [%s]", fieldName, strings.Join(lo.Map(ids, func(id string, _ int) string { return fmt.Sprintf("\"%s\"", id) }), ","))
	return opt
}

func (opt *deleteOption) WithPartition(partitionName string) *deleteOption {
	opt.partitionName = partitionName
	return opt
}

// WithNamespace scopes the delete request to a collection namespace. Delete
// tombstones are primary-key based, so callers must keep primary keys unique
// across namespaces in the same collection.
func (opt *deleteOption) WithNamespace(namespace string) *deleteOption {
	opt.namespace = &namespace
	return opt
}

func NewDeleteOption(collectionName string) *deleteOption {
	return &deleteOption{
		collectionName: collectionName,
		templateParams: make(map[string]any),
	}
}
