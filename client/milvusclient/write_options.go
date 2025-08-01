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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/row"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	columns       []column.Column
	partialUpdate bool
}

func (opt *columnBasedDataOption) WriteBackPKs(_ *entity.Schema, _ column.Column) error {
	// column based data option need not write back pk
	return nil
}

func (opt *columnBasedDataOption) processInsertColumns(colSchema *entity.Schema, columns ...column.Column) ([]*schemapb.FieldData, int, error) {
	// setup dynamic related var
	isDynamic := colSchema.EnableDynamicField

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
			return nil, 0, fmt.Errorf("param column %s has type %v but collection field definition is %v", col.Name(), col.Type(), field.DataType)
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

func (opt *columnBasedDataOption) WithPartition(partitionName string) *columnBasedDataOption {
	opt.partitionName = partitionName
	return opt
}

func (opt *columnBasedDataOption) WithPartialUpdate(partialUpdate bool) *columnBasedDataOption {
	opt.partialUpdate = partialUpdate
	return opt
}

func (opt *columnBasedDataOption) CollectionName() string {
	return opt.collName
}

func (opt *columnBasedDataOption) InsertRequest(coll *entity.Collection) (*milvuspb.InsertRequest, error) {
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	return &milvuspb.InsertRequest{
		CollectionName:  opt.collName,
		PartitionName:   opt.partitionName,
		FieldsData:      fieldsData,
		NumRows:         uint32(rowNum),
		SchemaTimestamp: coll.UpdateTimestamp,
	}, nil
}

func (opt *columnBasedDataOption) UpsertRequest(coll *entity.Collection) (*milvuspb.UpsertRequest, error) {
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	return &milvuspb.UpsertRequest{
		CollectionName:  opt.collName,
		PartitionName:   opt.partitionName,
		FieldsData:      fieldsData,
		NumRows:         uint32(rowNum),
		SchemaTimestamp: coll.UpdateTimestamp,
		PartialUpdate:   opt.partialUpdate,
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
	rows []any
}

func NewRowBasedInsertOption(collName string, rows ...any) *rowBasedDataOption {
	return &rowBasedDataOption{
		columnBasedDataOption: &columnBasedDataOption{
			collName: collName,
		},
		rows: rows,
	}
}

func (opt *rowBasedDataOption) InsertRequest(coll *entity.Collection) (*milvuspb.InsertRequest, error) {
	columns, err := row.AnyToColumns(opt.rows, coll.Schema)
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
		FieldsData:     fieldsData,
		NumRows:        uint32(rowNum),
	}, nil
}

func (opt *rowBasedDataOption) UpsertRequest(coll *entity.Collection) (*milvuspb.UpsertRequest, error) {
	columns, err := row.AnyToColumns(opt.rows, coll.Schema)
	if err != nil {
		return nil, err
	}
	opt.columnBasedDataOption.columns = columns
	fieldsData, rowNum, err := opt.processInsertColumns(coll.Schema, opt.columns...)
	if err != nil {
		return nil, err
	}
	return &milvuspb.UpsertRequest{
		CollectionName: opt.collName,
		PartitionName:  opt.partitionName,
		FieldsData:     fieldsData,
		NumRows:        uint32(rowNum),
		PartialUpdate:  opt.partialUpdate,
	}, nil
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

type DeleteOption interface {
	Request() *milvuspb.DeleteRequest
}

type deleteOption struct {
	collectionName string
	partitionName  string
	expr           string
}

func (opt *deleteOption) Request() *milvuspb.DeleteRequest {
	return &milvuspb.DeleteRequest{
		CollectionName: opt.collectionName,
		PartitionName:  opt.partitionName,
		Expr:           opt.expr,
	}
}

func (opt *deleteOption) WithExpr(expr string) *deleteOption {
	opt.expr = expr
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

func NewDeleteOption(collectionName string) *deleteOption {
	return &deleteOption{collectionName: collectionName}
}
