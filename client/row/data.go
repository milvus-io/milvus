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

package row

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/rowutil"
)

const (
	// MilvusTag struct tag const for milvus row based struct
	MilvusTag = rowutil.MilvusTag

	// MilvusSkipTagValue struct tag const for skip this field.
	MilvusSkipTagValue = rowutil.MilvusSkipTagValue

	// MilvusTagSep struct tag const for attribute separator
	MilvusTagSep = rowutil.MilvusTagSep

	// MilvusTagName struct tag const for field name
	MilvusTagName = rowutil.MilvusTagName

	// VectorDimTag struct tag const for vector dimension
	VectorDimTag = `DIM`

	// VectorTypeTag struct tag const for binary vector type
	VectorTypeTag = `VECTOR_TYPE`

	// MilvusPrimaryKey struct tag const for primary key indicator
	MilvusPrimaryKey = `PRIMARY_KEY`

	// MilvusAutoID struct tag const for auto id indicator
	MilvusAutoID = `AUTO_ID`

	// MilvusMaxLength struct tag const for max length
	MilvusMaxLength = `MAX_LENGTH`

	// DimMax dimension max value
	DimMax = 65535
)

// AnyToColumns converts input rows into column-based data.
// when schemas are provided, this method will use 0-th element
// otherwise, it shall try to parse schema from row[0]
func AnyToColumns(rows []interface{}, keepPkField bool, schemas ...*entity.Schema) ([]column.Column, error) {
	rowsLen := len(rows)
	if rowsLen == 0 {
		return []column.Column{}, errors.New("0 length column")
	}

	var sch *entity.Schema
	var err error
	// if schema not provided, try to parse from row
	if len(schemas) == 0 {
		//nolint rows number checked before
		sch, err = ParseSchema(rows[0])
		if err != nil {
			return []column.Column{}, err
		}
	} else {
		// use first schema provided
		sch = schemas[0]
	}

	isDynamic := sch.EnableDynamicField
	var dynamicCol *column.ColumnJSONBytes

	nameColumns := make(map[string]column.Column)
	nameSchemas := lo.SliceToMap(sch.Fields, func(fieldSchema *entity.Field) (string, entity.Field) {
		return fieldSchema.Name, *fieldSchema
	})
	columnCreators := getColumnCreators(sch)

	if isDynamic {
		dynamicCol = column.NewColumnJSONBytes("", make([][]byte, 0, rowsLen)).WithIsDynamic(true)
	}

	// getColumn is a closure to wrap fetch column related to field name
	getColumn := func(fieldName string) (column.Column, error) {
		// existing one
		column, ok := nameColumns[fieldName]
		if ok {
			return column, nil
		}

		fn, ok := columnCreators[fieldName]
		if ok {
			return fn(rowsLen)
		}

		return nil, errors.New("column not found")
	}

	for _, row := range rows {
		// collection schema name need not to be same, since receiver could has other names
		v := reflect.ValueOf(row)
		set, err := rowutil.ParseFields(v)
		if err != nil {
			return nil, err
		}

		for fieldName, candi := range set {
			fieldSch, ok := nameSchemas[fieldName]
			if ok && fieldSch.PrimaryKey && fieldSch.AutoID && !keepPkField {
				// remove pk field from candidates set, avoid adding it into dynamic column
				delete(set, fieldName)
				continue
			}

			column, err := getColumn(fieldName)
			if err != nil {
				// ignore candidate not exist in schema for now
				// if dynamic schema enabled, left candidates will be processed
				// TODO @congqixia, add strict mode if needed
				continue
			}
			nameColumns[fieldName] = column

			if candi.IsPtr {
				if candi.Value.IsNil() {
					err = column.AppendNull()
				} else {
					err = column.AppendValue(candi.Value.Elem().Interface())
				}
			} else {
				err = column.AppendValue(candi.Value.Interface())
			}
			if err != nil {
				return nil, err
			}
			delete(set, fieldName)
		}

		if isDynamic {
			m := make(map[string]interface{})
			for name, candi := range set {
				if candi.IsPtr {
					if candi.Value.IsNil() {
						m[name] = nil
					} else {
						m[name] = candi.Value.Elem().Interface()
					}
				} else {
					m[name] = candi.Value.Interface()
				}
			}
			bs, err := json.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal dynamic field %w", err)
			}
			err = dynamicCol.AppendValue(bs)
			if err != nil {
				return nil, fmt.Errorf("failed to append value to dynamic field %w", err)
			}
		}
	}
	columns := make([]column.Column, 0, len(nameColumns))
	for _, column := range nameColumns {
		columns = append(columns, column)
	}
	if isDynamic {
		columns = append(columns, dynamicCol)
	}
	return columns, nil
}

type columnCreator func(int) (column.Column, error)

func getColumnCreators(sch *entity.Schema) map[string]columnCreator {
	result := make(map[string]columnCreator)
	for _, field := range sch.Fields {
		// skip auto id pk field
		// if field.PrimaryKey && field.AutoID {
		// continue
		// }
		field := field
		result[field.Name] = func(rowsLen int) (column.Column, error) {
			var col column.Column
			switch field.DataType {
			case entity.FieldTypeBool:
				data := make([]bool, 0, rowsLen)
				col = column.NewColumnBool(field.Name, data)
			case entity.FieldTypeInt8:
				data := make([]int8, 0, rowsLen)
				col = column.NewColumnInt8(field.Name, data)
			case entity.FieldTypeInt16:
				data := make([]int16, 0, rowsLen)
				col = column.NewColumnInt16(field.Name, data)
			case entity.FieldTypeInt32:
				data := make([]int32, 0, rowsLen)
				col = column.NewColumnInt32(field.Name, data)
			case entity.FieldTypeInt64:
				data := make([]int64, 0, rowsLen)
				col = column.NewColumnInt64(field.Name, data)
			case entity.FieldTypeFloat:
				data := make([]float32, 0, rowsLen)
				col = column.NewColumnFloat(field.Name, data)
			case entity.FieldTypeDouble:
				data := make([]float64, 0, rowsLen)
				col = column.NewColumnDouble(field.Name, data)
			case entity.FieldTypeString, entity.FieldTypeVarChar:
				data := make([]string, 0, rowsLen)
				col = column.NewColumnVarChar(field.Name, data)
			case entity.FieldTypeText:
				data := make([]string, 0, rowsLen)
				col = column.NewColumnText(field.Name, data)
			case entity.FieldTypeTimestamptz:
				col = column.NewColumnTimestamptz(field.Name, nil)
			case entity.FieldTypeJSON:
				data := make([][]byte, 0, rowsLen)
				col = column.NewColumnJSONBytes(field.Name, data)
			case entity.FieldTypeGeometry:
				data := make([]string, 0, rowsLen)
				col = column.NewColumnGeometryWKT(field.Name, data)
			case entity.FieldTypeArray:
				if field.ElementType == entity.FieldTypeStruct {
					structColumn, err := column.NewColumnStructArrayFromSchema(field.Name, field.StructSchema)
					if err != nil {
						return nil, err
					}
					col = structColumn
				} else {
					col = NewArrayColumn(field)
					if col == nil {
						return nil, errors.Newf("unsupported element type %s for Array", field.ElementType.String())
					}
				}
			case entity.FieldTypeFloatVector:
				data := make([][]float32, 0, rowsLen)
				dimStr, has := field.TypeParams[entity.TypeParamDim]
				if !has {
					return nil, errors.New("vector field with no dim")
				}
				dim, err := strconv.ParseInt(dimStr, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("vector field with bad format dim: %s", err.Error())
				}
				col = column.NewColumnFloatVector(field.Name, int(dim), data)
			case entity.FieldTypeBinaryVector:
				data := make([][]byte, 0, rowsLen)
				dim, err := field.GetDim()
				if err != nil {
					return nil, err
				}
				col = column.NewColumnBinaryVector(field.Name, int(dim), data)
			case entity.FieldTypeFloat16Vector:
				data := make([][]byte, 0, rowsLen)
				dim, err := field.GetDim()
				if err != nil {
					return nil, err
				}
				col = column.NewColumnFloat16Vector(field.Name, int(dim), data)
			case entity.FieldTypeBFloat16Vector:
				data := make([][]byte, 0, rowsLen)
				dim, err := field.GetDim()
				if err != nil {
					return nil, err
				}
				col = column.NewColumnBFloat16Vector(field.Name, int(dim), data)
			case entity.FieldTypeSparseVector:
				data := make([]entity.SparseEmbedding, 0, rowsLen)
				col = column.NewColumnSparseVectors(field.Name, data)
			case entity.FieldTypeInt8Vector:
				data := make([][]int8, 0, rowsLen)
				dim, err := field.GetDim()
				if err != nil {
					return nil, err
				}
				col = column.NewColumnInt8Vector(field.Name, int(dim), data)
			}

			if field.Nullable {
				col.SetNullable(true)
			}
			return col, nil
		}
	}
	return result
}

func NewArrayColumn(f *entity.Field) column.Column {
	switch f.ElementType {
	case entity.FieldTypeBool:
		return column.NewColumnBoolArray(f.Name, nil)

	case entity.FieldTypeInt8:
		return column.NewColumnInt8Array(f.Name, nil)

	case entity.FieldTypeInt16:
		return column.NewColumnInt16Array(f.Name, nil)

	case entity.FieldTypeInt32:
		return column.NewColumnInt32Array(f.Name, nil)

	case entity.FieldTypeInt64:
		return column.NewColumnInt64Array(f.Name, nil)

	case entity.FieldTypeFloat:
		return column.NewColumnFloatArray(f.Name, nil)

	case entity.FieldTypeDouble:
		return column.NewColumnDoubleArray(f.Name, nil)

	case entity.FieldTypeVarChar:
		return column.NewColumnVarCharArray(f.Name, nil)

	default:
		return nil
	}
}

var timeType = reflect.TypeOf(time.Time{})

// CoerceValue converts value into a value assignable to targetType when
// unmarshalling read-back data into typed struct fields.
//
// TIMESTAMPTZ columns are transported as RFC3339Nano ISO strings, so string
// values are parsed into time.Time (or *time.Time) targets; string targets and
// any other type pair keep the value unchanged.
func CoerceValue(targetType reflect.Type, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if targetType == timeType {
		if s, ok := value.(string); ok {
			t, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return nil, fmt.Errorf("failed to parse timestamptz string %q: %w", s, err)
			}
			return t, nil
		}
	}
	return value, nil
}

func SetField(receiver any, fieldName string, value any) error {
	candidates, err := rowutil.ParseFields(reflect.ValueOf(receiver))
	if err != nil {
		return err
	}

	candidate, ok := candidates[fieldName]
	// if field not found, just return
	if !ok {
		return nil
	}

	if candidate.Value.CanSet() {
		if candidate.IsPtr {
			if value == nil {
				candidate.Value.Set(reflect.Zero(candidate.Value.Type()))
			} else {
				converted, err := CoerceValue(candidate.Value.Type().Elem(), value)
				if err != nil {
					return err
				}
				ptr := reflect.New(candidate.Value.Type().Elem())
				ptr.Elem().Set(reflect.ValueOf(converted))
				candidate.Value.Set(ptr)
			}
		} else {
			converted, err := CoerceValue(candidate.Value.Type(), value)
			if err != nil {
				return err
			}
			candidate.Value.Set(reflect.ValueOf(converted))
		}
	}

	return nil
}
