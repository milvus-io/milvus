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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

const (
	// MilvusTag struct tag const for milvus row based struct
	MilvusTag = `milvus`

	// MilvusSkipTagValue struct tag const for skip this field.
	MilvusSkipTagValue = `-`

	// MilvusTagSep struct tag const for attribute separator
	MilvusTagSep = `;`

	// MilvusTagName struct tag const for field name
	MilvusTagName = `NAME`

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

func AnyToColumns(rows []interface{}, schemas ...*entity.Schema) ([]column.Column, error) {
	rowsLen := len(rows)
	if rowsLen == 0 {
		return []column.Column{}, errors.New("0 length column")
	}

	var sch *entity.Schema
	var err error
	// if schema not provided, try to parse from row
	if len(schemas) == 0 {
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
	for _, field := range sch.Fields {
		// skip auto id pk field
		if field.PrimaryKey && field.AutoID {
			continue
		}
		switch field.DataType {
		case entity.FieldTypeBool:
			data := make([]bool, 0, rowsLen)
			col := column.NewColumnBool(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeInt8:
			data := make([]int8, 0, rowsLen)
			col := column.NewColumnInt8(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeInt16:
			data := make([]int16, 0, rowsLen)
			col := column.NewColumnInt16(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeInt32:
			data := make([]int32, 0, rowsLen)
			col := column.NewColumnInt32(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeInt64:
			data := make([]int64, 0, rowsLen)
			col := column.NewColumnInt64(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeFloat:
			data := make([]float32, 0, rowsLen)
			col := column.NewColumnFloat(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeDouble:
			data := make([]float64, 0, rowsLen)
			col := column.NewColumnDouble(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeString, entity.FieldTypeVarChar:
			data := make([]string, 0, rowsLen)
			col := column.NewColumnVarChar(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeJSON:
			data := make([][]byte, 0, rowsLen)
			col := column.NewColumnJSONBytes(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeArray:
			col := NewArrayColumn(field)
			if col == nil {
				return nil, errors.Newf("unsupported element type %s for Array", field.ElementType.String())
			}
			nameColumns[field.Name] = col
		case entity.FieldTypeFloatVector:
			data := make([][]float32, 0, rowsLen)
			dimStr, has := field.TypeParams[entity.TypeParamDim]
			if !has {
				return []column.Column{}, errors.New("vector field with no dim")
			}
			dim, err := strconv.ParseInt(dimStr, 10, 64)
			if err != nil {
				return []column.Column{}, fmt.Errorf("vector field with bad format dim: %s", err.Error())
			}
			col := column.NewColumnFloatVector(field.Name, int(dim), data)
			nameColumns[field.Name] = col
		case entity.FieldTypeBinaryVector:
			data := make([][]byte, 0, rowsLen)
			dim, err := field.GetDim()
			if err != nil {
				return []column.Column{}, err
			}
			col := column.NewColumnBinaryVector(field.Name, int(dim), data)
			nameColumns[field.Name] = col
		case entity.FieldTypeFloat16Vector:
			data := make([][]byte, 0, rowsLen)
			dim, err := field.GetDim()
			if err != nil {
				return []column.Column{}, err
			}
			col := column.NewColumnFloat16Vector(field.Name, int(dim), data)
			nameColumns[field.Name] = col
		case entity.FieldTypeBFloat16Vector:
			data := make([][]byte, 0, rowsLen)
			dim, err := field.GetDim()
			if err != nil {
				return []column.Column{}, err
			}
			col := column.NewColumnBFloat16Vector(field.Name, int(dim), data)
			nameColumns[field.Name] = col
		case entity.FieldTypeSparseVector:
			data := make([]entity.SparseEmbedding, 0, rowsLen)
			col := column.NewColumnSparseVectors(field.Name, data)
			nameColumns[field.Name] = col
		case entity.FieldTypeInt8Vector:
			data := make([][]int8, 0, rowsLen)
			dim, err := field.GetDim()
			if err != nil {
				return []column.Column{}, err
			}
			col := column.NewColumnInt8Vector(field.Name, int(dim), data)
			nameColumns[field.Name] = col
		}
	}

	if isDynamic {
		dynamicCol = column.NewColumnJSONBytes("", make([][]byte, 0, rowsLen)).WithIsDynamic(true)
	}

	for _, row := range rows {
		// collection schema name need not to be same, since receiver could has other names
		v := reflect.ValueOf(row)
		set, err := reflectValueCandi(v)
		if err != nil {
			return nil, err
		}

		for idx, field := range sch.Fields {
			// skip dynamic field if visible
			if isDynamic && field.IsDynamic {
				continue
			}
			// skip auto id pk field
			if field.PrimaryKey && field.AutoID {
				// remove pk field from candidates set, avoid adding it into dynamic column
				delete(set, field.Name)
				continue
			}
			column, ok := nameColumns[field.Name]
			if !ok {
				return nil, fmt.Errorf("expected unhandled field %s", field.Name)
			}

			candi, ok := set[field.Name]
			if !ok {
				return nil, fmt.Errorf("row %d does not has field %s", idx, field.Name)
			}
			err := column.AppendValue(candi.v.Interface())
			if err != nil {
				return nil, err
			}
			delete(set, field.Name)
		}

		if isDynamic {
			m := make(map[string]interface{})
			for name, candi := range set {
				m[name] = candi.v.Interface()
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

func SetField(receiver any, fieldName string, value any) error {
	candidates, err := reflectValueCandi(reflect.ValueOf(receiver))
	if err != nil {
		return err
	}

	candidate, ok := candidates[fieldName]
	// if field not found, just return
	if !ok {
		return nil
	}

	if candidate.v.CanSet() {
		candidate.v.Set(reflect.ValueOf(value))
	}

	return nil
}

type fieldCandi struct {
	name    string
	v       reflect.Value
	options map[string]string
}

func reflectValueCandi(v reflect.Value) (map[string]fieldCandi, error) {
	// unref **/***/... struct{}
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map: // map[string]any
		return getMapReflectCandidates(v), nil
	case reflect.Struct:
		return getStructReflectCandidates(v)
	default:
		return nil, fmt.Errorf("unsupport row type: %s", v.Kind().String())
	}
}

// getMapReflectCandidates converts input map into fieldCandidate struct.
// if value is struct/map etc, it will be treated as json data type directly(if schema say so).
func getMapReflectCandidates(v reflect.Value) map[string]fieldCandi {
	result := make(map[string]fieldCandi)
	iter := v.MapRange()
	for iter.Next() {
		key := iter.Key().String()
		result[key] = fieldCandi{
			name: key,
			v:    iter.Value(),
		}
	}
	return result
}

// getStructReflectCandidates parses struct fields into fieldCandidates.
// embedded struct will be flatten as field as well.
func getStructReflectCandidates(v reflect.Value) (map[string]fieldCandi, error) {
	result := make(map[string]fieldCandi)
	for i := 0; i < v.NumField(); i++ {
		ft := v.Type().Field(i)
		name := ft.Name

		// embedded struct, flatten all fields
		if ft.Anonymous && ft.Type.Kind() == reflect.Struct {
			embedCandidate, err := reflectValueCandi(v.Field(i))
			if err != nil {
				return nil, err
			}
			for key, candi := range embedCandidate {
				// check duplicated field name in different structs
				_, ok := result[key]
				if ok {
					return nil, fmt.Errorf("column has duplicated name: %s when parsing field: %s", key, ft.Name)
				}
				result[key] = candi
			}
			continue
		}

		tag, ok := ft.Tag.Lookup(MilvusTag)
		settings := make(map[string]string)
		if ok {
			if tag == MilvusSkipTagValue {
				continue
			}
			settings = ParseTagSetting(tag, MilvusTagSep)
			fn, has := settings[MilvusTagName]
			if has {
				// overwrite column to tag name
				name = fn
			}
		}
		_, ok = result[name]
		// duplicated
		if ok {
			return nil, fmt.Errorf("column has duplicated name: %s when parsing field: %s", name, ft.Name)
		}

		v := v.Field(i)
		if v.Kind() == reflect.Array {
			v = v.Slice(0, v.Len())
		}

		result[name] = fieldCandi{
			name:    name,
			v:       v,
			options: settings,
		}
	}

	return result, nil
}
