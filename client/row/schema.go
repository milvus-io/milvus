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
	"fmt"
	"go/ast"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/client/v2/entity"
)

// ParseSchema parses schema from interface{}.
func ParseSchema(r interface{}) (*entity.Schema, error) {
	sch := &entity.Schema{}
	t := reflect.TypeOf(r)
	if t.Kind() == reflect.Array || t.Kind() == reflect.Slice || t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// MapRow is not supported for schema definition
	// TODO add PrimaryKey() interface later
	if t.Kind() == reflect.Map {
		return nil, errors.New("map row is not supported for schema definition")
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported data type: %+v", r)
	}

	// Collection method not overwrited, try use Row type name
	if sch.CollectionName == "" {
		sch.CollectionName = t.Name()
		if sch.CollectionName == "" {
			return nil, errors.New("collection name not provided")
		}
	}
	sch.Fields = make([]*entity.Field, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		// ignore anonymous field for now
		if f.Anonymous || !ast.IsExported(f.Name) {
			continue
		}

		field := &entity.Field{
			Name: f.Name,
		}
		ft := f.Type
		if f.Type.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		fv := reflect.New(ft)
		tag := f.Tag.Get(MilvusTag)
		if tag == MilvusSkipTagValue {
			continue
		}
		tagSettings := ParseTagSetting(tag, MilvusTagSep)
		if _, has := tagSettings[MilvusPrimaryKey]; has {
			field.PrimaryKey = true
		}
		if _, has := tagSettings[MilvusAutoID]; has {
			field.AutoID = true
		}
		if name, has := tagSettings[MilvusTagName]; has {
			field.Name = name
		}
		switch reflect.Indirect(fv).Kind() {
		case reflect.Bool:
			field.DataType = entity.FieldTypeBool
		case reflect.Int8:
			field.DataType = entity.FieldTypeInt8
		case reflect.Int16:
			field.DataType = entity.FieldTypeInt16
		case reflect.Int32:
			field.DataType = entity.FieldTypeInt32
		case reflect.Int64:
			field.DataType = entity.FieldTypeInt64
		case reflect.Float32:
			field.DataType = entity.FieldTypeFloat
		case reflect.Float64:
			field.DataType = entity.FieldTypeDouble
		case reflect.String:
			field.DataType = entity.FieldTypeVarChar
			if maxLengthVal, has := tagSettings[MilvusMaxLength]; has {
				maxLength, err := strconv.ParseInt(maxLengthVal, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("max length value %s is not valued", maxLengthVal)
				}
				field.WithMaxLength(maxLength)
			}
		case reflect.Array:
			arrayLen := ft.Len()
			elemType := ft.Elem()
			switch elemType.Kind() {
			case reflect.Uint8:
				field.WithDataType(entity.FieldTypeBinaryVector)
				field.WithDim(int64(arrayLen) * 8)
			case reflect.Float32:
				field.WithDataType(entity.FieldTypeFloatVector)
				field.WithDim(int64(arrayLen))
			default:
				return nil, fmt.Errorf("field %s is array of %v, which is not supported", f.Name, elemType)
			}
		case reflect.Slice:
			dimStr, has := tagSettings[VectorDimTag]
			if !has {
				return nil, fmt.Errorf("field %s is slice but dim not provided", f.Name)
			}
			dim, err := strconv.ParseInt(dimStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("dim value %s is not valid", dimStr)
			}
			if dim < 1 || dim > DimMax {
				return nil, fmt.Errorf("dim value %d is out of range", dim)
			}
			field.WithDim(dim)

			elemType := ft.Elem()
			switch elemType.Kind() {
			case reflect.Uint8: // []byte, could be BinaryVector, fp16, bf 6
				switch tagSettings[VectorTypeTag] {
				case "fp16":
					field.DataType = entity.FieldTypeFloat16Vector
				case "bf16":
					field.DataType = entity.FieldTypeBFloat16Vector
				default:
					field.DataType = entity.FieldTypeBinaryVector
				}
			case reflect.Float32:
				field.DataType = entity.FieldTypeFloatVector
			case reflect.Int8:
				field.DataType = entity.FieldTypeInt8Vector
			default:
				return nil, fmt.Errorf("field %s is slice of %v, which is not supported", f.Name, elemType)
			}
		default:
			return nil, fmt.Errorf("field %s is %v, which is not supported", field.Name, ft)
		}
		sch.Fields = append(sch.Fields, field)
	}

	return sch, nil
}

// ParseTagSetting parses struct tag into map settings
func ParseTagSetting(str string, sep string) map[string]string {
	settings := map[string]string{}
	names := strings.Split(str, sep)

	for i := 0; i < len(names); i++ {
		j := i
		if len(names[j]) > 0 {
			for {
				if names[j][len(names[j])-1] == '\\' {
					i++
					names[j] = names[j][0:len(names[j])-1] + sep + names[i]
					names[i] = ""
				} else {
					break
				}
			}
		}

		values := strings.Split(names[j], ":")
		k := strings.TrimSpace(strings.ToUpper(values[0]))

		if len(values) >= 2 {
			settings[k] = strings.Join(values[1:], ":")
		} else if k != "" {
			settings[k] = k
		}
	}

	return settings
}
