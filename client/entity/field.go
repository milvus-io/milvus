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

package entity

import (
	"encoding/json"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// FieldType field data type alias type
// used in go:generate trick, DO NOT modify names & string
type FieldType int32

// Name returns field type name
func (t FieldType) Name() string {
	switch t {
	case FieldTypeBool:
		return "Bool"
	case FieldTypeInt8:
		return "Int8"
	case FieldTypeInt16:
		return "Int16"
	case FieldTypeInt32:
		return "Int32"
	case FieldTypeInt64:
		return "Int64"
	case FieldTypeFloat:
		return "Float"
	case FieldTypeDouble:
		return "Double"
	case FieldTypeTimestamptz:
		return "Timestamptz"
	case FieldTypeString:
		return "String"
	case FieldTypeVarChar:
		return "VarChar"
	case FieldTypeArray:
		return "Array"
	case FieldTypeJSON:
		return "JSON"
	case FieldTypeBinaryVector:
		return "BinaryVector"
	case FieldTypeFloatVector:
		return "FloatVector"
	case FieldTypeFloat16Vector:
		return "Float16Vector"
	case FieldTypeBFloat16Vector:
		return "BFloat16Vector"
	case FieldTypeInt8Vector:
		return "Int8Vector"
	default:
		return "undefined"
	}
}

// String returns field type
func (t FieldType) String() string {
	switch t {
	case FieldTypeBool:
		return "bool"
	case FieldTypeInt8:
		return "int8"
	case FieldTypeInt16:
		return "int16"
	case FieldTypeInt32:
		return "int32"
	case FieldTypeInt64:
		return "int64"
	case FieldTypeFloat:
		return "float32"
	case FieldTypeDouble:
		return "float64"
	case FieldTypeTimestamptz:
		return "timestamptz"
	case FieldTypeString:
		return "string"
	case FieldTypeVarChar:
		return "string"
	case FieldTypeArray:
		return "Array"
	case FieldTypeJSON:
		return "JSON"
	case FieldTypeBinaryVector:
		return "[]byte"
	case FieldTypeFloatVector:
		return "[]float32"
	case FieldTypeFloat16Vector:
		return "[]byte"
	case FieldTypeBFloat16Vector:
		return "[]byte"
	case FieldTypeInt8Vector:
		return "[]int8"
	default:
		return "undefined"
	}
}

// PbFieldType represents FieldType corresponding schema pb type
func (t FieldType) PbFieldType() (string, string) {
	switch t {
	case FieldTypeBool:
		return "Bool", "bool"
	case FieldTypeInt8:
		fallthrough
	case FieldTypeInt16:
		fallthrough
	case FieldTypeInt32:
		return "Int", "int32"
	case FieldTypeInt64:
		return "Long", "int64"
	case FieldTypeFloat:
		return "Float", "float32"
	case FieldTypeDouble:
		return "Double", "float64"
	case FieldTypeTimestamptz:
		return "Timestamptz", "int64" // Timestamptz
	case FieldTypeString:
		return "String", "string"
	case FieldTypeVarChar:
		return "VarChar", "string"
	case FieldTypeJSON:
		return "JSON", "JSON"
	case FieldTypeBinaryVector:
		return "[]byte", ""
	case FieldTypeFloatVector:
		return "[]float32", ""
	case FieldTypeFloat16Vector:
		return "[]byte", ""
	case FieldTypeBFloat16Vector:
		return "[]byte", ""
	case FieldTypeInt8Vector:
		return "[]int8", ""
	default:
		return "undefined", ""
	}
}

// Match schema definition
const (
	// FieldTypeNone zero value place holder
	FieldTypeNone FieldType = 0 // zero value place holder
	// FieldTypeBool field type boolean
	FieldTypeBool FieldType = 1
	// FieldTypeInt8 field type int8
	FieldTypeInt8 FieldType = 2
	// FieldTypeInt16 field type int16
	FieldTypeInt16 FieldType = 3
	// FieldTypeInt32 field type int32
	FieldTypeInt32 FieldType = 4
	// FieldTypeInt64 field type int64
	FieldTypeInt64 FieldType = 5
	// FieldTypeFloat field type float
	FieldTypeFloat FieldType = 10
	// FieldTypeDouble field type double
	FieldTypeDouble FieldType = 11
	// FieldTypeTimestamptz field type timestamptz
	FieldTypeTimestamptz FieldType = 15
	// FieldTypeString field type string
	FieldTypeString FieldType = 20
	// FieldTypeVarChar field type varchar
	FieldTypeVarChar FieldType = 21 // variable-length strings with a specified maximum length
	// FieldTypeArray field type Array
	FieldTypeArray FieldType = 22
	// FieldTypeJSON field type JSON
	FieldTypeJSON FieldType = 23
	// FieldTypeBinaryVector field type binary vector
	FieldTypeBinaryVector FieldType = 100
	// FieldTypeFloatVector field type float vector
	FieldTypeFloatVector FieldType = 101
	// FieldTypeBinaryVector field type float16 vector
	FieldTypeFloat16Vector FieldType = 102
	// FieldTypeBinaryVector field type bf16 vector
	FieldTypeBFloat16Vector FieldType = 103
	// FieldTypeBinaryVector field type sparse vector
	FieldTypeSparseVector FieldType = 104
	// FieldTypeInt8Vector field type int8 vector
	FieldTypeInt8Vector FieldType = 105
)

// Field represent field schema in milvus
type Field struct {
	ID              int64  // field id, generated when collection is created, input value is ignored
	Name            string // field name
	PrimaryKey      bool   // is primary key
	AutoID          bool   // is auto id
	Description     string
	DataType        FieldType
	TypeParams      map[string]string
	IndexParams     map[string]string
	IsDynamic       bool
	IsPartitionKey  bool
	IsClusteringKey bool
	ElementType     FieldType
	DefaultValue    *schemapb.ValueField
	Nullable        bool
}

// ProtoMessage generates corresponding FieldSchema
func (f *Field) ProtoMessage() *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:         f.ID,
		Name:            f.Name,
		Description:     f.Description,
		IsPrimaryKey:    f.PrimaryKey,
		AutoID:          f.AutoID,
		DataType:        schemapb.DataType(f.DataType),
		TypeParams:      MapKvPairs(f.TypeParams),
		IndexParams:     MapKvPairs(f.IndexParams),
		IsDynamic:       f.IsDynamic,
		IsPartitionKey:  f.IsPartitionKey,
		IsClusteringKey: f.IsClusteringKey,
		ElementType:     schemapb.DataType(f.ElementType),
		Nullable:        f.Nullable,
		DefaultValue:    f.DefaultValue,
	}
}

// NewField creates a new Field with map initialized.
func NewField() *Field {
	return &Field{
		TypeParams:  make(map[string]string),
		IndexParams: make(map[string]string),
	}
}

func (f *Field) WithName(name string) *Field {
	f.Name = name
	return f
}

func (f *Field) WithDescription(desc string) *Field {
	f.Description = desc
	return f
}

func (f *Field) WithDataType(dataType FieldType) *Field {
	f.DataType = dataType
	return f
}

func (f *Field) WithIsPrimaryKey(isPrimaryKey bool) *Field {
	f.PrimaryKey = isPrimaryKey
	return f
}

func (f *Field) WithIsAutoID(isAutoID bool) *Field {
	f.AutoID = isAutoID
	return f
}

func (f *Field) WithIsDynamic(isDynamic bool) *Field {
	f.IsDynamic = isDynamic
	return f
}

func (f *Field) WithIsPartitionKey(isPartitionKey bool) *Field {
	f.IsPartitionKey = isPartitionKey
	return f
}

func (f *Field) WithIsClusteringKey(isClusteringKey bool) *Field {
	f.IsClusteringKey = isClusteringKey
	return f
}

func (f *Field) WithNullable(nullable bool) *Field {
	f.Nullable = nullable
	return f
}

func (f *Field) WithDefaultValueBool(defaultValue bool) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_BoolData{
			BoolData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithDefaultValueInt(defaultValue int32) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_IntData{
			IntData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithDefaultValueLong(defaultValue int64) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_LongData{
			LongData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithDefaultValueFloat(defaultValue float32) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_FloatData{
			FloatData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithDefaultValueDouble(defaultValue float64) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_DoubleData{
			DoubleData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithDefaultValueTimestamptz(defaultValue int64) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_TimestamptzData{
			TimestamptzData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithDefaultValueString(defaultValue string) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_StringData{
			StringData: defaultValue,
		},
	}
	return f
}

func (f *Field) WithTypeParams(key string, value string) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams[key] = value
	return f
}

func (f *Field) WithDim(dim int64) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams[TypeParamDim] = strconv.FormatInt(dim, 10)
	return f
}

func (f *Field) GetDim() (int64, error) {
	dimStr, has := f.TypeParams[TypeParamDim]
	if !has {
		return -1, errors.New("field with no dim")
	}
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return -1, errors.Newf("field with bad format dim: %s", err.Error())
	}
	return dim, nil
}

func (f *Field) WithMaxLength(maxLen int64) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams[TypeParamMaxLength] = strconv.FormatInt(maxLen, 10)
	return f
}

func (f *Field) WithElementType(eleType FieldType) *Field {
	f.ElementType = eleType
	return f
}

func (f *Field) WithMaxCapacity(maxCap int64) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams[TypeParamMaxCapacity] = strconv.FormatInt(maxCap, 10)
	return f
}

func (f *Field) WithEnableAnalyzer(enable bool) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams["enable_analyzer"] = strconv.FormatBool(enable)
	return f
}

func (f *Field) WithAnalyzerParams(params map[string]any) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	bs, _ := json.Marshal(params)
	f.TypeParams["analyzer_params"] = string(bs)
	return f
}

func (f *Field) WithMultiAnalyzerParams(params map[string]any) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	bs, _ := json.Marshal(params)
	f.TypeParams["multi_analyzer_params"] = string(bs)
	return f
}

func (f *Field) WithEnableMatch(enable bool) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams[TypeParamEnableMatch] = strconv.FormatBool(enable)
	return f
}

// ReadProto parses FieldSchema
func (f *Field) ReadProto(p *schemapb.FieldSchema) *Field {
	f.ID = p.GetFieldID()
	f.Name = p.GetName()
	f.PrimaryKey = p.GetIsPrimaryKey()
	f.AutoID = p.GetAutoID()
	f.Description = p.GetDescription()
	f.DataType = FieldType(p.GetDataType())
	f.TypeParams = KvPairsMap(p.GetTypeParams())
	f.IndexParams = KvPairsMap(p.GetIndexParams())
	f.IsDynamic = p.GetIsDynamic()
	f.IsPartitionKey = p.GetIsPartitionKey()
	f.IsClusteringKey = p.GetIsClusteringKey()
	f.ElementType = FieldType(p.GetElementType())
	f.DefaultValue = p.GetDefaultValue()
	f.Nullable = p.GetNullable()

	return f
}
