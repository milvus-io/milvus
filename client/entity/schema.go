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
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

const (
	// TypeParamDim is the const for field type param dimension
	TypeParamDim = "dim"

	// TypeParamMaxLength is the const for varchar type maximal length
	TypeParamMaxLength = "max_length"

	// TypeParamMaxCapacity is the const for array type max capacity
	TypeParamMaxCapacity = `max_capacity`

	// ClStrong strong consistency level
	ClStrong ConsistencyLevel = ConsistencyLevel(commonpb.ConsistencyLevel_Strong)
	// ClBounded bounded consistency level with default tolerance of 5 seconds
	ClBounded ConsistencyLevel = ConsistencyLevel(commonpb.ConsistencyLevel_Bounded)
	// ClSession session consistency level
	ClSession ConsistencyLevel = ConsistencyLevel(commonpb.ConsistencyLevel_Session)
	// ClEvenually eventually consistency level
	ClEventually ConsistencyLevel = ConsistencyLevel(commonpb.ConsistencyLevel_Eventually)
	// ClCustomized customized consistency level and users pass their own `guarantee_timestamp`.
	ClCustomized ConsistencyLevel = ConsistencyLevel(commonpb.ConsistencyLevel_Customized)
)

// ConsistencyLevel enum type for collection Consistency Level
type ConsistencyLevel commonpb.ConsistencyLevel

// CommonConsistencyLevel returns corresponding commonpb.ConsistencyLevel
func (cl ConsistencyLevel) CommonConsistencyLevel() commonpb.ConsistencyLevel {
	return commonpb.ConsistencyLevel(cl)
}

// Schema represents schema info of collection in milvus
type Schema struct {
	CollectionName     string
	Description        string
	AutoID             bool
	Fields             []*Field
	EnableDynamicField bool

	pkField *Field
}

// NewSchema creates an empty schema object.
func NewSchema() *Schema {
	return &Schema{}
}

// WithName sets the name value of schema, returns schema itself.
func (s *Schema) WithName(name string) *Schema {
	s.CollectionName = name
	return s
}

// WithDescription sets the description value of schema, returns schema itself.
func (s *Schema) WithDescription(desc string) *Schema {
	s.Description = desc
	return s
}

func (s *Schema) WithAutoID(autoID bool) *Schema {
	s.AutoID = autoID
	return s
}

func (s *Schema) WithDynamicFieldEnabled(dynamicEnabled bool) *Schema {
	s.EnableDynamicField = dynamicEnabled
	return s
}

// WithField adds a field into schema and returns schema itself.
func (s *Schema) WithField(f *Field) *Schema {
	if f.PrimaryKey {
		s.pkField = f
	}
	s.Fields = append(s.Fields, f)
	return s
}

// ProtoMessage returns corresponding server.CollectionSchema
func (s *Schema) ProtoMessage() *schemapb.CollectionSchema {
	r := &schemapb.CollectionSchema{
		Name:               s.CollectionName,
		Description:        s.Description,
		AutoID:             s.AutoID,
		EnableDynamicField: s.EnableDynamicField,
	}
	r.Fields = make([]*schemapb.FieldSchema, 0, len(s.Fields))
	for _, field := range s.Fields {
		r.Fields = append(r.Fields, field.ProtoMessage())
	}
	return r
}

// ReadProto parses proto Collection Schema
func (s *Schema) ReadProto(p *schemapb.CollectionSchema) *Schema {
	s.Description = p.GetDescription()
	s.CollectionName = p.GetName()
	s.Fields = make([]*Field, 0, len(p.GetFields()))
	for _, fp := range p.GetFields() {
		field := NewField().ReadProto(fp)
		if fp.GetAutoID() {
			s.AutoID = true
		}
		if field.PrimaryKey {
			s.pkField = field
		}
		s.Fields = append(s.Fields, field)
	}
	s.EnableDynamicField = p.GetEnableDynamicField()
	return s
}

// PKFieldName returns pk field name for this schemapb.
func (s *Schema) PKFieldName() string {
	if s.pkField == nil {
		return ""
	}
	return s.pkField.Name
}

// PKField returns PK Field schema for this schema.
func (s *Schema) PKField() *Field {
	return s.pkField
}

// Field represent field schema in milvus
type Field struct {
	ID             int64  // field id, generated when collection is created, input value is ignored
	Name           string // field name
	PrimaryKey     bool   // is primary key
	AutoID         bool   // is auto id
	Description    string
	DataType       FieldType
	TypeParams     map[string]string
	IndexParams    map[string]string
	IsDynamic      bool
	IsPartitionKey bool
	ElementType    FieldType
}

// ProtoMessage generates corresponding FieldSchema
func (f *Field) ProtoMessage() *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:        f.ID,
		Name:           f.Name,
		Description:    f.Description,
		IsPrimaryKey:   f.PrimaryKey,
		AutoID:         f.AutoID,
		DataType:       schemapb.DataType(f.DataType),
		TypeParams:     MapKvPairs(f.TypeParams),
		IndexParams:    MapKvPairs(f.IndexParams),
		IsDynamic:      f.IsDynamic,
		IsPartitionKey: f.IsPartitionKey,
		ElementType:    schemapb.DataType(f.ElementType),
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

/*
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

func (f *Field) WithDefaultValueString(defaultValue string) *Field {
	f.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_StringData{
			StringData: defaultValue,
		},
	}
	return f
}*/

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
	f.ElementType = FieldType(p.GetElementType())

	return f
}

// MapKvPairs converts map into commonpb.KeyValuePair slice
func MapKvPairs(m map[string]string) []*commonpb.KeyValuePair {
	pairs := make([]*commonpb.KeyValuePair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		})
	}
	return pairs
}

// KvPairsMap converts commonpb.KeyValuePair slices into map
func KvPairsMap(kvps []*commonpb.KeyValuePair) map[string]string {
	m := make(map[string]string)
	for _, kvp := range kvps {
		m[kvp.Key] = kvp.Value
	}
	return m
}
