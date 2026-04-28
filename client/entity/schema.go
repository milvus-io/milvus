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
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	// TypeParamDim is the const for field type param dimension
	TypeParamDim = "dim"

	// TypeParamMaxLength is the const for varchar type maximal length
	TypeParamMaxLength = "max_length"

	// TypeParamMaxCapacity is the const for array type max capacity
	TypeParamMaxCapacity = `max_capacity`

	// TypeParamEnableMatch is the const for enable text match
	TypeParamEnableMatch = `enable_match`

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
	Functions          []*Function
	ExternalSource     string // External data source (e.g., "s3://bucket/path")
	ExternalSpec       string // External source config (JSON)

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

// WithExternalSource sets the external source for the schema (e.g., "s3://bucket/path").
func (s *Schema) WithExternalSource(externalSource string) *Schema {
	s.ExternalSource = externalSource
	return s
}

// WithExternalSpec sets the external spec configuration (JSON format).
func (s *Schema) WithExternalSpec(externalSpec string) *Schema {
	s.ExternalSpec = externalSpec
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

func (s *Schema) WithFunction(f *Function) *Schema {
	s.Functions = append(s.Functions, f)
	return s
}

// ProtoMessage returns corresponding server.CollectionSchema
func (s *Schema) ProtoMessage() *schemapb.CollectionSchema {
	r := &schemapb.CollectionSchema{
		Name:               s.CollectionName,
		Description:        s.Description,
		AutoID:             s.AutoID,
		EnableDynamicField: s.EnableDynamicField,
		ExternalSource:     s.ExternalSource,
		ExternalSpec:       s.ExternalSpec,
	}
	r.Fields = lo.FilterMap(s.Fields, func(field *Field, _ int) (*schemapb.FieldSchema, bool) {
		if field.DataType == FieldTypeArray && field.ElementType == FieldTypeStruct {
			return nil, false
		}
		return field.ProtoMessage(), true
	})

	r.Functions = lo.Map(s.Functions, func(function *Function, _ int) *schemapb.FunctionSchema {
		return function.ProtoMessage()
	})

	r.StructArrayFields = lo.FilterMap(s.Fields, func(field *Field, _ int) (*schemapb.StructArrayFieldSchema, bool) {
		if field.DataType != FieldTypeArray || field.ElementType != FieldTypeStruct {
			return nil, false
		}
		f := &schemapb.StructArrayFieldSchema{
			Name:        field.Name,
			Description: field.Description,
			TypeParams:  MapKvPairs(field.TypeParams),
		}
		// max_capacity declared on the parent struct field must be carried onto each sub-field's
		// type params — the server validates it per sub-field on insert.
		parentMaxCap, hasParentMaxCap := field.TypeParams[TypeParamMaxCapacity]
		if field.StructSchema != nil {
			f.Fields = lo.Map(field.StructSchema.Fields, func(sub *Field, _ int) *schemapb.FieldSchema {
				// translate to ArrayStruct
				p := sub.ProtoMessage()
				p.ElementType = p.DataType
				if typeutil.IsVectorType(p.DataType) {
					p.DataType = schemapb.DataType_ArrayOfVector
				} else {
					p.DataType = schemapb.DataType_Array
				}
				if hasParentMaxCap {
					if _, ok := sub.TypeParams[TypeParamMaxCapacity]; !ok {
						p.TypeParams = append(p.TypeParams, &commonpb.KeyValuePair{
							Key:   TypeParamMaxCapacity,
							Value: parentMaxCap,
						})
					}
				}
				return p
			})
		}
		return f, true
	})

	return r
}

// ReadProto parses proto Collection Schema
func (s *Schema) ReadProto(p *schemapb.CollectionSchema) *Schema {
	s.Description = p.GetDescription()
	s.CollectionName = p.GetName()
	s.EnableDynamicField = p.GetEnableDynamicField()
	s.ExternalSource = p.GetExternalSource()
	s.ExternalSpec = p.GetExternalSpec()
	// fields
	s.Fields = make([]*Field, 0, len(p.GetFields())+len(p.GetStructArrayFields()))
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
	// struct array fields
	for _, sp := range p.GetStructArrayFields() {
		s.Fields = append(s.Fields, structArrayFieldFromProto(sp))
	}
	// functions
	s.Functions = lo.Map(p.GetFunctions(), func(fn *schemapb.FunctionSchema, _ int) *Function {
		return NewFunction().ReadProto(fn)
	})

	return s
}

// structArrayFieldFromProto reverses the ProtoMessage transformation for struct array fields.
// Server returns struct sub-fields with DataType_Array / DataType_ArrayOfVector and the original
// element type carried in ElementType. We restore the user-facing Field where DataType is the
// original (scalar or vector) type.
func structArrayFieldFromProto(p *schemapb.StructArrayFieldSchema) *Field {
	structSchema := NewStructSchema()
	for _, sf := range p.GetFields() {
		field := NewField().ReadProto(sf)
		// unwrap Array/ArrayOfVector wrapper added by ProtoMessage()
		switch sf.GetDataType() {
		case schemapb.DataType_Array, schemapb.DataType_ArrayOfVector:
			field.DataType = FieldType(sf.GetElementType())
			field.ElementType = 0
		}
		structSchema.WithField(field)
	}
	return &Field{
		ID:           p.GetFieldID(),
		Name:         p.GetName(),
		Description:  p.GetDescription(),
		DataType:     FieldTypeArray,
		ElementType:  FieldTypeStruct,
		TypeParams:   KvPairsMap(p.GetTypeParams()),
		StructSchema: structSchema,
	}
}

// Validate performs client-side sanity checks on the schema. Currently enforces struct-array
// sub-field rules; may grow over time. Callers should invoke this before CreateCollection; the
// CreateCollection client path invokes it automatically so users opt in by default.
func (s *Schema) Validate() error {
	if s == nil {
		return errors.New("nil schema")
	}
	for _, field := range s.Fields {
		if field == nil {
			return errors.New("schema contains nil field")
		}
		if field.DataType == FieldTypeArray && field.ElementType == FieldTypeStruct {
			if err := field.StructSchema.Validate(field.Name); err != nil {
				return err
			}
		}
	}
	return nil
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
