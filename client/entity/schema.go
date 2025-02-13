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
	"github.com/samber/lo"

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
	}
	r.Fields = lo.Map(s.Fields, func(field *Field, _ int) *schemapb.FieldSchema {
		return field.ProtoMessage()
	})

	r.Functions = lo.Map(s.Functions, func(function *Function, _ int) *schemapb.FunctionSchema {
		return function.ProtoMessage()
	})

	return r
}

// ReadProto parses proto Collection Schema
func (s *Schema) ReadProto(p *schemapb.CollectionSchema) *Schema {
	s.Description = p.GetDescription()
	s.CollectionName = p.GetName()
	s.EnableDynamicField = p.GetEnableDynamicField()
	// fields
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
	// functions
	s.Functions = lo.Map(p.GetFunctions(), func(fn *schemapb.FunctionSchema, _ int) *Function {
		return NewFunction().ReadProto(fn)
	})

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
