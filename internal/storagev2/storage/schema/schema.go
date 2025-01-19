// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/common/utils"
	"github.com/milvus-io/milvus/pkg/proto/storagev2pb"
)

// Schema is a wrapper of arrow schema
type Schema struct {
	schema       *arrow.Schema
	scalarSchema *arrow.Schema
	vectorSchema *arrow.Schema
	deleteSchema *arrow.Schema

	options *SchemaOptions
}

func (s *Schema) Schema() *arrow.Schema {
	return s.schema
}

func (s *Schema) Options() *SchemaOptions {
	return s.options
}

func NewSchema(schema *arrow.Schema, options *SchemaOptions) *Schema {
	return &Schema{
		schema:  schema,
		options: options,
	}
}

func (s *Schema) Validate() error {
	err := s.options.Validate(s.schema)
	if err != nil {
		return err
	}
	err = s.BuildScalarSchema()
	if err != nil {
		return err
	}
	err = s.BuildVectorSchema()
	if err != nil {
		return err
	}
	err = s.BuildDeleteSchema()
	if err != nil {
		return err
	}
	return nil
}

func (s *Schema) ScalarSchema() *arrow.Schema {
	return s.scalarSchema
}

func (s *Schema) VectorSchema() *arrow.Schema {
	return s.vectorSchema
}

func (s *Schema) DeleteSchema() *arrow.Schema {
	return s.deleteSchema
}

func (s *Schema) FromProtobuf(schema *storagev2pb.Schema) error {
	schemaType, err := utils.FromProtobufSchema(schema.ArrowSchema)
	if err != nil {
		return err
	}

	s.schema = schemaType
	s.options.FromProtobuf(schema.GetSchemaOptions())
	s.BuildScalarSchema()
	s.BuildVectorSchema()
	s.BuildDeleteSchema()
	return nil
}

func (s *Schema) ToProtobuf() (*storagev2pb.Schema, error) {
	schema := &storagev2pb.Schema{}
	arrowSchema, err := utils.ToProtobufSchema(s.schema)
	if err != nil {
		return nil, err
	}
	schema.ArrowSchema = arrowSchema
	schema.SchemaOptions = s.options.ToProtobuf()
	return schema, nil
}

func (s *Schema) BuildScalarSchema() error {
	fields := make([]arrow.Field, 0, len(s.schema.Fields()))
	for _, field := range s.schema.Fields() {
		if field.Name == s.options.VectorColumn {
			continue
		}
		fields = append(fields, field)
	}
	offsetFiled := arrow.Field{Name: constant.OffsetFieldName, Type: arrow.DataType(&arrow.Int64Type{})}
	fields = append(fields, offsetFiled)
	s.scalarSchema = arrow.NewSchema(fields, nil)

	return nil
}

func (s *Schema) BuildVectorSchema() error {
	fields := make([]arrow.Field, 0, len(s.schema.Fields()))
	for _, field := range s.schema.Fields() {
		if field.Name == s.options.VectorColumn ||
			field.Name == s.options.PrimaryColumn ||
			field.Name == s.options.VersionColumn {
			fields = append(fields, field)
		}
	}
	s.vectorSchema = arrow.NewSchema(fields, nil)

	return nil
}

func (s *Schema) BuildDeleteSchema() error {
	pkColumn, ok := s.schema.FieldsByName(s.options.PrimaryColumn)
	if !ok {
		return ErrPrimaryColumnNotFound
	}
	versionField, ok := s.schema.FieldsByName(s.options.VersionColumn)
	if !ok {
		return ErrVersionColumnNotFound
	}
	fields := make([]arrow.Field, 0, 2)
	fields = append(fields, pkColumn[0])
	fields = append(fields, versionField[0])
	s.deleteSchema = arrow.NewSchema(fields, nil)
	return nil
}
