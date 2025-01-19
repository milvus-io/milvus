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
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/proto/storagev2pb"
)

var (
	ErrPrimaryColumnNotFound = errors.New("primary column not found")
	ErrPrimaryColumnType     = errors.New("primary column is not int64 or string")
	ErrPrimaryColumnEmpty    = errors.New("primary column is empty")
	ErrVersionColumnNotFound = errors.New("version column not found")
	ErrVersionColumnType     = errors.New("version column is not int64")
	ErrVectorColumnNotFound  = errors.New("vector column not found")
	ErrVectorColumnType      = errors.New("vector column is not fixed size binary or fixed size list")
	ErrVectorColumnEmpty     = errors.New("vector column is empty")
)

type SchemaOptions struct {
	PrimaryColumn string
	VersionColumn string
	VectorColumn  string
}

func DefaultSchemaOptions() *SchemaOptions {
	return &SchemaOptions{
		PrimaryColumn: "",
		VersionColumn: "",
		VectorColumn:  "",
	}
}

func (o *SchemaOptions) ToProtobuf() *storagev2pb.SchemaOptions {
	options := &storagev2pb.SchemaOptions{}
	options.PrimaryColumn = o.PrimaryColumn
	options.VersionColumn = o.VersionColumn
	options.VectorColumn = o.VectorColumn
	return options
}

func (o *SchemaOptions) FromProtobuf(options *storagev2pb.SchemaOptions) {
	o.PrimaryColumn = options.PrimaryColumn
	o.VersionColumn = options.VersionColumn
	o.VectorColumn = options.VectorColumn
}

func (o *SchemaOptions) Validate(schema *arrow.Schema) error {
	if o.PrimaryColumn != "" {
		primaryField, ok := schema.FieldsByName(o.PrimaryColumn)
		if !ok {
			return ErrPrimaryColumnNotFound
		} else if primaryField[0].Type.ID() != arrow.STRING && primaryField[0].Type.ID() != arrow.INT64 {
			return ErrPrimaryColumnType
		}
	} else {
		return ErrPrimaryColumnEmpty
	}
	if o.VersionColumn != "" {
		versionField, ok := schema.FieldsByName(o.VersionColumn)
		if !ok {
			return ErrVersionColumnNotFound
		} else if versionField[0].Type.ID() != arrow.INT64 {
			return ErrVersionColumnType
		}
	}
	if o.VectorColumn != "" {
		vectorField, b := schema.FieldsByName(o.VectorColumn)
		if !b {
			return ErrVectorColumnNotFound
		} else if vectorField[0].Type.ID() != arrow.FIXED_SIZE_BINARY && vectorField[0].Type.ID() != arrow.FIXED_SIZE_LIST {
			return ErrVectorColumnType
		}
	} else {
		return ErrVectorColumnEmpty
	}
	return nil
}

func (o *SchemaOptions) HasVersionColumn() bool {
	return o.VersionColumn != ""
}
