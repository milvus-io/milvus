/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// columnOrigin describes provenance only, independently of the column shape.
// Both declared and computed columns can contain ordinary values or structs.
type columnOrigin uint8

const (
	// Computed and system columns derive their shape from Arrow.
	columnComputed columnOrigin = iota
	// A field or struct-array declaration was explicitly attached by a setter.
	columnDeclared
)

// columnMetadata is the single type/identity/nullability authority for a column.
// Declared columns own either field or structure, which determines their schema
// representation independently of origin. Computed/system columns can have a
// partial FieldSchema; their shape comes from Arrow, and an optional ID never
// establishes declared origin.
// Schema pointers are immutable snapshots, shared by copies and row selections.
type columnMetadata struct {
	origin    columnOrigin
	field     *schemapb.FieldSchema
	structure *schemapb.StructArrayFieldSchema
	hasID     bool
}

func (m columnMetadata) dataType() (schemapb.DataType, bool) {
	if m.structure != nil {
		return schemapb.DataType_ArrayOfStruct, true
	}
	return m.field.GetDataType(), m.field.GetDataType() != schemapb.DataType_None
}

func (m columnMetadata) id() (int64, bool) {
	if m.structure != nil {
		return m.structure.GetFieldID(), true
	}
	return m.field.GetFieldID(), m.origin == columnDeclared || m.hasID
}

func (m columnMetadata) nullable() bool {
	if m.structure != nil {
		return m.structure.GetNullable()
	}
	return m.field.GetNullable()
}

// mutableField creates a private copy before a builder updates a descriptor.
func (m columnMetadata) mutableField() *schemapb.FieldSchema {
	if m.field == nil {
		return &schemapb.FieldSchema{}
	}
	return proto.Clone(m.field).(*schemapb.FieldSchema)
}

// SetFieldSchema atomically replaces the entire column declaration. The owned
// snapshot includes logical type, ID, nullability, element types and parameters.
func (b *DataFrameBuilder) SetFieldSchema(name string, field *schemapb.FieldSchema) *DataFrameBuilder {
	if b.result == nil || field == nil {
		return b
	}
	snapshot := proto.Clone(field).(*schemapb.FieldSchema)
	snapshot.Name = name
	b.result.fieldMetadata[name] = columnMetadata{origin: columnDeclared, field: snapshot}
	return b
}

// SetStructArraySchema atomically replaces the parent and child declarations.
func (b *DataFrameBuilder) SetStructArraySchema(name string, field *schemapb.StructArrayFieldSchema) *DataFrameBuilder {
	if b.result == nil || field == nil {
		return b
	}
	snapshot := proto.Clone(field).(*schemapb.StructArrayFieldSchema)
	snapshot.Name = name
	b.result.fieldMetadata[name] = columnMetadata{origin: columnDeclared, structure: snapshot}
	return b
}

// SetFieldType sets a computed/system type or confirms a declaration's type.
// It cannot partially rewrite a collection declaration or change column origin.
func (b *DataFrameBuilder) SetFieldType(name string, dataType schemapb.DataType) error {
	if b.result == nil {
		return merr.WrapErrServiceInternalMsg("builder already built")
	}
	meta := b.result.fieldMetadata[name]
	if meta.origin != columnComputed {
		declared, _ := meta.dataType()
		if declared != dataType {
			return merr.WrapErrServiceInternalMsg("cannot change declared column %s from %s to %s; replace its field definition", name, declared, dataType)
		}
		return nil
	}
	meta.field = meta.mutableField()
	meta.field.DataType = dataType
	b.result.fieldMetadata[name] = meta
	return nil
}

func (b *DataFrameBuilder) SetFieldID(name string, fieldID int64) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	meta := b.result.fieldMetadata[name]
	if meta.structure != nil {
		meta.structure = proto.Clone(meta.structure).(*schemapb.StructArrayFieldSchema)
		meta.structure.FieldID = fieldID
	} else {
		meta.field = meta.mutableField()
		meta.field.FieldID = fieldID
		meta.hasID = true
	}
	b.result.fieldMetadata[name] = meta
	return b
}

func (b *DataFrameBuilder) SetFieldNullable(name string, nullable bool) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	meta := b.result.fieldMetadata[name]
	if meta.structure != nil {
		meta.structure = proto.Clone(meta.structure).(*schemapb.StructArrayFieldSchema)
		meta.structure.Nullable = nullable
		for _, child := range meta.structure.Fields {
			if child != nil {
				child.Nullable = nullable
			}
		}
	} else {
		meta.field = meta.mutableField()
		meta.field.Nullable = nullable
	}
	b.result.fieldMetadata[name] = meta
	return b
}

func (df *DataFrame) FieldType(name string) (schemapb.DataType, bool) {
	return df.fieldMetadata[name].dataType()
}

func (df *DataFrame) FieldID(name string) (int64, bool) {
	return df.fieldMetadata[name].id()
}

func (df *DataFrame) FieldNullable(name string) bool {
	return df.fieldMetadata[name].nullable()
}

// IsComputedColumn uses explicit origin, not the presence/absence of a schema.
func (df *DataFrame) IsComputedColumn(name string) bool {
	meta, ok := df.fieldMetadata[name]
	return ok && meta.origin == columnComputed
}

func (df *DataFrame) FieldSchema(name string) *schemapb.FieldSchema {
	meta := df.fieldMetadata[name]
	if meta.origin == columnDeclared && meta.field != nil {
		return proto.Clone(meta.field).(*schemapb.FieldSchema)
	}
	return nil
}

func (df *DataFrame) StructArraySchema(name string) *schemapb.StructArrayFieldSchema {
	meta := df.fieldMetadata[name]
	if meta.origin == columnDeclared && meta.structure != nil {
		return proto.Clone(meta.structure).(*schemapb.StructArrayFieldSchema)
	}
	return nil
}
