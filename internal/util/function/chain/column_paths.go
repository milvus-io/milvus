// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ColumnReferenceRoot identifies a struct root. Exact DataFrame column names
// always take precedence, including already materialized JSON path columns.
func ColumnReferenceRoot(name string) string {
	if index := strings.IndexAny(name, ".[ "); index >= 0 {
		return name[:index]
	}
	return name
}

func splitStructPath(name string) ([]string, error) {
	root := ColumnReferenceRoot(name)
	if root == "" {
		return nil, merr.WrapErrParameterInvalidMsg("invalid struct reference %q", name)
	}
	parts := []string{root}
	rest := name[len(root):]
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			end := strings.IndexAny(rest, ".[ ")
			if end < 0 {
				end = len(rest)
			}
			if end == 0 {
				return nil, merr.WrapErrParameterInvalidMsg("empty struct path component in %q", name)
			}
			parts = append(parts, rest[:end])
			rest = rest[end:]
		case '[':
			if len(rest) < 4 || rest[1] != '"' && rest[1] != '\'' {
				return nil, merr.WrapErrParameterInvalidMsg("struct paths require quoted field names: %q", name)
			}
			quote := rest[1]
			end := 2
			for end < len(rest) {
				if rest[end] == '\\' {
					end += 2
					continue
				}
				if rest[end] == quote {
					break
				}
				end++
			}
			if end+1 >= len(rest) || rest[end+1] != ']' {
				return nil, merr.WrapErrParameterInvalidMsg("invalid quoted struct path %q", name)
			}
			part, err := unquoteStructKey(rest[2:end], quote)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("invalid quoted struct field in %q", name)
			}
			parts = append(parts, part)
			rest = rest[end+2:]
		default:
			return nil, merr.WrapErrParameterInvalidMsg("invalid struct path %q", name)
		}
	}
	return parts, nil
}

func unquoteStructKey(contents string, quote byte) (string, error) {
	var result strings.Builder
	for contents != "" {
		value, _, rest, err := strconv.UnquoteChar(contents, quote)
		if err != nil {
			return "", err
		}
		result.WriteRune(value)
		contents = rest
	}
	return result.String(), nil
}

// ResolveColumn returns an owned retained/materialized column. The caller must
// Release it. Struct parent nulls are applied to projected child values.
func (df *DataFrame) ResolveColumn(name string, pool memory.Allocator) (*arrow.Chunked, error) {
	field, err := df.resolveArrowField(name)
	if err != nil {
		return nil, err
	}
	if column := df.Column(name); column != nil {
		column.Retain()
		return column, nil
	}
	parts, err := splitStructPath(name)
	if err != nil {
		return nil, err
	}
	root := df.Column(parts[0])
	if root == nil || len(parts) == 1 {
		return nil, merr.WrapErrParameterInvalidMsg("column %q not found", name)
	}
	chunks := make([]arrow.Array, len(root.Chunks()))
	defer func() {
		for _, chunk := range chunks {
			if chunk != nil {
				chunk.Release()
			}
		}
	}()
	for group, rootChunk := range root.Chunks() {
		current := rootChunk
		current.Retain()
		for _, part := range parts[1:] {
			structure, ok := current.(*array.Struct)
			if !ok {
				current.Release()
				return nil, merr.WrapErrParameterInvalidMsg("column %q does not contain struct field %q", name, part)
			}
			index, ok := structure.DataType().(*arrow.StructType).FieldIdx(part)
			if !ok {
				current.Release()
				return nil, merr.WrapErrParameterInvalidMsg("column %q has no struct field %q", name, part)
			}
			indices := make([]int, structure.Len())
			for row := range indices {
				indices[row] = row
				if structure.IsNull(row) {
					indices[row] = -1
				}
			}
			child, err := takeArrayWithNulls(pool, structure.Field(index), indices)
			current.Release()
			if err != nil {
				return nil, merr.Wrap(err, "project struct child")
			}
			current = child
		}
		chunks[group] = current
	}
	return arrow.NewChunked(field.Type, chunks), nil
}

func (df *DataFrame) resolveArrowField(name string) (arrow.Field, error) {
	if df.schema == nil {
		return arrow.Field{}, merr.WrapErrServiceInternal("DataFrame schema is nil")
	}
	if indexes := df.schema.FieldIndices(name); len(indexes) > 0 {
		if len(indexes) != 1 {
			return arrow.Field{}, merr.WrapErrParameterInvalidMsg("column %q is ambiguous", name)
		}
		return df.schema.Field(indexes[0]), nil
	}
	parts, err := splitStructPath(name)
	if err != nil {
		return arrow.Field{}, err
	}
	indexes := df.schema.FieldIndices(parts[0])
	if len(indexes) != 1 {
		return arrow.Field{}, merr.WrapErrParameterInvalidMsg("column %q not found", name)
	}
	field := df.schema.Field(indexes[0])
	nullable := field.Nullable
	for _, part := range parts[1:] {
		structure, ok := field.Type.(*arrow.StructType)
		if !ok {
			return arrow.Field{}, merr.WrapErrParameterInvalidMsg("column %q is not a struct path", name)
		}
		index, ok := structure.FieldIdx(part)
		if !ok {
			return arrow.Field{}, merr.WrapErrParameterInvalidMsg("column %q has no struct field %q", name, part)
		}
		matches := 0
		for _, child := range structure.Fields() {
			if child.Name == part {
				matches++
			}
		}
		if matches != 1 {
			return arrow.Field{}, merr.WrapErrParameterInvalidMsg("column %q has ambiguous struct field %q", name, part)
		}
		field = structure.Field(index)
		nullable = nullable || field.Nullable
	}
	field.Name = name
	field.Nullable = nullable
	return field, nil
}

func (o *BaseOp) readResolvedColumns(opName string, df *DataFrame, pool memory.Allocator) ([]*arrow.Chunked, error) {
	columns := make([]*arrow.Chunked, 0, len(o.inputs))
	for _, name := range o.inputs {
		column, err := df.ResolveColumn(name, pool)
		if err != nil {
			releaseColumns(columns)
			return nil, merr.Wrapf(err, "%s input %q", opName, name)
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func releaseColumns(columns []*arrow.Chunked) {
	for _, column := range columns {
		if column != nil {
			column.Release()
		}
	}
}
