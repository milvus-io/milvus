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
	"github.com/apache/arrow/go/v17/arrow"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ValidateMergeColumnMetadata checks that values from different inputs can
// share one result-column definition before merged columns are constructed.
// Nullability can widen across inputs and is joined when constructing output.
func ValidateMergeColumnMetadata(inputs []*DataFrame) error {
	first := make(map[string]*DataFrame)
	for inputIdx, input := range inputs {
		if input == nil {
			return merr.WrapErrServiceInternalMsg("merge input %d is nil", inputIdx)
		}
		for _, name := range input.ColumnNames() {
			// Identity and score have dedicated merge contracts (including empty IDs).
			if name == types.IDFieldName || name == types.ScoreFieldName {
				continue
			}
			meta, ok := input.fieldMetadata[name]
			if !ok {
				return merr.WrapErrServiceInternalMsg("merge column %s has no metadata", name)
			}
			source, exists := first[name]
			if !exists {
				first[name] = input
				continue
			}
			if !arrow.TypeEqual(source.Column(name).DataType(), input.Column(name).DataType()) {
				return merr.WrapErrFunctionFailedMsg("merge_op: input[%d] column %s type %s does not match output type", inputIdx, name, input.Column(name).DataType())
			}
			if !compatibleColumnMetadata(source, input, name, meta) {
				return merr.WrapErrServiceInternalMsg("merge column %s has incompatible field definitions", name)
			}
		}
	}
	return nil
}

func compatibleColumnMetadata(first, next *DataFrame, name string, b columnMetadata) bool {
	a := first.fieldMetadata[name]
	if a.origin != b.origin {
		return false
	}
	switch a.origin {
	case columnDeclared:
		if a.structure == nil && b.structure == nil {
			return compatibleFieldDefinition(a.field, b.field)
		}
		if a.structure == nil || b.structure == nil || a.structure.FieldID != b.structure.FieldID ||
			!sameTypeParams(a.structure.TypeParams, b.structure.TypeParams) || len(a.structure.Fields) != len(b.structure.Fields) {
			return false
		}
		children := make(map[string]*schemapb.FieldSchema, len(a.structure.Fields))
		for _, field := range a.structure.Fields {
			if field == nil || children[field.Name] != nil {
				return false
			}
			children[field.Name] = field
		}
		for _, field := range b.structure.Fields {
			if field == nil || !compatibleFieldDefinition(children[field.Name], field) {
				return false
			}
			delete(children, field.Name)
		}
		return len(children) == 0
	case columnComputed:
		at, aok := first.FieldType(name)
		bt, bok := next.FieldType(name)
		if !aok {
			at, _ = ToMilvusType(first.Column(name).DataType())
		}
		if !bok {
			bt, _ = ToMilvusType(next.Column(name).DataType())
		}
		return at == bt
	default:
		return false
	}
}

func compatibleFieldDefinition(a, b *schemapb.FieldSchema) bool {
	return a != nil && b != nil && a.FieldID == b.FieldID && a.DataType == b.DataType &&
		a.ElementType == b.ElementType && a.ElementNullable == b.ElementNullable &&
		proto.Equal(a.TypeSchema, b.TypeSchema) && sameTypeParams(a.TypeParams, b.TypeParams)
}

// Parameter ordering is not part of a field's logical definition.
func sameTypeParams(a, b []*commonpb.KeyValuePair) bool {
	if len(a) != len(b) {
		return false
	}
	values := make(map[string]string, len(a))
	for _, param := range a {
		if param == nil {
			return false
		}
		if _, exists := values[param.Key]; exists {
			return false
		}
		values[param.Key] = param.Value
	}
	for _, param := range b {
		if param == nil {
			return false
		}
		if value, exists := values[param.Key]; !exists || value != param.Value {
			return false
		}
		delete(values, param.Key)
	}
	return len(values) == 0
}
