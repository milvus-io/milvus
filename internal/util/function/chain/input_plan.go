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

package chain

import (
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ResolvedChainInput is the schema-aware intermediate representation of one
// Function Chain input. It maps the logical column used by an operator to the
// physical schema field that must be read and, for JSON fields, the nested path
// that must be projected into a runtime Arrow column.
type ResolvedChainInput struct {
	// LogicalName is the complete name referenced by the chain and used for the
	// projected runtime column, for example metadata["price"] or $meta["ctr"].
	LogicalName string
	// SourceFieldID identifies the physical schema field to fetch. Multiple JSON
	// paths may share the same source field ID.
	SourceFieldID int64
	// FieldName is the physical root field name, for example metadata or $meta.
	FieldName string
	// DataType is the schema type of the physical root field, not the inferred
	// type of a nested JSON value.
	DataType schemapb.DataType
	// Nullable is the nullability declared by the physical schema field. JSON
	// path columns are always materialized as nullable regardless of this value.
	Nullable bool
	// NestedPath contains the JSON keys below the root field. It is empty only
	// for an ordinary scalar field; complete JSON roots are not supported.
	NestedPath []string
	// DataTypeHint controls the projected JSON-path column type. It is required
	// for JSON paths and unused for ordinary scalar fields.
	DataTypeHint schemapb.DataType
}

// DataFrameInputPlan contains the schema-resolved inputs needed to materialize
// a Function Chain DataFrame. Consumers decide how to load each input from its
// DataType and may group entries by SourceFieldID to batch JSON projections.
type DataFrameInputPlan struct {
	Inputs []ResolvedChainInput
}

// PhysicalFieldIDs returns deduplicated physical field IDs needed to build the DataFrame.
func (p *DataFrameInputPlan) PhysicalFieldIDs() []int64 {
	if p == nil {
		return nil
	}
	ids := make([]int64, 0, len(p.Inputs))
	seen := make(map[int64]struct{}, cap(ids))
	for _, input := range p.Inputs {
		if _, ok := seen[input.SourceFieldID]; ok {
			continue
		}
		seen[input.SourceFieldID] = struct{}{}
		ids = append(ids, input.SourceFieldID)
	}
	return ids
}

// PhysicalFieldNames returns deduplicated physical field names needed to build the DataFrame.
func (p *DataFrameInputPlan) PhysicalFieldNames() []string {
	if p == nil {
		return nil
	}
	names := make([]string, 0, len(p.Inputs))
	seen := make(map[string]struct{}, cap(names))
	for _, input := range p.Inputs {
		if _, ok := seen[input.FieldName]; ok {
			continue
		}
		seen[input.FieldName] = struct{}{}
		names = append(names, input.FieldName)
	}
	return names
}

// CompileDataFrameInputPlan resolves schema-backed chain inputs and builds their physical fetch plan.
func CompileDataFrameInputPlan(repr *ChainRepr, schema *schemapb.CollectionSchema) (*DataFrameInputPlan, error) {
	if repr == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, merr.Wrap(err, "function chain input plan: create schema helper")
	}
	return CompileDataFrameInputPlanWithSchemaHelper(repr, schemaHelper)
}

// CompileDataFrameInputPlanWithSchemaHelper resolves inputs using an existing schema helper.
func CompileDataFrameInputPlanWithSchemaHelper(
	repr *ChainRepr,
	schemaHelper *typeutil.SchemaHelper,
) (*DataFrameInputPlan, error) {
	if repr == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	if schemaHelper == nil {
		return nil, merr.WrapErrServiceInternal("function chain input plan: schema helper is nil")
	}
	plan := &DataFrameInputPlan{Inputs: make([]ResolvedChainInput, 0)}
	produced := make(map[string]struct{})
	inputOffsets := make(map[string]int)

	for opIdx := range repr.Operators {
		op := &repr.Operators[opIdx]
		if err := normalizeOperatorInputDataTypes(op); err != nil {
			return nil, merr.Wrapf(err, "op[%d]", opIdx)
		}

		for inputIdx, name := range op.Inputs {
			if _, ok := produced[name]; ok {
				continue
			}
			hint := op.InputDataTypes[inputIdx]
			if isRuntimeFunctionChainInput(name) {
				if hint != schemapb.DataType_None {
					return nil, merr.WrapErrParameterInvalidMsg(
						"op[%d] input %q: system input does not accept data type hint %s",
						opIdx, name, hint.String())
				}
				continue
			}

			resolved, err := resolveSchemaChainInput(schemaHelper, name, hint)
			if err != nil {
				return nil, merr.Wrapf(err, "op[%d] input %q", opIdx, name)
			}
			input := *resolved
			key := resolvedInputIdentity(input)
			if offset, ok := inputOffsets[key]; ok {
				if input.DataType == schemapb.DataType_JSON {
					mergedHint, err := mergeInputHints(plan.Inputs[offset].DataTypeHint, input.DataTypeHint)
					if err != nil {
						return nil, merr.Wrapf(err, "op[%d] input %q", opIdx, name)
					}
					plan.Inputs[offset].DataTypeHint = mergedHint
				}
				continue
			}
			inputOffsets[key] = len(plan.Inputs)
			plan.Inputs = append(plan.Inputs, input)
		}

		for _, output := range op.Outputs {
			if err := validateChainOutputName(schemaHelper, output); err != nil {
				return nil, merr.Wrapf(err, "op[%d] output %q", opIdx, output)
			}
			produced[output] = struct{}{}
		}
	}
	return plan, nil
}

func validateChainOutputName(schemaHelper *typeutil.SchemaHelper, name string) error {
	if isExplicitMetaInput(name) {
		return merr.WrapErrParameterInvalidMsg(
			"JSON root or path cannot be used as a function chain output")
	}
	if field, err := schemaHelper.GetFieldFromName(name); err == nil {
		if field.GetDataType() == schemapb.DataType_JSON {
			return merr.WrapErrParameterInvalidMsg(
				"JSON root or path cannot be used as a function chain output")
		}
		return nil
	}
	if !strings.Contains(name, "[") {
		return nil
	}

	var columnInfo *planpb.ColumnInfo
	if err := planparserv2.ParseIdentifier(schemaHelper, name, func(expr *planpb.Expr) error {
		columnInfo = expr.GetColumnExpr().GetInfo()
		return nil
	}); err != nil {
		return nil
	}
	if columnInfo != nil {
		field, err := schemaHelper.GetFieldFromID(columnInfo.GetFieldId())
		if err == nil && field.GetDataType() == schemapb.DataType_JSON {
			return merr.WrapErrParameterInvalidMsg(
				"JSON root or path cannot be used as a function chain output")
		}
	}
	return nil
}

func normalizeOperatorInputDataTypes(op *OperatorRepr) error {
	if op.InputDataTypes == nil {
		op.InputDataTypes = make([]schemapb.DataType, len(op.Inputs))
		return nil
	}
	if len(op.InputDataTypes) != len(op.Inputs) {
		return merr.WrapErrParameterInvalidMsg(
			"input data types count %d does not match input count %d",
			len(op.InputDataTypes), len(op.Inputs))
	}
	return nil
}

func isRuntimeFunctionChainInput(name string) bool {
	switch name {
	case types.IDFieldName, types.ScoreFieldName:
		return true
	default:
		return false
	}
}

func resolveSchemaChainInput(
	schemaHelper *typeutil.SchemaHelper,
	name string,
	hint schemapb.DataType,
) (*ResolvedChainInput, error) {
	if IsFunctionChainSystemName(name) {
		return nil, merr.WrapErrParameterInvalidMsg("unsupported function chain system input %q", name)
	}

	// Schema field names may also be expression keywords (for example,
	// "threshold"). Resolve exact names before parsing nested path expressions.
	var nestedPath []string
	field, err := schemaHelper.GetFieldFromName(name)
	if err != nil {
		var columnInfo *planpb.ColumnInfo
		if err := planparserv2.ParseIdentifier(schemaHelper, name, func(expr *planpb.Expr) error {
			columnInfo = expr.GetColumnExpr().GetInfo()
			return nil
		}); err != nil {
			return nil, merr.Wrap(err, "resolve function chain input")
		}
		if columnInfo == nil {
			return nil, merr.WrapErrParameterInvalidMsg("function chain input %q did not resolve to a column", name)
		}
		field, err = schemaHelper.GetFieldFromID(columnInfo.GetFieldId())
		if err != nil {
			return nil, merr.Wrap(err, "resolve function chain input field")
		}
		nestedPath = columnInfo.GetNestedPath()
	}
	if field.GetIsDynamic() && !isExplicitMetaInput(name) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"dynamic field input %q must use explicit %s[...] syntax", name, common.MetaFieldName)
	}
	if len(nestedPath) > 0 && field.GetDataType() != schemapb.DataType_JSON {
		return nil, merr.WrapErrParameterInvalidMsg(
			"function chain input %q uses nested path on unsupported field type %s",
			name, field.GetDataType().String())
	}
	if err := validateResolvedInputHint(field.GetDataType(), nestedPath, hint); err != nil {
		return nil, err
	}
	if field.GetDataType() != schemapb.DataType_JSON {
		if _, err := ToArrowType(field.GetDataType()); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg(
				"function chain input %q has unsupported field type %s", name, field.GetDataType().String())
		}
	}

	return &ResolvedChainInput{
		LogicalName:   name,
		SourceFieldID: field.GetFieldID(),
		FieldName:     field.GetName(),
		DataType:      field.GetDataType(),
		Nullable:      field.GetNullable(),
		NestedPath:    append([]string(nil), nestedPath...),
		DataTypeHint:  hint,
	}, nil
}

func isExplicitMetaInput(name string) bool {
	return name == common.MetaFieldName || strings.HasPrefix(name, common.MetaFieldName+"[")
}

func validateResolvedInputHint(fieldType schemapb.DataType, nestedPath []string, hint schemapb.DataType) error {
	if fieldType != schemapb.DataType_JSON {
		if hint == schemapb.DataType_None {
			return nil
		}
		fieldArrowType, fieldErr := ToArrowType(fieldType)
		hintArrowType, hintErr := ToArrowType(hint)
		if fieldErr != nil || hintErr != nil || !arrow.TypeEqual(fieldArrowType, hintArrowType) {
			return merr.WrapErrParameterInvalidMsg(
				"data type hint %s is incompatible with schema field type %s", hint.String(), fieldType.String())
		}
		return nil
	}
	if len(nestedPath) == 0 {
		return merr.WrapErrParameterInvalidMsg(
			"complete JSON root input is not supported; specify a JSON path")
	}
	if hint == schemapb.DataType_None {
		return merr.WrapErrParameterInvalidMsg("JSON path input requires an explicit data_type")
	}
	if !isSupportedJSONProjectionHint(hint) {
		return merr.WrapErrParameterInvalidMsg("unsupported JSON path data type hint %s", hint.String())
	}
	return nil
}

func isSupportedJSONProjectionHint(hint schemapb.DataType) bool {
	switch hint {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int64,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar:
		return true
	default:
		return false
	}
}

func resolvedInputIdentity(input ResolvedChainInput) string {
	return input.LogicalName
}

func mergeInputHints(left, right schemapb.DataType) (schemapb.DataType, error) {
	if left == schemapb.DataType_None {
		return right, nil
	}
	if right == schemapb.DataType_None || left == right {
		return left, nil
	}
	return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg(
		"conflicting data type hints %s and %s for the same JSON path", left.String(), right.String())
}

// ValidateMaterializedInput checks a materialized scalar or JSON-path column
// against the input plan shared by all Function Chain stages.
func ValidateMaterializedInput(df *DataFrame, input ResolvedChainInput) error {
	name := input.LogicalName
	column := df.Column(name)
	if column == nil {
		return merr.WrapErrServiceInternalMsg("materialized input %q is missing", name)
	}
	expectedType := input.DataType
	if input.DataType == schemapb.DataType_JSON {
		expectedType = input.DataTypeHint
	}
	expectedArrowType, err := ToArrowType(expectedType)
	if err != nil {
		return merr.Wrapf(err, "materialized input %q", name)
	}
	if !arrow.TypeEqual(column.DataType(), expectedArrowType) {
		return merr.WrapErrServiceInternalMsg(
			"materialized input %q type mismatch: expected %s, got %s",
			name, expectedArrowType.Name(), column.DataType().Name())
	}
	if fieldType, ok := df.FieldType(name); !ok || fieldType != expectedType {
		return merr.WrapErrServiceInternalMsg(
			"materialized input %q has invalid Milvus data type metadata", name)
	}
	fieldID, hasFieldID := df.FieldID(name)
	if input.DataType == schemapb.DataType_JSON {
		if hasFieldID {
			return merr.WrapErrServiceInternalMsg(
				"materialized JSON path %q unexpectedly has field id metadata", name)
		}
	} else if !hasFieldID || fieldID != input.SourceFieldID {
		return merr.WrapErrServiceInternalMsg(
			"materialized scalar input %q has invalid field id metadata", name)
	}
	return nil
}
