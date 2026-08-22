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

package schemautil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type AlterSchemaAddKind int

const (
	AlterSchemaAddField AlterSchemaAddKind = iota + 1
	AlterSchemaAddFunction
	AlterSchemaAddFunctionField
)

type AlterSchemaAddPlan struct {
	Kind     AlterSchemaAddKind
	Field    *schemapb.FieldSchema
	Function *schemapb.FunctionSchema
	// Index meta bound to the newly added field, parsed from
	// FieldInfo.index_name/extra_params. A vector-type function output field
	// always materializes a bound index so that bump-schema-version compaction
	// results are never blocked on a missing vector index; empty or AUTOINDEX
	// params resolve to concrete ones via the AutoIndex config at prepare.
	IndexName        string
	IndexExtraParams []*commonpb.KeyValuePair
}

func (p *AlterSchemaAddPlan) HasField() bool {
	return p != nil && p.Field != nil
}

func (p *AlterSchemaAddPlan) HasFunction() bool {
	return p != nil && p.Function != nil
}

func ParseAlterSchemaAddRequest(addRequest *milvuspb.AlterCollectionSchemaRequest_AddRequest) (*AlterSchemaAddPlan, error) {
	if addRequest == nil {
		return nil, merr.WrapErrParameterInvalidMsg("add_request is nil")
	}

	fieldInfos := addRequest.GetFieldInfos()
	funcSchemas := addRequest.GetFuncSchema()
	if len(funcSchemas) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("For now, at most one function schema is supported")
	}
	if len(fieldInfos) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("For now, only one field info is supported")
	}

	var field *schemapb.FieldSchema
	var indexName string
	var indexExtraParams []*commonpb.KeyValuePair
	if len(fieldInfos) == 1 {
		fieldInfo := fieldInfos[0]
		if fieldInfo == nil || fieldInfo.GetFieldSchema() == nil {
			return nil, merr.WrapErrParameterInvalidMsg("fieldSchema is nil in fieldInfos")
		}
		field = fieldInfo.GetFieldSchema()
		indexName = fieldInfo.GetIndexName()
		indexExtraParams = fieldInfo.GetExtraParams()
	}

	var function *schemapb.FunctionSchema
	if len(funcSchemas) == 1 {
		function = funcSchemas[0]
		if function == nil {
			return nil, merr.WrapErrParameterInvalidMsg("function schema is nil")
		}
	}

	switch {
	case field != nil && function != nil:
		return &AlterSchemaAddPlan{
			Kind:             AlterSchemaAddFunctionField,
			Field:            field,
			Function:         function,
			IndexName:        indexName,
			IndexExtraParams: indexExtraParams,
		}, nil
	case field != nil:
		return &AlterSchemaAddPlan{Kind: AlterSchemaAddField, Field: field}, nil
	case function != nil:
		return &AlterSchemaAddPlan{Kind: AlterSchemaAddFunction, Function: function}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("fieldInfos and function schema are both empty")
	}
}

func ValidateAlterSchemaAddFunctionPlan(plan *AlterSchemaAddPlan) error {
	if !plan.HasFunction() {
		return nil
	}

	function := plan.Function
	switch plan.Kind {
	case AlterSchemaAddFunction:
		return merr.WrapErrParameterInvalidMsg(
			"adding a function over existing fields is not supported; use add_function_field to add the function together with its new output field")
	case AlterSchemaAddFunctionField:
		if err := validateAddFunctionFieldAllowed(function); err != nil {
			return err
		}
		if err := validateAddFunctionFieldInputOutput(function); err != nil {
			return err
		}
		if function.GetOutputFieldNames()[0] != plan.Field.GetName() {
			return merr.WrapErrParameterInvalidMsg(
				"function output field %q must be the newly-added field %q",
				function.GetOutputFieldNames()[0],
				plan.Field.GetName(),
			)
		}
		// A vector output field always gets a bound index (empty/AUTOINDEX params
		// resolve via the AutoIndex config at prepare): without an index meta,
		// bump-schema-version compaction results can never pass the
		// indexed-visibility check and the whole backfill pipeline silently stalls.
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("unknown alter schema add request kind")
	}
}

// ValidateAddFunctionBackfillConfig rejects adding a function to an existing
// collection unless the storage and compaction paths that materialize its
// output for pre-existing sealed segments are enabled. New writes compute the
// function output at flush, so this guard applies only to add_function_field:
//   - Compaction must be enabled so the backfill infrastructure is running.
//   - StorageV3 is required because schema-version bump works only on V3 segments.
//   - schema-version bump compaction backfills pre-existing V3 segments.
//   - storage-version compaction upgrades pre-existing V2 segments before backfill.
func ValidateAddFunctionBackfillConfig(compactionEnabled, storageV3Enabled, bumpSchemaVersionEnabled, storageVersionEnabled bool) error {
	if !compactionEnabled {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires compaction; enable dataCoord.enableCompaction")
	}
	if !storageV3Enabled {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires StorageV3; enable common.storage.useLoonFFI")
	}
	if !bumpSchemaVersionEnabled {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires schema-bump compaction to backfill existing segments; enable dataCoord.compaction.bumpSchemaVersion.enabled")
	}
	if !storageVersionEnabled {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires the storage-version upgrade compaction; enable dataCoord.compaction.storageVersion.enabled")
	}
	return nil
}

// ValidateAddFunctionInputNotText rejects BM25/MinHash backfill from TEXT
// fields because existing TEXT values are stored as binary LOB references.
func ValidateAddFunctionInputNotText(schema *schemapb.CollectionSchema, function *schemapb.FunctionSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash:
	default:
		return nil
	}

	fieldByName := make(map[string]*schemapb.FieldSchema, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		fieldByName[field.GetName()] = field
	}
	for _, inputFieldName := range function.GetInputFieldNames() {
		if field, ok := fieldByName[inputFieldName]; ok && field.GetDataType() == schemapb.DataType_Text {
			return merr.WrapErrParameterInvalidMsg(
				"adding a %s function with a TEXT input field (%s) is not supported: its output cannot be backfilled into existing segments; use a VARCHAR input field",
				function.GetType().String(), inputFieldName)
		}
	}
	return nil
}

func validateAddFunctionFieldAllowed(function *schemapb.FunctionSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("For now, only BM25 and MinHash functions are supported in add_function_field interface")
	}
}

func validateAddFunctionFieldInputOutput(function *schemapb.FunctionSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(function.GetInputFieldNames()) != 1 || len(function.GetOutputFieldNames()) != 1 {
			return merr.WrapErrParameterInvalidMsg("BM25 function should have exactly one input field and exactly one output field")
		}
		return nil
	case schemapb.FunctionType_MinHash:
		if len(function.GetInputFieldNames()) != 1 || len(function.GetOutputFieldNames()) != 1 {
			return merr.WrapErrParameterInvalidMsg("MinHash function should have exactly one input field and exactly one output field")
		}
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported function type in alter schema task: %s", function.GetType().String())
	}
}

// CheckNoFunctionCascade rejects a new function whose input field is the output
// field of an existing function. Cascade (function-on-function) is unsupported:
// the executor has no topological execution and backfill has no dependency
// ordering, so a chained output can never be materialized in a defined order.
func CheckNoFunctionCascade(existingFunctions []*schemapb.FunctionSchema, newFunction *schemapb.FunctionSchema) error {
	if newFunction == nil {
		return nil
	}
	existingOutputs := typeutil.NewSet[string]()
	for _, fn := range existingFunctions {
		existingOutputs.Insert(fn.GetOutputFieldNames()...)
	}
	for _, input := range newFunction.GetInputFieldNames() {
		if existingOutputs.Contain(input) {
			return merr.WrapErrParameterInvalidMsg(
				"function %s input field %s is the output of another function; function cascade is not supported",
				newFunction.GetName(), input)
		}
	}
	return nil
}
