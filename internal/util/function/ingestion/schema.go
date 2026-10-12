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

package ingestion

import (
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateCollection validates the persistent insertion contract. Query-stage
// chains are request-scoped and cannot be attached to a collection schema.
func ValidateCollection(schema *schemapb.CollectionSchema) error {
	if schema == nil {
		return merr.WrapErrParameterMissingMsg("collection schema is required")
	}
	chains := schema.GetFunctionChains()
	if len(chains) == 0 {
		return nil
	}
	if len(chains) != 1 {
		return merr.WrapErrParameterInvalidMsg("collection supports one ingestion function chain")
	}
	if typeutil.IsExternalCollection(schema) {
		return merr.WrapErrParameterInvalidMsg("external collections cannot have an ingestion function chain")
	}
	plan := chains[0]
	if plan == nil || plan.GetStage() != schemapb.FunctionChainStage_FunctionChainStageIngestion {
		return merr.WrapErrParameterInvalidMsg("collection function chain must have ingestion stage")
	}
	if strings.TrimSpace(plan.GetName()) == "" {
		return merr.WrapErrParameterInvalidMsg("collection ingestion function chain must have a name")
	}
	if err := transform.ValidatePlan(plan); err != nil {
		return err
	}
	last := plan.GetOps()[len(plan.GetOps())-1]
	if last.GetOp() != transform.OpSelect {
		return nil
	}
	outputs := last.GetOutputs()
	if len(outputs) == 0 {
		outputs = last.GetInputs()
	}
	projected := make(map[string]struct{}, len(outputs))
	known := make(map[string]struct{}, len(schema.GetFields())+len(schema.GetStructArrayFields()))
	for _, field := range schema.GetFields() {
		known[field.GetName()] = struct{}{}
	}
	for _, field := range schema.GetStructArrayFields() {
		known[field.GetName()] = struct{}{}
	}
	if schema.GetEnableDynamicField() {
		known[common.MetaFieldName] = struct{}{}
	}
	for _, name := range outputs {
		if _, exists := known[name]; !exists {
			return merr.WrapErrParameterInvalidMsg("ingestion final projection output %q is not a collection field", name)
		}
		projected[name] = struct{}{}
	}
	generated := make(map[string]struct{})
	for _, function := range schema.GetFunctions() {
		for _, name := range function.GetOutputFieldNames() {
			generated[name] = struct{}{}
		}
	}
	for _, field := range schema.GetFields() {
		name := field.GetName()
		isSystem := name == common.RowIDFieldName || name == common.TimeStampFieldName || name == common.MetaFieldName || name == common.NamespaceFieldName
		if field.GetAutoID() || field.GetNullable() || field.GetDefaultValue() != nil || isSystem {
			continue
		}
		if _, exists := generated[field.GetName()]; exists {
			continue
		}
		if _, exists := projected[field.GetName()]; !exists {
			return merr.WrapErrParameterInvalidMsg("ingestion final projection omits required collection field %q", field.GetName())
		}
	}
	for _, field := range schema.GetStructArrayFields() {
		if !field.GetNullable() {
			if _, exists := projected[field.GetName()]; !exists {
				return merr.WrapErrParameterInvalidMsg("ingestion final projection omits required struct field %q", field.GetName())
			}
		}
	}
	return nil
}

// ResourceNames returns immutable wheel resources used by the collection's
// PyUDF expressions. RootCoord binds their IDs to collection lifetime.
func ResourceNames(schema *schemapb.CollectionSchema) ([]string, error) {
	var names []string
	seen := make(map[string]struct{})
	for _, plan := range schema.GetFunctionChains() {
		for _, op := range plan.GetOps() {
			expr := op.GetExpr()
			if expr.GetName() != "py_udf" {
				continue
			}
			value := expr.GetParams()["resource_name"]
			if value == nil {
				return nil, merr.WrapErrParameterMissingMsg("py_udf resource_name is required")
			}
			v, ok := value.Value.(*schemapb.FunctionParamValue_StringValue)
			if !ok || v == nil || strings.TrimSpace(v.StringValue) == "" {
				return nil, merr.WrapErrParameterInvalidMsg("py_udf resource_name must be a non-empty string")
			}
			if _, exists := seen[v.StringValue]; !exists {
				seen[v.StringValue] = struct{}{}
				names = append(names, v.StringValue)
			}
		}
	}
	return names, nil
}

// RejectUnsupportedWrite prevents write paths that do not execute collection
// ingestion plans from silently accepting untransformed records.
func RejectUnsupportedWrite(schema *schemapb.CollectionSchema, operation string) error {
	if len(schema.GetFunctionChains()) != 0 {
		return merr.WrapErrAsInputError(merr.WrapErrOperationNotSupportedMsg("%s is not supported for collections with an ingestion function chain", operation))
	}
	return nil
}
