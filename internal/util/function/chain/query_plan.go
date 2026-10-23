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
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// QueryChainPlan is static preparation only. Grep argument parsing and matcher
// compilation belong to the execution entry, not to this schema-aware planner.
type QueryChainPlan struct {
	Chain     *schemapb.FunctionChain
	InputPlan *DataFrameInputPlan
	// OutputSchema describes the final projection, including zero-row results.
	OutputSchema *arrow.Schema
}

// PrepareQueryChain validates the phase-one Query chain subset and returns an
// owned copy for the distributed plan. It never parses arguments or compiles a
// regex. Unknown/conflicting operator fields are rejected rather than ignored.
func PrepareQueryChain(chains []*schemapb.FunctionChain, schema *schemapb.CollectionSchema) (*QueryChainPlan, error) {
	if len(chains) == 0 {
		return nil, nil
	}
	if len(chains) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("Query accepts at most one function chain")
	}
	pb := chains[0]
	if pb == nil {
		return nil, merr.WrapErrParameterInvalidMsg("Query function chain is nil")
	}
	if pb.GetStage() != schemapb.FunctionChainStage_FunctionChainStageQueryScan {
		return nil, merr.WrapErrParameterInvalidMsg("Query requires the QueryScan function chain stage")
	}
	if schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("Query chain schema is nil")
	}
	if len(pb.GetOps()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("QueryScan requires exactly one grep operator")
	}

	if len(pb.GetOutputColumns()) > 0 && strings.TrimSpace(pb.GetName()) == "" {
		return nil, merr.WrapErrParameterInvalidMsg("Query chain with output_columns requires a name")
	}
	fields := make(map[string]*schemapb.FieldSchema)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fields[field.GetName()] = field
	}
	// Parent struct names are collection fields too, even though
	// GetAllFieldSchemas returns only their children.
	for _, field := range schema.GetStructArrayFields() {
		fields[field.Name] = &schemapb.FieldSchema{Name: field.Name, FieldID: field.FieldID, DataType: schemapb.DataType_ArrayOfStruct}
	}
	produced := make(map[string]arrow.Field)
	fetched := make(map[int64]struct{})
	inputs := &DataFrameInputPlan{}
	for index, op := range pb.GetOps() {
		if err := prepareQueryOp(op, fields, produced, fetched, inputs); err != nil {
			return nil, merr.Wrapf(err, "Query chain op[%d]", index)
		}
	}
	exports := make(map[string]struct{})
	outputFields := make([]arrow.Field, 0, len(pb.GetOutputColumns()))
	for _, name := range pb.GetOutputColumns() {
		if _, ok := produced[name]; !ok {
			return nil, merr.WrapErrParameterInvalidMsg("Query chain output %q is not produced", name)
		}
		if _, ok := exports[name]; ok {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate Query chain output %q", name)
		}
		exports[name] = struct{}{}
		outputFields = append(outputFields, produced[name])
	}
	return &QueryChainPlan{Chain: proto.Clone(pb).(*schemapb.FunctionChain), InputPlan: inputs, OutputSchema: arrow.NewSchema(outputFields, nil)}, nil
}

func prepareQueryOp(op *schemapb.FunctionChainOp, fields map[string]*schemapb.FieldSchema,
	produced map[string]arrow.Field, fetched map[int64]struct{}, inputs *DataFrameInputPlan,
) error {
	if op == nil || op.GetExpr() != nil {
		return merr.WrapErrParameterInvalidMsg("QueryScan requires an independent grep operator without an expression")
	}
	repr := &OperatorRepr{Type: op.GetOp(), Inputs: op.GetInputs(), Outputs: op.GetOutputs(), Params: op.GetParams()}
	if _, err := validateGrepRepr(repr); err != nil {
		return err
	}
	input, output := repr.Inputs[0], repr.Outputs[0]
	if _, ok := fields[output]; ok || output == common.RowIDFieldName || output == common.TimeStampFieldName {
		return merr.WrapErrParameterInvalidMsg("grep output %q conflicts with a schema field", output)
	}
	field, ok := fields[input]
	if !ok || field.GetIsDynamic() || (field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_Text) {
		return merr.WrapErrParameterInvalidMsg("grep input %q must be a schema VarChar or Text field", input)
	}
	// This is a native field dependency, not a request to materialize text in
	// the candidate DataFrame. GrepOp's runtime Inputs contains only offsets.
	if _, ok := fetched[field.GetFieldID()]; !ok {
		inputs.Inputs = append(inputs.Inputs, ResolvedChainInput{
			LogicalName: input, SourceFieldID: field.GetFieldID(), FieldName: input,
			DataType: field.GetDataType(), Nullable: field.GetNullable(),
		})
		fetched[field.GetFieldID()] = struct{}{}
	}
	declared, err := grepOutputFields(repr)
	if err != nil {
		return err
	}
	for _, field := range declared {
		produced[field.Name] = field
	}
	return nil
}

// PrepareQueryPlanChainExecution validates and prepares the supported worker-side QueryScan subset.
func PrepareQueryPlanChainExecution(plan *planpb.PlanNode, schema *schemapb.CollectionSchema, isIterator bool) (*QueryChainPlan, error) {
	if len(plan.GetQuerynodeFunctionChains()) == 0 {
		return nil, nil
	}
	query := plan.GetQuery()
	if query == nil {
		return nil, merr.WrapErrServiceInternalMsg("Query chains require a QueryPlanNode")
	}
	if query.GetPredicates().GetElementFilterExpr() != nil {
		return nil, merr.WrapErrParameterInvalidMsg("QueryScan supports only document-level Query")
	}
	if query.GetIsCount() || len(query.GetAggregates()) > 0 || len(query.GetGroupByFieldIds()) > 0 ||
		len(query.GetOrderByFields()) > 0 || isIterator || query.GetQueryIteratorCursor() != nil {
		return nil, merr.WrapErrParameterInvalidMsg("Query chains do not support count, aggregation, group-by, order-by or iterator")
	}
	return PrepareQueryChain(plan.GetQuerynodeFunctionChains(), schema)
}
