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

package dql

import (
	"strings"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// queryScanGrepOpType is the only operator a QueryScan chain may contain.
const queryScanGrepOpType = "grep"

// queryScanGrep is the validated content of a QueryScan Grep chain.
type queryScanGrep struct {
	input     *schemapb.FieldSchema
	output    string
	arguments string
	// exported is true when the chain lists its output in output_columns.
	exported bool
}

// prepareQueryFunctionChains rewrites the QueryScan Grep chain of a Query
// into the plan the equivalent request without a chain would get:
//
//	filter:        <request filter> AND grep(input, arguments)
//	output_fields: ..., GREP_MATCHES(input, arguments) AS output   (if exported)
//
// The options that decide whether a row matches go to both; the options that
// only shape the returned lines (-m N, -A/-B/-C) go to the output item. No
// chain is sent to the QueryNode.
func (t *QueryTask) prepareQueryFunctionChains() error {
	if len(t.request.GetFunctionChains()) == 0 {
		return nil
	}
	if t.plan.GetQuery() == nil || t.schema == nil || t.queryParams == nil {
		return merr.WrapErrServiceInternalMsg("Query chain preparation requires a query plan, schema and query params")
	}
	query := t.plan.GetQuery()
	if query.GetIsCount() || len(query.GetAggregates()) > 0 || len(query.GetGroupByFieldIds()) > 0 ||
		len(query.GetOrderByFields()) > 0 || t.queryParams.isIterator {
		return merr.WrapErrParameterInvalidMsg("Query chains do not support count, aggregation, group-by, order-by or iterator")
	}
	scan, err := validateQueryScanChains(t.request.GetFunctionChains(), t.schema.CollectionSchema)
	if err != nil {
		return err
	}
	grepMatches, err := planparserv2.NewGrepMatches(scan.input, scan.arguments)
	if err != nil {
		return err
	}
	query.Predicates = planparserv2.AndGrepPredicate(query.GetPredicates(), proto.Clone(grepMatches.GetSpec()).(*planpb.GrepExpr))
	if !scan.exported {
		return nil
	}

	if t.outputProjection == nil {
		t.outputProjection = &outputProjection{keepSource: make(map[string]bool)}
	}
	for _, name := range t.userOutputFields {
		if name == scan.output {
			return merr.WrapErrParameterInvalidMsg("grep output %q conflicts with an output field", scan.output)
		}
	}
	p := t.outputProjection
	p.virtualItems = append(p.virtualItems, &virtualItem{
		id:          requestLocalOutputIDStart - int64(len(p.virtualItems)),
		column:      planparserv2.GrepMatchesFunctionName + "(" + scan.input.GetName() + ")",
		name:        scan.output,
		dataType:    schemapb.DataType_JSON,
		grepMatches: grepMatches,
	})
	t.userOutputFields = append(t.userOutputFields, scan.output)
	t.plan.ProjectionItems = p.planItems(false)
	return nil
}

// validateQueryScanChains checks the request shape of the QueryScan stage:
// one chain with one independent grep operator that reads one VarChar or Text
// schema field and writes one new column.
func validateQueryScanChains(chains []*schemapb.FunctionChain, schema *schemapb.CollectionSchema) (*queryScanGrep, error) {
	if len(chains) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("Query accepts at most one function chain")
	}
	pb := chains[0]
	if pb == nil {
		return nil, merr.WrapErrParameterInvalidMsg("query function chain[0] is nil")
	}
	if pb.GetStage() != schemapb.FunctionChainStage_FunctionChainStageQueryScan {
		return nil, merr.WrapErrParameterInvalidMsg("function chain stage %s is not supported in Query", pb.GetStage().String())
	}
	if len(pb.GetOps()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("QueryScan requires exactly one grep operator")
	}
	if len(pb.GetOutputColumns()) > 0 && strings.TrimSpace(pb.GetName()) == "" {
		return nil, merr.WrapErrParameterInvalidMsg("Query chain with output_columns requires a name")
	}
	op := pb.GetOps()[0]
	if op == nil || op.GetExpr() != nil || op.GetOp() != queryScanGrepOpType {
		return nil, merr.WrapErrParameterInvalidMsg("QueryScan requires an independent grep operator without an expression")
	}
	if len(op.GetInputs()) != 1 || len(op.GetOutputs()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("grep requires exactly one input and one output")
	}
	input, output := op.GetInputs()[0], op.GetOutputs()[0]
	for _, name := range []string{input, output} {
		if name == "" || strings.TrimSpace(name) != name || strings.HasPrefix(name, "$") {
			return nil, merr.WrapErrParameterInvalidMsg("invalid grep field name %q", name)
		}
	}
	if input == output {
		return nil, merr.WrapErrParameterInvalidMsg("grep output must not overwrite its input field")
	}
	arguments, ok := op.GetParams()["arguments"].GetValue().(*schemapb.FunctionParamValue_StringValue)
	if !ok || arguments == nil {
		return nil, merr.WrapErrParameterInvalidMsg("grep arguments must be a string")
	}
	if len(op.GetParams()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("grep accepts only the arguments parameter; use Query limit")
	}

	fields := make(map[string]*schemapb.FieldSchema)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fields[field.GetName()] = field
	}
	_, isStruct := lo.Find(schema.GetStructArrayFields(), func(f *schemapb.StructArrayFieldSchema) bool { return f.GetName() == output })
	if _, isField := fields[output]; isField || isStruct || output == common.RowIDFieldName || output == common.TimeStampFieldName {
		return nil, merr.WrapErrParameterInvalidMsg("grep output %q conflicts with a schema field", output)
	}
	field, ok := fields[input]
	if !ok || field.GetIsDynamic() || (field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_Text) {
		return nil, merr.WrapErrParameterInvalidMsg("grep input %q must be a schema VarChar or Text field", input)
	}

	scan := &queryScanGrep{input: field, output: output, arguments: arguments.StringValue}
	for i, name := range pb.GetOutputColumns() {
		if name != output {
			return nil, merr.WrapErrParameterInvalidMsg("Query chain output %q is not produced", name)
		}
		if i > 0 {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate Query chain output %q", name)
		}
		scan.exported = true
	}
	return scan, nil
}
