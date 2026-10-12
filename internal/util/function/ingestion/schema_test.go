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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCollectionStageBinding(t *testing.T) {
	plan := &schemapb.FunctionChain{Name: "write", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{{Op: "select", Inputs: []string{"text"}}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "text", DataType: schemapb.DataType_VarChar}}, FunctionChains: []*schemapb.FunctionChain{plan}}
	require.NoError(t, ValidateCollection(schema))
	require.ErrorIs(t, RejectUnsupportedWrite(schema, "upsert"), merr.ErrOperationNotSupported)
	require.ErrorIs(t, RejectUnsupportedWrite(schema, "import"), merr.ErrOperationNotSupported)
	require.Equal(t, merr.InputError, merr.GetErrorType(RejectUnsupportedWrite(schema, "upsert")))
	plan.Stage = schemapb.FunctionChainStage_FunctionChainStagePreProcess
	require.ErrorIs(t, ValidateCollection(schema), merr.ErrParameterInvalid)
	plan.Stage = schemapb.FunctionChainStage_FunctionChainStageIngestion
	schema.FunctionChains = append(schema.FunctionChains, plan)
	require.ErrorIs(t, ValidateCollection(schema), merr.ErrParameterInvalid)
	schema.FunctionChains = nil
	require.NoError(t, RejectUnsupportedWrite(schema, "upsert"))
}

func TestFinalProjectionCannotBecomeUnboundAfterSchemaChange(t *testing.T) {
	plan := &schemapb.FunctionChain{Name: "write", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{{Op: "select", Inputs: []string{"text"}, Outputs: []string{"text"}}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "text", DataType: schemapb.DataType_VarChar}}, FunctionChains: []*schemapb.FunctionChain{plan}}
	require.NoError(t, ValidateCollection(schema))
	schema.Fields = nil
	require.ErrorIs(t, ValidateCollection(schema), merr.ErrParameterInvalid)
	schema.Fields = []*schemapb.FieldSchema{{Name: "text", DataType: schemapb.DataType_VarChar}, {Name: "tags", DataType: schemapb.DataType_Array}}
	require.ErrorIs(t, ValidateCollection(schema), merr.ErrParameterInvalid, "new required storage fields must be produced by the projection")
	schema.Fields[1].Nullable = true
	require.NoError(t, ValidateCollection(schema))
}

func TestWheelResourceNames(t *testing.T) {
	resource := &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: "tagger.whl"}}
	op := &schemapb.FunctionChainOp{Op: "map", Expr: &schemapb.FunctionChainExpr{Name: "py_udf", Params: map[string]*schemapb.FunctionParamValue{"resource_name": resource}}}
	schema := &schemapb.CollectionSchema{FunctionChains: []*schemapb.FunctionChain{{Ops: []*schemapb.FunctionChainOp{op, op}}}}
	names, err := ResourceNames(schema)
	require.NoError(t, err)
	require.Equal(t, []string{"tagger.whl"}, names)
	op.Expr.Params["resource_name"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}}
	_, err = ResourceNames(schema)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	delete(op.Expr.Params, "resource_name")
	_, err = ResourceNames(schema)
	require.ErrorIs(t, err, merr.ErrParameterMissing)
}
