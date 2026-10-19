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

package internalpb_test

import (
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func wireRoundTrip(t *testing.T, src, dst proto.Message) {
	t.Helper()
	data, err := proto.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(data, dst); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(src, dst) {
		t.Fatalf("wire round trip changed %T", src)
	}
}

// This checks message transport only; it does not exercise Proxy plan building.
func TestQueryChainRequestWireContract(t *testing.T) {
	stage := schemapb.FunctionChainStage_FunctionChainStageQueryScan
	if int32(stage) != 7 || stage.String() != "FunctionChainStageQueryScan" {
		t.Fatalf("unexpected QueryScan stage contract: %d %s", stage, stage)
	}
	arguments := `-i -e "user_id=[0-9]{3,6}\b.*timeout"`
	request := &milvuspb.QueryRequest{
		CollectionName: "logs",
		FunctionChains: []*schemapb.FunctionChain{{
			Name:          "find_errors",
			Stage:         schemapb.FunctionChainStage_FunctionChainStageQueryScan,
			OutputColumns: []string{"matches"},
			Ops: []*schemapb.FunctionChainOp{{
				Op: "map", Outputs: []string{"matches"},
				Expr: &schemapb.FunctionChainExpr{
					Name: "grep",
					Args: []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{
						Column: &schemapb.FunctionChainColumnArg{Name: "message"},
					}}},
					Params: map[string]*schemapb.FunctionParamValue{
						"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: arguments}},
					},
				},
			}},
		}},
	}
	decoded := &milvuspb.QueryRequest{}
	wireRoundTrip(t, request, decoded)
	plan := &planpb.PlanNode{
		Node:                    &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		QuerynodeFunctionChains: decoded.GetFunctionChains(),
	}
	data, err := proto.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	internal := &internalpb.RetrieveRequest{SerializedExprPlan: data}
	transported := &internalpb.RetrieveRequest{}
	wireRoundTrip(t, internal, transported)
	got := &planpb.PlanNode{}
	if err := proto.Unmarshal(transported.GetSerializedExprPlan(), got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(plan, got) {
		t.Fatal("serialized plan lost chain configuration")
	}
	if got.GetQuerynodeFunctionChains()[0].GetOps()[0].GetExpr().GetParams()["arguments"].GetStringValue() != arguments {
		t.Fatal("raw regex escapes changed")
	}
}

func resultFixture() []*schemapb.FieldData {
	return []*schemapb.FieldData{{FieldId: 1, FieldName: "matches", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{math.MinInt64, math.MaxInt64}}}}}}}
}

func TestQueryComputedResultsWireContract(t *testing.T) {
	internal := &internalpb.RetrieveResults{ComputedResults: resultFixture()}
	transported := &internalpb.RetrieveResults{}
	wireRoundTrip(t, internal, transported)
	response := &milvuspb.QueryResults{ComputedResults: transported.GetComputedResults()}
	wireRoundTrip(t, response, &milvuspb.QueryResults{})
	data, err := protojson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &milvuspb.QueryResults{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(response, decoded) {
		t.Fatal("JSON lost nested types or integer precision")
	}

	empty := &milvuspb.QueryResults{ComputedResults: []*schemapb.FieldData{}}
	got := &milvuspb.QueryResults{}
	wireRoundTrip(t, empty, got)
	if len(got.GetComputedResults()) != 0 {
		t.Fatal("empty result must have no computed columns")
	}
	ordinary := &milvuspb.QueryResults{}
	wireRoundTrip(t, ordinary, &milvuspb.QueryResults{})
	if ordinary.GetComputedResults() != nil {
		t.Fatal("ordinary Query acquired extra results")
	}
}

// Unknown-field preservation is wire compatibility, not capability negotiation:
// an old server can still ignore a chain and must not be used to execute it.
func TestQueryNewFieldsSurviveLegacyWireReader(t *testing.T) {
	cases := []struct {
		message proto.Message
		added   protoreflect.Name
	}{
		{&milvuspb.QueryRequest{CollectionName: "logs", FunctionChains: []*schemapb.FunctionChain{{
			Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan, OutputColumns: []string{"matches"},
		}}}, "function_chains"},
		{&milvuspb.QueryResults{CollectionName: "logs", ComputedResults: resultFixture()}, "computed_results"},
		{&internalpb.RetrieveResults{ReqID: 42, ComputedResults: resultFixture()}, "computed_results"},
	}
	for _, tc := range cases {
		descriptor := tc.message.ProtoReflect().Descriptor()
		t.Run(string(descriptor.FullName()), func(t *testing.T) {
			file := protodesc.ToFileDescriptorProto(descriptor.ParentFile())
			for _, message := range file.MessageType {
				if message.GetName() != string(descriptor.Name()) {
					continue
				}
				fields := message.Field[:0]
				for _, field := range message.Field {
					if field.GetName() != string(tc.added) {
						fields = append(fields, field)
					}
				}
				message.Field = fields
			}
			legacyFile, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
			if err != nil {
				t.Fatal(err)
			}
			legacy := dynamicpb.NewMessage(legacyFile.Messages().ByName(descriptor.Name()))
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(data, legacy); err != nil {
				t.Fatal(err)
			}
			if len(legacy.GetUnknown()) == 0 {
				t.Fatal("new field did not survive as unknown data")
			}
			data, err = proto.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			restored := tc.message.ProtoReflect().Type().New().Interface()
			if err := proto.Unmarshal(data, restored); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(tc.message, restored) {
				t.Fatal("legacy wire round trip lost data")
			}
		})
	}
}
