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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestCollectionFunctionChainPersistsAndClones(t *testing.T) {
	plan := &schemapb.FunctionChain{Name: "ingestion", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{{Op: "select", Inputs: []string{"text"}, Outputs: []string{"text"}}}}
	collection := &Collection{Name: "docs", FunctionChains: []*schemapb.FunctionChain{plan}}
	// RootCoord stores basic collection metadata separately from fields; the
	// persistent chain must be present even without WithFields/WithFunctions.
	encoded, err := proto.Marshal(MarshalCollectionModel(collection))
	require.NoError(t, err)
	persisted := MarshalCollectionModel(collection)
	require.NoError(t, proto.Unmarshal(encoded, persisted))
	reloaded := UnmarshalCollectionModel(persisted)
	require.True(t, proto.Equal(plan, reloaded.ToCollectionSchemaPB().GetFunctionChains()[0]))
	clone := collection.Clone()
	clone.FunctionChains[0].Ops[0].Outputs[0] = "rewritten"
	require.Equal(t, "text", collection.FunctionChains[0].Ops[0].GetOutputs()[0])
	require.False(t, collection.Equal(*clone))
	require.True(t, collection.Equal(*reloaded))
	schema := collection.ToCollectionSchemaPB()
	schema.FunctionChains[0].Name = "changed"
	require.Equal(t, "ingestion", collection.FunctionChains[0].GetName())
}

func TestCollectionSchemaUpdateRetainsFunctionChain(t *testing.T) {
	plan := &schemapb.FunctionChain{Name: "ingestion", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{{Op: "select", Inputs: []string{"text"}}}}
	collection := &Collection{}
	schema := &schemapb.CollectionSchema{Version: 4, FunctionChains: []*schemapb.FunctionChain{plan}}
	collection.ApplyUpdates(&message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}, &message.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{Schema: schema}})
	require.Equal(t, int32(4), collection.ToCollectionSchemaPB().GetVersion())
	require.True(t, proto.Equal(plan, collection.ToCollectionSchemaPB().GetFunctionChains()[0]))
	schema.FunctionChains[0].Name = "mutated"
	require.Equal(t, "ingestion", collection.FunctionChains[0].GetName())
}
