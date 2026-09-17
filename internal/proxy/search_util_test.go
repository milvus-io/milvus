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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestConvertHybridSearchToSearchKeepsNamespace(t *testing.T) {
	namespace := "tenant_a"

	searchReq := convertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		CollectionName: "coll",
		Namespace:      &namespace,
		Requests: []*milvuspb.SearchRequest{
			{Nq: 1},
		},
	})

	assert.Equal(t, namespace, searchReq.GetNamespace())
}

func TestConvertHybridSearchToSearchKeepsFunctionChains(t *testing.T) {
	functionChains := []*schemapb.FunctionChain{
		{
			Name:  "hybrid_rerank",
			Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
			Ops: []*schemapb.FunctionChainOp{
				{Op: "merge"},
				{Op: "limit"},
			},
		},
	}

	searchReq := convertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		CollectionName: "coll",
		FunctionChains: functionChains,
		Requests: []*milvuspb.SearchRequest{
			{Nq: 1},
		},
	})

	assert.Equal(t, functionChains, searchReq.GetFunctionChains())
}

func TestConvertHybridSearchToSearchKeepsSubSearchFunctionChainsByIndex(t *testing.T) {
	l0Chain := &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
		Ops:   []*schemapb.FunctionChainOp{{Op: "map"}},
	}
	l1Chain := &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		Ops:   []*schemapb.FunctionChainOp{{Op: "limit"}},
	}

	searchReq := convertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		CollectionName: "coll",
		Requests: []*milvuspb.SearchRequest{
			{Nq: 1, FunctionChains: []*schemapb.FunctionChain{l0Chain}},
			{Nq: 1},
			{Nq: 1, FunctionChains: []*schemapb.FunctionChain{l1Chain}},
		},
	})

	require.Len(t, searchReq.GetSubReqs(), 3)
	assert.Equal(t, []*schemapb.FunctionChain{l0Chain}, searchReq.GetSubReqs()[0].GetFunctionChains())
	assert.Empty(t, searchReq.GetSubReqs()[1].GetFunctionChains())
	assert.Equal(t, []*schemapb.FunctionChain{l1Chain}, searchReq.GetSubReqs()[2].GetFunctionChains())
}

func TestSubSearchRequestFunctionChainsWireRoundTrip(t *testing.T) {
	chain := &schemapb.FunctionChain{
		Name:  "sub_rerank",
		Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: "limit",
				Params: map[string]*schemapb.FunctionParamValue{
					"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 5}},
				},
			},
		},
	}
	original := &milvuspb.SubSearchRequest{
		Nq:             1,
		FunctionChains: []*schemapb.FunctionChain{chain},
	}

	field := original.ProtoReflect().Descriptor().Fields().ByName("function_chains")
	require.NotNil(t, field)
	assert.Equal(t, int32(8), int32(field.Number()))

	wire, err := proto.Marshal(original)
	require.NoError(t, err)

	roundTrip := &milvuspb.SubSearchRequest{}
	require.NoError(t, proto.Unmarshal(wire, roundTrip))
	require.Len(t, roundTrip.GetFunctionChains(), 1)
	assert.True(t, proto.Equal(chain, roundTrip.GetFunctionChains()[0]))
}
