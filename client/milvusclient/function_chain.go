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

package milvusclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/functionchain"
)

// WithFunctionChains replaces the chains applied to this ANN request.
// Chains and the legacy FunctionScore/FunctionReranker are mutually exclusive.
func (r *AnnRequest) WithFunctionChains(chains ...*functionchain.FunctionChain) *AnnRequest {
	r.functionChains = append([]*functionchain.FunctionChain(nil), chains...)
	return r
}

func (opt *searchOption) WithFunctionChains(chains ...*functionchain.FunctionChain) *searchOption {
	opt.annRequest.WithFunctionChains(chains...)
	return opt
}

// WithFunctionChains sets top-level hybrid search chains. Supported stages and
// chain combinations are validated by the server. Legacy rerankers cannot be combined.
func (opt *hybridSearchOption) WithFunctionChains(chains ...*functionchain.FunctionChain) *hybridSearchOption {
	opt.functionChains = append([]*functionchain.FunctionChain(nil), chains...)
	return opt
}

// WithFunctionChains accepts at most one non-empty QueryScan chain.
func (opt *queryOption) WithFunctionChains(chains ...*functionchain.FunctionChain) *queryOption {
	opt.functionChains = append([]*functionchain.FunctionChain(nil), chains...)
	return opt
}

func buildFunctionChains(chains []*functionchain.FunctionChain, kind string, hasRanker bool) ([]*schemapb.FunctionChain, error) {
	if len(chains) == 0 {
		return nil, nil
	}
	if hasRanker {
		return nil, fmt.Errorf("function chains and ranker cannot be used together")
	}
	if kind == "query" && len(chains) > 1 {
		return nil, fmt.Errorf("query supports at most one QueryScan chain")
	}
	result := make([]*schemapb.FunctionChain, 0, len(chains))
	for i, c := range chains {
		p, err := c.ProtoMessage()
		if err != nil {
			return nil, fmt.Errorf("function chain %d: %w", i, err)
		}
		switch kind {
		case "search":
			if p.Stage == functionchain.Unspecified {
				return nil, fmt.Errorf("unspecified function chain stage is unsupported for search")
			}
		case "query":
			if p.Stage != functionchain.QueryScan || len(p.Ops) == 0 {
				return nil, fmt.Errorf("query requires a non-empty QueryScan chain")
			}
		}
		result = append(result, p)
	}
	return result, nil
}

// NewGrepOption creates a QueryScan grep query with limit 100. The optional
// matchField defaults to "matches". Matching details are returned as a JSON
// column; line numbers are one-based and spans are UTF-8 byte offsets.
// Requires a server supporting QueryScan grep. Older servers may ignore the
// chain and return ordinary query results without grep filtering or an error.
// Pagination uses WithLimit/WithOffset.
func NewGrepOption(collectionName, textField, arguments string, matchField ...string) *queryOption {
	output := "matches"
	if len(matchField) == 1 {
		output = matchField[0]
	}
	chain := functionchain.NewFunctionChain(functionchain.QueryScan).WithName("grep").WithOutputColumns(output).Grep(textField, arguments, matchField...)
	return NewQueryOption(collectionName).WithLimit(100).WithFunctionChains(chain)
}

// Grep executes a query configured with NewGrepOption. It returns matching-line
// details as a JSON column alongside ordinary fields, using the Query RPC.
// Callers must verify that their server supports QueryScan grep: older servers
// may silently ignore the chain and return unfiltered rows without match details.
// Empty results cannot distinguish unsupported servers from no matches. The SDK
// performs no server capability or returned-column checks.
func (c *Client) Grep(ctx context.Context, option QueryOption, callOptions ...grpc.CallOption) (ResultSet, error) {
	return c.Query(ctx, option, callOptions...)
}
