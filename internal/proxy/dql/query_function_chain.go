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

package dql

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// prepareQueryFunctionChains checks request shape and routes chains to QueryNode.
// Operator semantics, field dependencies and service limits belong to QueryNode.
// Forward an owned copy without changing parameters or the output projection.
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
	chains := t.request.GetFunctionChains()
	seen := make(map[schemapb.FunctionChainStage]struct{}, len(chains))
	forwarded := make([]*schemapb.FunctionChain, 0, len(chains))
	for i, pb := range chains {
		if pb == nil {
			return merr.WrapErrParameterInvalidMsg("query function chain[%d] is nil", i)
		}
		stage := pb.GetStage()
		if stage != schemapb.FunctionChainStage_FunctionChainStageQueryScan {
			return merr.WrapErrParameterInvalidMsg("function chain stage %s is not supported in Query", stage.String())
		}
		if _, exists := seen[stage]; exists {
			return merr.WrapErrParameterInvalidMsg("query function chain stage %s appears more than once", stage.String())
		}
		seen[stage] = struct{}{}
		if len(pb.GetOps()) == 0 {
			return merr.WrapErrParameterInvalidMsg("query function chain[%d] must contain at least one op", i)
		}
		forwarded = append(forwarded, proto.Clone(pb).(*schemapb.FunctionChain))
	}
	t.plan.QuerynodeFunctionChains = forwarded
	return nil
}
