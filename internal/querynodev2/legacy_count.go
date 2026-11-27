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

package querynodev2

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// upgradeLegacyCountRequest rewrites a count(*) request that a 2.6 sender
// built into the aggregate form that this node executes.
//
// A 2.6 Proxy or delegator encodes count(*) as RetrieveRequest.IsCount plus
// QueryPlanNode.is_count, with no Aggregates. This node selects the
// aggregation path from Aggregates alone. Without this rewrite the request
// runs as a plain retrieve with no output fields and answers with an empty
// result, which the 2.6 sender rejects.
//
// The rewrite adds one count aggregate to the request and to the serialized
// plan. It keeps IsCount and is_count set, so a 2.6 worker that receives the
// forwarded request still answers with the legacy count column. That column
// has the shape of one count aggregate, and the aggregate reduce accepts it.
//
// A request that already carries Aggregates or GroupByFieldIds is not legacy
// and is returned unchanged. A legacy request whose plan cannot be rewritten
// is rejected, so that it does not run as an empty plain retrieve.
func upgradeLegacyCountRequest(ctx context.Context, req *internalpb.RetrieveRequest) error {
	if !req.GetIsCount() || len(req.GetAggregates()) > 0 || len(req.GetGroupByFieldIds()) > 0 {
		return nil
	}

	err := rewriteLegacyCountRequest(req)
	if err != nil {
		mlog.Warn(ctx, "failed to upgrade legacy count request", mlog.Err(err))
	}
	return err
}

func rewriteLegacyCountRequest(req *internalpb.RetrieveRequest) error {
	plan := &planpb.PlanNode{}
	if err := proto.Unmarshal(req.GetSerializedExprPlan(), plan); err != nil {
		return merr.Wrap(err, "failed to unmarshal legacy count(*) plan")
	}
	query := plan.GetQuery()
	if query == nil {
		return merr.WrapErrServiceInternalMsg("legacy count(*) request carries no query plan")
	}
	query.Aggregates = []*planpb.Aggregate{countAllAggregate()}

	serialized, err := proto.Marshal(plan)
	if err != nil {
		return merr.Wrap(err, "failed to marshal upgraded count(*) plan")
	}
	req.SerializedExprPlan = serialized
	req.Aggregates = []*planpb.Aggregate{countAllAggregate()}
	return nil
}

// countAllAggregate is count(*): the Proxy encodes it as a count aggregate
// with field id 0.
func countAllAggregate() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_count, FieldId: 0}
}
