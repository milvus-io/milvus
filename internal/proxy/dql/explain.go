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
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ExplainSearch describes the plan a search request would send to the query
// nodes without executing it. It runs only the search task's PreExecute,
// minus the query-text embedding through the collection's model provider
// and that step's checks (empty query text, batch limit, provider
// configuration), so nothing reaches the scheduler or a query node, and a
// request PreExecute rejects fails with the error Search would return.
func ExplainSearch(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.SearchRequest) (*planparserv2.PlanExplanation, error) {
	// one plan per sub-request
	if len(request.GetSubReqs()) > 0 {
		return nil, merr.WrapErrParameterInvalidMsg("explain does not support hybrid search")
	}
	// Same test as the search preflight: empty ids are an ordinary search;
	// non-empty ids are resolved by a query before any plan exists.
	if ids := request.GetIds(); ids != nil && typeutil.GetSizeOfIDs(ids) > 0 {
		return nil, merr.WrapErrParameterInvalidMsg("explain does not support search by primary key, pass query vectors instead")
	}
	// building a semantic highlighter connects to its model provider; a
	// lexical one is built from the schema and explained as the search
	// carries it
	if request.GetHighlighter().GetType() == commonpb.HighlightType_Semantic {
		return nil, merr.WrapErrParameterInvalidMsg("explain does not support semantic highlighting")
	}
	task := NewSearchTask(ctx, node, nil, request, false, false, nil)
	task.planOnly = true
	// The scheduler stamps a non-strong read with the local clock instead of
	// a TSO; PreExecute derives the collection TTL window from that stamp, so
	// a task that is never enqueued needs the same one.
	task.SetTs(tsoutil.ComposeTS(time.Now().UnixMilli(), 0))
	if err := task.PreExecute(ctx); err != nil {
		return nil, err
	}
	plan := &planpb.PlanNode{}
	if err := proto.Unmarshal(task.SerializedExprPlan, plan); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "decode the plan PreExecute serialized")
	}
	// nil options: the caller supplied the template values, so nothing needs
	// redacting, and candidate indexes would need the index descriptions.
	return planparserv2.ExplainPlan(plan, task.schema.SchemaHelper, nil)
}
