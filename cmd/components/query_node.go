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

package components

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcquerynode "github.com/milvus-io/milvus/internal/distributed/querynode"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// QueryNode implements QueryNode grpc server
type QueryNode struct {
	ctx context.Context
	svr *grpcquerynode.Server
}

// NewQueryNode creates a new QueryNode
func NewQueryNode(ctx context.Context, factory dependency.Factory) (*QueryNode, error) {
	svr, err := grpcquerynode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &QueryNode{
		ctx: ctx,
		svr: svr,
	}, nil

}

// Run starts service
func (q *QueryNode) Run() error {
	if err := q.svr.Run(); err != nil {
		log.Error("QueryNode starts error", zap.Error(err))
		return err
	}
	log.Debug("QueryNode successfully started")
	return nil
}

// Stop terminates service
func (q *QueryNode) Stop() error {
	if err := q.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns QueryNode's states
func (q *QueryNode) Health(ctx context.Context) commonpb.StateCode {
	resp, err := q.svr.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (q *QueryNode) GetName() string {
	return typeutil.QueryNodeRole
}
