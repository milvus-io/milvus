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
	grpcquerycoord "github.com/milvus-io/milvus/internal/distributed/querycoord"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// QueryCoord implements QueryCoord grpc server
type QueryCoord struct {
	ctx context.Context
	svr *grpcquerycoord.Server
}

// NewQueryCoord creates a new QueryCoord
func NewQueryCoord(ctx context.Context, factory dependency.Factory) (*QueryCoord, error) {
	svr, err := grpcquerycoord.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &QueryCoord{
		ctx: ctx,
		svr: svr,
	}, nil
}

// Run starts service
func (qs *QueryCoord) Run() error {
	if err := qs.svr.Run(); err != nil {
		log.Error("QueryCoord starts error", zap.Error(err))
		return err
	}
	log.Debug("QueryCoord successfully started")
	return nil
}

// Stop terminates service
func (qs *QueryCoord) Stop() error {
	if err := qs.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns QueryCoord's states
func (qs *QueryCoord) Health(ctx context.Context) commonpb.StateCode {
	resp, err := qs.svr.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (qs *QueryCoord) GetName() string {
	return typeutil.QueryCoordRole
}
