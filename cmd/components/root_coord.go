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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	rc "github.com/milvus-io/milvus/internal/distributed/rootcoord"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// RootCoord implements RoodCoord grpc server
type RootCoord struct {
	ctx context.Context
	svr *rc.Server
}

// NewRootCoord creates a new RoorCoord
func NewRootCoord(ctx context.Context, factory dependency.Factory) (*RootCoord, error) {
	svr, err := rc.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &RootCoord{
		ctx: ctx,
		svr: svr,
	}, nil
}

// Run starts service
func (rc *RootCoord) Run() error {
	if err := rc.svr.Run(); err != nil {
		log.Error("RootCoord starts error", zap.Error(err))
		return err
	}
	log.Info("RootCoord successfully started")
	return nil
}

// Stop terminates service
func (rc *RootCoord) Stop() error {
	if err := rc.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns RootCoord's states
func (rc *RootCoord) Health(ctx context.Context) commonpb.StateCode {
	resp, err := rc.svr.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (rc *RootCoord) GetName() string {
	return typeutil.RootCoordRole
}
