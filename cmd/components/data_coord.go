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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcdatacoordclient "github.com/milvus-io/milvus/internal/distributed/datacoord"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DataCoord implements grpc server of DataCoord server
type DataCoord struct {
	ctx context.Context
	svr *grpcdatacoordclient.Server
}

// NewDataCoord creates a new DataCoord
func NewDataCoord(ctx context.Context, factory dependency.Factory) (*DataCoord, error) {
	s, err := grpcdatacoordclient.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &DataCoord{
		ctx: ctx,
		svr: s,
	}, nil
}

// Prepare prepares service
func (s *DataCoord) Prepare() error {
	return s.svr.Prepare()
}

// Run starts service
func (s *DataCoord) Run() error {
	if err := s.svr.Run(); err != nil {
		log.Ctx(s.ctx).Error("DataCoord starts error", zap.Error(err))
		return err
	}
	log.Ctx(s.ctx).Debug("DataCoord successfully started")
	return nil
}

// Stop terminates service
func (s *DataCoord) Stop() error {
	timeout := paramtable.Get().DataCoordCfg.GracefulStopTimeout.GetAsDuration(time.Second)
	return exitWhenStopTimeout(s.svr.Stop, timeout)
}

// GetComponentStates returns DataCoord's states
func (s *DataCoord) Health(ctx context.Context) commonpb.StateCode {
	resp, err := s.svr.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (s *DataCoord) GetName() string {
	return typeutil.DataCoordRole
}
