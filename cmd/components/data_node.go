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
	grpcdatanode "github.com/milvus-io/milvus/internal/distributed/datanode"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// DataNode implements DataNode grpc server
type DataNode struct {
	ctx context.Context
	svr *grpcdatanode.Server
}

// NewDataNode creates a new DataNode
func NewDataNode(ctx context.Context, factory dependency.Factory) (*DataNode, error) {
	svr, err := grpcdatanode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &DataNode{
		ctx: ctx,
		svr: svr,
	}, nil
}

// Run starts service
func (d *DataNode) Run() error {
	if err := d.svr.Run(); err != nil {
		log.Error("DataNode starts error", zap.Error(err))
		return err
	}
	log.Debug("Datanode successfully started")
	return nil
}

// Stop terminates service
func (d *DataNode) Stop() error {
	if err := d.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns DataNode's states
func (d *DataNode) Health(ctx context.Context) commonpb.StateCode {
	resp, err := d.svr.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (d *DataNode) GetName() string {
	return typeutil.DataNodeRole
}
