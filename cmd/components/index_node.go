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
	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// IndexNode implements IndexNode grpc server
type IndexNode struct {
	svr *grpcindexnode.Server
}

// NewIndexNode creates a new IndexNode
func NewIndexNode(ctx context.Context, factory dependency.Factory) (*IndexNode, error) {
	var err error
	n := &IndexNode{}
	svr, err := grpcindexnode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil

}

// Run starts service
func (n *IndexNode) Run() error {
	if err := n.svr.Run(); err != nil {
		log.Error("IndexNode starts error", zap.Error(err))
		return err
	}
	log.Debug("IndexNode successfully started")
	return nil
}

// Stop terminates service
func (n *IndexNode) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns IndexNode's states
func (n *IndexNode) Health(ctx context.Context) commonpb.StateCode {
	resp, err := n.svr.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	if err != nil {
		return commonpb.StateCode_Abnormal
	}
	return resp.State.GetStateCode()
}

func (n *IndexNode) GetName() string {
	return typeutil.IndexNodeRole
}
