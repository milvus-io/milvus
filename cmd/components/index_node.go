// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package components

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
)

// IndexNode implements IndexNode grpc server
type IndexNode struct {
	svr *grpcindexnode.Server
}

// NewIndexNode creates a new IndexNode
func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	var err error
	n := &IndexNode{}
	svr, err := grpcindexnode.NewServer(ctx)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil

}

// Run starts service
func (n *IndexNode) Run() error {
	if err := n.svr.Run(); err != nil {
		return err
	}
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
func (n *IndexNode) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return n.svr.GetComponentStates(ctx, request)
}
