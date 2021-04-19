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

	grpcproxynode "github.com/zilliztech/milvus-distributed/internal/distributed/proxynode"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type ProxyNode struct {
	svr *grpcproxynode.Server
}

func NewProxyNode(ctx context.Context, factory msgstream.Factory) (*ProxyNode, error) {
	var err error
	n := &ProxyNode{}

	svr, err := grpcproxynode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil
}

func (n *ProxyNode) Run() error {
	if err := n.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (n *ProxyNode) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}
