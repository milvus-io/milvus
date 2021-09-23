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

	grpcproxy "github.com/milvus-io/milvus/internal/distributed/proxy"
	"github.com/milvus-io/milvus/internal/msgstream"
)

type Proxy struct {
	svr *grpcproxy.Server
}

// NewProxy creates a new Proxy
func NewProxy(ctx context.Context, factory msgstream.Factory) (*Proxy, error) {
	var err error
	n := &Proxy{}

	svr, err := grpcproxy.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil
}

// Run starts service
func (n *Proxy) Run() error {
	if err := n.svr.Run(); err != nil {
		return err
	}
	return nil
}

// Stop terminates service
func (n *Proxy) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}

func (n *Proxy) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return n.svr.GetComponentStates(ctx, request)
}
