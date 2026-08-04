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

package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type RootCoordTransferRPCClient struct {
	client rootcoordpb.RootCoordClient
}

func NewRootCoordTransferRPCClient(client rootcoordpb.RootCoordClient) *RootCoordTransferRPCClient {
	return &RootCoordTransferRPCClient{client: client}
}

func (c *RootCoordTransferRPCClient) CatalogTransferPrepare(ctx context.Context, req *rootcoordpb.CatalogTransferPrepareRequest) error {
	status, err := c.client.CatalogTransferPrepare(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (c *RootCoordTransferRPCClient) CatalogTransferDeactivate(ctx context.Context, req *rootcoordpb.CatalogTransferDeactivateRequest) error {
	status, err := c.client.CatalogTransferDeactivate(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (c *RootCoordTransferRPCClient) CatalogTransferApply(ctx context.Context, req *rootcoordpb.CatalogTransferApplyRequest) error {
	status, err := c.client.CatalogTransferApply(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (c *RootCoordTransferRPCClient) CatalogTransferAbort(ctx context.Context, req *rootcoordpb.CatalogTransferAbortRequest) error {
	status, err := c.client.CatalogTransferAbort(ctx, req)
	return merr.CheckRPCCall(status, err)
}
