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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *Core) CreateRowPolicy(ctx context.Context, req *milvuspb.CreateRowPolicyRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported")), nil
}

func (c *Core) UpdateRowPolicy(ctx context.Context, req *milvuspb.UpdateRowPolicyRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported")), nil
}

func (c *Core) DropRowPolicy(ctx context.Context, req *milvuspb.DropRowPolicyRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported")), nil
}

func (c *Core) ListRowPolicies(ctx context.Context, req *milvuspb.ListRowPoliciesRequest) (*milvuspb.ListRowPoliciesResponse, error) {
	return &milvuspb.ListRowPoliciesResponse{Status: merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported"))}, nil
}

func (c *Core) SetRLSPrincipalTags(ctx context.Context, req *milvuspb.SetRLSPrincipalTagsRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported")), nil
}

func (c *Core) GetRLSPrincipalTags(ctx context.Context, req *milvuspb.GetRLSPrincipalTagsRequest) (*milvuspb.GetRLSPrincipalTagsResponse, error) {
	return &milvuspb.GetRLSPrincipalTagsResponse{Status: merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported"))}, nil
}

func (c *Core) ListRLSPrincipals(ctx context.Context, req *milvuspb.ListRLSPrincipalsRequest) (*milvuspb.ListRLSPrincipalsResponse, error) {
	return &milvuspb.ListRLSPrincipalsResponse{Status: merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported"))}, nil
}

func (c *Core) DeleteRLSPrincipalTags(ctx context.Context, req *milvuspb.DeleteRLSPrincipalTagsRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "RLS is not supported")), nil
}
