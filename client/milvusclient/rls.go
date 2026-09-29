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

package milvusclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// CreateRowPolicy creates a row policy for a collection.
func (c *Client) CreateRowPolicy(ctx context.Context, option CreateRowPolicyOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("CreateRowPolicyOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.CreateRowPolicy(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// UpdateRowPolicy replaces an existing row policy definition.
func (c *Client) UpdateRowPolicy(ctx context.Context, option UpdateRowPolicyOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("UpdateRowPolicyOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.UpdateRowPolicy(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// DropRowPolicy drops a row policy from a collection.
func (c *Client) DropRowPolicy(ctx context.Context, option DropRowPolicyOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("DropRowPolicyOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.DropRowPolicy(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// ListRowPolicies lists row policies for a collection.
func (c *Client) ListRowPolicies(ctx context.Context, option ListRowPoliciesOption, callOptions ...grpc.CallOption) ([]*milvuspb.RowPolicy, error) {
	if option == nil {
		return nil, merr.WrapErrParameterInvalid("ListRowPoliciesOption", "nil", "option cannot be nil")
	}

	var policies []*milvuspb.RowPolicy
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.ListRowPolicies(ctx, option.Request(), callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		policies = resp.GetPolicies()
		return nil
	})
	return policies, err
}

// SetRLSPrincipalTags incrementally sets tags for a principal in a collection.
func (c *Client) SetRLSPrincipalTags(ctx context.Context, option SetRLSPrincipalTagsOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("SetRLSPrincipalTagsOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.SetRLSPrincipalTags(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// GetRLSPrincipalTags returns the stored JSON tag object for a principal.
func (c *Client) GetRLSPrincipalTags(ctx context.Context, option GetRLSPrincipalTagsOption, callOptions ...grpc.CallOption) (string, error) {
	if option == nil {
		return "", merr.WrapErrParameterInvalid("GetRLSPrincipalTagsOption", "nil", "option cannot be nil")
	}

	var tags string
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.GetRLSPrincipalTags(ctx, option.Request(), callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		tags = resp.GetTags()
		return nil
	})
	return tags, err
}

// ListRLSPrincipals lists principal identifiers that have stored tags.
func (c *Client) ListRLSPrincipals(ctx context.Context, option ListRLSPrincipalsOption, callOptions ...grpc.CallOption) ([]string, error) {
	if option == nil {
		return nil, merr.WrapErrParameterInvalid("ListRLSPrincipalsOption", "nil", "option cannot be nil")
	}

	var principals []string
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.ListRLSPrincipals(ctx, option.Request(), callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		principals = resp.GetPrincipalNames()
		return nil
	})
	return principals, err
}

// DeleteRLSPrincipalTags deletes selected tags, or all tags when no keys are provided.
func (c *Client) DeleteRLSPrincipalTags(ctx context.Context, option DeleteRLSPrincipalTagsOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("DeleteRLSPrincipalTagsOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.DeleteRLSPrincipalTags(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}
