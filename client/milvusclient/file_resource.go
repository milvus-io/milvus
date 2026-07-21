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

	"github.com/samber/lo"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// AddFileResource registers a remote file with Milvus.
func (c *Client) AddFileResource(ctx context.Context, option AddFileResourceOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("AddFileResourceOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.AddFileResource(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// RemoveFileResource unregisters a remote file from Milvus.
func (c *Client) RemoveFileResource(ctx context.Context, option RemoveFileResourceOption, callOptions ...grpc.CallOption) error {
	if option == nil {
		return merr.WrapErrParameterInvalid("RemoveFileResourceOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.RemoveFileResource(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// ListFileResources lists all remote files registered with Milvus.
func (c *Client) ListFileResources(ctx context.Context, option ListFileResourcesOption, callOptions ...grpc.CallOption) ([]*entity.FileResource, error) {
	if option == nil {
		return nil, merr.WrapErrParameterInvalid("ListFileResourcesOption", "nil", "option cannot be nil")
	}

	req := option.Request()
	var resources []*entity.FileResource
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.ListFileResources(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		resources = lo.Map(resp.GetResources(), func(resource *milvuspb.FileResourceInfo, _ int) *entity.FileResource {
			return &entity.FileResource{
				ID:   resource.GetId(),
				Name: resource.GetName(),
				Path: resource.GetPath(),
			}
		})
		return nil
	})
	return resources, err
}
