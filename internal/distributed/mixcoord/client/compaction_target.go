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

package grpcmixcoordclient

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func (c *Client) CreateCompactionTarget(ctx context.Context, req *datapb.CreateCompactionTargetRequest, opts ...grpc.CallOption) (*datapb.CreateCompactionTargetResponse, error) {
	// Let the existing client acquire its connection and handle routing
	// errors, but cap application dispatch at one invocation and retain the
	// raw response, even if its business Status is normally retryable.
	var response *datapb.CreateCompactionTargetResponse
	called := false
	_, err := c.grpcClient.Call(retry.WithMaxAttemptsContext(ctx, 1), func(client MixCoordClient) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		called = true
		var err error
		response, err = client.CreateCompactionTarget(ctx, req, opts...)
		return response, err
	})
	if !called || status.Code(err) == codes.Unimplemented {
		return nil, err
	}
	if response != nil && response.GetStatus() != nil {
		return response, nil
	}
	return nil, merr.Wrap(merr.ErrCompactionTargetCreateOutcomeUnknown, "no reliable coordinator response")
}

func (c *Client) GetCompactionTargetBinding(ctx context.Context, req *datapb.GetCompactionTargetBindingRequest, opts ...grpc.CallOption) (*datapb.GetCompactionTargetBindingResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetCompactionTargetBindingResponse, error) {
		return client.GetCompactionTargetBinding(ctx, req, opts...)
	})
}

func (c *Client) DescribeCompactionTarget(ctx context.Context, req *datapb.DescribeCompactionTargetRequest, opts ...grpc.CallOption) (*datapb.DescribeCompactionTargetResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.DescribeCompactionTargetResponse, error) {
		return client.DescribeCompactionTarget(ctx, req, opts...)
	})
}

func (c *Client) ListCompactionTargets(ctx context.Context, req *datapb.ListCompactionTargetsRequest, opts ...grpc.CallOption) (*datapb.ListCompactionTargetsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.ListCompactionTargetsResponse, error) {
		return client.ListCompactionTargets(ctx, req, opts...)
	})
}

func (c *Client) DropCompactionTarget(ctx context.Context, req *datapb.DropCompactionTargetRequest, opts ...grpc.CallOption) (*datapb.DropCompactionTargetResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.DropCompactionTargetResponse, error) {
		return client.DropCompactionTarget(ctx, req, opts...)
	})
}
