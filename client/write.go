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

package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func (c *Client) Insert(ctx context.Context, option InsertOption, callOptions ...grpc.CallOption) error {
	collection, err := c.getCollection(ctx, option.CollectionName())
	if err != nil {
		return err
	}
	req, err := option.InsertRequest(collection)
	if err != nil {
		return err
	}
	err = c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Insert(ctx, req, callOptions...)

		return merr.CheckRPCCall(resp, err)
	})
	return err
}

func (c *Client) Delete(ctx context.Context, option DeleteOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Delete(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (c *Client) Upsert(ctx context.Context, option UpsertOption, callOptions ...grpc.CallOption) error {
	collection, err := c.getCollection(ctx, option.CollectionName())
	if err != nil {
		return err
	}
	req, err := option.UpsertRequest(collection)
	if err != nil {
		return err
	}

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Upsert(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}
