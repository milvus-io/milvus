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
	"math"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type InsertResult struct {
	InsertCount int64
	IDs         column.Column
}

func (c *Client) Insert(ctx context.Context, option InsertOption, callOptions ...grpc.CallOption) (InsertResult, error) {
	result := InsertResult{}
	err := c.retryIfSchemaError(ctx, option.CollectionName(), func(ctx context.Context) (uint64, error) {
		collection, err := c.getCollection(ctx, option.CollectionName())
		if err != nil {
			return math.MaxUint64, err
		}
		req, err := option.InsertRequest(collection)
		if err != nil {
			// return schema mismatch err to retry with newer schema
			return collection.UpdateTimestamp, merr.WrapErrCollectionSchemaMisMatch(err)
		}

		return collection.UpdateTimestamp, c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
			resp, err := milvusService.Insert(ctx, req, callOptions...)

			err = merr.CheckRPCCall(resp, err)
			if err != nil {
				return err
			}

			result.InsertCount = resp.GetInsertCnt()
			result.IDs, err = column.IDColumns(collection.Schema, resp.GetIDs(), 0, -1)
			if err != nil {
				return err
			}

			// write back pks if needed
			// pks values shall be written back to struct if receiver field exists
			return option.WriteBackPKs(collection.Schema, result.IDs)
		})
	})
	return result, err
}

type DeleteResult struct {
	DeleteCount int64
}

func (c *Client) Delete(ctx context.Context, option DeleteOption, callOptions ...grpc.CallOption) (DeleteResult, error) {
	req := option.Request()

	result := DeleteResult{}
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.Delete(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		result.DeleteCount = resp.GetDeleteCnt()
		return nil
	})
	return result, err
}

type UpsertResult struct {
	UpsertCount int64
	IDs         column.Column
}

func (c *Client) Upsert(ctx context.Context, option UpsertOption, callOptions ...grpc.CallOption) (UpsertResult, error) {
	result := UpsertResult{}
	err := c.retryIfSchemaError(ctx, option.CollectionName(), func(ctx context.Context) (uint64, error) {
		collection, err := c.getCollection(ctx, option.CollectionName())
		if err != nil {
			return math.MaxUint64, err
		}
		req, err := option.UpsertRequest(collection)
		if err != nil {
			// return schema mismatch err to retry with newer schema
			return collection.UpdateTimestamp, merr.WrapErrCollectionSchemaMisMatch(err)
		}
		return collection.UpdateTimestamp, c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
			resp, err := milvusService.Upsert(ctx, req, callOptions...)
			if err = merr.CheckRPCCall(resp, err); err != nil {
				return err
			}
			result.UpsertCount = resp.GetUpsertCnt()
			result.IDs, err = column.IDColumns(collection.Schema, resp.GetIDs(), 0, -1)
			if err != nil {
				return err
			}
			return nil
		})
	})
	return result, err
}
