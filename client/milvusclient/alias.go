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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func (c *Client) CreateAlias(ctx context.Context, option CreateAliasOption, callOptions ...grpc.CallOption) error {
	req := option.Request()
	// capture the database before the RPC so a concurrent UseDatabase cannot
	// move the cache update to the wrong (database, collection) key
	db := c.getCurrentDB()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreateAlias(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
	if err != nil {
		return err
	}
	// an alias resolves to the collection, so share its write timestamp;
	// evict the alias schema cache in case it was cached before the alias existed
	c.copyWriteTsToAlias(db, req.GetCollectionName(), req.GetAlias())
	return nil
}

func (c *Client) DescribeAlias(ctx context.Context, option DescribeAliasOption, callOptions ...grpc.CallOption) (*entity.Alias, error) {
	req := option.Request()

	var alias *entity.Alias
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DescribeAlias(ctx, req, callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		alias = &entity.Alias{
			DbName:         resp.GetDbName(),
			Alias:          resp.GetAlias(),
			CollectionName: resp.GetCollection(),
		}

		return nil
	})

	return alias, err
}

func (c *Client) DropAlias(ctx context.Context, option DropAliasOption, callOptions ...grpc.CallOption) error {
	req := option.Request()
	// capture the database before the RPC so a concurrent UseDatabase cannot
	// move the cache eviction to the wrong (database, collection) key
	db := c.getCurrentDB()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DropAlias(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
	if err != nil {
		return err
	}
	// the alias no longer resolves to any collection, drop its cached
	// write timestamp and schema
	aliasKey := newCollectionCacheKey(db, req.GetAlias())
	c.collCache.EvictWriteTs(aliasKey)
	c.collCache.Evict(aliasKey)
	return nil
}

func (c *Client) AlterAlias(ctx context.Context, option AlterAliasOption, callOptions ...grpc.CallOption) error {
	req := option.Request()
	// capture the database before the RPC so a concurrent UseDatabase cannot
	// move the cache update to the wrong (database, collection) key
	db := c.getCurrentDB()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.AlterAlias(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
	if err != nil {
		return err
	}
	// the alias now resolves to a (possibly different) collection, refresh
	// its write timestamp from the collection and evict the alias schema
	c.copyWriteTsToAlias(db, req.GetCollectionName(), req.GetAlias())
	return nil
}

// copyWriteTsToAlias shares the collection's write timestamp with the alias and
// evicts any cached alias schema. Mirrors the java/cpp CollectionTsCache.copy
// + schema invalidation performed on create/alter alias.
//
// The alias write timestamp is a snapshot taken at create/alter-alias time:
// writes through the collection name update only the collection entry, so a
// Session-consistency read issued through the alias can fall back to
// eventually(1) until the next alias operation. This mirrors pymilvus/java/cpp
// and is a known shared limitation across all Milvus SDKs.
func (c *Client) copyWriteTsToAlias(db, collectionName, alias string) {
	c.collCache.CopyWriteTs(newCollectionCacheKey(db, collectionName), newCollectionCacheKey(db, alias))
	c.collCache.Evict(newCollectionCacheKey(db, alias))
}

func (c *Client) ListAliases(ctx context.Context, option ListAliasesOption, callOptions ...grpc.CallOption) ([]string, error) {
	req := option.Request()

	var aliases []string
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListAliases(ctx, req, callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		aliases = resp.GetAliases()
		return nil
	})

	return aliases, err
}
