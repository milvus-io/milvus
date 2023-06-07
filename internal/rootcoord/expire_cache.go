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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type expireCacheConfig struct {
	withDropFlag bool
}

func (c expireCacheConfig) apply(req *proxypb.InvalidateCollMetaCacheRequest) {
	if !c.withDropFlag {
		return
	}
	if req.GetBase() == nil {
		req.Base = commonpbutil.NewMsgBase()
	}
	req.Base.MsgType = commonpb.MsgType_DropCollection
}

func defaultExpireCacheConfig() expireCacheConfig {
	return expireCacheConfig{withDropFlag: false}
}

type expireCacheOpt func(c *expireCacheConfig)

func expireCacheWithDropFlag() expireCacheOpt {
	return func(c *expireCacheConfig) {
		c.withDropFlag = true
	}
}

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, dbName string, collNames []string, collectionID UniqueID, ts typeutil.Timestamp, opts ...expireCacheOpt) error {
	// if collectionID is specified, invalidate all the collection meta cache with the specified collectionID and return
	if collectionID != InvalidCollectionID {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(c.session.ServerID),
			),
			DbName:       dbName,
			CollectionID: collectionID,
		}
		return c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req, opts...)
	}

	// if only collNames are specified, invalidate the collection meta cache with the specified collectionName
	for _, collName := range collNames {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(0), //TODO, msg type
				commonpbutil.WithMsgID(0),   //TODO, msg id
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(c.session.ServerID),
			),
			DbName:         dbName,
			CollectionName: collName,
		}
		err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req, opts...)
		if err != nil {
			// TODO: try to expire all or directly return err?
			return err
		}
	}
	return nil
}
