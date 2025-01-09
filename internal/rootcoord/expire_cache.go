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

	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, dbName string, collNames []string, collectionID UniqueID, partitionName string, ts typeutil.Timestamp, opts ...proxyutil.ExpireCacheOpt) error {
	// if only collNames are specified, invalidate the collection meta cache with the specified collectionName
	for _, collName := range collNames {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(c.session.ServerID),
			),
			DbName:         dbName,
			CollectionName: collName,
			CollectionID:   collectionID,
			PartitionName:  partitionName,
		}
		err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req, opts...)
		if err != nil {
			// TODO: try to expire all or directly return err?
			return err
		}
	}
	return nil
}
