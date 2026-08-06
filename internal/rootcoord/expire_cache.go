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
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, dbName string, collNames []string, collectionID UniqueID, partitionName string, ts typeutil.Timestamp, opts ...proxyutil.ExpireCacheOpt) error {
	// if only collNames are specified, invalidate the collection meta cache with the specified collectionName
	for _, collName := range collNames {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(c.session.GetServerID()),
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

// ExpireTransferMetaCache invalidates every name that can resolve to a
// transferred collection and also carries the collection id for id-keyed caches.
func (c *Core) ExpireTransferMetaCache(ctx context.Context, dbName string, collectionName string, aliases []string, collectionID UniqueID, ts typeutil.Timestamp, opts ...proxyutil.ExpireCacheOpt) error {
	names := make([]string, 0, len(aliases)+1)
	seen := make(map[string]struct{}, len(aliases)+1)
	appendName := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	appendName(collectionName)
	for _, alias := range aliases {
		appendName(alias)
	}
	if len(names) == 0 && collectionID != InvalidCollectionID {
		names = append(names, "")
	}
	return c.ExpireMetaCache(ctx, dbName, names, collectionID, "", ts, opts...)
}
