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

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) tryPromoteReadyLoadConfigReplicas(ctx context.Context) {
	if s == nil || s.meta == nil || s.meta.ReplicaManager == nil ||
		s.targetMgr == nil || s.dist == nil || s.dist.ChannelDistManager == nil || s.nodeMgr == nil {
		return
	}
	replicas := s.meta.GetQueryInvisibleReplicas(ctx)
	if len(replicas) == 0 {
		return
	}
	replicaIDs := make([]typeutil.UniqueID, 0, len(replicas))
	for _, replica := range replicas {
		if err := s.checkReplicaServiceable(ctx, replica); err != nil {
			continue
		}
		replicaIDs = append(replicaIDs, replica.GetID())
	}
	if len(replicaIDs) == 0 {
		return
	}

	collections := s.meta.SetReplicasQueryVisible(ctx, replicaIDs...)
	if len(collections) == 0 {
		return
	}

	mlog.Info(ctx, "load config replicas are query visible",
		mlog.Int64s("collections", collections),
		mlog.Int64s("replicas", replicaIDs))
	if s.proxyClientManager == nil {
		return
	}
	if err := s.proxyClientManager.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: collections,
	}); err != nil {
		mlog.Warn(ctx, "failed to invalidate proxy shard leader cache after promoting load config replicas",
			mlog.Int64s("collections", collections),
			mlog.Err(err))
	}
}
