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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
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
	for _, replica := range replicas {
		if err := s.checkReplicaServiceable(ctx, replica); err != nil {
			return
		}
	}

	replicaIDs := make([]typeutil.UniqueID, 0, len(replicas))
	for _, replica := range replicas {
		replicaIDs = append(replicaIDs, replica.GetID())
	}
	collections := s.meta.SetReplicasQueryVisible(ctx, replicaIDs...)
	if len(collections) == 0 {
		return
	}

	log.Ctx(ctx).Info("load config replicas are query visible",
		zap.Int64s("collections", collections),
		zap.Int64s("replicas", replicaIDs))
	if s.proxyClientManager == nil {
		return
	}
	if err := s.proxyClientManager.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: collections,
	}); err != nil {
		log.Ctx(ctx).Warn("failed to invalidate proxy shard leader cache after promoting load config replicas",
			zap.Int64s("collections", collections),
			zap.Error(err))
	}
}
