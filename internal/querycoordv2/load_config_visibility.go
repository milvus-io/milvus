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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	// Keep query traffic out of a resource group until its pending replicas
	// across all collections are ready.
	byResourceGroup := lo.GroupBy(replicas, (*meta.Replica).GetResourceGroup)
	replicaIDs := make([]typeutil.UniqueID, 0, len(replicas))
	for _, group := range byResourceGroup {
		if lo.EveryBy(group, func(replica *meta.Replica) bool {
			return s.checkReplicaServiceable(ctx, replica) == nil
		}) {
			for _, replica := range group {
				replicaIDs = append(replicaIDs, replica.GetID())
			}
		}
	}
	if len(replicaIDs) == 0 {
		return
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
