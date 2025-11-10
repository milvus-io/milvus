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

package job

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type ReleaseCollectionJob struct {
	*BaseJob
	result            message.BroadcastResultDropLoadConfigMessageV2
	dist              *meta.DistributionManager
	meta              *meta.Meta
	broker            meta.Broker
	cluster           session.Cluster
	targetMgr         meta.TargetManagerInterface
	targetObserver    *observers.TargetObserver
	checkerController *checkers.CheckerController
	proxyManager      proxyutil.ProxyClientManagerInterface
}

func NewReleaseCollectionJob(ctx context.Context,
	result message.BroadcastResultDropLoadConfigMessageV2,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	checkerController *checkers.CheckerController,
	proxyManager proxyutil.ProxyClientManagerInterface,
) *ReleaseCollectionJob {
	return &ReleaseCollectionJob{
		BaseJob:           NewBaseJob(ctx, 0, result.Message.Header().GetCollectionId()),
		result:            result,
		dist:              dist,
		meta:              meta,
		broker:            broker,
		targetMgr:         targetMgr,
		targetObserver:    targetObserver,
		checkerController: checkerController,
		proxyManager:      proxyManager,
	}
}

func (job *ReleaseCollectionJob) Execute() error {
	collectionID := job.result.Message.Header().GetCollectionId()
	log := log.Ctx(job.ctx).With(zap.Int64("collectionID", collectionID))

	if !job.meta.CollectionManager.Exist(job.ctx, collectionID) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	err := job.meta.CollectionManager.RemoveCollection(job.ctx, collectionID)
	if err != nil {
		msg := "failed to remove collection"
		log.Warn(msg, zap.Error(err))
		return errors.Wrap(err, msg)
	}

	err = job.meta.ReplicaManager.RemoveCollection(job.ctx, collectionID)
	if err != nil {
		msg := "failed to remove replicas"
		log.Warn(msg, zap.Error(err))
	}

	job.targetObserver.ReleaseCollection(collectionID)

	// try best discard cache
	// shall not affect releasing if failed
	job.proxyManager.InvalidateCollectionMetaCache(job.ctx,
		&proxypb.InvalidateCollMetaCacheRequest{
			CollectionID: collectionID,
		},
		proxyutil.SetMsgType(commonpb.MsgType_ReleaseCollection))

	// try best clean shard leader cache
	job.proxyManager.InvalidateShardLeaderCache(job.ctx, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: []int64{collectionID},
	})

	if err = WaitCollectionReleased(job.ctx, job.dist, job.checkerController, collectionID); err != nil {
		log.Warn("failed to wait collection released", zap.Error(err))
		// return nil to avoid infinite retry on DDL callback
		return nil
	}
	log.Info("release collection job done", zap.Int64("collectionID", collectionID))
	return nil
}
