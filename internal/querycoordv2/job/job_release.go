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
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type ReleaseCollectionJob struct {
	*BaseJob
	req               *querypb.ReleaseCollectionRequest
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
	req *querypb.ReleaseCollectionRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	checkerController *checkers.CheckerController,
	proxyManager proxyutil.ProxyClientManagerInterface,
) *ReleaseCollectionJob {
	return &ReleaseCollectionJob{
		BaseJob:           NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:               req,
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
	req := job.req
	log := log.Ctx(job.ctx).With(zap.Int64("collectionID", req.GetCollectionID()))

	if !job.meta.CollectionManager.Exist(job.ctx, req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	err := job.meta.CollectionManager.RemoveCollection(job.ctx, req.GetCollectionID())
	if err != nil {
		msg := "failed to remove collection"
		log.Warn(msg, zap.Error(err))
		return errors.Wrap(err, msg)
	}

	err = job.meta.ReplicaManager.RemoveCollection(job.ctx, req.GetCollectionID())
	if err != nil {
		msg := "failed to remove replicas"
		log.Warn(msg, zap.Error(err))
	}

	job.targetObserver.ReleaseCollection(req.GetCollectionID())

	// try best discard cache
	// shall not affect releasing if failed
	job.proxyManager.InvalidateCollectionMetaCache(job.ctx,
		&proxypb.InvalidateCollMetaCacheRequest{
			CollectionID: req.GetCollectionID(),
		},
		proxyutil.SetMsgType(commonpb.MsgType_ReleaseCollection))

	// try best clean shard leader cache
	job.proxyManager.InvalidateShardLeaderCache(job.ctx, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: []int64{req.GetCollectionID()},
	})

	waitCollectionReleased(job.dist, job.checkerController, req.GetCollectionID())
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return nil
}

type ReleasePartitionJob struct {
	*BaseJob
	releasePartitionsOnly bool

	req               *querypb.ReleasePartitionsRequest
	dist              *meta.DistributionManager
	meta              *meta.Meta
	broker            meta.Broker
	cluster           session.Cluster
	targetMgr         meta.TargetManagerInterface
	targetObserver    *observers.TargetObserver
	checkerController *checkers.CheckerController
	proxyManager      proxyutil.ProxyClientManagerInterface
}

func NewReleasePartitionJob(ctx context.Context,
	req *querypb.ReleasePartitionsRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	checkerController *checkers.CheckerController,
	proxyManager proxyutil.ProxyClientManagerInterface,
) *ReleasePartitionJob {
	return &ReleasePartitionJob{
		BaseJob:           NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:               req,
		dist:              dist,
		meta:              meta,
		broker:            broker,
		targetMgr:         targetMgr,
		targetObserver:    targetObserver,
		checkerController: checkerController,
		proxyManager:      proxyManager,
	}
}

func (job *ReleasePartitionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	if !job.meta.CollectionManager.Exist(job.ctx, req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	loadedPartitions := job.meta.CollectionManager.GetPartitionsByCollection(job.ctx, req.GetCollectionID())
	toRelease := lo.FilterMap(loadedPartitions, func(partition *meta.Partition, _ int) (int64, bool) {
		return partition.GetPartitionID(), lo.Contains(req.GetPartitionIDs(), partition.GetPartitionID())
	})

	if len(toRelease) == 0 {
		log.Warn("releasing partition(s) not loaded")
		return nil
	}

	// If all partitions are released, clear all
	if len(toRelease) == len(loadedPartitions) {
		log.Info("release partitions covers all partitions, will remove the whole collection")
		err := job.meta.CollectionManager.RemoveCollection(job.ctx, req.GetCollectionID())
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			return errors.Wrap(err, msg)
		}
		err = job.meta.ReplicaManager.RemoveCollection(job.ctx, req.GetCollectionID())
		if err != nil {
			log.Warn("failed to remove replicas", zap.Error(err))
		}
		job.targetObserver.ReleaseCollection(req.GetCollectionID())
		// try best discard cache
		// shall not affect releasing if failed
		job.proxyManager.InvalidateCollectionMetaCache(job.ctx,
			&proxypb.InvalidateCollMetaCacheRequest{
				CollectionID: req.GetCollectionID(),
			},
			proxyutil.SetMsgType(commonpb.MsgType_ReleaseCollection))
		// try best clean shard leader cache
		job.proxyManager.InvalidateShardLeaderCache(job.ctx, &proxypb.InvalidateShardLeaderCacheRequest{
			CollectionIDs: []int64{req.GetCollectionID()},
		})

		waitCollectionReleased(job.dist, job.checkerController, req.GetCollectionID())
	} else {
		err := job.meta.CollectionManager.RemovePartition(job.ctx, req.GetCollectionID(), toRelease...)
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			return errors.Wrap(err, msg)
		}
		job.targetObserver.ReleasePartition(req.GetCollectionID(), toRelease...)
		// wait current target updated, so following querys will act as expected
		waitCurrentTargetUpdated(job.ctx, job.targetObserver, job.req.GetCollectionID())
		waitCollectionReleased(job.dist, job.checkerController, req.GetCollectionID(), toRelease...)
	}
	return nil
}
