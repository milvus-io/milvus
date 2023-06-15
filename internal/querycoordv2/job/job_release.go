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

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
)

type ReleaseCollectionJob struct {
	*BaseJob
	req               *querypb.ReleaseCollectionRequest
	dist              *meta.DistributionManager
	meta              *meta.Meta
	broker            meta.Broker
	cluster           session.Cluster
	targetMgr         *meta.TargetManager
	targetObserver    *observers.TargetObserver
	checkerController *checkers.CheckerController
}

func NewReleaseCollectionJob(ctx context.Context,
	req *querypb.ReleaseCollectionRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	cluster session.Cluster,
	targetMgr *meta.TargetManager,
	targetObserver *observers.TargetObserver,
	checkerController *checkers.CheckerController,
) *ReleaseCollectionJob {
	return &ReleaseCollectionJob{
		BaseJob:           NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:               req,
		dist:              dist,
		meta:              meta,
		broker:            broker,
		cluster:           cluster,
		targetMgr:         targetMgr,
		targetObserver:    targetObserver,
		checkerController: checkerController,
	}
}

func (job *ReleaseCollectionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(zap.Int64("collectionID", req.GetCollectionID()))

	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	loadedPartitions := job.meta.CollectionManager.GetPartitionsByCollection(req.GetCollectionID())
	toRelease := lo.Map(loadedPartitions, func(partition *meta.Partition, _ int) int64 {
		return partition.GetPartitionID()
	})
	releasePartitions(job.ctx, job.meta, job.cluster, req.GetCollectionID(), toRelease...)

	err := job.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove collection"
		log.Warn(msg, zap.Error(err))
		return errors.Wrap(err, msg)
	}

	err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove replicas"
		log.Warn(msg, zap.Error(err))
	}

	job.targetMgr.RemoveCollection(req.GetCollectionID())
	job.targetObserver.ReleaseCollection(req.GetCollectionID())
	waitCollectionReleased(job.dist, job.checkerController, req.GetCollectionID())
	metrics.QueryCoordNumCollections.WithLabelValues().Dec()
	metrics.QueryCoordNumPartitions.WithLabelValues().Sub(float64(len(toRelease)))
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
	targetMgr         *meta.TargetManager
	targetObserver    *observers.TargetObserver
	checkerController *checkers.CheckerController
}

func NewReleasePartitionJob(ctx context.Context,
	req *querypb.ReleasePartitionsRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	cluster session.Cluster,
	targetMgr *meta.TargetManager,
	targetObserver *observers.TargetObserver,
	checkerController *checkers.CheckerController,
) *ReleasePartitionJob {
	return &ReleasePartitionJob{
		BaseJob:           NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:               req,
		dist:              dist,
		meta:              meta,
		broker:            broker,
		cluster:           cluster,
		targetMgr:         targetMgr,
		targetObserver:    targetObserver,
		checkerController: checkerController,
	}
}

func (job *ReleasePartitionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	loadedPartitions := job.meta.CollectionManager.GetPartitionsByCollection(req.GetCollectionID())
	toRelease := lo.FilterMap(loadedPartitions, func(partition *meta.Partition, _ int) (int64, bool) {
		return partition.GetPartitionID(), lo.Contains(req.GetPartitionIDs(), partition.GetPartitionID())
	})

	if len(toRelease) == 0 {
		log.Warn("releasing partition(s) not loaded")
		return nil
	}
	releasePartitions(job.ctx, job.meta, job.cluster, req.GetCollectionID(), toRelease...)

	// If all partitions are released and LoadType is LoadPartition, clear all
	if len(toRelease) == len(loadedPartitions) &&
		job.meta.GetLoadType(req.GetCollectionID()) == querypb.LoadType_LoadPartition {
		log.Info("release partitions covers all partitions, will remove the whole collection")
		err := job.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			return errors.Wrap(err, msg)
		}
		err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			log.Warn("failed to remove replicas", zap.Error(err))
		}
		job.targetMgr.RemoveCollection(req.GetCollectionID())
		job.targetObserver.ReleaseCollection(req.GetCollectionID())
		metrics.QueryCoordNumCollections.WithLabelValues().Dec()
		waitCollectionReleased(job.dist, job.checkerController, req.GetCollectionID())
	} else {
		err := job.meta.CollectionManager.RemovePartition(toRelease...)
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			return errors.Wrap(err, msg)
		}
		job.targetMgr.RemovePartition(req.GetCollectionID(), toRelease...)
		waitCollectionReleased(job.dist, job.checkerController, req.GetCollectionID(), toRelease...)
	}
	metrics.QueryCoordNumPartitions.WithLabelValues().Sub(float64(len(toRelease)))
	return nil
}
