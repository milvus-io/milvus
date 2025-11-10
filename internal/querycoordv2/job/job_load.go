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
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/eventlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type LoadCollectionJob struct {
	*BaseJob

	result             message.BroadcastResultAlterLoadConfigMessageV2
	undo               *UndoList
	dist               *meta.DistributionManager
	meta               *meta.Meta
	broker             meta.Broker
	targetMgr          meta.TargetManagerInterface
	targetObserver     *observers.TargetObserver
	collectionObserver *observers.CollectionObserver
	checkerController  *checkers.CheckerController
	nodeMgr            *session.NodeManager
}

func NewLoadCollectionJob(
	ctx context.Context,
	result message.BroadcastResultAlterLoadConfigMessageV2,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	collectionObserver *observers.CollectionObserver,
	checkerController *checkers.CheckerController,
	nodeMgr *session.NodeManager,
) *LoadCollectionJob {
	return &LoadCollectionJob{
		BaseJob:            NewBaseJob(ctx, 0, result.Message.Header().GetCollectionId()),
		result:             result,
		undo:               NewUndoList(ctx, meta, targetMgr, targetObserver),
		dist:               dist,
		meta:               meta,
		broker:             broker,
		targetMgr:          targetMgr,
		targetObserver:     targetObserver,
		collectionObserver: collectionObserver,
		checkerController:  checkerController,
		nodeMgr:            nodeMgr,
	}
}

func (job *LoadCollectionJob) Execute() error {
	req := job.result.Message.Header()
	vchannels := job.result.GetVChannelsWithoutControlChannel()

	log := log.Ctx(job.ctx).With(zap.Int64("collectionID", req.GetCollectionId()))
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionId())

	// 1. create replica if not exist
	if _, err := utils.SpawnReplicasWithReplicaConfig(job.ctx, job.meta, meta.SpawnWithReplicaConfigParams{
		CollectionID: req.GetCollectionId(),
		Channels:     vchannels,
		Configs:      req.GetReplicas(),
	}); err != nil {
		return err
	}

	collInfo, err := job.broker.DescribeCollection(job.ctx, req.GetCollectionId())
	if err != nil {
		return err
	}

	// 2. put load info meta
	fieldIndexIDs := make(map[int64]int64, len(req.GetLoadFields()))
	fieldIDs := make([]int64, 0, len(req.GetLoadFields()))
	for _, loadField := range req.GetLoadFields() {
		if loadField.GetIndexId() != 0 {
			fieldIndexIDs[loadField.GetFieldId()] = loadField.GetIndexId()
		}
		fieldIDs = append(fieldIDs, loadField.GetFieldId())
	}
	replicaNumber := int32(len(req.GetReplicas()))
	partitions := lo.Map(req.GetPartitionIds(), func(partID int64, _ int) *meta.Partition {
		return &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID:  req.GetCollectionId(),
				PartitionID:   partID,
				ReplicaNumber: replicaNumber,
				Status:        querypb.LoadStatus_Loading,
				FieldIndexID:  fieldIndexIDs,
			},
			CreatedAt: time.Now(),
		}
	})

	ctx, sp := otel.Tracer(typeutil.QueryCoordRole).Start(job.ctx, "LoadCollection", trace.WithNewRoot())
	collection := &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:             req.GetCollectionId(),
			ReplicaNumber:            replicaNumber,
			Status:                   querypb.LoadStatus_Loading,
			FieldIndexID:             fieldIndexIDs,
			LoadType:                 querypb.LoadType_LoadCollection,
			LoadFields:               fieldIDs,
			DbID:                     req.GetDbId(),
			UserSpecifiedReplicaMode: req.GetUserSpecifiedReplicaMode(),
		},
		CreatedAt: time.Now(),
		LoadSpan:  sp,
		Schema:    collInfo.GetSchema(),
	}
	incomingPartitions := typeutil.NewSet(req.GetPartitionIds()...)
	currentPartitions := job.meta.CollectionManager.GetPartitionsByCollection(job.ctx, req.GetCollectionId())
	toReleasePartitions := make([]int64, 0)
	for _, partition := range currentPartitions {
		if !incomingPartitions.Contain(partition.GetPartitionID()) {
			toReleasePartitions = append(toReleasePartitions, partition.GetPartitionID())
		}
	}
	if len(toReleasePartitions) > 0 {
		job.targetObserver.ReleasePartition(req.GetCollectionId(), toReleasePartitions...)
		if err := job.meta.CollectionManager.RemovePartition(job.ctx, req.GetCollectionId(), toReleasePartitions...); err != nil {
			return errors.Wrap(err, "failed to remove partitions")
		}
	}

	if err = job.meta.CollectionManager.PutCollection(job.ctx, collection, partitions...); err != nil {
		msg := "failed to store collection and partitions"
		log.Warn(msg, zap.Error(err))
		return errors.Wrap(err, msg)
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Start load collection %d", collection.CollectionID)))
	metrics.QueryCoordNumPartitions.WithLabelValues().Add(float64(len(partitions)))

	log.Info("put collection and partitions done",
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.Int64s("partitions", req.GetPartitionIds()),
		zap.Int64s("toReleasePartitions", toReleasePartitions),
	)

	// 5. update next target, no need to rollback if pull target failed, target observer will pull target in periodically
	if _, err = job.targetObserver.UpdateNextTarget(req.GetCollectionId()); err != nil {
		return err
	}

	// 6. register load task into collection observer
	job.collectionObserver.LoadPartitions(ctx, req.GetCollectionId(), incomingPartitions.Collect())

	// 7. wait for partition released if any partition is released
	if len(toReleasePartitions) > 0 {
		if err = WaitCurrentTargetUpdated(ctx, job.targetObserver, req.GetCollectionId()); err != nil {
			log.Warn("failed to wait current target updated", zap.Error(err))
			// return nil to avoid infinite retry on DDL callback
			return nil
		}
		if err = WaitCollectionReleased(ctx, job.dist, job.checkerController, req.GetCollectionId(), toReleasePartitions...); err != nil {
			log.Warn("failed to wait partition released", zap.Error(err))
			// return nil to avoid infinite retry on DDL callback
			return nil
		}
		log.Info("wait for partition released done", zap.Int64s("toReleasePartitions", toReleasePartitions))
	}
	return nil
}
