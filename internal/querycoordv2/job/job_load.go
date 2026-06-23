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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/eventlog"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	proxyManager       proxyutil.ProxyClientManagerInterface
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
	proxyManager proxyutil.ProxyClientManagerInterface,
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
		proxyManager:       proxyManager,
	}
}

func (job *LoadCollectionJob) Execute() error {
	req := job.result.Message.Header()
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionId())

	collInfo, err := job.broker.DescribeCollection(job.ctx, req.GetCollectionId())
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	// 1. resolve replica config: use local cluster-level config if this is a replicated message
	replicas := req.GetReplicas()
	if req.GetUseLocalReplicaConfig() {
		localReplicas, err := getLocalReplicaConfig(job.ctx, job.meta, req.GetCollectionId())
		if err != nil {
			return err
		}
		replicas = localReplicas
		mlog.Info(context.TODO(), "using local cluster-level replica config for replicated load",
			mlog.Int("localReplicaCount", len(localReplicas)))
	}

	// 2. create replica if not exist (may also remove redundant replicas)
	if _, err := utils.SpawnReplicasWithReplicaConfig(job.ctx, job.meta, meta.SpawnWithReplicaConfigParams{
		CollectionID: req.GetCollectionId(),
		Channels:     collInfo.GetVirtualChannelNames(),
		Configs:      replicas,
	}); err != nil {
		return err
	}

	// 2.1 invalidate shard leader cache after replica changes, so proxies stop
	// routing to released replicas' shard leaders before async cleanup happens.
	if job.proxyManager != nil {
		job.proxyManager.InvalidateShardLeaderCache(job.ctx, &proxypb.InvalidateShardLeaderCacheRequest{
			CollectionIDs: []int64{req.GetCollectionId()},
		})
	}

	// 3. put load info meta
	fieldIndexIDs := make(map[int64]int64, len(req.GetLoadFields()))
	fieldIDs := make([]int64, 0, len(req.GetLoadFields()))
	for _, loadField := range req.GetLoadFields() {
		if loadField.GetIndexId() != 0 {
			fieldIndexIDs[loadField.GetFieldId()] = loadField.GetIndexId()
		}
		fieldIDs = append(fieldIDs, loadField.GetFieldId())
	}
	replicaNumber := int32(len(replicas))
	currentPartitions := job.meta.CollectionManager.GetPartitionsByCollection(job.ctx, req.GetCollectionId())
	currentPartitionMap := lo.SliceToMap(currentPartitions, func(partition *meta.Partition) (int64, *meta.Partition) {
		return partition.GetPartitionID(), partition
	})
	forceSyncWarmup := req.GetForceSyncWarmup()
	for _, partitionID := range req.GetPartitionIds() {
		if current, ok := currentPartitionMap[partitionID]; ok {
			forceSyncWarmup = forceSyncWarmup || current.GetForceSyncWarmup()
		}
	}
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
			ForceSyncWarmup:          forceSyncWarmup,
		},
		CreatedAt: time.Now(),
		LoadSpan:  sp,
		Schema:    collInfo.GetSchema(),
	}
	incomingPartitions := typeutil.NewSet(req.GetPartitionIds()...)
	partitions, newPartitionCount := buildPartitionsToPersist(
		req.GetCollectionId(),
		req.GetPartitionIds(),
		currentPartitionMap,
		replicaNumber,
		fieldIndexIDs,
		req.GetForceSyncWarmup(),
	)
	toReleasePartitions := make([]int64, 0)
	for _, partition := range currentPartitions {
		if !incomingPartitions.Contain(partition.GetPartitionID()) {
			toReleasePartitions = append(toReleasePartitions, partition.GetPartitionID())
		}
	}
	if len(toReleasePartitions) > 0 {
		job.targetObserver.ReleasePartition(req.GetCollectionId(), toReleasePartitions...)
		if err := job.meta.CollectionManager.RemovePartition(job.ctx, req.GetCollectionId(), toReleasePartitions...); err != nil {
			return merr.Wrap(err, "failed to remove partitions")
		}
	}

	if err = job.meta.CollectionManager.PutCollection(job.ctx, collection, partitions...); err != nil {
		msg := "failed to store collection and partitions"
		mlog.Warn(job.ctx, msg, mlog.Err(err))
		return merr.Wrapf(err, "%s", msg)
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Start load collection %d", collection.CollectionID)))
	metrics.QueryCoordNumPartitions.WithLabelValues().Add(float64(newPartitionCount))

	mlog.Info(context.TODO(), "put collection and partitions done",
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.Int64s("partitions", req.GetPartitionIds()),
		mlog.Int("persistedPartitions", len(partitions)),
		mlog.Int("newPartitions", newPartitionCount),
		mlog.Int64s("toReleasePartitions", toReleasePartitions),
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
			mlog.Warn(context.TODO(), "failed to wait current target updated", mlog.Err(err))
			// return nil to avoid infinite retry on DDL callback
			return nil
		}
		if err = WaitCollectionReleased(ctx, job.dist, job.checkerController, req.GetCollectionId(), toReleasePartitions...); err != nil {
			mlog.Warn(context.TODO(), "failed to wait partition released", mlog.Err(err))
			// return nil to avoid infinite retry on DDL callback
			return nil
		}
		mlog.Info(context.TODO(), "wait for partition released done", mlog.Int64s("toReleasePartitions", toReleasePartitions))
	}
	return nil
}

// getLocalReplicaConfig reads the local cluster-level replica config and generates LoadReplicaConfig entries.
// It uses generateReplicas to ensure idempotency on WAL replay by reusing existing replicas from meta.
// If local config is not set, defaults to 1 replica in __default_resource_group.
func getLocalReplicaConfig(ctx context.Context, m *meta.Meta, collectionID int64) ([]*messagespb.LoadReplicaConfig, error) {
	replicaNum := int(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt64())
	rgs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()

	if replicaNum <= 0 {
		replicaNum = 1
	}
	if len(rgs) == 0 {
		rgs = []string{meta.DefaultResourceGroupName}
	}

	// Use AssignReplica to determine expected replica distribution per RG
	expectedReplicaNumber, err := utils.AssignReplica(ctx, m, rgs, int32(replicaNum), false)
	if err != nil {
		return nil, err
	}

	// Get current replicas from meta for idempotent generation
	currentReplicas := m.GetByCollection(ctx, collectionID)
	currentReplicaMap := make(map[int64]*meta.Replica)
	for _, r := range currentReplicas {
		currentReplicaMap[r.GetID()] = r
	}

	// Use generateReplicas which reuses existing replicas (idempotent on replay)
	req := &AlterLoadConfigRequest{
		Meta:     m,
		Current:  CurrentLoadConfig{Replicas: currentReplicaMap},
		Expected: ExpectedLoadConfig{ExpectedReplicaNumber: expectedReplicaNumber},
	}
	return req.generateReplicas(ctx)
}

func buildPartitionsToPersist(
	collectionID int64,
	partitionIDs []int64,
	currentPartitions map[int64]*meta.Partition,
	replicaNumber int32,
	fieldIndexIDs map[int64]int64,
	requestForceSyncWarmup bool,
) ([]*meta.Partition, int) {
	partitions := make([]*meta.Partition, 0, len(partitionIDs))
	newPartitionCount := 0

	for _, partitionID := range partitionIDs {
		current, ok := currentPartitions[partitionID]
		if !ok {
			newPartitionCount++
			partitions = append(partitions, &meta.Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID:    collectionID,
					PartitionID:     partitionID,
					ReplicaNumber:   replicaNumber,
					Status:          querypb.LoadStatus_Loading,
					FieldIndexID:    fieldIndexIDs,
					ForceSyncWarmup: requestForceSyncWarmup,
				},
				CreatedAt: time.Now(),
			})
			continue
		}

		updated := current.Clone()
		updated.ReplicaNumber = replicaNumber
		updated.FieldIndexID = fieldIndexIDs
		if !proto.Equal(current.PartitionLoadInfo, updated.PartitionLoadInfo) {
			partitions = append(partitions, updated)
		}
	}

	return partitions, newPartitionCount
}
