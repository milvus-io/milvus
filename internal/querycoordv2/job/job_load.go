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
	"maps"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

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

	// Classify the request before spawn, because spawn is what mutates the
	// replica meta both of the following read.
	incrementalExpansion := job.isIncrementalExpansion(req, replicas)

	// Snapshot the resource groups that already hold a replica of this
	// collection, for the same reason: once spawn has run, every resource group
	// named in replicas holds one, the diff below comes back empty, and the
	// resource group this request adds would never get an observer task.
	preSpawnRGs := typeutil.NewSet[string]()
	if incrementalExpansion {
		for _, replica := range job.meta.GetByCollection(job.ctx, req.GetCollectionId()) {
			preSpawnRGs.Insert(replica.GetResourceGroup())
		}
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
	fieldIndexIDs, fieldIDs := requestedLoadFields(req)
	replicaNumber := int32(len(replicas))

	ctx, sp := otel.Tracer(typeutil.QueryCoordRole).Start(job.ctx, "LoadCollection", trace.WithNewRoot())

	// 3.1 incremental resource group expansion: this request adds resource
	// groups to a collection that is already loaded and changes nothing else,
	// so the collection and partition meta it would write is the meta that is
	// already there -- except for Status and LoadPercentage, which the write
	// would reset to Loading/0 and take the resource groups that are serving
	// right now down with them. Skip the overwrite and register an observer
	// task per added resource group instead.
	if incrementalExpansion {
		// Nothing keeps this span on this path: the collection holds the
		// LoadSpan of the load that created it, and that span is what
		// UpdateCollectionLoadPercent ends. End this one here rather than leak
		// it; the trace ID it carries still keys the tasks registered below.
		defer sp.End()

		mlog.Info(job.ctx, "incremental resource group expansion, keeping loaded collection meta",
			mlog.Int64("collectionID", req.GetCollectionId()),
			mlog.Int32("replicaNumber", replicaNumber))

		// The replica count is the one property of the collection this request
		// legitimately changes; the predicate has established that everything
		// else is unchanged. UpdateReplicaNumber writes it to the collection and
		// its partitions without touching their Status or LoadPercentage, so
		// the already-serving resource groups stay Loaded at 100%. Leaving it
		// stale instead would misreport the collection's replica count to every
		// later UpdateLoadConfig, which reads it as the replica number to keep.
		if err := job.meta.UpdateReplicaNumber(job.ctx, req.GetCollectionId(), replicaNumber, req.GetUserSpecifiedReplicaMode()); err != nil {
			msg := "failed to update replica number"
			mlog.Warn(job.ctx, msg, mlog.Err(err))
			return merr.Wrapf(err, "%s", msg)
		}

		// The target is unchanged -- same partitions, same segments -- but the
		// pull is cheap and keeps this path's ordering identical to the one
		// below, where the observer only ever sees a populated next target.
		if _, err = job.targetObserver.UpdateNextTarget(req.GetCollectionId()); err != nil {
			return err
		}

		// One task per resource group this request adds. The predicate
		// guarantees every replica being added lives in one of them, so no
		// added replica is left unobserved, and resource groups that were
		// already there keep the state -- and the tasks -- they already had.
		for _, replica := range replicas {
			rgName := replica.GetResourceGroupName()
			if preSpawnRGs.Contain(rgName) {
				continue
			}
			preSpawnRGs.Insert(rgName)
			job.collectionObserver.LoadPartitions(ctx, req.GetCollectionId(), req.GetPartitionIds(), rgName)
		}
		return nil
	}

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
	currentPartitions := job.meta.GetPartitionsByCollection(job.ctx, req.GetCollectionId())
	toReleasePartitions := make([]int64, 0)
	for _, partition := range currentPartitions {
		if !incomingPartitions.Contain(partition.GetPartitionID()) {
			toReleasePartitions = append(toReleasePartitions, partition.GetPartitionID())
		}
	}
	if len(toReleasePartitions) > 0 {
		job.targetObserver.ReleasePartition(req.GetCollectionId(), toReleasePartitions...)
		if err := job.meta.RemovePartition(job.ctx, req.GetCollectionId(), toReleasePartitions...); err != nil {
			return merr.Wrap(err, "failed to remove partitions")
		}
	}

	if err = job.meta.PutCollection(job.ctx, collection, partitions...); err != nil {
		msg := "failed to store collection and partitions"
		mlog.Warn(job.ctx, msg, mlog.Err(err))
		return merr.Wrapf(err, "%s", msg)
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Start load collection %d", collection.CollectionID)))
	metrics.QueryCoordNumPartitions.WithLabelValues().Add(float64(len(partitions)))

	mlog.Info(context.TODO(), "put collection and partitions done",
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.Int64s("partitions", req.GetPartitionIds()),
		mlog.Int64s("toReleasePartitions", toReleasePartitions),
	)

	// 5. update next target, no need to rollback if pull target failed, target observer will pull target in periodically
	if _, err = job.targetObserver.UpdateNextTarget(req.GetCollectionId()); err != nil {
		return err
	}

	// 6. register load task into collection observer. The empty resource group
	// keeps the watcher collection-wide, which is the semantics this job has
	// always had: it loads the collection as a whole, not one resource group of
	// it.
	job.collectionObserver.LoadPartitions(ctx, req.GetCollectionId(), incomingPartitions.Collect(), "")

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

// requestedLoadFields splits the request's load-field configs into the
// field-to-index map and the plain list of field IDs that meta.Collection
// stores, so that the predicate below compares exactly the values the
// collection would have been overwritten with.
func requestedLoadFields(req *messagespb.AlterLoadConfigMessageHeader) (map[int64]int64, []int64) {
	fieldIndexIDs := make(map[int64]int64, len(req.GetLoadFields()))
	fieldIDs := make([]int64, 0, len(req.GetLoadFields()))
	for _, loadField := range req.GetLoadFields() {
		if loadField.GetIndexId() != 0 {
			fieldIndexIDs[loadField.GetFieldId()] = loadField.GetIndexId()
		}
		fieldIDs = append(fieldIDs, loadField.GetFieldId())
	}
	return fieldIndexIDs, fieldIDs
}

// isIncrementalExpansion reports whether req asks for nothing but additional
// resource groups on a collection that is already loaded. That is the one shape
// of request whose collection/partition meta write is pure loss: the write
// stores values identical to the stored ones except for Status and
// LoadPercentage, which it resets to Loading and 0, dropping resource groups
// that are serving queries right now out of Loaded until the observer has
// walked them back up to 100%.
//
// Every other request keeps the overwrite. The predicate is therefore
// deliberately conservative -- it is false unless every part of the request
// matches what is stored -- and in particular it is false for a first load, for
// a reload, and for a plain replica-number change, so a deployment that never
// loads one collection into several resource groups never leaves the path this
// job has always taken.
//
// The legs, and what each one protects:
//
//   - collection exists and is Loaded: there is serving state to protect, and
//     an unfinished load must keep its Loading status and its observer task.
//   - same DbID, same LoadType, same partition set, same load fields, same
//     field-to-index map: everything the skipped write would have stored is
//     already stored, so skipping it loses nothing. The partition leg also
//     covers the partition-release branch below, which the fast path skips.
//   - every existing replica appears in the new set, in the same resource
//     group: no replica is being released or moved, so no resource group is
//     losing state that the collection meta still claims it has.
//   - the new replica set is strictly larger, and every added replica lives in
//     a resource group that holds none of this collection's replicas today:
//     this is the "adds resource groups" part, and it is what lets the caller
//     cover every added replica with one observer task per added resource
//     group. An extra replica in a resource group that is already loaded would
//     be left with no task at all, so that request keeps the overwrite.
func (job *LoadCollectionJob) isIncrementalExpansion(req *messagespb.AlterLoadConfigMessageHeader, newReplicas []*messagespb.LoadReplicaConfig) bool {
	// Only a deployment that scopes load requests to the resource groups they
	// name (queryCoord.resourceGroupScopedLoad) gets the keep-loaded fast path.
	// The native add-resource-group semantics - reset to Loading, block the
	// caller until the new resource group loads, release the collection if it
	// cannot - stay byte-for-byte what they were on a stock binary, including
	// their failure visibility: such a deployment watches per-resource-group
	// progress itself, a native SDK caller has only the collection-wide
	// answer, and handing it an instant 100% would hide a failed expansion.
	if !paramtable.Get().QueryCoordCfg.ResourceGroupScopedLoad.GetAsBool() {
		return false
	}
	existing := job.meta.GetCollection(job.ctx, req.GetCollectionId())
	if existing == nil || existing.GetStatus() != querypb.LoadStatus_Loaded {
		return false
	}
	if existing.GetDbID() != req.GetDbId() {
		return false
	}
	if existing.GetLoadType() != querypb.LoadType_LoadCollection {
		return false
	}

	incomingPartitions := typeutil.NewSet(req.GetPartitionIds()...)
	currentPartitions := job.meta.GetPartitionsByCollection(job.ctx, req.GetCollectionId())
	if len(currentPartitions) != incomingPartitions.Len() {
		return false
	}
	for _, partition := range currentPartitions {
		if !incomingPartitions.Contain(partition.GetPartitionID()) {
			return false
		}
	}

	fieldIndexIDs, fieldIDs := requestedLoadFields(req)
	if !maps.Equal(typeutil.NewSet(fieldIDs...), typeutil.NewSet(existing.GetLoadFields()...)) {
		return false
	}
	if !maps.Equal(fieldIndexIDs, existing.GetFieldIndexID()) {
		return false
	}

	existingReplicas := job.meta.GetByCollection(job.ctx, req.GetCollectionId())
	if len(newReplicas) <= len(existingReplicas) {
		return false
	}
	existingRGByReplica := make(map[int64]string, len(existingReplicas))
	loadedRGs := typeutil.NewSet[string]()
	for _, replica := range existingReplicas {
		existingRGByReplica[replica.GetID()] = replica.GetResourceGroup()
		loadedRGs.Insert(replica.GetResourceGroup())
	}

	seen := typeutil.NewSet[int64]()
	for _, replica := range newReplicas {
		seen.Insert(replica.GetReplicaId())
		rgName, isExisting := existingRGByReplica[replica.GetReplicaId()]
		if isExisting {
			if rgName != replica.GetResourceGroupName() {
				return false // an existing replica is being moved to another resource group
			}
			continue
		}
		if loadedRGs.Contain(replica.GetResourceGroupName()) {
			return false // an added replica would land in a resource group that has no task of its own
		}
	}
	for _, replica := range existingReplicas {
		if !seen.Contain(replica.GetID()) {
			return false // an existing replica is being released
		}
	}

	return true
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
