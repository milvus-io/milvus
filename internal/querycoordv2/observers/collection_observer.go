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

package observers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/eventlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type CollectionObserver struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dist                 *meta.DistributionManager
	meta                 *meta.Meta
	targetMgr            meta.TargetManagerInterface
	targetObserver       *TargetObserver
	checkerController    *checkers.CheckerController
	partitionLoadedCount map[int64]int

	loadTasks *typeutil.ConcurrentMap[string, LoadTask]

	proxyManager proxyutil.ProxyClientManagerInterface

	startOnce sync.Once
	stopOnce  sync.Once
}

type LoadTask struct {
	LoadType     querypb.LoadType
	CollectionID int64
	PartitionIDs []int64
}

func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	targetObserver *TargetObserver,
	checherController *checkers.CheckerController,
	proxyManager proxyutil.ProxyClientManagerInterface,
) *CollectionObserver {
	ob := &CollectionObserver{
		dist:                 dist,
		meta:                 meta,
		targetMgr:            targetMgr,
		targetObserver:       targetObserver,
		checkerController:    checherController,
		partitionLoadedCount: make(map[int64]int),
		loadTasks:            typeutil.NewConcurrentMap[string, LoadTask](),
		proxyManager:         proxyManager,
	}

	// Add load task for collection recovery
	collections := meta.GetAllCollections(context.TODO())
	for _, collection := range collections {
		ob.LoadCollection(context.Background(), collection.GetCollectionID())
	}

	return ob
}

func (ob *CollectionObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		observePeriod := Params.QueryCoordCfg.CollectionObserverInterval.GetAsDuration(time.Millisecond)
		ob.wg.Add(1)
		go func() {
			defer ob.wg.Done()

			ticker := time.NewTicker(observePeriod)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					log.Info("CollectionObserver stopped")
					return

				case <-ticker.C:
					ob.Observe(ctx)
				}
			}
		}()
	})
}

func (ob *CollectionObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()
	})
}

func (ob *CollectionObserver) LoadCollection(ctx context.Context, collectionID int64) {
	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	key := traceID.String()

	if !traceID.IsValid() {
		key = fmt.Sprintf("LoadCollection_%d", collectionID)
	}

	ob.loadTasks.Insert(key, LoadTask{LoadType: querypb.LoadType_LoadCollection, CollectionID: collectionID})
	ob.checkerController.Check()
}

func (ob *CollectionObserver) LoadPartitions(ctx context.Context, collectionID int64, partitionIDs []int64) {
	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	key := traceID.String()
	if !traceID.IsValid() {
		key = fmt.Sprintf("LoadPartition_%d_%v", collectionID, partitionIDs)
	}

	ob.loadTasks.Insert(key, LoadTask{LoadType: querypb.LoadType_LoadPartition, CollectionID: collectionID, PartitionIDs: partitionIDs})
	ob.checkerController.Check()
}

func (ob *CollectionObserver) Observe(ctx context.Context) {
	ob.observeTimeout(ctx)
	ob.observeLoadStatus(ctx)
}

func (ob *CollectionObserver) observeTimeout(ctx context.Context) {
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		collection := ob.meta.CollectionManager.GetCollection(ctx, task.CollectionID)
		// collection released
		if collection == nil {
			log.Info("Load Collection Task canceled, collection removed from meta", zap.Int64("collectionID", task.CollectionID), zap.String("traceID", traceID))
			ob.loadTasks.Remove(traceID)
			return true
		}

		switch task.LoadType {
		case querypb.LoadType_LoadCollection:
			if collection.GetStatus() == querypb.LoadStatus_Loading &&
				time.Now().After(collection.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
				log.Info("load collection timeout, cancel it",
					zap.Int64("collectionID", collection.GetCollectionID()),
					zap.Duration("loadTime", time.Since(collection.CreatedAt)))
				ob.meta.CollectionManager.RemoveCollection(ctx, collection.GetCollectionID())
				ob.meta.ReplicaManager.RemoveCollection(ctx, collection.GetCollectionID())
				ob.targetObserver.ReleaseCollection(collection.GetCollectionID())
				ob.loadTasks.Remove(traceID)
			}
		case querypb.LoadType_LoadPartition:
			partitionIDs := typeutil.NewSet(task.PartitionIDs...)
			partitions := ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)
			partitions = lo.Filter(partitions, func(partition *meta.Partition, _ int) bool {
				return partitionIDs.Contain(partition.GetPartitionID())
			})

			// all partition released
			if len(partitions) == 0 {
				log.Info("Load Partitions Task canceled, collection removed from meta",
					zap.Int64("collectionID", task.CollectionID),
					zap.Int64s("partitionIDs", task.PartitionIDs),
					zap.String("traceID", traceID))
				ob.loadTasks.Remove(traceID)
				return true
			}

			working := false
			for _, partition := range partitions {
				if time.Now().Before(partition.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
					working = true
					break
				}
			}
			// only all partitions timeout means task timeout
			if !working {
				log.Info("load partitions timeout, cancel it",
					zap.Int64("collectionID", task.CollectionID),
					zap.Int64s("partitionIDs", task.PartitionIDs))
				for _, partition := range partitions {
					ob.meta.CollectionManager.RemovePartition(ctx, partition.CollectionID, partition.GetPartitionID())
					ob.targetObserver.ReleasePartition(partition.GetCollectionID(), partition.GetPartitionID())
				}

				// all partition timeout, remove collection
				if len(ob.meta.CollectionManager.GetPartitionsByCollection(ctx, task.CollectionID)) == 0 {
					log.Info("collection timeout due to all partition removed", zap.Int64("collection", task.CollectionID))

					ob.meta.CollectionManager.RemoveCollection(ctx, task.CollectionID)
					ob.meta.ReplicaManager.RemoveCollection(ctx, task.CollectionID)
					ob.targetObserver.ReleaseCollection(task.CollectionID)
				}
			}
		}
		return true
	})
}

func (ob *CollectionObserver) readyToObserve(ctx context.Context, collectionID int64) bool {
	metaExist := (ob.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := ob.targetMgr.IsNextTargetExist(ctx, collectionID) || ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (ob *CollectionObserver) observeLoadStatus(ctx context.Context) {
	loading := false
	observeTaskNum := 0
	observeStart := time.Now()
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		loading = true
		observeTaskNum++

		start := time.Now()
		collection := ob.meta.CollectionManager.GetCollection(ctx, task.CollectionID)
		if collection == nil {
			return true
		}

		var partitions []*meta.Partition
		switch task.LoadType {
		case querypb.LoadType_LoadCollection:
			partitions = ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)
		case querypb.LoadType_LoadPartition:
			partitionIDs := typeutil.NewSet[int64](task.PartitionIDs...)
			partitions = ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)
			partitions = lo.Filter(partitions, func(partition *meta.Partition, _ int) bool {
				return partitionIDs.Contain(partition.GetPartitionID())
			})
		}

		loaded := true
		hasUpdate := false

		channelTargetNum, subChannelCount := ob.observeChannelStatus(ctx, task.CollectionID)

		for _, partition := range partitions {
			if partition.LoadPercentage == 100 {
				continue
			}
			if ob.readyToObserve(ctx, partition.CollectionID) {
				replicaNum := ob.meta.GetReplicaNumber(ctx, partition.GetCollectionID())
				has := ob.observePartitionLoadStatus(ctx, partition, replicaNum, channelTargetNum, subChannelCount)
				if has {
					hasUpdate = true
				}
			}
			partition = ob.meta.GetPartition(ctx, partition.PartitionID)
			if partition != nil && partition.LoadPercentage != 100 {
				loaded = false
			}
		}

		if hasUpdate {
			ob.observeCollectionLoadStatus(ctx, task.CollectionID)
		}

		// all partition loaded, finish task
		if len(partitions) > 0 && loaded {
			log.Info("Load task finish",
				zap.String("traceID", traceID),
				zap.Int64("collectionID", task.CollectionID),
				zap.Int64s("partitionIDs", task.PartitionIDs),
				zap.Stringer("loadType", task.LoadType))
			ob.loadTasks.Remove(traceID)
		}

		log.Info("observe collection done", zap.Int64("collectionID", task.CollectionID), zap.Duration("dur", time.Since(start)))
		return true
	})

	if observeTaskNum > 0 {
		log.Info("observe all collections done", zap.Int("num", observeTaskNum), zap.Duration("dur", time.Since(observeStart)))
	}

	// trigger check logic when loading collections/partitions
	if loading {
		ob.checkerController.Check()
	}
}

func (ob *CollectionObserver) observeChannelStatus(ctx context.Context, collectionID int64) (int, int) {
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)

	channelTargetNum := len(channelTargets)
	if channelTargetNum == 0 {
		log.Info("channels in target is empty, waiting for new target content")
		return 0, 0
	}

	subChannelCount := 0
	for _, channel := range channelTargets {
		delegatorList := ob.dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(channel.GetChannelName()))
		nodes := lo.Map(delegatorList, func(v *meta.DmChannel, _ int) int64 { return v.Node })
		group := utils.GroupNodesByReplica(ctx, ob.meta.ReplicaManager, collectionID, nodes)
		subChannelCount += len(group)
	}
	return channelTargetNum, subChannelCount
}

func (ob *CollectionObserver) observePartitionLoadStatus(ctx context.Context, partition *meta.Partition, replicaNum int32, channelTargetNum, subChannelCount int) bool {
	segmentTargets := ob.targetMgr.GetSealedSegmentsByPartition(ctx, partition.GetCollectionID(), partition.GetPartitionID(), meta.NextTarget)

	targetNum := len(segmentTargets) + channelTargetNum
	if targetNum == 0 {
		log.Info("segments and channels in target are both empty, waiting for new target content")
		return false
	}

	log.Ctx(ctx).WithRateGroup("qcv2.observePartitionLoadStatus", 1, 60).RatedInfo(10, "partition targets",
		zap.Int64("collectionID", partition.GetCollectionID()),
		zap.Int64("partitionID", partition.GetPartitionID()),
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", channelTargetNum),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", replicaNum),
	)
	loadedCount := subChannelCount
	loadPercentage := int32(0)

	for _, segment := range segmentTargets {
		delegatorList := ob.dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(segment.GetInsertChannel()))
		loadedSegmentNodes := make([]int64, 0)
		for _, delegator := range delegatorList {
			if delegator.View.Segments[segment.GetID()] != nil {
				loadedSegmentNodes = append(loadedSegmentNodes, delegator.Node)
			}
		}
		group := utils.GroupNodesByReplica(ctx, ob.meta.ReplicaManager, partition.GetCollectionID(), loadedSegmentNodes)
		loadedCount += len(group)
	}
	loadPercentage = int32(loadedCount * 100 / (targetNum * int(replicaNum)))

	if loadedCount <= ob.partitionLoadedCount[partition.GetPartitionID()] && loadPercentage != 100 {
		ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
		return false
	}

	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	if loadPercentage == 100 {
		if !ob.targetObserver.Check(ctx, partition.GetCollectionID(), partition.PartitionID) {
			log.Ctx(ctx).Warn("failed to manual check current target, skip update load status",
				zap.Int64("collectionID", partition.GetCollectionID()),
				zap.Int64("partitionID", partition.GetPartitionID()))
			return false
		}
		delete(ob.partitionLoadedCount, partition.GetPartitionID())
	}
	err := ob.meta.CollectionManager.UpdatePartitionLoadPercent(ctx, partition.PartitionID, loadPercentage)
	if err != nil {
		log.Ctx(ctx).Warn("failed to update partition load percentage",
			zap.Int64("collectionID", partition.GetCollectionID()),
			zap.Int64("partitionID", partition.GetPartitionID()))
	}
	log.Ctx(ctx).Info("partition load status updated",
		zap.Int64("collectionID", partition.GetCollectionID()),
		zap.Int64("partitionID", partition.GetPartitionID()),
		zap.Int32("partitionLoadPercentage", loadPercentage),
		zap.Int("subChannelCount", subChannelCount),
		zap.Int("loadSegmentCount", loadedCount-subChannelCount),
	)
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("partition %d load percentage update: %d", partition.PartitionID, loadPercentage)))
	return true
}

func (ob *CollectionObserver) observeCollectionLoadStatus(ctx context.Context, collectionID int64) {
	collectionPercentage, err := ob.meta.CollectionManager.UpdateCollectionLoadPercent(ctx, collectionID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to update collection load percentage", zap.Int64("collectionID", collectionID))
	}
	log.Ctx(ctx).Info("collection load status updated",
		zap.Int64("collectionID", collectionID),
		zap.Int32("collectionLoadPercentage", collectionPercentage),
	)
	if collectionPercentage == 100 {
		ob.invalidateCache(ctx, collectionID)
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("collection %d load percentage update: %d", collectionID, collectionPercentage)))
}

func (ob *CollectionObserver) invalidateCache(ctx context.Context, collectionID int64) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Second))
	defer cancel()
	err := ob.proxyManager.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
		CollectionID: collectionID,
	}, proxyutil.SetMsgType(commonpb.MsgType_LoadCollection))
	if err != nil {
		log.Warn("failed to invalidate proxy's shard leader cache", zap.Error(err))
		return
	}
}
