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
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/eventlog"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	collections := meta.GetAllCollections()
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
	ob.observeTimeout()
	ob.observeLoadStatus(ctx)
}

func (ob *CollectionObserver) observeTimeout() {
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		collection := ob.meta.CollectionManager.GetCollection(task.CollectionID)
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
				ob.meta.CollectionManager.RemoveCollection(collection.GetCollectionID())
				ob.meta.ReplicaManager.RemoveCollection(collection.GetCollectionID())
				ob.targetMgr.RemoveCollection(collection.GetCollectionID())
				ob.loadTasks.Remove(traceID)
			}
		case querypb.LoadType_LoadPartition:
			partitionIDs := typeutil.NewSet(task.PartitionIDs...)
			partitions := ob.meta.GetPartitionsByCollection(task.CollectionID)
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
					ob.meta.CollectionManager.RemovePartition(partition.CollectionID, partition.GetPartitionID())
					ob.targetMgr.RemovePartition(partition.GetCollectionID(), partition.GetPartitionID())
				}

				// all partition timeout, remove collection
				if len(ob.meta.CollectionManager.GetPartitionsByCollection(task.CollectionID)) == 0 {
					log.Info("collection timeout due to all partition removed", zap.Int64("collection", task.CollectionID))

					ob.meta.CollectionManager.RemoveCollection(task.CollectionID)
					ob.meta.ReplicaManager.RemoveCollection(task.CollectionID)
					ob.targetMgr.RemoveCollection(task.CollectionID)
				}
			}
		}
		return true
	})
}

func (ob *CollectionObserver) readyToObserve(collectionID int64) bool {
	metaExist := (ob.meta.GetCollection(collectionID) != nil)
	targetExist := ob.targetMgr.IsNextTargetExist(collectionID) || ob.targetMgr.IsCurrentTargetExist(collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (ob *CollectionObserver) observeLoadStatus(ctx context.Context) {
	loading := false
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		loading = true

		collection := ob.meta.CollectionManager.GetCollection(task.CollectionID)
		if collection == nil {
			return true
		}

		var partitions []*meta.Partition
		switch task.LoadType {
		case querypb.LoadType_LoadCollection:
			partitions = ob.meta.GetPartitionsByCollection(task.CollectionID)
		case querypb.LoadType_LoadPartition:
			partitionIDs := typeutil.NewSet[int64](task.PartitionIDs...)
			partitions = ob.meta.GetPartitionsByCollection(task.CollectionID)
			partitions = lo.Filter(partitions, func(partition *meta.Partition, _ int) bool {
				return partitionIDs.Contain(partition.GetPartitionID())
			})
		}

		loaded := true
		for _, partition := range partitions {
			if partition.LoadPercentage == 100 {
				continue
			}
			if ob.readyToObserve(partition.CollectionID) {
				replicaNum := ob.meta.GetReplicaNumber(partition.GetCollectionID())
				ob.observePartitionLoadStatus(ctx, partition, replicaNum)
			}
			partition = ob.meta.GetPartition(partition.PartitionID)
			if partition != nil && partition.LoadPercentage != 100 {
				loaded = false
			}
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

		return true
	})

	// trigger check logic when loading collections/partitions
	if loading {
		ob.checkerController.Check()
	}
}

func (ob *CollectionObserver) observePartitionLoadStatus(ctx context.Context, partition *meta.Partition, replicaNum int32) {
	log := log.Ctx(ctx).WithRateGroup("qcv2.observePartitionLoadStatus", 1, 60).With(
		zap.Int64("collectionID", partition.GetCollectionID()),
		zap.Int64("partitionID", partition.GetPartitionID()),
	)

	segmentTargets := ob.targetMgr.GetSealedSegmentsByPartition(partition.GetCollectionID(), partition.GetPartitionID(), meta.NextTarget)
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(partition.GetCollectionID(), meta.NextTarget)

	targetNum := len(segmentTargets) + len(channelTargets)
	if targetNum == 0 {
		log.Info("segments and channels in target are both empty, waiting for new target content")
		return
	}

	log.RatedInfo(10, "partition targets",
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", len(channelTargets)),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", replicaNum),
	)
	loadedCount := 0
	loadPercentage := int32(0)

	for _, channel := range channelTargets {
		views := ob.dist.LeaderViewManager.GetByFilter(meta.WithChannelName2LeaderView(channel.GetChannelName()))
		nodes := lo.Map(views, func(v *meta.LeaderView, _ int) int64 { return v.ID })
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager, partition.GetCollectionID(), nodes)
		loadedCount += len(group)
	}
	subChannelCount := loadedCount
	for _, segment := range segmentTargets {
		views := ob.dist.LeaderViewManager.GetByFilter(meta.WithSegment2LeaderView(segment.GetID(), false))
		nodes := lo.Map(views, func(view *meta.LeaderView, _ int) int64 { return view.ID })
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager, partition.GetCollectionID(), nodes)
		loadedCount += len(group)
	}
	if loadedCount > 0 {
		log.Info("partition load progress",
			zap.Int("subChannelCount", subChannelCount),
			zap.Int("loadSegmentCount", loadedCount-subChannelCount))
	}
	loadPercentage = int32(loadedCount * 100 / (targetNum * int(replicaNum)))

	if loadedCount <= ob.partitionLoadedCount[partition.GetPartitionID()] && loadPercentage != 100 {
		ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
		return
	}

	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	if loadPercentage == 100 {
		if !ob.targetObserver.Check(ctx, partition.GetCollectionID(), partition.PartitionID) {
			log.Warn("failed to manual check current target, skip update load status")
			return
		}
		delete(ob.partitionLoadedCount, partition.GetPartitionID())
	}
	collectionPercentage, err := ob.meta.CollectionManager.UpdateLoadPercent(partition.PartitionID, loadPercentage)
	if err != nil {
		log.Warn("failed to update load percentage")
	}
	log.Info("load status updated",
		zap.Int32("partitionLoadPercentage", loadPercentage),
		zap.Int32("collectionLoadPercentage", collectionPercentage),
	)
	if collectionPercentage == 100 {
		ob.invalidateCache(ctx, partition.GetCollectionID())
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("collection %d load percentage update: %d", partition.CollectionID, loadPercentage)))
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
