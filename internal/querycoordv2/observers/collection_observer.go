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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/eventlog"
	"github.com/milvus-io/milvus/pkg/log"
)

type CollectionObserver struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dist                 *meta.DistributionManager
	meta                 *meta.Meta
	targetMgr            *meta.TargetManager
	targetObserver       *TargetObserver
	leaderObserver       *LeaderObserver
	checkerController    *checkers.CheckerController
	partitionLoadedCount map[int64]int

	stopOnce sync.Once
}

func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	targetObserver *TargetObserver,
	leaderObserver *LeaderObserver,
	checherController *checkers.CheckerController,
) *CollectionObserver {
	return &CollectionObserver{
		dist:                 dist,
		meta:                 meta,
		targetMgr:            targetMgr,
		targetObserver:       targetObserver,
		leaderObserver:       leaderObserver,
		checkerController:    checherController,
		partitionLoadedCount: make(map[int64]int),
	}
}

func (ob *CollectionObserver) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	ob.cancel = cancel

	const observePeriod = time.Second
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
}

func (ob *CollectionObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()
	})
}

func (ob *CollectionObserver) Observe(ctx context.Context) {
	ob.observeTimeout()
	ob.observeLoadStatus(ctx)
}

func (ob *CollectionObserver) observeTimeout() {
	collections := ob.meta.CollectionManager.GetAllCollections()
	for _, collection := range collections {
		if collection.GetStatus() != querypb.LoadStatus_Loading ||
			time.Now().Before(collection.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
			continue
		}

		log.Info("load collection timeout, cancel it",
			zap.Int64("collectionID", collection.GetCollectionID()),
			zap.Duration("loadTime", time.Since(collection.CreatedAt)))
		ob.meta.CollectionManager.RemoveCollection(collection.GetCollectionID())
		ob.meta.ReplicaManager.RemoveCollection(collection.GetCollectionID())
		ob.targetMgr.RemoveCollection(collection.GetCollectionID())
	}

	partitions := utils.GroupPartitionsByCollection(ob.meta.CollectionManager.GetAllPartitions())
	for collection, partitions := range partitions {
		for _, partition := range partitions {
			if partition.GetStatus() != querypb.LoadStatus_Loading ||
				time.Now().Before(partition.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
				continue
			}

			log.Info("load partition timeout, cancel it",
				zap.Int64("collectionID", collection),
				zap.Int64("partitionID", partition.GetPartitionID()),
				zap.Duration("loadTime", time.Since(partition.CreatedAt)))
			ob.meta.CollectionManager.RemovePartition(collection, partition.GetPartitionID())
			ob.targetMgr.RemovePartition(partition.GetCollectionID(), partition.GetPartitionID())
		}
		// all partition timeout, remove collection
		if len(ob.meta.CollectionManager.GetPartitionsByCollection(collection)) == 0 {
			log.Info("collection timeout due to all partition removed", zap.Int64("collection", collection))

			ob.meta.CollectionManager.RemoveCollection(collection)
			ob.meta.ReplicaManager.RemoveCollection(collection)
			ob.targetMgr.RemoveCollection(collection)
		}
	}
}

func (ob *CollectionObserver) readyToObserve(collectionID int64) bool {
	metaExist := (ob.meta.GetCollection(collectionID) != nil)
	targetExist := ob.targetMgr.IsNextTargetExist(collectionID) || ob.targetMgr.IsCurrentTargetExist(collectionID)

	return metaExist && targetExist
}

func (ob *CollectionObserver) observeLoadStatus(ctx context.Context) {
	partitions := ob.meta.CollectionManager.GetAllPartitions()
	if len(partitions) > 0 {
		log.Info("observe partitions status", zap.Int("partitionNum", len(partitions)))
	}
	loading := false
	for _, partition := range partitions {
		if partition.LoadPercentage == 100 {
			continue
		}
		if ob.readyToObserve(partition.CollectionID) {
			replicaNum := ob.meta.GetReplicaNumber(partition.GetCollectionID())
			ob.observePartitionLoadStatus(ctx, partition, replicaNum)
			loading = true
		}
	}
	// trigger check logic when loading collections/partitions
	if loading {
		ob.checkerController.Check()
	}
}

func (ob *CollectionObserver) observePartitionLoadStatus(ctx context.Context, partition *meta.Partition, replicaNum int32) {
	log := log.With(
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

	log.Info("partition targets",
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", len(channelTargets)),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", replicaNum),
	)
	loadedCount := 0
	loadPercentage := int32(0)

	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			partition.GetCollectionID(),
			ob.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		loadedCount += len(group)
	}
	subChannelCount := loadedCount
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			partition.GetCollectionID(),
			ob.dist.LeaderViewManager.GetSealedSegmentDist(segment.GetID()))
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
		if !ob.targetObserver.Check(ctx, partition.GetCollectionID()) {
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
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("collection %d load percentage update: %d", partition.CollectionID, loadPercentage)))
}
