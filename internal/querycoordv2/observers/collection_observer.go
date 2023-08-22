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
	stopCh chan struct{}

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
		stopCh:               make(chan struct{}),
		dist:                 dist,
		meta:                 meta,
		targetMgr:            targetMgr,
		targetObserver:       targetObserver,
		leaderObserver:       leaderObserver,
		checkerController:    checherController,
		partitionLoadedCount: make(map[int64]int),
	}
}

func (ob *CollectionObserver) Start(ctx context.Context) {
	const observePeriod = time.Second
	go func() {
		ticker := time.NewTicker(observePeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info("CollectionObserver stopped due to context canceled")
				return

			case <-ob.stopCh:
				log.Info("CollectionObserver stopped")
				return

			case <-ticker.C:
				ob.Observe()
			}
		}
	}()
}

func (ob *CollectionObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.stopCh)
	})
}

func (ob *CollectionObserver) Observe() {
	ob.observeTimeout()
	ob.observeLoadStatus()
}

func (ob *CollectionObserver) observeTimeout() {
	partitions := utils.GroupPartitionsByCollection(
		ob.meta.CollectionManager.GetAllPartitions())
	if len(partitions) == 0 {
		return
	}

	log.Info("observes load timeout",
		zap.Int("partitionNum", len(partitions)),
	)
	for collectionID, partitions := range partitions {
		log := log.With(
			zap.Int64("collectionID", collectionID),
		)
		collection := ob.meta.CollectionManager.GetCollection(collectionID)
		if collection.GetStatus() == querypb.LoadStatus_Loading &&
			time.Since(collection.UpdatedAt) > Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second) {
			log.Info("load collection timeout, cancel it",
				zap.Duration("loadTime", time.Since(collection.CreatedAt)))
			ob.meta.CollectionManager.RemoveCollection(collection.GetCollectionID())
			ob.meta.ReplicaManager.RemoveCollection(collection.GetCollectionID())
			ob.targetMgr.RemoveCollection(collection.GetCollectionID())
			continue
		}

		for _, partition := range partitions {
			if partition.GetStatus() == querypb.LoadStatus_Loading &&
				time.Since(partition.UpdatedAt) > Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second) {
				log.Info("load partition timeout, cancel it",
					zap.Int64("partitionID", partition.GetPartitionID()),
					zap.Duration("loadTime", time.Since(partition.CreatedAt)))
				ob.meta.CollectionManager.RemovePartition(partition.GetPartitionID())
				ob.targetMgr.RemovePartition(partition.GetCollectionID(), partition.GetPartitionID())

				// all partition timeout, remove collection
				if len(ob.meta.CollectionManager.GetPartitionsByCollection(collectionID)) == 0 {
					log.Info("collection timeout due to all partition removed")

					ob.meta.CollectionManager.RemoveCollection(collectionID)
					ob.meta.ReplicaManager.RemoveCollection(collectionID)
					ob.targetMgr.RemoveCollection(collectionID)
				}
			}
		}
	}
}

func (ob *CollectionObserver) readyToObserve(collectionID int64) bool {
	metaExist := (ob.meta.GetCollection(collectionID) != nil)
	targetExist := ob.targetMgr.IsNextTargetExist(collectionID) || ob.targetMgr.IsCurrentTargetExist(collectionID)

	return metaExist && targetExist
}

func (ob *CollectionObserver) observeLoadStatus() {
	partitions := ob.meta.CollectionManager.GetAllPartitions()
	if len(partitions) == 0 {
		return
	}

	log.Info("observe load status",
		zap.Int("partitionNum", len(partitions)),
	)

	loadedPartitions := make([]*meta.Partition, 0)
	for _, partition := range partitions {
		if !ob.readyToObserve(partition.GetCollectionID()) ||
			partition.LoadPercentage == 100 && ob.targetMgr.IsCurrentTargetExist(partition.GetCollectionID()) {
			continue
		}

		replicaNum := ob.meta.GetReplicaNumber(partition.GetCollectionID())
		percentage, isAdvanced := ob.observePartitionLoadStatus(partition, replicaNum)
		if percentage == 100 {
			loadedPartitions = append(loadedPartitions, partition)
			delete(ob.partitionLoadedCount, partition.GetPartitionID())
		} else if isAdvanced {
			ob.meta.CollectionManager.UpdateLoadPercent(partition.GetPartitionID(), percentage)
		}
	}

	// group partitions by collection,
	// then we can check the target and version only once for each collection,
	// this will be much faster if many partitions exist.
	toUpdate := utils.GroupPartitionsByCollection(loadedPartitions)
	for collectionID, partitions := range toUpdate {
		log := log.With(
			zap.Int64("collectionID", collectionID),
		)
		var (
			percentage int32
			err        error
		)
		for _, partition := range partitions {
			percentage, err = ob.meta.CollectionManager.UpdateLoadPercent(partition.GetPartitionID(), 100)
			if err != nil {
				log.Warn("failed to update load percentage",
					zap.Int64("partitionID", partition.GetPartitionID()),
					zap.Error(err),
				)
				break
			}
		}

		if percentage == 100 &&
			ob.targetObserver.Check(collectionID) &&
			ob.leaderObserver.CheckTargetVersion(collectionID) {
			log.Info("collection load done")
		}

		log.Info("load status updated",
			zap.Int32("collectionLoadPercentage", percentage),
		)
		eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("collection %d load percentage update: %d", collectionID, percentage)))
	}
}

// observePartitionLoadStatus checks whether the given partition is loaded,
// returns the load percentage and whether the progress advanced,
// this doesn't update the metadata, as after loaded we need to update target and leader view.
func (ob *CollectionObserver) observePartitionLoadStatus(partition *meta.Partition, replicaNum int32) (int32, bool) {
	log := log.With(
		zap.Int64("collectionID", partition.GetCollectionID()),
		zap.Int64("partitionID", partition.GetPartitionID()),
	)

	segmentTargets := ob.targetMgr.GetHistoricalSegmentsByPartition(partition.GetCollectionID(), partition.GetPartitionID(), meta.NextTarget)
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(partition.GetCollectionID(), meta.NextTarget)

	targetNum := len(segmentTargets) + len(channelTargets)
	if targetNum == 0 {
		log.Info("segments and channels in target are both empty, waiting for new target content")
		return 0, false
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
		return loadPercentage, false
	}

	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	return loadPercentage, true
}
