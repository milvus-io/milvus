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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

type CollectionObserver struct {
	stopCh chan struct{}

	dist                     *meta.DistributionManager
	meta                     *meta.Meta
	targetMgr                *meta.TargetManager
	targetObserver           *TargetObserver
	collectionLoadedCount    map[int64]int
	partitionLoadedCount     map[int64]int
	collectionNextTargetTime map[int64]time.Time

	stopOnce sync.Once
}

func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	targetObserver *TargetObserver,
) *CollectionObserver {
	return &CollectionObserver{
		stopCh:                   make(chan struct{}),
		dist:                     dist,
		meta:                     meta,
		targetMgr:                targetMgr,
		targetObserver:           targetObserver,
		collectionLoadedCount:    make(map[int64]int),
		partitionLoadedCount:     make(map[int64]int),
		collectionNextTargetTime: make(map[int64]time.Time),
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

	partitions := utils.GroupPartitionsByCollection(
		ob.meta.CollectionManager.GetAllPartitions())
	if len(partitions) > 0 {
		log.Info("observes partitions timeout", zap.Int("partitionNum", len(partitions)))
	}
	for collection, partitions := range partitions {
		log := log.With(
			zap.Int64("collectionID", collection),
		)
		for _, partition := range partitions {
			if partition.GetStatus() != querypb.LoadStatus_Loading ||
				time.Now().Before(partition.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
				continue
			}

			log.Info("load partition timeout, cancel all partitions",
				zap.Int64("partitionID", partition.GetPartitionID()),
				zap.Duration("loadTime", time.Since(partition.CreatedAt)))
			// TODO(yah01): Now, releasing part of partitions is not allowed
			ob.meta.CollectionManager.RemoveCollection(partition.GetCollectionID())
			ob.meta.ReplicaManager.RemoveCollection(partition.GetCollectionID())
			ob.targetMgr.RemoveCollection(partition.GetCollectionID())
			break
		}
	}
}

func (ob *CollectionObserver) observeLoadStatus() {
	collections := ob.meta.CollectionManager.GetAllCollections()
	for _, collection := range collections {
		if collection.LoadPercentage == 100 {
			continue
		}
		ob.observeCollectionLoadStatus(collection)
	}

	partitions := ob.meta.CollectionManager.GetAllPartitions()
	if len(partitions) > 0 {
		log.Info("observe partitions status", zap.Int("partitionNum", len(partitions)))
	}
	for _, partition := range partitions {
		if partition.LoadPercentage == 100 {
			continue
		}
		ob.observePartitionLoadStatus(partition)
	}
}

func (ob *CollectionObserver) observeCollectionLoadStatus(collection *meta.Collection) {
	log := log.With(zap.Int64("collectionID", collection.GetCollectionID()))

	segmentTargets := ob.targetMgr.GetHistoricalSegmentsByCollection(collection.GetCollectionID(), meta.NextTarget)
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(collection.GetCollectionID(), meta.NextTarget)
	targetNum := len(segmentTargets) + len(channelTargets)
	log.Info("collection targets",
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", len(channelTargets)),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", collection.GetReplicaNumber()),
	)

	updated := collection.Clone()
	loadedCount := 0
	if targetNum == 0 {
		log.Info("No segment/channel in target need to be loaded!")
		updated.LoadPercentage = 100
	} else {
		for _, channel := range channelTargets {
			group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
				collection.GetCollectionID(),
				ob.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
			loadedCount += len(group)
		}
		subChannelCount := loadedCount
		for _, segment := range segmentTargets {
			group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
				collection.GetCollectionID(),
				ob.dist.LeaderViewManager.GetSealedSegmentDist(segment.GetID()))
			loadedCount += len(group)
		}
		if loadedCount > 0 {
			log.Info("collection load progress",
				zap.Int("subChannelCount", subChannelCount),
				zap.Int("loadSegmentCount", loadedCount-subChannelCount),
			)
		}

		updated.LoadPercentage = int32(loadedCount * 100 / (targetNum * int(collection.GetReplicaNumber())))
	}

	targetTime := ob.targetMgr.GetNextTargetCreateTime(collection.CollectionID)
	lastTime, ok := ob.collectionNextTargetTime[collection.CollectionID]

	if ok && targetTime.Equal(lastTime) &&
		loadedCount <= ob.collectionLoadedCount[collection.GetCollectionID()] &&
		updated.LoadPercentage != 100 {
		return
	}

	ob.collectionNextTargetTime[collection.GetCollectionID()] = targetTime
	ob.collectionLoadedCount[collection.GetCollectionID()] = loadedCount
	if updated.LoadPercentage == 100 && ob.targetObserver.Check(updated.GetCollectionID()) {
		delete(ob.collectionLoadedCount, collection.GetCollectionID())
		updated.Status = querypb.LoadStatus_Loaded
		ob.meta.CollectionManager.UpdateCollection(updated)

		elapsed := time.Since(updated.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	} else {
		ob.meta.CollectionManager.UpdateCollectionInMemory(updated)
	}
	log.Info("collection load status updated",
		zap.Int32("loadPercentage", updated.LoadPercentage),
		zap.Int32("collectionStatus", int32(updated.GetStatus())))
}

func (ob *CollectionObserver) observePartitionLoadStatus(partition *meta.Partition) {
	log := log.With(
		zap.Int64("collectionID", partition.GetCollectionID()),
		zap.Int64("partitionID", partition.GetPartitionID()),
	)

	segmentTargets := ob.targetMgr.GetHistoricalSegmentsByPartition(partition.GetCollectionID(), partition.GetPartitionID(), meta.NextTarget)
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(partition.GetCollectionID(), meta.NextTarget)
	targetNum := len(segmentTargets) + len(channelTargets)
	log.Info("partition targets",
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", len(channelTargets)),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", partition.GetReplicaNumber()),
	)

	loadedCount := 0
	updated := partition.Clone()
	if targetNum == 0 {
		log.Info("No segment/channel in target need to be loaded!")
		updated.LoadPercentage = 100
	} else {
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
		updated.LoadPercentage = int32(loadedCount * 100 / (targetNum * int(partition.GetReplicaNumber())))
	}

	targetTime := ob.targetMgr.GetNextTargetCreateTime(partition.GetCollectionID())
	lastTime, ok := ob.collectionNextTargetTime[partition.GetCollectionID()]

	if ok && targetTime.Equal(lastTime) &&
		loadedCount <= ob.partitionLoadedCount[partition.GetPartitionID()] &&
		updated.LoadPercentage != 100 {
		return
	}
	ob.collectionNextTargetTime[partition.GetCollectionID()] = targetTime
	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	if updated.LoadPercentage == 100 && ob.targetObserver.Check(updated.GetCollectionID()) {
		delete(ob.partitionLoadedCount, partition.GetPartitionID())
		updated.Status = querypb.LoadStatus_Loaded
		ob.meta.CollectionManager.PutPartition(updated)

		elapsed := time.Since(updated.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	} else {
		ob.meta.CollectionManager.UpdatePartitionInMemory(updated)
	}
	log.Info("partition load status updated",
		zap.Int32("loadPercentage", updated.LoadPercentage),
		zap.Int32("partitionStatus", int32(updated.GetStatus())))
}
