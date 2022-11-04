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
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

type CollectionHandoffStatus int32
type HandoffEventStatus int32

const (
	// CollectionHandoffStatusRegistered start receive handoff event
	CollectionHandoffStatusRegistered CollectionHandoffStatus = iota + 1
	// CollectionHandoffStatusStarted start trigger handoff event
	CollectionHandoffStatusStarted
)

const (
	HandoffEventStatusReceived HandoffEventStatus = iota + 1
	HandoffEventStatusTriggered
)

type HandoffEvent struct {
	Segment *querypb.SegmentInfo
	Status  HandoffEventStatus
}

type queue []int64

type HandoffObserver struct {
	store    meta.Store
	c        chan struct{}
	wg       sync.WaitGroup
	meta     *meta.Meta
	dist     *meta.DistributionManager
	target   *meta.TargetManager
	broker   meta.Broker
	revision int64

	collectionStatus map[int64]CollectionHandoffStatus
	handoffEventLock sync.RWMutex
	handoffEvents    map[int64]*HandoffEvent
	// collection id -> queue
	handoffSubmitOrders map[int64]queue
	// collectionId -> loaded partitionId, only for load collection case
	loadedPartitions map[int64]typeutil.UniqueSet

	stopOnce sync.Once
}

func NewHandoffObserver(store meta.Store, meta *meta.Meta, dist *meta.DistributionManager, target *meta.TargetManager, broker meta.Broker) *HandoffObserver {
	return &HandoffObserver{
		store:               store,
		c:                   make(chan struct{}),
		meta:                meta,
		dist:                dist,
		target:              target,
		broker:              broker,
		collectionStatus:    map[int64]CollectionHandoffStatus{},
		handoffEvents:       map[int64]*HandoffEvent{},
		handoffSubmitOrders: map[int64]queue{},
		loadedPartitions:    map[int64]typeutil.Set[int64]{},
	}
}

func (ob *HandoffObserver) Register(collectionIDs ...int64) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()
	log.Info("Register handoff for collection",
		zap.Int64s("collectionIDs", collectionIDs))

	for _, collectionID := range collectionIDs {
		ob.collectionStatus[collectionID] = CollectionHandoffStatusRegistered
	}
}

func (ob *HandoffObserver) Unregister(ctx context.Context, collectionIDs ...int64) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()
	log.Info("Unregister handoff for collection",
		zap.Int64s("collectionIDs", collectionIDs))

	for _, collectionID := range collectionIDs {
		delete(ob.collectionStatus, collectionID)
		delete(ob.handoffSubmitOrders, collectionID)
	}

	collectionSet := typeutil.NewUniqueSet(collectionIDs...)
	for segmentID, event := range ob.handoffEvents {
		if collectionSet.Contain(event.Segment.GetCollectionID()) {
			delete(ob.handoffEvents, segmentID)
		}
	}
}

func (ob *HandoffObserver) StartHandoff(collectionIDs ...int64) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()

	for _, collectionID := range collectionIDs {
		ob.collectionStatus[collectionID] = CollectionHandoffStatusStarted
	}
}

func (ob *HandoffObserver) GetEventNum() int {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()

	return len(ob.handoffEvents)
}

func (ob *HandoffObserver) consumeOutdatedHandoffEvent(ctx context.Context) error {
	_, handoffReqValues, revision, err := ob.store.LoadHandoffWithRevision()
	if err != nil {
		log.Error("reloadFromKV: LoadWithRevision from kv failed", zap.Error(err))
		return err
	}
	// set watch start revision
	ob.revision = revision

	for _, value := range handoffReqValues {
		segmentInfo := &querypb.SegmentInfo{}
		err := proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			log.Error("reloadFromKV: unmarshal failed", zap.Error(err))
			return err
		}
		ob.cleanEvent(ctx, segmentInfo)
	}

	return nil
}

func (ob *HandoffObserver) Start(ctx context.Context) error {
	log.Info("Start reload handoff event from etcd")
	if err := ob.consumeOutdatedHandoffEvent(ctx); err != nil {
		log.Error("handoff observer reload from kv failed", zap.Error(err))
		return err
	}
	log.Info("Finish reload handoff event from etcd")

	ob.wg.Add(1)
	go ob.schedule(ctx)

	return nil
}

func (ob *HandoffObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.c)
		ob.wg.Wait()
	})
}

func (ob *HandoffObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("start watch Segment handoff loop")
	ticker := time.NewTicker(Params.QueryCoordCfg.CheckHandoffInterval)
	log.Info("handoff interval", zap.String("interval", Params.QueryCoordCfg.CheckHandoffInterval.String()))
	watchChan := ob.store.WatchHandoffEvent(ob.revision + 1)
	for {
		select {
		case <-ctx.Done():
			log.Info("close handoff handler due to context done!")
			return
		case <-ob.c:
			log.Info("close handoff handler")
			return

		case resp, ok := <-watchChan:
			if !ok {
				log.Error("watch Segment handoff loop failed because watch channel is closed!")
				return
			}

			if err := resp.Err(); err != nil {
				log.Warn("receive error handoff event from etcd",
					zap.Error(err))
			}

			for _, event := range resp.Events {
				segmentInfo := &querypb.SegmentInfo{}
				err := proto.Unmarshal(event.Kv.Value, segmentInfo)
				if err != nil {
					log.Error("failed to deserialize handoff event", zap.Error(err))
					continue
				}

				switch event.Type {
				case mvccpb.PUT:
					ob.tryHandoff(ctx, segmentInfo)
				default:
					log.Warn("HandoffObserver: receive event",
						zap.String("type", event.Type.String()),
						zap.String("key", string(event.Kv.Key)),
					)
				}
			}

		case <-ticker.C:
			for _, event := range ob.handoffEvents {
				switch event.Status {
				case HandoffEventStatusReceived:
					ob.tryHandoff(ctx, event.Segment)
				case HandoffEventStatusTriggered:
					ob.tryRelease(ctx, event)
				}
			}

			ob.tryClean(ctx)
		}
	}
}

func (ob *HandoffObserver) tryHandoff(ctx context.Context, segment *querypb.SegmentInfo) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()

	indexIDs := lo.Map(segment.GetIndexInfos(), func(indexInfo *querypb.FieldIndexInfo, _ int) int64 { return indexInfo.GetIndexID() })
	log := log.With(zap.Int64("collectionID", segment.GetCollectionID()),
		zap.Int64("partitionID", segment.GetPartitionID()),
		zap.Int64("segmentID", segment.GetSegmentID()),
		zap.Bool("fake", segment.GetIsFake()),
		zap.Int64s("indexIDs", indexIDs),
	)

	log.Info("try handoff segment...")
	status, collectionRegistered := ob.collectionStatus[segment.GetCollectionID()]
	if Params.QueryCoordCfg.AutoHandoff &&
		collectionRegistered &&
		ob.checkLoadStatus(segment) &&
		(segment.GetIsFake() || ob.meta.CollectionManager.ContainAnyIndex(segment.GetCollectionID(), indexIDs...)) {
		event := ob.handoffEvents[segment.SegmentID]
		if event == nil {
			// record submit order
			_, ok := ob.handoffSubmitOrders[segment.GetCollectionID()]
			if !ok {
				ob.handoffSubmitOrders[segment.GetCollectionID()] = make([]int64, 0)
			}
			ob.handoffSubmitOrders[segment.GetCollectionID()] = append(ob.handoffSubmitOrders[segment.GetCollectionID()], segment.GetSegmentID())
		}

		if status == CollectionHandoffStatusRegistered {
			if event == nil {
				// keep all handoff event, waiting collection ready and to trigger handoff
				ob.handoffEvents[segment.GetSegmentID()] = &HandoffEvent{
					Segment: segment,
					Status:  HandoffEventStatusReceived,
				}
			}
			return
		}

		ob.handoffEvents[segment.GetSegmentID()] = &HandoffEvent{
			Segment: segment,
			Status:  HandoffEventStatusTriggered,
		}

		if !segment.GetIsFake() {
			log.Info("start to do handoff...")
			ob.handoff(segment)
		}
	} else {
		// ignore handoff task
		log.Info("handoff event trigger failed due to collection/partition is not loaded!")
		ob.cleanEvent(ctx, segment)
	}
}

func (ob *HandoffObserver) checkLoadStatus(segment *querypb.SegmentInfo) bool {
	if ob.meta.GetCollection(segment.GetCollectionID()) != nil {
		// if collection is loaded, should check whether the partition has been droped!
		if ob.loadedPartitions[segment.GetCollectionID()] == nil {
			ob.loadedPartitions[segment.GetCollectionID()] = typeutil.NewUniqueSet()
		}

		// should updated loaded partitions when meet new partitionID
		if !ob.loadedPartitions[segment.GetCollectionID()].Contain(segment.GetPartitionID()) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := retry.Do(ctx, func() error {
				partitionIDs, err := ob.broker.GetPartitions(ctx, segment.GetCollectionID())
				if err == nil {
					ob.loadedPartitions[segment.GetCollectionID()].Insert(partitionIDs...)
					return nil
				}
				return err
			}, retry.Attempts(5))

			if err != nil {
				// collection has been dropped or released
				if strings.Contains(err.Error(), "CollectionNotExists") ||
					ob.meta.GetCollection(segment.GetCollectionID()) == nil {
					return false
				}
				// collection not released , but can get partition list to check handoff
				log.Warn("handoff check load status failed due to get partitions failed",
					zap.Int64("collectionID", segment.GetCollectionID()),
					zap.Int64("partitionID", segment.GetPartitionID()),
					zap.String("channel", segment.GetDmChannel()),
					zap.Int64("segmentID", segment.GetSegmentID()))
				return false
			}
		}

		return ob.loadedPartitions[segment.GetCollectionID()].Contain(segment.GetPartitionID())
	}

	return ob.meta.GetPartition(segment.GetPartitionID()) != nil
}

func (ob *HandoffObserver) handoff(segment *querypb.SegmentInfo) {
	targets := ob.target.GetSegmentsByCollection(segment.GetCollectionID(), segment.GetPartitionID())
	// when handoff event load a Segment, it sobuld remove all recursive handoff compact from
	uniqueSet := typeutil.NewUniqueSet()
	recursiveCompactFrom := ob.getOverrideSegmentInfo(targets, segment.CompactionFrom...)
	uniqueSet.Insert(recursiveCompactFrom...)
	uniqueSet.Insert(segment.GetCompactionFrom()...)

	segmentInfo := &datapb.SegmentInfo{
		ID:                  segment.GetSegmentID(),
		CollectionID:        segment.GetCollectionID(),
		PartitionID:         segment.GetPartitionID(),
		NumOfRows:           segment.NumRows,
		InsertChannel:       segment.GetDmChannel(),
		State:               segment.GetSegmentState(),
		CreatedByCompaction: segment.GetCreatedByCompaction(),
		CompactionFrom:      uniqueSet.Collect(),
	}

	log.Info("HandoffObserver: handoff Segment, register to target")
	ob.target.HandoffSegment(segmentInfo, segmentInfo.CompactionFrom...)
}

func (ob *HandoffObserver) isSegmentReleased(id int64) bool {
	return len(ob.dist.LeaderViewManager.GetSegmentDist(id)) == 0
}

func (ob *HandoffObserver) isGrowingSegmentReleased(id int64) bool {
	return len(ob.dist.LeaderViewManager.GetGrowingSegmentDist(id)) == 0
}

func (ob *HandoffObserver) isSealedSegmentLoaded(segment *querypb.SegmentInfo) bool {
	// must be sealed Segment loaded in all replica, in case of handoff between growing and sealed
	nodes := ob.dist.LeaderViewManager.GetSealedSegmentDist(segment.GetSegmentID())
	replicas := utils.GroupNodesByReplica(ob.meta.ReplicaManager, segment.GetCollectionID(), nodes)
	return len(replicas) == len(ob.meta.ReplicaManager.GetByCollection(segment.GetCollectionID()))
}

func (ob *HandoffObserver) getOverrideSegmentInfo(handOffSegments []*datapb.SegmentInfo, segmentIDs ...int64) []int64 {
	overrideSegments := make([]int64, 0)
	for _, segmentID := range segmentIDs {
		for _, segmentInHandoff := range handOffSegments {
			if segmentID == segmentInHandoff.ID {
				toReleaseSegments := ob.getOverrideSegmentInfo(handOffSegments, segmentInHandoff.CompactionFrom...)
				if len(toReleaseSegments) > 0 {
					overrideSegments = append(overrideSegments, toReleaseSegments...)
				}

				overrideSegments = append(overrideSegments, segmentID)
			}
		}
	}

	return overrideSegments
}

func (ob *HandoffObserver) isAllCompactFromHandoffCompleted(segmentInfo *querypb.SegmentInfo) bool {
	for _, segID := range segmentInfo.CompactionFrom {
		_, ok := ob.handoffEvents[segID]
		if ok {
			return false
		}
	}
	return true
}

func (ob *HandoffObserver) tryRelease(ctx context.Context, event *HandoffEvent) {
	segment := event.Segment

	if ob.isSealedSegmentLoaded(segment) || !ob.isSegmentExistOnTarget(segment) {
		// Note: the fake segment will not add into target segments, in order to guarantee
		// the all parent segments are released we check handoff events list instead of to
		// check segment from the leader view, or might miss some segments to release.
		if segment.GetIsFake() && !ob.isAllCompactFromHandoffCompleted(segment) {
			log.Debug("try to release fake segments fails, due to the dependencies haven't complete handoff.",
				zap.Int64("segmentID", segment.GetSegmentID()),
				zap.Bool("faked", segment.GetIsFake()),
				zap.Int64s("sourceSegments", segment.CompactionFrom),
			)
			return
		}

		compactSource := segment.CompactionFrom
		if len(compactSource) == 0 {
			return
		}
		log.Info("remove compactFrom segments",
			zap.Int64("collectionID", segment.GetCollectionID()),
			zap.Int64("partitionID", segment.GetPartitionID()),
			zap.Int64("segmentID", segment.GetSegmentID()),
			zap.Bool("faked", segment.GetIsFake()),
			zap.Int64s("sourceSegments", compactSource),
		)
		for _, toRelease := range compactSource {
			// when handoff happens between growing and sealed, both with same Segment id, so can't remove from target here
			if segment.CreatedByCompaction {
				ob.target.RemoveSegment(toRelease)
			}
		}
	}
}

func (ob *HandoffObserver) tryClean(ctx context.Context) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()

	for collectionID, partitionSubmitOrder := range ob.handoffSubmitOrders {
		pos := 0
		for _, segmentID := range partitionSubmitOrder {
			event, ok := ob.handoffEvents[segmentID]
			if !ok {
				continue
			}

			segment := event.Segment
			if ob.isAllCompactFromReleased(segment) {
				log.Info("HandoffObserver: clean handoff event after handoff finished!",
					zap.Int64("collectionID", segment.GetCollectionID()),
					zap.Int64("partitionID", segment.GetPartitionID()),
					zap.Int64("segmentID", segment.GetSegmentID()),
					zap.Bool("faked", segment.GetIsFake()),
				)
				err := ob.cleanEvent(ctx, segment)
				if err == nil {
					delete(ob.handoffEvents, segment.GetSegmentID())
				}
				pos++
			} else {
				break
			}
		}
		ob.handoffSubmitOrders[collectionID] = partitionSubmitOrder[pos:]
	}
}

func (ob *HandoffObserver) cleanEvent(ctx context.Context, segmentInfo *querypb.SegmentInfo) error {
	log := log.With(
		zap.Int64("collectionID", segmentInfo.CollectionID),
		zap.Int64("partitionID", segmentInfo.PartitionID),
		zap.Int64("segmentID", segmentInfo.SegmentID),
	)

	// add retry logic
	err := retry.Do(ctx, func() error {
		return ob.store.RemoveHandoffEvent(segmentInfo)
	}, retry.Attempts(5))

	if err != nil {
		log.Warn("failed to clean handoff event from etcd", zap.Error(err))
	}
	return err
}

func (ob *HandoffObserver) isSegmentExistOnTarget(segmentInfo *querypb.SegmentInfo) bool {
	return ob.target.ContainSegment(segmentInfo.SegmentID)
}

func (ob *HandoffObserver) isAllCompactFromReleased(segmentInfo *querypb.SegmentInfo) bool {
	if !segmentInfo.CreatedByCompaction {
		return ob.isGrowingSegmentReleased(segmentInfo.SegmentID)
	}
	for _, segment := range segmentInfo.CompactionFrom {
		if !ob.isSegmentReleased(segment) {
			return false
		}
	}
	return true
}
