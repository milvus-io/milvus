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

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type targetOp int

func (op *targetOp) String() string {
	switch *op {
	case UpdateCollection:
		return "UpdateCollection"
	case ReleaseCollection:
		return "ReleaseCollection"
	case ReleasePartition:
		return "ReleasePartition"
	default:
		return "Unknown"
	}
}

const (
	UpdateCollection targetOp = iota + 1
	ReleaseCollection
	ReleasePartition
	UpdatePartition
)

type targetUpdateRequest struct {
	CollectionID  int64
	PartitionIDs  []int64
	Notifier      chan error
	ReadyNotifier chan struct{}
	opType        targetOp
}

type initRequest struct{}

type nextTargetProgress struct {
	targetVersion        int64
	readySegmentReplicas int
	readyReplicaChannels int
	// lastUpdated records when this target version was installed or when its
	// readiness last advanced.
	lastUpdated time.Time
}

type TargetObserver struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	meta      *meta.Meta
	targetMgr meta.TargetManagerInterface
	distMgr   *meta.DistributionManager
	broker    meta.Broker
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	initChan chan initRequest
	// nextTargetProgresses records the loading progress checkpoint for each
	// collection's next target.
	nextTargetProgresses *typeutil.ConcurrentMap[int64, nextTargetProgress]
	// nextTargetStale records collections whose next target must be refreshed.
	// A refresh consumes the marker before the RPC and restores it on failure or
	// no-op, so a notification arriving during the RPC remains pending.
	nextTargetStale *typeutil.ConcurrentSet[int64]
	updateChan      chan targetUpdateRequest
	mut             sync.Mutex                // Guard readyNotifiers
	readyNotifiers  map[int64][]chan struct{} // CollectionID -> Notifiers

	// loadingDispatcher updates targets for collections that are loading (also collections without a current target).
	loadingDispatcher *taskDispatcher[int64]
	// loadedDispatcher updates targets for loaded collections.
	loadedDispatcher *taskDispatcher[int64]

	keylocks *lock.KeyLock[int64]

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewTargetObserver(
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	distMgr *meta.DistributionManager,
	broker meta.Broker,
	cluster session.Cluster,
	nodeMgr *session.NodeManager,
) *TargetObserver {
	result := &TargetObserver{
		meta:                 meta,
		targetMgr:            targetMgr,
		distMgr:              distMgr,
		broker:               broker,
		cluster:              cluster,
		nodeMgr:              nodeMgr,
		nextTargetProgresses: typeutil.NewConcurrentMap[int64, nextTargetProgress](),
		nextTargetStale:      typeutil.NewConcurrentSet[int64](),
		updateChan:           make(chan targetUpdateRequest, 10),
		readyNotifiers:       make(map[int64][]chan struct{}),
		initChan:             make(chan initRequest),
		keylocks:             lock.NewKeyLock[int64](),
	}

	result.loadingDispatcher = newTaskDispatcher(result.check)
	result.loadedDispatcher = newTaskDispatcher(result.check)
	return result
}

func (ob *TargetObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		ob.loadingDispatcher.Start()
		ob.loadedDispatcher.Start()

		ob.wg.Add(1)
		go func() {
			defer ob.wg.Done()
			ob.schedule(ctx)
		}()

		// after target observer start, update target for all collection
		ob.initChan <- initRequest{}
	})
}

func (ob *TargetObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()

		ob.loadingDispatcher.Stop()
		ob.loadedDispatcher.Stop()
	})
}

func (ob *TargetObserver) schedule(ctx context.Context) {
	mlog.Info(ctx, "Start update next target loop")

	interval := params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "Close target observer")
			return

		case <-ob.initChan:
			for _, collectionID := range ob.meta.GetAll(ctx) {
				ob.init(ctx, collectionID)
			}
			mlog.Info(ctx, "target observer init done")

		case <-ticker.C:
			ob.clean()

			collections := ob.meta.GetAllCollections(ctx)
			var loadedIDs, loadingIDs []int64
			for _, c := range collections {
				if c.GetStatus() == querypb.LoadStatus_Loaded {
					loadedIDs = append(loadedIDs, c.GetCollectionID())
				} else {
					loadingIDs = append(loadingIDs, c.GetCollectionID())
				}
			}

			ob.loadedDispatcher.AddTask(loadedIDs...)
			ob.loadingDispatcher.AddTask(loadingIDs...)

			// apply dynamic update only when changed
			newInterval := params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second)
			if newInterval != interval {
				interval = newInterval
				select {
				case <-ticker.C:
				default:
				}
				ticker.Reset(interval)
			}

		case req := <-ob.updateChan:
			mlog.Info(ctx, "manually trigger update target",
				mlog.FieldCollectionID(req.CollectionID),
				mlog.String("opType", req.opType.String()),
			)
			switch req.opType {
			case UpdateCollection:
				ob.keylocks.Lock(req.CollectionID)
				err := ob.updateNextTarget(ctx, req.CollectionID)
				ob.keylocks.Unlock(req.CollectionID)
				if err != nil {
					mlog.Warn(ctx, "failed to manually update next target",
						mlog.FieldCollectionID(req.CollectionID),
						mlog.String("opType", req.opType.String()),
						mlog.Err(err))
					close(req.ReadyNotifier)
				} else {
					ob.mut.Lock()
					ob.readyNotifiers[req.CollectionID] = append(ob.readyNotifiers[req.CollectionID], req.ReadyNotifier)
					ob.mut.Unlock()
				}
				req.Notifier <- err
			case ReleaseCollection:
				ob.mut.Lock()
				for _, notifier := range ob.readyNotifiers[req.CollectionID] {
					close(notifier)
				}
				delete(ob.readyNotifiers, req.CollectionID)
				ob.mut.Unlock()

				ob.keylocks.Lock(req.CollectionID)
				ob.targetMgr.RemoveCollection(ctx, req.CollectionID)
				ob.keylocks.Unlock(req.CollectionID)
				req.Notifier <- nil
			case ReleasePartition:
				ob.keylocks.Lock(req.CollectionID)
				ob.targetMgr.RemovePartitionFromNextTarget(ctx, req.CollectionID, req.PartitionIDs...)
				ob.keylocks.Unlock(req.CollectionID)
				req.Notifier <- nil
			case UpdatePartition:
				// Fast path: check with read lock first
				ob.keylocks.RLock(req.CollectionID)
				exists := ob.targetMgr.IsCurrentTargetExist(ctx, req.CollectionID, req.PartitionIDs[0])
				ob.keylocks.RUnlock(req.CollectionID)

				if exists {
					close(req.ReadyNotifier)
					req.Notifier <- nil
				} else {
					// Slow path: need to update next target
					ob.keylocks.Lock(req.CollectionID)
					// Double check after acquiring write lock
					if ob.targetMgr.IsCurrentTargetExist(ctx, req.CollectionID, req.PartitionIDs[0]) {
						close(req.ReadyNotifier)
						req.Notifier <- nil
					} else {
						err := ob.updateNextTarget(ctx, req.CollectionID)
						if err != nil {
							mlog.Warn(ctx, "failed to manually update next target",
								mlog.FieldCollectionID(req.CollectionID),
								mlog.String("opType", req.opType.String()),
								mlog.Err(err))
							close(req.ReadyNotifier)
						} else {
							ob.mut.Lock()
							ob.readyNotifiers[req.CollectionID] = append(ob.readyNotifiers[req.CollectionID], req.ReadyNotifier)
							ob.mut.Unlock()
						}
						req.Notifier <- err
					}
					ob.keylocks.Unlock(req.CollectionID)
				}
			}
			mlog.Info(ctx, "manually trigger update target done",
				mlog.FieldCollectionID(req.CollectionID),
				mlog.String("opType", req.opType.String()))
		}
	}
}

// Check whether provided collection is has current target.
// If not, submit an async task into dispatcher.
func (ob *TargetObserver) Check(ctx context.Context, collectionID int64, partitionID int64) bool {
	result := ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, partitionID)
	if !result {
		ob.loadingDispatcher.AddTask(collectionID)
	}
	return result
}

func (ob *TargetObserver) TriggerUpdateCurrentTarget(collectionID int64) {
	ob.loadingDispatcher.AddTask(collectionID)
}

// MarkNextTargetStale marks the collection's next target as stale when the
// failed segment still belongs to that target.
func (ob *TargetObserver) MarkNextTargetStale(collectionID, segmentID int64) {
	if ob.targetMgr.GetSealedSegment(context.TODO(), collectionID, segmentID, meta.NextTarget) == nil {
		return
	}

	// A concurrent target replacement can turn this into a conservative false
	// positive. That only causes an extra refresh and avoids coupling tasks to a
	// target generation.
	ob.nextTargetStale.Upsert(collectionID)
}

func (ob *TargetObserver) check(ctx context.Context, collectionID int64) {
	ob.keylocks.Lock(collectionID)
	defer ob.keylocks.Unlock(collectionID)

	// if collection release, skip check
	if ob.meta.GetCollection(ctx, collectionID) == nil {
		return
	}

	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(ctx, collectionID)
	}

	if ob.shouldUpdateNextTarget(ctx, collectionID) {
		// update next target in collection level
		if err := ob.updateNextTarget(ctx, collectionID); err != nil {
			return
		}

		// sync next target to delegator if current target not exist, to support partial search
		if !ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, -1) {
			newVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
			ob.syncNextTargetToDelegator(ctx, collectionID, ob.distMgr.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID)), newVersion)
		}
	}

	// Update the all-replicas checkpoint metric
	ob.updateAllReplicasCheckpointMetric(ctx, collectionID)
}

func (ob *TargetObserver) init(ctx context.Context, collectionID int64) {
	ob.keylocks.Lock(collectionID)
	// pull next target first if not exist
	if !ob.targetMgr.IsNextTargetExist(ctx, collectionID) {
		ob.updateNextTarget(ctx, collectionID)
	}

	// try to update current target if all segment/channel are ready
	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(ctx, collectionID)
	}
	ob.keylocks.Unlock(collectionID)

	// refresh collection loading status upon restart
	ob.check(ctx, collectionID)
}

// UpdateNextTarget updates the next target,
// returns a channel which will be closed when the next target is ready,
// or returns error if failed to pull target
func (ob *TargetObserver) UpdateNextTarget(collectionID int64) (chan struct{}, error) {
	notifier := make(chan error)
	readyCh := make(chan struct{})
	defer close(notifier)

	ob.updateChan <- targetUpdateRequest{
		CollectionID:  collectionID,
		opType:        UpdateCollection,
		Notifier:      notifier,
		ReadyNotifier: readyCh,
	}
	return readyCh, <-notifier
}

func (ob *TargetObserver) UpdatePartition(collectionID int64, partitionID int64) (chan struct{}, error) {
	notifier := make(chan error)
	readyCh := make(chan struct{})
	defer close(notifier)
	ob.updateChan <- targetUpdateRequest{
		CollectionID:  collectionID,
		PartitionIDs:  []int64{partitionID},
		opType:        UpdatePartition,
		Notifier:      notifier,
		ReadyNotifier: readyCh,
	}
	return readyCh, <-notifier
}

func (ob *TargetObserver) ReleaseCollection(collectionID int64) {
	notifier := make(chan error)
	defer close(notifier)
	ob.updateChan <- targetUpdateRequest{
		CollectionID: collectionID,
		opType:       ReleaseCollection,
		Notifier:     notifier,
	}
	<-notifier
}

func (ob *TargetObserver) ReleasePartition(collectionID int64, partitionID ...int64) {
	notifier := make(chan error)
	defer close(notifier)
	ob.updateChan <- targetUpdateRequest{
		CollectionID: collectionID,
		PartitionIDs: partitionID,
		opType:       ReleasePartition,
		Notifier:     notifier,
	}
	<-notifier
}

func (ob *TargetObserver) clean() {
	collectionSet := typeutil.NewUniqueSet(ob.meta.GetAll(context.TODO())...)
	// Clear next target progress for collections that have been removed.
	ob.nextTargetProgresses.Range(func(collectionID int64, _ nextTargetProgress) bool {
		if !collectionSet.Contain(collectionID) {
			ob.nextTargetProgresses.Remove(collectionID)
		}
		return true
	})
	ob.nextTargetStale.Range(func(collectionID int64) bool {
		if !collectionSet.Contain(collectionID) {
			ob.nextTargetStale.Remove(collectionID)
		}
		return true
	})

	ob.mut.Lock()
	defer ob.mut.Unlock()
	for collectionID, notifiers := range ob.readyNotifiers {
		if !collectionSet.Contain(collectionID) {
			for i := range notifiers {
				close(notifiers[i])
			}
			delete(ob.readyNotifiers, collectionID)
		}
	}
}

// shouldUpdateNextTarget determines whether the observer should refresh the
// collection's next target. A missing next target is created normally. An
// existing next target is force-refreshed by either of these mechanisms:
//  1. The scheduler marks it stale after a target segment can no longer be
//     loaded, for example when a load task returns ErrSegmentNotFound.
//  2. Replica-scoped segment and channel readiness does not advance during
//     NextTargetSurviveTime. A target version change or progress in any tracked
//     readiness dimension establishes a new baseline and restarts the timer.
func (ob *TargetObserver) shouldUpdateNextTarget(ctx context.Context, collectionID int64) bool {
	if ob.nextTargetStale.Contain(collectionID) {
		mlog.Info(ctx, "force update next target",
			mlog.FieldCollectionID(collectionID),
			mlog.String("reason", "next target is marked stale"))
		return true
	}

	if !ob.targetMgr.IsNextTargetExist(ctx, collectionID) {
		return true
	}

	nextVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	progress, hasProgress := ob.nextTargetProgresses.Get(collectionID)
	if !hasProgress {
		mlog.Info(ctx, "force update next target",
			mlog.FieldCollectionID(collectionID),
			mlog.Int64("targetVersion", nextVersion),
			mlog.String("reason", "next target progress is missing"))
		return true
	}
	if progress.targetVersion != nextVersion {
		ob.recordNextTargetProgress(collectionID, ob.sampleNextTargetProgress(ctx, collectionID, nextVersion))
		return false
	}

	if time.Since(progress.lastUpdated) <= params.Params.QueryCoordCfg.NextTargetSurviveTime.GetAsDuration(time.Second) {
		return false
	}

	currentProgress := ob.sampleNextTargetProgress(ctx, collectionID, nextVersion)
	if currentProgress.readySegmentReplicas > progress.readySegmentReplicas ||
		currentProgress.readyReplicaChannels > progress.readyReplicaChannels {
		// Preserve high-water counts so readiness oscillation cannot reset the timer.
		currentProgress.readySegmentReplicas = max(currentProgress.readySegmentReplicas, progress.readySegmentReplicas)
		currentProgress.readyReplicaChannels = max(currentProgress.readyReplicaChannels, progress.readyReplicaChannels)
		ob.recordNextTargetProgress(collectionID, currentProgress)
		return false
	}

	mlog.Info(ctx, "force update next target",
		mlog.FieldCollectionID(collectionID),
		mlog.Int64("targetVersion", nextVersion),
		mlog.Int("previousReadySegmentReplicas", progress.readySegmentReplicas),
		mlog.Int("readySegmentReplicas", currentProgress.readySegmentReplicas),
		mlog.Int("previousReadyReplicaChannels", progress.readyReplicaChannels),
		mlog.Int("readyReplicaChannels", currentProgress.readyReplicaChannels),
		mlog.String("reason", "next target loading made no progress"))
	return true
}

func (ob *TargetObserver) updateNextTarget(ctx context.Context, collectionID int64) error {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	oldVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	hadStale := ob.nextTargetStale.TryRemove(collectionID)

	log.RatedInfo(ctx, rate.Limit(10), "observer trigger update next target")
	err := ob.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	if err != nil {
		if hadStale {
			ob.nextTargetStale.Upsert(collectionID)
		}
		log.Warn(ctx, "failed to update next target for collection",
			mlog.Err(err))
		return err
	}

	newVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	if newVersion <= oldVersion {
		if hadStale {
			ob.nextTargetStale.Upsert(collectionID)
		}
		log.RatedInfo(ctx, rate.Limit(10), "next target refresh did not install a new version",
			mlog.Int64("oldVersion", oldVersion),
			mlog.Int64("newVersion", newVersion))
		return nil
	}

	ob.recordNextTargetProgress(collectionID, ob.sampleNextTargetProgress(ctx, collectionID, newVersion))
	return nil
}

func (ob *TargetObserver) recordNextTargetProgress(collectionID int64, progress nextTargetProgress) {
	progress.lastUpdated = time.Now()
	ob.nextTargetProgresses.Insert(collectionID, progress)
}

func (ob *TargetObserver) sampleNextTargetProgress(ctx context.Context, collectionID, targetVersion int64) nextTargetProgress {
	return nextTargetProgress{
		targetVersion:        targetVersion,
		readySegmentReplicas: ob.countReadyNextTargetSegmentReplicas(ctx, collectionID),
		readyReplicaChannels: ob.countReadyNextTargetReplicaChannels(ctx, collectionID, targetVersion),
	}
}

func (ob *TargetObserver) countReadyNextTargetSegmentReplicas(ctx context.Context, collectionID int64) int {
	targetSegments := ob.targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.NextTarget)
	distSegments := ob.distMgr.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID))
	readySegmentReplicas := 0

	for _, replica := range ob.meta.GetByCollection(ctx, collectionID) {
		replicaSegments := make(map[int64][]*meta.Segment)
		for _, segment := range distSegments {
			if _, ok := targetSegments[segment.GetID()]; ok && replica.Contains(segment.Node) {
				replicaSegments[segment.GetID()] = append(replicaSegments[segment.GetID()], segment)
			}
		}

		for segmentID, targetSegment := range targetSegments {
			segments := replicaSegments[segmentID]
			if len(segments) == 0 {
				continue
			}

			ready := lo.ContainsBy(segments, func(segment *meta.Segment) bool {
				segmentReady, err := utils.IsSegmentDataReadyForTarget(segment, targetSegment)
				// Progress sampling treats comparison errors as not ready. Detailed
				// diagnostics remain in CheckSegmentDataReady, avoiding log noise from
				// repeated periodic samples.
				return err == nil && segmentReady
			})
			if ready {
				readySegmentReplicas++
			}
		}
	}

	return readySegmentReplicas
}

func (ob *TargetObserver) countReadyNextTargetReplicaChannels(ctx context.Context, collectionID, targetVersion int64) int {
	targetChannels := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	readyReplicaChannels := 0

	for _, replica := range ob.meta.GetByCollection(ctx, collectionID) {
		for channelName := range targetChannels {
			delegators := ob.distMgr.ChannelDistManager.GetByFilter(
				meta.WithReplica2Channel(replica),
				meta.WithChannelName2Channel(channelName),
			)

			if lo.ContainsBy(delegators, func(delegator *meta.DmChannel) bool {
				delegatorDataReady, delegatorSynced, _ := ob.getDelegatorReadiness(delegator, targetVersion)
				return delegatorDataReady || delegatorSynced
			}) {
				readyReplicaChannels++
			}
		}
	}

	return readyReplicaChannels
}

func (ob *TargetObserver) getDelegatorReadiness(channel *meta.DmChannel, targetVersion int64) (bool, bool, error) {
	if channel == nil || channel.View == nil {
		return false, false, nil
	}
	err := utils.CheckDelegatorDataReady(ob.nodeMgr, ob.targetMgr, channel.View, meta.NextTarget)
	dataReady := err == nil
	synced := targetVersion == channel.View.TargetVersion && channel.IsServiceable()
	return dataReady, synced, err
}

func (ob *TargetObserver) shouldUpdateCurrentTarget(ctx context.Context, collectionID int64) bool {
	if ob.nextTargetStale.Contain(collectionID) {
		return false
	}

	replicaNum := ob.meta.GetReplicaNumber(ctx, collectionID)
	log := mlog.With(
		mlog.FieldCollectionID(collectionID),
		mlog.Int32("replicaNum", replicaNum),
	)

	// check channel first
	channelNames := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		log.RatedInfo(ctx, rate.Limit(10), "next target is empty, no need to update")
		return false
	}

	newVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)

	// checkDelegatorDataReady checks if a delegator is ready for the next target.
	// A delegator is considered ready if:
	// 1. Its target version matches the new version and it is serviceable, OR
	// 2. Its data is ready for the next target (all segments and channels are loaded)
	checkDelegatorDataReady := func(replica *meta.Replica, channel *meta.DmChannel) bool {
		if channel == nil {
			return false
		}
		dataReadyForNextTarget, syncedWithNextTarget, err := ob.getDelegatorReadiness(channel, newVersion)
		if !dataReadyForNextTarget {
			delegatorTargetVersion := int64(0)
			if channel.View != nil {
				delegatorTargetVersion = channel.View.TargetVersion
			}
			log.Info(ctx, "check delegator",
				mlog.FieldCollectionID(collectionID),
				mlog.Int64("replicaID", replica.GetID()),
				mlog.FieldNodeID(channel.Node),
				mlog.String("channelName", channel.GetChannelName()),
				mlog.Int64("targetVersion", delegatorTargetVersion),
				mlog.Int64("newTargetVersion", newVersion),
				mlog.Bool("isServiceable", channel.IsServiceable()),
				mlog.Int64("version", channel.Version),
				mlog.Err(err),
			)
		}
		return syncedWithNextTarget || dataReadyForNextTarget
	}

	// Iterate through each replica to check if all its delegators are ready.
	// this approach ensures each replica has at least one ready delegator for every channel.
	// This prevents the issue where some replicas may lack nodes during dynamic replica scaling,
	// while the total count still meets the threshold.
	readyDelegatorsInCollection := make([]*meta.DmChannel, 0)
	replicas := ob.meta.GetByCollection(ctx, collectionID)
	for _, replica := range replicas {
		for channel := range channelNames {
			// Filter delegators by replica to ensure we only check delegators belonging to this replica
			delegatorList := ob.distMgr.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica), meta.WithChannelName2Channel(channel))
			readyDelegatorsInCollection = append(readyDelegatorsInCollection,
				lo.Filter(delegatorList, func(ch *meta.DmChannel, _ int) bool {
					return checkDelegatorDataReady(replica, ch)
				})...)
		}
	}

	syncSuccess := ob.syncNextTargetToDelegator(ctx, collectionID, readyDelegatorsInCollection, newVersion)
	syncedChannelNames := lo.Uniq(lo.Map(readyDelegatorsInCollection, func(ch *meta.DmChannel, _ int) string { return ch.ChannelName }))
	// only after all channel are synced, we can consider the current target is ready
	if !syncSuccess || !lo.Every(syncedChannelNames, lo.Keys(channelNames)) {
		return false
	}

	// segment data satisfies next target spec
	return !paramtable.Get().QueryCoordCfg.UpdateTargetNeedSegmentDataReady.GetAsBool() ||
		utils.CheckSegmentDataReady(ctx, collectionID, ob.distMgr, ob.targetMgr, meta.NextTarget) == nil
}

// sync next target info to delegator as readable snapshot
// 1. if next target is changed before delegator becomes serviceable, we need to sync the new next target to delegator to support partial search
// 2. if next target is ready to read, we need to sync the next target to delegator to support full search
func (ob *TargetObserver) syncNextTargetToDelegator(ctx context.Context, collectionID int64, collReadyDelegatorList []*meta.DmChannel, newVersion int64) bool {
	var partitions []int64
	var indexInfo []*indexpb.IndexInfo
	var err error
	for _, d := range collReadyDelegatorList {
		updateVersionAction := ob.genSyncAction(ctx, d.View, newVersion)
		replica := ob.meta.GetByCollectionAndNode(ctx, collectionID, d.Node)
		if replica == nil {
			mlog.Warn(ctx, "replica not found", mlog.FieldNodeID(d.Node), mlog.FieldCollectionID(collectionID))
			// should not happen, don't update current target if replica not found
			return false
		}
		// init all the meta information
		if partitions == nil {
			partitions, err = utils.GetPartitions(ctx, ob.targetMgr, collectionID)
			if err != nil {
				mlog.Warn(ctx, "failed to get partitions", mlog.Err(err))
				return false
			}

			// Get collection index info
			indexInfo, err = ob.broker.ListIndexes(ctx, collectionID)
			if err != nil {
				mlog.Warn(ctx, "fail to get index info of collection", mlog.Err(err))
				return false
			}
		}

		if !ob.syncToDelegator(ctx, replica, d.View, updateVersionAction, partitions, indexInfo) {
			return false
		}
	}
	return true
}

func (ob *TargetObserver) syncToDelegator(ctx context.Context, replica *meta.Replica, LeaderView *meta.LeaderView, action *querypb.SyncAction,
	partitions []int64, indexInfo []*indexpb.IndexInfo,
) bool {
	replicaID := replica.GetID()

	log := mlog.With(
		mlog.Int64("leaderID", LeaderView.ID),
		mlog.FieldCollectionID(LeaderView.CollectionID),
		mlog.String("channel", LeaderView.Channel),
	)

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
		),
		CollectionID: LeaderView.CollectionID,
		ReplicaID:    replicaID,
		Channel:      LeaderView.Channel,
		Actions:      []*querypb.SyncAction{action},
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:      ob.meta.GetLoadType(ctx, LeaderView.CollectionID),
			CollectionID:  LeaderView.CollectionID,
			PartitionIDs:  partitions,
			ResourceGroup: replica.GetResourceGroup(),
		},
		Version:       time.Now().UnixNano(),
		IndexInfoList: indexInfo,
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	resp, err := ob.cluster.SyncDistribution(ctx, LeaderView.ID, req)
	if err != nil {
		log.Warn(ctx, "failed to sync distribution", mlog.Err(err))
		return false
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn(ctx, "failed to sync distribution", mlog.String("reason", resp.GetReason()))
		return false
	}

	return true
}

// sync next target info to delegator
// 1. if next target is changed before delegator becomes serviceable, we need to sync the new next target to delegator to support partial search
// 2. if next target is ready to read, we need to sync the next target to delegator to support full search
func (ob *TargetObserver) genSyncAction(ctx context.Context, leaderView *meta.LeaderView, targetVersion int64) *querypb.SyncAction {
	mlog.RatedInfo(ctx, rate.Limit(10), "Update readable segment version",
		mlog.FieldCollectionID(leaderView.CollectionID),
		mlog.String("channelName", leaderView.Channel),
		mlog.FieldNodeID(leaderView.ID),
		mlog.Int64("oldVersion", leaderView.TargetVersion),
		mlog.Int64("newVersion", targetVersion),
	)

	sealedSegments := ob.targetMgr.GetSealedSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	growingSegments := ob.targetMgr.GetGrowingSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	droppedSegments := ob.targetMgr.GetDroppedSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	channel := ob.targetMgr.GetDmChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTargetFirst)
	sealedSegmentRowCount := lo.MapValues(sealedSegments, func(segment *datapb.SegmentInfo, _ int64) int64 {
		return segment.GetNumOfRows()
	})

	action := &querypb.SyncAction{
		Type:                  querypb.SyncType_UpdateVersion,
		GrowingInTarget:       growingSegments.Collect(),
		SealedInTarget:        lo.Keys(sealedSegmentRowCount),
		DroppedInTarget:       droppedSegments,
		TargetVersion:         targetVersion,
		SealedSegmentRowCount: sealedSegmentRowCount,
	}

	if channel != nil {
		action.Checkpoint = channel.GetSeekPosition()
		// used to clean delete buffer in delegator, cause delete record before this ts already be dispatch to sealed segments
		action.DeleteCP = channel.GetDeleteCheckpoint()
	}

	return action
}

func (ob *TargetObserver) updateAllReplicasCheckpointMetric(ctx context.Context, collectionID int64) {
	channels := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		return
	}
	currentVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)
	if currentVersion == 0 {
		return
	}
	replicas := ob.meta.GetByCollection(ctx, collectionID)
	if len(replicas) == 0 {
		return
	}

	for channelName, dmlChannel := range channels {
		allReady := true
		for _, replica := range replicas {
			delegators := ob.distMgr.ChannelDistManager.GetByFilter(
				meta.WithReplica2Channel(replica),
				meta.WithChannelName2Channel(channelName),
			)
			hasReady := lo.ContainsBy(delegators, func(ch *meta.DmChannel) bool {
				return ch.View != nil &&
					ch.View.TargetVersion >= currentVersion &&
					ch.IsServiceable()
			})
			if !hasReady {
				allReady = false
				break
			}
		}
		if allReady {
			ts, _ := tsoutil.ParseTS(dmlChannel.GetSeekPosition().GetTimestamp())
			metrics.QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds.WithLabelValues(
				paramtable.GetStringNodeID(),
				channelName,
			).Set(float64(ts.Unix()))
		}
	}
}

func (ob *TargetObserver) updateCurrentTarget(ctx context.Context, collectionID int64) {
	mlog.RatedInfo(ctx, rate.Limit(10), "observer trigger update current target", mlog.FieldCollectionID(collectionID))
	if ob.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID) {
		ob.mut.Lock()
		defer ob.mut.Unlock()
		notifiers := ob.readyNotifiers[collectionID]
		for _, notifier := range notifiers {
			close(notifier)
		}
		// Reuse the capacity of notifiers slice
		if notifiers != nil {
			ob.readyNotifiers[collectionID] = notifiers[:0]
		}
	}
}
