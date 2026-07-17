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
	// nextTargetLastUpdate map[int64]time.Time
	nextTargetLastUpdate *typeutil.ConcurrentMap[int64, time.Time]
	updateChan           chan targetUpdateRequest
	mut                  sync.Mutex                // Guard readyNotifiers
	readyNotifiers       map[int64][]chan struct{} // CollectionID -> Notifiers

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
		nextTargetLastUpdate: typeutil.NewConcurrentMap[int64, time.Time](),
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

func (ob *TargetObserver) check(ctx context.Context, collectionID int64) {
	ob.keylocks.Lock(collectionID)
	defer ob.keylocks.Unlock(collectionID)

	// if collection release, skip check
	if ob.meta.GetCollection(ctx, collectionID) == nil {
		return
	}

	if ob.syncAndPromoteChannels(ctx, collectionID) {
		ob.notifyCurrentTargetReady(collectionID)
	}

	if ob.shouldUpdateNextTarget(ctx, collectionID) {
		// update next target in collection level
		ob.updateNextTarget(ctx, collectionID)

		// sync next target to delegator if current target not exist, to support partial search
		if !ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, -1) {
			ob.syncNextTargetToDelegator(ctx, collectionID, ob.distMgr.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID)))
		}
	}

	// Update the all-replicas checkpoint metric
	ob.updateAllReplicasCheckpointMetric(ctx, collectionID)
}

func (ob *TargetObserver) init(ctx context.Context, collectionID int64) {
	// pull next target first if not exist
	if !ob.targetMgr.IsNextTargetExist(ctx, collectionID) {
		ob.updateNextTarget(ctx, collectionID)
	}

	// promote whatever channels are already ready
	if ob.syncAndPromoteChannels(ctx, collectionID) {
		ob.notifyCurrentTargetReady(collectionID)
	}
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
	// for collection which has been removed from target, try to clear nextTargetLastUpdate
	ob.nextTargetLastUpdate.Range(func(collectionID int64, _ time.Time) bool {
		if !collectionSet.Contain(collectionID) {
			ob.nextTargetLastUpdate.Remove(collectionID)
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

func (ob *TargetObserver) shouldUpdateNextTarget(ctx context.Context, collectionID int64) bool {
	// Replacing the next target no longer strands anybody: the version a delegator is synced to is
	// retained by the TargetManager for as long as that delegator reads it, so there is nothing to
	// pin. Refresh it whenever it is missing or has expired.
	return !ob.targetMgr.IsNextTargetExist(ctx, collectionID) || ob.isNextTargetExpired(collectionID)
}

func (ob *TargetObserver) isNextTargetExpired(collectionID int64) bool {
	lastUpdated, has := ob.nextTargetLastUpdate.Get(collectionID)
	if !has {
		return true
	}
	return time.Since(lastUpdated) > params.Params.QueryCoordCfg.NextTargetSurviveTime.GetAsDuration(time.Second)
}

func (ob *TargetObserver) updateNextTarget(ctx context.Context, collectionID int64) error {
	log := mlog.With(mlog.FieldCollectionID(collectionID))

	log.RatedInfo(ctx, rate.Limit(10), "observer trigger update next target")
	err := ob.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "failed to update next target for collection",
			mlog.Err(err))
		return err
	}
	ob.updateNextTargetTimestamp(collectionID)
	return nil
}

func (ob *TargetObserver) updateNextTargetTimestamp(collectionID int64) {
	ob.nextTargetLastUpdate.Insert(collectionID, time.Now())
}

// syncAndPromoteChannels advances every channel of a collection independently: a channel whose
// delegators are all ready for its next target is synced to that version and promoted, no matter
// what the other channels are doing. A channel that cannot become ready (a segment that will not
// load, a node under pressure) therefore no longer freezes the rest of the collection.
//
// Promoting a channel does not discard the version it replaces: the TargetManager keeps that
// version's segment set alive for as long as a delegator is still reading it, so the release path
// can always tell whether a redundant segment is still being served.
//
// Returns true when every channel of the collection is now on the next target.
func (ob *TargetObserver) syncAndPromoteChannels(ctx context.Context, collectionID int64) bool {
	log := mlog.With(mlog.FieldCollectionID(collectionID))

	channelNames := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		log.RatedInfo(ctx, rate.Limit(10), "next target is empty, no need to update")
		return false
	}
	replicas := ob.meta.GetByCollection(ctx, collectionID)
	if len(replicas) == 0 {
		return false
	}

	allPromoted := true
	for channel := range channelNames {
		if !ob.syncAndPromoteChannel(ctx, collectionID, channel, replicas) {
			allPromoted = false
		}
	}
	return allPromoted
}

// syncAndPromoteChannel handles one channel: every delegator serving it (one per replica) must be
// ready for the channel's next target; then they are synced to it and the channel is promoted.
func (ob *TargetObserver) syncAndPromoteChannel(ctx context.Context, collectionID int64, channel string, replicas []*meta.Replica) bool {
	log := mlog.With(mlog.FieldCollectionID(collectionID), mlog.String("channel", channel))

	newVersion := ob.targetMgr.GetChannelTargetVersion(ctx, collectionID, channel, meta.NextTarget)
	if newVersion <= 0 {
		return false
	}

	// Readiness is checked per channel: a stuck segment on another channel must not block this one.
	if paramtable.Get().QueryCoordCfg.UpdateTargetNeedSegmentDataReady.GetAsBool() &&
		utils.CheckChannelSegmentDataReady(ctx, collectionID, channel, ob.distMgr, ob.targetMgr, meta.NextTarget) != nil {
		return false
	}

	toSync := make([]*meta.DmChannel, 0, len(replicas))
	for _, replica := range replicas {
		delegatorList := ob.distMgr.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica), meta.WithChannelName2Channel(channel))
		if len(delegatorList) == 0 {
			return false
		}
		for _, delegator := range delegatorList {
			if delegator == nil || delegator.View == nil {
				return false
			}
			if delegator.View.TargetVersion == newVersion && delegator.IsServiceable() {
				continue // already serving the next target
			}
			err := utils.CheckDelegatorDataReady(ob.nodeMgr, ob.targetMgr, delegator.View, meta.NextTarget)
			if err != nil {
				log.Info(ctx, "check delegator",
					mlog.Int64("replicaID", replica.GetID()),
					mlog.FieldNodeID(delegator.Node),
					mlog.Int64("targetVersion", delegator.View.TargetVersion),
					mlog.Int64("newTargetVersion", newVersion),
					mlog.Bool("isServiceable", delegator.IsServiceable()),
					mlog.Err(err))
				return false
			}
			toSync = append(toSync, delegator)
		}
	}

	if len(toSync) > 0 && !ob.syncNextTargetToDelegator(ctx, collectionID, toSync) {
		return false
	}

	promoted := ob.targetMgr.UpdateChannelCurrentTarget(ctx, collectionID, channel)
	if promoted {
		log.Info(ctx, "channel promoted to its next target",
			mlog.Int64("version", newVersion), mlog.Int("syncedDelegators", len(toSync)))
	}
	return promoted
}

// sync next target info to delegator as readable snapshot
// 1. if next target is changed before delegator becomes serviceable, we need to sync the new next target to delegator to support partial search
// 2. if next target is ready to read, we need to sync the next target to delegator to support full search
func (ob *TargetObserver) syncNextTargetToDelegator(ctx context.Context, collectionID int64, collReadyDelegatorList []*meta.DmChannel) bool {
	var partitions []int64
	var indexInfo []*indexpb.IndexInfo
	var err error
	for _, d := range collReadyDelegatorList {
		// the version a delegator is synced to is its own channel's next version
		newVersion := ob.targetMgr.GetChannelTargetVersion(ctx, collectionID, d.View.Channel, meta.NextTarget)
		if newVersion <= 0 {
			mlog.Warn(ctx, "no next target version for channel, skip sync",
				mlog.FieldCollectionID(collectionID), mlog.String("channel", d.View.Channel))
			return false
		}
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
	replicas := ob.meta.GetByCollection(ctx, collectionID)
	if len(replicas) == 0 {
		return
	}

	for channelName, dmlChannel := range channels {
		// each channel has its own current version
		currentVersion := ob.targetMgr.GetChannelTargetVersion(ctx, collectionID, channelName, meta.CurrentTarget)
		if currentVersion == 0 {
			continue
		}
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

// notifyCurrentTargetReady wakes up the waiters of a collection whose channels have all reached the
// next target. Channels are promoted individually by syncAndPromoteChannel.
func (ob *TargetObserver) notifyCurrentTargetReady(collectionID int64) {
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
