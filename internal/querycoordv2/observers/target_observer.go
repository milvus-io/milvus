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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type checkRequest struct {
	CollectionID int64
	Notifier     chan bool
}

type targetUpdateRequest struct {
	CollectionID  int64
	Notifier      chan error
	ReadyNotifier chan struct{}
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

	initChan    chan initRequest
	manualCheck chan checkRequest
	// nextTargetLastUpdate map[int64]time.Time
	nextTargetLastUpdate *typeutil.ConcurrentMap[int64, time.Time]
	updateChan           chan targetUpdateRequest
	mut                  sync.Mutex                // Guard readyNotifiers
	readyNotifiers       map[int64][]chan struct{} // CollectionID -> Notifiers

	dispatcher *taskDispatcher[int64]
	keylocks   *lock.KeyLock[int64]

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewTargetObserver(
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	distMgr *meta.DistributionManager,
	broker meta.Broker,
	cluster session.Cluster,
) *TargetObserver {
	result := &TargetObserver{
		meta:                 meta,
		targetMgr:            targetMgr,
		distMgr:              distMgr,
		broker:               broker,
		cluster:              cluster,
		manualCheck:          make(chan checkRequest, 10),
		nextTargetLastUpdate: typeutil.NewConcurrentMap[int64, time.Time](),
		updateChan:           make(chan targetUpdateRequest),
		readyNotifiers:       make(map[int64][]chan struct{}),
		initChan:             make(chan initRequest),
		keylocks:             lock.NewKeyLock[int64](),
	}

	dispatcher := newTaskDispatcher(result.check)
	result.dispatcher = dispatcher
	return result
}

func (ob *TargetObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		ob.dispatcher.Start()

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

		ob.dispatcher.Stop()
	})
}

func (ob *TargetObserver) schedule(ctx context.Context) {
	log.Info("Start update next target loop")

	ticker := time.NewTicker(params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("Close target observer")
			return

		case <-ob.initChan:
			for _, collectionID := range ob.meta.GetAll() {
				ob.init(ctx, collectionID)
			}
			log.Info("target observer init done")

		case <-ticker.C:
			ob.clean()
			ob.dispatcher.AddTask(ob.meta.GetAll()...)

		case req := <-ob.updateChan:
			log := log.With(zap.Int64("collectionID", req.CollectionID))
			log.Info("manually trigger update next target")
			ob.keylocks.Lock(req.CollectionID)
			err := ob.updateNextTarget(req.CollectionID)
			ob.keylocks.Unlock(req.CollectionID)
			if err != nil {
				log.Warn("failed to manually update next target", zap.Error(err))
				close(req.ReadyNotifier)
			} else {
				ob.mut.Lock()
				ob.readyNotifiers[req.CollectionID] = append(ob.readyNotifiers[req.CollectionID], req.ReadyNotifier)
				ob.mut.Unlock()
			}

			log.Info("manually trigger update target done")
			req.Notifier <- err
			log.Info("notify manually trigger update target done")
		}
	}
}

// Check whether provided collection is has current target.
// If not, submit an async task into dispatcher.
func (ob *TargetObserver) Check(ctx context.Context, collectionID int64, partitionID int64) bool {
	result := ob.targetMgr.IsCurrentTargetExist(collectionID, partitionID)
	if !result {
		ob.dispatcher.AddTask(collectionID)
	}
	return result
}

func (ob *TargetObserver) check(ctx context.Context, collectionID int64) {
	if !ob.meta.Exist(collectionID) {
		ob.ReleaseCollection(collectionID)
		ob.targetMgr.RemoveCollection(collectionID)
		log.Info("collection has been removed from target observer",
			zap.Int64("collectionID", collectionID))
		return
	}

	ob.keylocks.Lock(collectionID)
	defer ob.keylocks.Unlock(collectionID)

	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(collectionID)
	}

	if ob.shouldUpdateNextTarget(collectionID) {
		// update next target in collection level
		ob.updateNextTarget(collectionID)
	}
}

func (ob *TargetObserver) init(ctx context.Context, collectionID int64) {
	// pull next target first if not exist
	if !ob.targetMgr.IsNextTargetExist(collectionID) {
		ob.updateNextTarget(collectionID)
	}

	// try to update current target if all segment/channel are ready
	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(collectionID)
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
		Notifier:      notifier,
		ReadyNotifier: readyCh,
	}
	return readyCh, <-notifier
}

func (ob *TargetObserver) ReleaseCollection(collectionID int64) {
	ob.mut.Lock()
	defer ob.mut.Unlock()
	for _, notifier := range ob.readyNotifiers[collectionID] {
		close(notifier)
	}
	delete(ob.readyNotifiers, collectionID)
}

func (ob *TargetObserver) clean() {
	collectionSet := typeutil.NewUniqueSet(ob.meta.GetAll()...)
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

func (ob *TargetObserver) shouldUpdateNextTarget(collectionID int64) bool {
	return !ob.targetMgr.IsNextTargetExist(collectionID) || ob.isNextTargetExpired(collectionID)
}

func (ob *TargetObserver) isNextTargetExpired(collectionID int64) bool {
	lastUpdated, has := ob.nextTargetLastUpdate.Get(collectionID)
	if !has {
		return true
	}
	return time.Since(lastUpdated) > params.Params.QueryCoordCfg.NextTargetSurviveTime.GetAsDuration(time.Second)
}

func (ob *TargetObserver) updateNextTarget(collectionID int64) error {
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.TargetObserver", 1, 60).
		With(zap.Int64("collectionID", collectionID))

	log.RatedInfo(10, "observer trigger update next target")
	err := ob.targetMgr.UpdateCollectionNextTarget(collectionID)
	if err != nil {
		log.Warn("failed to update next target for collection",
			zap.Error(err))
		return err
	}
	ob.updateNextTargetTimestamp(collectionID)
	return nil
}

func (ob *TargetObserver) updateNextTargetTimestamp(collectionID int64) {
	ob.nextTargetLastUpdate.Insert(collectionID, time.Now())
}

func (ob *TargetObserver) shouldUpdateCurrentTarget(ctx context.Context, collectionID int64) bool {
	replicaNum := ob.meta.CollectionManager.GetReplicaNumber(collectionID)
	log := log.Ctx(ctx).WithRateGroup(
		fmt.Sprintf("qcv2.TargetObserver-%d", collectionID),
		10,
		60,
	).With(
		zap.Int64("collectionID", collectionID),
		zap.Int32("replicaNum", replicaNum),
	)

	// check channel first
	channelNames := ob.targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		log.RatedInfo(10, "next target is empty, no need to update")
		return false
	}

	for _, channel := range channelNames {
		views := ob.distMgr.LeaderViewManager.GetByFilter(meta.WithChannelName2LeaderView(channel.GetChannelName()))
		nodes := lo.Map(views, func(v *meta.LeaderView, _ int) int64 { return v.ID })
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager, collectionID, nodes)
		if int32(len(group)) < replicaNum {
			log.RatedInfo(10, "channel not ready",
				zap.Int("readyReplicaNum", len(group)),
				zap.String("channelName", channel.GetChannelName()),
			)
			return false
		}
	}

	// and last check historical segment
	SealedSegments := ob.targetMgr.GetSealedSegmentsByCollection(collectionID, meta.NextTarget)
	for _, segment := range SealedSegments {
		views := ob.distMgr.LeaderViewManager.GetByFilter(meta.WithSegment2LeaderView(segment.GetID(), false))
		nodes := lo.Map(views, func(view *meta.LeaderView, _ int) int64 { return view.ID })
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager, collectionID, nodes)
		if int32(len(group)) < replicaNum {
			log.RatedInfo(10, "segment not ready",
				zap.Int("readyReplicaNum", len(group)),
				zap.Int64("segmentID", segment.GetID()),
			)
			return false
		}
	}

	replicas := ob.meta.ReplicaManager.GetByCollection(collectionID)
	actions := make([]*querypb.SyncAction, 0, 1)
	for _, replica := range replicas {
		leaders := ob.distMgr.ChannelDistManager.GetShardLeadersByReplica(replica)
		for ch, leaderID := range leaders {
			actions = actions[:0]
			leaderView := ob.distMgr.LeaderViewManager.GetLeaderShardView(leaderID, ch)
			if leaderView == nil {
				log.RatedInfo(10, "leader view not ready",
					zap.Int64("nodeID", leaderID),
					zap.String("channel", ch),
				)
				continue
			}
			updateVersionAction := ob.checkNeedUpdateTargetVersion(ctx, leaderView)
			if updateVersionAction != nil {
				actions = append(actions, updateVersionAction)
			}
			if !ob.sync(ctx, replica, leaderView, actions) {
				return false
			}
		}
	}

	return true
}

func (ob *TargetObserver) sync(ctx context.Context, replica *meta.Replica, leaderView *meta.LeaderView, diffs []*querypb.SyncAction) bool {
	if len(diffs) == 0 {
		return true
	}
	replicaID := replica.GetID()

	log := log.With(
		zap.Int64("leaderID", leaderView.ID),
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.String("channel", leaderView.Channel),
	)

	collectionInfo, err := ob.broker.DescribeCollection(ctx, leaderView.CollectionID)
	if err != nil {
		log.Warn("failed to get collection info", zap.Error(err))
		return false
	}
	partitions, err := utils.GetPartitions(ob.meta.CollectionManager, leaderView.CollectionID)
	if err != nil {
		log.Warn("failed to get partitions", zap.Error(err))
		return false
	}

	// Get collection index info
	indexInfo, err := ob.broker.ListIndexes(ctx, collectionInfo.GetCollectionID())
	if err != nil {
		log.Warn("fail to get index info of collection", zap.Error(err))
		return false
	}

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
		),
		CollectionID: leaderView.CollectionID,
		ReplicaID:    replicaID,
		Channel:      leaderView.Channel,
		Actions:      diffs,
		Schema:       collectionInfo.GetSchema(),
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:      ob.meta.GetLoadType(leaderView.CollectionID),
			CollectionID:  leaderView.CollectionID,
			PartitionIDs:  partitions,
			DbName:        collectionInfo.GetDbName(),
			ResourceGroup: replica.GetResourceGroup(),
		},
		Version:       time.Now().UnixNano(),
		IndexInfoList: indexInfo,
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	resp, err := ob.cluster.SyncDistribution(ctx, leaderView.ID, req)
	if err != nil {
		log.Warn("failed to sync distribution", zap.Error(err))
		return false
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to sync distribution", zap.String("reason", resp.GetReason()))
		return false
	}

	return true
}

func (ob *TargetObserver) checkCollectionLeaderVersionIsCurrent(ctx context.Context, collectionID int64) bool {
	replicas := ob.meta.ReplicaManager.GetByCollection(collectionID)
	for _, replica := range replicas {
		leaders := ob.distMgr.ChannelDistManager.GetShardLeadersByReplica(replica)
		for ch, leaderID := range leaders {
			leaderView := ob.distMgr.LeaderViewManager.GetLeaderShardView(leaderID, ch)
			if leaderView == nil {
				return false
			}

			action := ob.checkNeedUpdateTargetVersion(ctx, leaderView)
			if action != nil {
				return false
			}
		}
	}
	return true
}

func (ob *TargetObserver) checkNeedUpdateTargetVersion(ctx context.Context, leaderView *meta.LeaderView) *querypb.SyncAction {
	log.Ctx(ctx).WithRateGroup("qcv2.LeaderObserver", 1, 60)
	targetVersion := ob.targetMgr.GetCollectionTargetVersion(leaderView.CollectionID, meta.NextTarget)

	if targetVersion <= leaderView.TargetVersion {
		return nil
	}

	log.RatedInfo(10, "Update readable segment version",
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.String("channelName", leaderView.Channel),
		zap.Int64("nodeID", leaderView.ID),
		zap.Int64("oldVersion", leaderView.TargetVersion),
		zap.Int64("newVersion", targetVersion),
	)

	sealedSegments := ob.targetMgr.GetSealedSegmentsByChannel(leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	growingSegments := ob.targetMgr.GetGrowingSegmentsByChannel(leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	droppedSegments := ob.targetMgr.GetDroppedSegmentsByChannel(leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	channel := ob.targetMgr.GetDmChannel(leaderView.CollectionID, leaderView.Channel, meta.NextTargetFirst)

	action := &querypb.SyncAction{
		Type:            querypb.SyncType_UpdateVersion,
		GrowingInTarget: growingSegments.Collect(),
		SealedInTarget:  lo.Keys(sealedSegments),
		DroppedInTarget: droppedSegments,
		TargetVersion:   targetVersion,
	}

	if channel != nil {
		action.Checkpoint = channel.GetSeekPosition()
	}

	return action
}

func (ob *TargetObserver) updateCurrentTarget(collectionID int64) {
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.TargetObserver", 1, 60)
	log.RatedInfo(10, "observer trigger update current target", zap.Int64("collectionID", collectionID))
	if ob.targetMgr.UpdateCollectionCurrentTarget(collectionID) {
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
