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
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	log.Info("Start update next target loop")

	ticker := time.NewTicker(params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("Close target observer")
			return

		case <-ob.initChan:
			for _, collectionID := range ob.meta.GetAll(ctx) {
				ob.init(ctx, collectionID)
			}
			log.Info("target observer init done")

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

		case req := <-ob.updateChan:
			log.Info("manually trigger update target",
				zap.Int64("collectionID", req.CollectionID),
				zap.String("opType", req.opType.String()),
			)
			switch req.opType {
			case UpdateCollection:
				ob.keylocks.Lock(req.CollectionID)
				err := ob.updateNextTarget(ctx, req.CollectionID)
				ob.keylocks.Unlock(req.CollectionID)
				if err != nil {
					log.Warn("failed to manually update next target",
						zap.Int64("collectionID", req.CollectionID),
						zap.String("opType", req.opType.String()),
						zap.Error(err))
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
				ob.targetMgr.RemovePartition(ctx, req.CollectionID, req.PartitionIDs...)
				req.Notifier <- nil
			}
			log.Info("manually trigger update target done",
				zap.Int64("collectionID", req.CollectionID),
				zap.String("opType", req.opType.String()))
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
	if ob.meta.CollectionManager.GetCollection(ctx, collectionID) == nil {
		return
	}

	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(ctx, collectionID)
	}

	if ob.shouldUpdateNextTarget(ctx, collectionID) {
		// update next target in collection level
		ob.updateNextTarget(ctx, collectionID)
	}
}

func (ob *TargetObserver) init(ctx context.Context, collectionID int64) {
	// pull next target first if not exist
	if !ob.targetMgr.IsNextTargetExist(ctx, collectionID) {
		ob.updateNextTarget(ctx, collectionID)
	}

	// try to update current target if all segment/channel are ready
	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(ctx, collectionID)
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
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.TargetObserver", 1, 60).
		With(zap.Int64("collectionID", collectionID))

	log.RatedInfo(10, "observer trigger update next target")
	err := ob.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
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
	replicaNum := ob.meta.CollectionManager.GetReplicaNumber(ctx, collectionID)
	log := log.Ctx(ctx).WithRateGroup(
		fmt.Sprintf("qcv2.TargetObserver-%d", collectionID),
		10,
		60,
	).With(
		zap.Int64("collectionID", collectionID),
		zap.Int32("replicaNum", replicaNum),
	)

	// check channel first
	channelNames := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		log.RatedInfo(10, "next target is empty, no need to update")
		return false
	}

	collectionReadyLeaders := make([]*meta.LeaderView, 0)
	for channel := range channelNames {
		channelReadyLeaders := lo.Filter(ob.distMgr.LeaderViewManager.GetByFilter(meta.WithChannelName2LeaderView(channel)), func(leader *meta.LeaderView, _ int) bool {
			return utils.CheckDelegatorDataReady(ob.nodeMgr, ob.targetMgr, leader, meta.NextTarget) == nil
		})

		// to avoid stuck here in dynamic increase replica case, we just check available delegator number
		if int32(len(channelReadyLeaders)) < replicaNum {
			log.RatedInfo(10, "channel not ready",
				zap.Int("readyReplicaNum", len(channelReadyLeaders)),
				zap.String("channelName", channel),
			)
			return false
		}
		collectionReadyLeaders = append(collectionReadyLeaders, channelReadyLeaders...)
	}

	var partitions []int64
	var indexInfo []*indexpb.IndexInfo
	var err error
	newVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	for _, leader := range collectionReadyLeaders {
		updateVersionAction := ob.checkNeedUpdateTargetVersion(ctx, leader, newVersion)
		if updateVersionAction == nil {
			continue
		}
		replica := ob.meta.ReplicaManager.GetByCollectionAndNode(ctx, collectionID, leader.ID)
		if replica == nil {
			log.Warn("replica not found", zap.Int64("nodeID", leader.ID), zap.Int64("collectionID", collectionID))
			continue
		}
		// init all the meta information
		if partitions == nil {
			partitions, err = utils.GetPartitions(ctx, ob.targetMgr, collectionID)
			if err != nil {
				log.Warn("failed to get partitions", zap.Error(err))
				return false
			}

			// Get collection index info
			indexInfo, err = ob.broker.ListIndexes(ctx, collectionID)
			if err != nil {
				log.Warn("fail to get index info of collection", zap.Error(err))
				return false
			}
		}

		if !ob.sync(ctx, replica, leader, []*querypb.SyncAction{updateVersionAction}, partitions, indexInfo) {
			return false
		}
	}
	return true
}

func (ob *TargetObserver) sync(ctx context.Context, replica *meta.Replica, leaderView *meta.LeaderView, diffs []*querypb.SyncAction,
	partitions []int64, indexInfo []*indexpb.IndexInfo,
) bool {
	if len(diffs) == 0 {
		return true
	}
	replicaID := replica.GetID()

	log := log.With(
		zap.Int64("leaderID", leaderView.ID),
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.String("channel", leaderView.Channel),
	)

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
		),
		CollectionID: leaderView.CollectionID,
		ReplicaID:    replicaID,
		Channel:      leaderView.Channel,
		Actions:      diffs,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:      ob.meta.GetLoadType(ctx, leaderView.CollectionID),
			CollectionID:  leaderView.CollectionID,
			PartitionIDs:  partitions,
			ResourceGroup: replica.GetResourceGroup(),
		},
		Version:       time.Now().UnixNano(),
		IndexInfoList: indexInfo,
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
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

func (ob *TargetObserver) checkNeedUpdateTargetVersion(ctx context.Context, leaderView *meta.LeaderView, targetVersion int64) *querypb.SyncAction {
	log.Ctx(ctx).WithRateGroup("qcv2.LeaderObserver", 1, 60)
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

	sealedSegments := ob.targetMgr.GetSealedSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	growingSegments := ob.targetMgr.GetGrowingSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	droppedSegments := ob.targetMgr.GetDroppedSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTarget)
	channel := ob.targetMgr.GetDmChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.NextTargetFirst)

	action := &querypb.SyncAction{
		Type:            querypb.SyncType_UpdateVersion,
		GrowingInTarget: growingSegments.Collect(),
		SealedInTarget:  lo.Keys(sealedSegments),
		DroppedInTarget: droppedSegments,
		TargetVersion:   targetVersion,
	}

	if channel != nil {
		action.Checkpoint = channel.GetSeekPosition()
		action.L0InTarget = channel.GetLevelZeroSegmentIds()
	}

	return action
}

func (ob *TargetObserver) updateCurrentTarget(ctx context.Context, collectionID int64) {
	log := log.Ctx(ctx).WithRateGroup("qcv2.TargetObserver", 1, 60)
	log.RatedInfo(10, "observer trigger update current target", zap.Int64("collectionID", collectionID))
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
