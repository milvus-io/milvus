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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type checkRequest struct {
	CollectionID int64
	Notifier     chan bool
}

type targetUpdateRequest struct {
	CollectionID  int64
	PartitionIDs  []int64
	Notifier      chan error
	ReadyNotifier chan struct{}
}

type TargetObserver struct {
	c         chan struct{}
	wg        sync.WaitGroup
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    meta.Broker

	manualCheck          chan checkRequest
	nextTargetLastUpdate map[int64]time.Time
	updateChan           chan targetUpdateRequest
	mut                  sync.Mutex                // Guard readyNotifiers
	readyNotifiers       map[int64][]chan struct{} // CollectionID -> Notifiers

	stopOnce sync.Once
}

func NewTargetObserver(meta *meta.Meta, targetMgr *meta.TargetManager, distMgr *meta.DistributionManager, broker meta.Broker) *TargetObserver {
	return &TargetObserver{
		c:                    make(chan struct{}),
		meta:                 meta,
		targetMgr:            targetMgr,
		distMgr:              distMgr,
		broker:               broker,
		manualCheck:          make(chan checkRequest, 10),
		nextTargetLastUpdate: make(map[int64]time.Time),
		updateChan:           make(chan targetUpdateRequest),
		readyNotifiers:       make(map[int64][]chan struct{}),
	}
}

func (ob *TargetObserver) Start(ctx context.Context) {
	ob.wg.Add(1)
	go ob.schedule(ctx)
}

func (ob *TargetObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.c)
		ob.wg.Wait()
	})
}

func (ob *TargetObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("Start update next target loop")

	ticker := time.NewTicker(params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("Close target observer due to context canceled")
			return
		case <-ob.c:
			log.Info("Close target observer")
			return

		case <-ticker.C:
			ob.clean()
			ob.tryUpdateTarget()

		case req := <-ob.manualCheck:
			ob.check(req.CollectionID)
			req.Notifier <- ob.targetMgr.IsCurrentTargetExist(req.CollectionID)

		case req := <-ob.updateChan:
			err := ob.updateNextTarget(req.CollectionID, req.PartitionIDs...)
			if err != nil {
				close(req.ReadyNotifier)
			} else {
				ob.mut.Lock()
				ob.readyNotifiers[req.CollectionID] = append(ob.readyNotifiers[req.CollectionID], req.ReadyNotifier)
				ob.mut.Unlock()
			}

			req.Notifier <- err
		}
	}
}

// Check checks whether the next target is ready,
// and updates the current target if it is,
// returns true if current target is not nil
func (ob *TargetObserver) Check(collectionID int64) bool {
	notifier := make(chan bool)
	ob.manualCheck <- checkRequest{
		CollectionID: collectionID,
		Notifier:     notifier,
	}
	return <-notifier
}

func (ob *TargetObserver) check(collectionID int64) {
	if ob.shouldUpdateCurrentTarget(collectionID) {
		ob.updateCurrentTarget(collectionID)
	}

	if ob.shouldUpdateNextTarget(collectionID) {
		// update next target in collection level
		ob.updateNextTarget(collectionID)
	}
}

// UpdateNextTarget updates the next target,
// returns a channel which will be closed when the next target is ready,
// or returns error if failed to pull target
func (ob *TargetObserver) UpdateNextTarget(collectionID int64, partitionIDs ...int64) (chan struct{}, error) {
	notifier := make(chan error)
	readyCh := make(chan struct{})
	defer close(notifier)

	ob.updateChan <- targetUpdateRequest{
		CollectionID:  collectionID,
		PartitionIDs:  partitionIDs,
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

func (ob *TargetObserver) tryUpdateTarget() {
	collections := ob.meta.GetAll()
	for _, collectionID := range collections {
		ob.check(collectionID)
	}

	collectionSet := typeutil.NewUniqueSet(collections...)
	// for collection which has been removed from target, try to clear nextTargetLastUpdate
	for collection := range ob.nextTargetLastUpdate {
		if !collectionSet.Contain(collection) {
			delete(ob.nextTargetLastUpdate, collection)
		}
	}
}

func (ob *TargetObserver) clean() {
	collections := typeutil.NewSet(ob.meta.GetAll()...)

	ob.mut.Lock()
	defer ob.mut.Unlock()
	for collectionID, notifiers := range ob.readyNotifiers {
		if !collections.Contain(collectionID) {
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
	return time.Since(ob.nextTargetLastUpdate[collectionID]) > params.Params.QueryCoordCfg.NextTargetSurviveTime.GetAsDuration(time.Second)
}

func (ob *TargetObserver) updateNextTarget(collectionID int64, partitionIDs ...int64) error {
	log := log.With(zap.Int64("collectionID", collectionID), zap.Int64s("partIDs", partitionIDs))

	log.Info("observer trigger update next target")
	var err error
	if len(partitionIDs) == 0 {
		err = ob.targetMgr.UpdateCollectionNextTarget(collectionID)
	} else {
		err = ob.targetMgr.UpdateCollectionNextTargetWithPartitions(collectionID, partitionIDs...)
	}
	if err != nil {
		log.Error("failed to update next target for collection",
			zap.Error(err))
		return err
	}
	ob.updateNextTargetTimestamp(collectionID)
	return nil
}

func (ob *TargetObserver) updateNextTargetTimestamp(collectionID int64) {
	ob.nextTargetLastUpdate[collectionID] = time.Now()
}

func (ob *TargetObserver) shouldUpdateCurrentTarget(collectionID int64) bool {
	replicaNum := ob.meta.CollectionManager.GetReplicaNumber(collectionID)

	// check channel first
	channelNames := ob.targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		return false
	}

	for _, channel := range channelNames {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			collectionID,
			ob.distMgr.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		if int32(len(group)) < replicaNum {
			return false
		}
	}

	// and last check historical segment
	historicalSegments := ob.targetMgr.GetHistoricalSegmentsByCollection(collectionID, meta.NextTarget)
	for _, segment := range historicalSegments {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			collectionID,
			ob.distMgr.LeaderViewManager.GetSealedSegmentDist(segment.GetID()))
		if int32(len(group)) < replicaNum {
			return false
		}
	}

	return true
}

func (ob *TargetObserver) updateCurrentTarget(collectionID int64) {
	log.Warn("observer trigger update current target",
		zap.Int64("collectionID", collectionID))
	ob.targetMgr.UpdateCollectionCurrentTarget(collectionID)

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
