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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// check replica, find read only nodes and remove it from replica if all segment/channel has been moved
type ReplicaObserver struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	meta    *meta.Meta
	distMgr *meta.DistributionManager

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewReplicaObserver(meta *meta.Meta, distMgr *meta.DistributionManager) *ReplicaObserver {
	return &ReplicaObserver{
		meta:    meta,
		distMgr: distMgr,
	}
}

func (ob *ReplicaObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		ob.wg.Add(1)
		go ob.schedule(ctx)
	})
}

func (ob *ReplicaObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()
	})
}

func (ob *ReplicaObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("Start check replica loop")

	listener := ob.meta.ResourceManager.ListenNodeChanged()
	for {
		ob.waitNodeChangedOrTimeout(ctx, listener)
		// stop if the context is canceled.
		if ctx.Err() != nil {
			log.Info("Stop check replica observer")
			return
		}

		// do check once.
		ob.checkNodesInReplica()
	}
}

func (ob *ReplicaObserver) waitNodeChangedOrTimeout(ctx context.Context, listener *syncutil.VersionedListener) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, params.Params.QueryCoordCfg.CheckNodeInReplicaInterval.GetAsDuration(time.Second))
	defer cancel()
	listener.Wait(ctxWithTimeout)
}

func (ob *ReplicaObserver) checkNodesInReplica() {
	log := log.Ctx(context.Background()).WithRateGroup("qcv2.replicaObserver", 1, 60)
	collections := ob.meta.GetAll()
	for _, collectionID := range collections {
		utils.RecoverReplicaOfCollection(ob.meta, collectionID)
	}

	// check all ro nodes, remove it from replica if all segment/channel has been moved
	for _, collectionID := range collections {
		replicas := ob.meta.ReplicaManager.GetByCollection(collectionID)
		for _, replica := range replicas {
			roNodes := replica.GetRONodes()
			rwNodes := replica.GetRWNodes()
			if len(roNodes) == 0 {
				continue
			}
			log.RatedInfo(10, "found ro nodes in replica",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("RONodes", roNodes),
			)
			removeNodes := make([]int64, 0, len(roNodes))
			for _, node := range roNodes {
				channels := ob.distMgr.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(node))
				segments := ob.distMgr.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID), meta.WithNodeID(node))
				if len(channels) == 0 && len(segments) == 0 {
					removeNodes = append(removeNodes, node)
				}
			}
			if len(removeNodes) == 0 {
				continue
			}
			logger := log.With(
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("removedNodes", removeNodes),
				zap.Int64s("roNodes", roNodes),
				zap.Int64s("rwNodes", rwNodes),
			)
			if err := ob.meta.ReplicaManager.RemoveNode(replica.GetID(), removeNodes...); err != nil {
				logger.Warn("fail to remove node from replica", zap.Error(err))
				continue
			}
			logger.Info("all segment/channel has been removed from ro node, try to remove it from replica")
		}
	}
}
