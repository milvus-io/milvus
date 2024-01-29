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
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// check replica, find outbound nodes and remove it from replica if all segment/channel has been moved
type ReplicaObserver struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	meta    *meta.Meta
	distMgr *meta.DistributionManager

	stopOnce sync.Once
}

func NewReplicaObserver(meta *meta.Meta, distMgr *meta.DistributionManager) *ReplicaObserver {
	return &ReplicaObserver{
		meta:    meta,
		distMgr: distMgr,
	}
}

func (ob *ReplicaObserver) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	ob.cancel = cancel

	ob.wg.Add(1)
	go ob.schedule(ctx)
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
		// remove nodes from replica which has been transferred to other rg
		replicas := ob.meta.ReplicaManager.GetByCollection(collectionID)
		for _, replica := range replicas {
			outboundNodes := ob.meta.ResourceManager.CheckOutboundNodes(replica)
			if len(outboundNodes) > 0 {
				log.RatedInfo(10, "found outbound nodes in replica",
					zap.Int64("collectionID", replica.GetCollectionID()),
					zap.Int64("replicaID", replica.GetID()),
					zap.Int64s("allOutboundNodes", outboundNodes.Collect()),
				)

				for node := range outboundNodes {
					channels := ob.distMgr.ChannelDistManager.GetByCollectionAndNode(collectionID, node)
					segments := ob.distMgr.SegmentDistManager.GetByCollectionAndNode(collectionID, node)

					if len(channels) == 0 && len(segments) == 0 {
						replica.RemoveNode(node)
						log.Info("all segment/channel has been removed from outbound node, remove it from replica",
							zap.Int64("collectionID", replica.GetCollectionID()),
							zap.Int64("replicaID", replica.GetID()),
							zap.Int64("removedNodes", node),
							zap.Int64s("availableNodes", replica.GetNodes()),
						)
					}
				}
			}
		}

		// Check unused nodes in resource group.
		replicasByRG := utils.GroupReplicasByResourceGroup(replicas)
		for rgName, replicasInRG := range replicasByRG {
			nodes, err := ob.meta.ResourceManager.GetNodes(rgName)
			if err != nil {
				log.Warn("fail to get nodes when check unused nodes in resource group", zap.Error(err))
				continue
			}

			// Check if node is still in used by replicas in same collection.
			// A query node cannot load multi replicas in same collection.
			// Wait for the query node to release the old replica before
			// assign the node into expected replica.
			inUsedNodes := typeutil.NewUniqueSet()
			for _, replica := range replicas {
				inUsedNodes.Insert(replica.GetNodes()...)
			}
			unUsedNodes := typeutil.NewUniqueSet()
			for _, node := range nodes {
				if !inUsedNodes.Contain(node) {
					unUsedNodes.Insert(node)
				}
			}

			if unUsedNodes.Len() > 0 {
				for _, node := range unUsedNodes.Collect() {
					utils.AddNodesToReplicas(ob.meta, replicasInRG, node)
				}
			}
		}
	}
}
