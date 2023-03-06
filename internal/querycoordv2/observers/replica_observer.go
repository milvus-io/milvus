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
)

// check replica, find outbound nodes and remove it from replica if all segment/channel has been moved
type ReplicaObserver struct {
	c       chan struct{}
	wg      sync.WaitGroup
	meta    *meta.Meta
	distMgr *meta.DistributionManager

	stopOnce sync.Once
}

func NewReplicaObserver(meta *meta.Meta, distMgr *meta.DistributionManager) *ReplicaObserver {
	return &ReplicaObserver{
		c:       make(chan struct{}),
		meta:    meta,
		distMgr: distMgr,
	}
}

func (ob *ReplicaObserver) Start(ctx context.Context) {
	ob.wg.Add(1)
	go ob.schedule(ctx)
}

func (ob *ReplicaObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.c)
		ob.wg.Wait()
	})
}

func (ob *ReplicaObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("Start check replica loop")

	ticker := time.NewTicker(params.Params.QueryCoordCfg.CheckNodeInReplicaInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("Close replica observer due to context canceled")
			return
		case <-ob.c:
			log.Info("Close replica observer")
			return

		case <-ticker.C:
			ob.checkNodesInReplica()
		}
	}
}

func (ob *ReplicaObserver) checkNodesInReplica() {
	collections := ob.meta.GetAll()
	for _, collectionID := range collections {
		removedNodes := make([]int64, 0)
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
						removedNodes = append(removedNodes, node)
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

		// assign removed nodes to other replicas in current rg
		for _, node := range removedNodes {
			rg, err := ob.meta.ResourceManager.FindResourceGroupByNode(node)
			if err != nil {
				// unreachable logic path
				log.Warn("found node which does not belong to any resource group", zap.Int64("nodeID", node))
				continue
			}
			replicas := ob.meta.ReplicaManager.GetByCollectionAndRG(collectionID, rg)
			utils.AddNodesToReplicas(ob.meta, replicas, node)
		}
	}
}
