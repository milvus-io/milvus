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

package job

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

type UndoList struct {
	PartitionsLoaded  bool // indicates if partitions loaded in QueryNodes during loading
	TargetUpdated     bool // indicates if target updated during loading
	NewReplicaCreated bool // indicates if created new replicas during loading

	CollectionID   int64
	LackPartitions []int64

	ctx            context.Context
	meta           *meta.Meta
	cluster        session.Cluster
	targetMgr      *meta.TargetManager
	targetObserver *observers.TargetObserver
}

func NewUndoList(ctx context.Context, meta *meta.Meta,
	cluster session.Cluster, targetMgr *meta.TargetManager, targetObserver *observers.TargetObserver) *UndoList {
	return &UndoList{
		ctx:            ctx,
		meta:           meta,
		cluster:        cluster,
		targetMgr:      targetMgr,
		targetObserver: targetObserver,
	}
}

func (u *UndoList) RollBack() {
	if u.PartitionsLoaded {
		releasePartitions(u.ctx, u.meta, u.cluster, true, u.CollectionID, u.LackPartitions...)
	}
	if u.TargetUpdated {
		if !u.meta.CollectionManager.Exist(u.CollectionID) {
			u.targetMgr.RemoveCollection(u.CollectionID)
			u.targetObserver.ReleaseCollection(u.CollectionID)
		} else {
			u.targetMgr.RemovePartition(u.CollectionID, u.LackPartitions...)
		}
	}
	if u.NewReplicaCreated {
		u.meta.ReplicaManager.RemoveCollection(u.CollectionID)
	}
}
