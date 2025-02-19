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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type UndoList struct {
	IsTargetUpdated  bool // indicates if target updated during loading
	IsReplicaCreated bool // indicates if created new replicas during loading
	IsNewCollection  bool // indicates if created new collection during loading

	CollectionID   int64
	LackPartitions []int64

	ctx            context.Context
	meta           *meta.Meta
	targetMgr      meta.TargetManagerInterface
	targetObserver *observers.TargetObserver
}

func NewUndoList(ctx context.Context, meta *meta.Meta,
	targetMgr meta.TargetManagerInterface, targetObserver *observers.TargetObserver,
) *UndoList {
	return &UndoList{
		ctx:            ctx,
		meta:           meta,
		targetMgr:      targetMgr,
		targetObserver: targetObserver,
	}
}

func (u *UndoList) RollBack() {
	log := log.Ctx(u.ctx).With(
		zap.Int64("collectionID", u.CollectionID),
		zap.Int64s("partitionIDs", u.LackPartitions),
	)

	log.Warn("rollback failed loading request...",
		zap.Bool("isNewCollection", u.IsNewCollection),
		zap.Bool("isReplicaCreated", u.IsReplicaCreated),
		zap.Bool("isTargetUpdated", u.IsTargetUpdated),
	)

	var err error
	if u.IsNewCollection || u.IsReplicaCreated {
		err = u.meta.CollectionManager.RemoveCollection(u.ctx, u.CollectionID)
	} else {
		err = u.meta.CollectionManager.RemovePartition(u.ctx, u.CollectionID, u.LackPartitions...)
	}
	if err != nil {
		log.Warn("failed to rollback collection from meta", zap.Error(err))
	}

	if u.IsTargetUpdated {
		if u.IsNewCollection {
			u.targetObserver.ReleaseCollection(u.CollectionID)
		} else {
			u.targetObserver.ReleasePartition(u.CollectionID, u.LackPartitions...)
		}
	}
}
