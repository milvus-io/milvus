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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func diffFileResourceIDs(oldIDs []int64, newIDs []int64) ([]int64, []int64) {
	oldSet := make(map[int64]struct{}, len(oldIDs))
	for _, id := range oldIDs {
		oldSet[id] = struct{}{}
	}
	newSet := make(map[int64]struct{}, len(newIDs))
	for _, id := range newIDs {
		newSet[id] = struct{}{}
	}

	added := make([]int64, 0)
	addedSet := make(map[int64]struct{}, len(newIDs))
	for _, id := range newIDs {
		if _, ok := oldSet[id]; ok {
			continue
		}
		if _, ok := addedSet[id]; !ok {
			added = append(added, id)
			addedSet[id] = struct{}{}
		}
	}
	removed := make([]int64, 0)
	removedSet := make(map[int64]struct{}, len(oldIDs))
	for _, id := range oldIDs {
		if _, ok := newSet[id]; ok {
			continue
		}
		if _, ok := removedSet[id]; !ok {
			removed = append(removed, id)
			removedSet[id] = struct{}{}
		}
	}
	return added, removed
}

func (mt *MetaTable) validateAddedFileResourceRefsLocked(collectionID int64, addedIDs []int64) error {
	for _, id := range addedIDs {
		if _, ok := mt.fileResourceID2Meta[id]; !ok {
			return merr.WrapErrServiceInternalMsg("collection %d references missing file resource %d", collectionID, id)
		}
	}
	return nil
}

type alterCollectionFileResourceRefReserver interface {
	reserveAlterCollectionFileResourceRefs(collectionID int64, ids []int64) error
}

func reserveFileResourceRefs(meta IMetaTable, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return meta.IncFileResourceRefCnt(ids)
}

func reserveAlterCollectionFileResourceRefs(meta IMetaTable, collectionID int64, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if reserver, ok := meta.(alterCollectionFileResourceRefReserver); ok {
		return reserver.reserveAlterCollectionFileResourceRefs(collectionID, ids)
	}
	return reserveFileResourceRefs(meta, ids)
}

func (mt *MetaTable) reserveAlterCollectionFileResourceRefs(collectionID int64, ids []int64) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if err := mt.incFileResourceRefCntLocked(ids); err != nil {
		return err
	}
	mt.recordFileResourceRefHoldLocked(collectionID, ids)
	return nil
}

func (mt *MetaTable) recordFileResourceRefHoldLocked(collectionID int64, ids []int64) {
	if len(ids) == 0 {
		return
	}
	if mt.fileResourceRefHolds == nil {
		mt.fileResourceRefHolds = make(map[int64]map[int64]int)
	}
	holds := mt.fileResourceRefHolds[collectionID]
	if holds == nil {
		holds = make(map[int64]int, len(ids))
		mt.fileResourceRefHolds[collectionID] = holds
	}
	for _, id := range ids {
		holds[id]++
	}
}

func (mt *MetaTable) consumeFileResourceRefHoldLocked(collectionID int64, resourceID int64) bool {
	holds := mt.fileResourceRefHolds[collectionID]
	if holds == nil || holds[resourceID] == 0 {
		return false
	}
	holds[resourceID]--
	if holds[resourceID] == 0 {
		delete(holds, resourceID)
	}
	if len(holds) == 0 {
		delete(mt.fileResourceRefHolds, collectionID)
	}
	return true
}

func (mt *MetaTable) applyAlterCollectionFileResourceRefCntLocked(ctx context.Context, collectionID int64, addedIDs, removedIDs []int64) {
	for _, id := range addedIDs {
		if mt.consumeFileResourceRefHoldLocked(collectionID, id) {
			// A request-path reservation already incremented the effective refCnt.
			// Keep the count unchanged; updating collID2Meta below turns it into a
			// committed collection reference. WAL replay/replicated tasks have no
			// reservation for this collection, so they fall through and increment here.
			continue
		}
		mt.fileResourceRefCnt[id]++
	}
	for _, id := range removedIDs {
		if mt.fileResourceRefCnt[id] > 0 {
			mt.fileResourceRefCnt[id]--
		} else {
			mlog.Warn(ctx, "AlterCollection: file resource refCnt underflow",
				mlog.Int64("collectionID", collectionID), mlog.Int64("fileResourceID", id))
		}
	}
}
