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

package datacoord

import (
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

type SegmentReferenceManager struct {
	etcdKV kv.BaseKV

	// taskID -> (nodeID -> segmentReferenceLock), taskID must be globally unique in a component
	segmentsLock    map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock
	segmentReferCnt map[UniqueID]int
	lock            sync.RWMutex
}

func NewSegmentReferenceManager(etcdKV kv.BaseKV, onlineIDs []UniqueID) (*SegmentReferenceManager, error) {
	log.Info("create a new segment reference manager")
	segReferManager := &SegmentReferenceManager{
		etcdKV:          etcdKV,
		segmentsLock:    make(map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock),
		segmentReferCnt: map[UniqueID]int{},
		lock:            sync.RWMutex{},
	}
	_, values, err := segReferManager.etcdKV.LoadWithPrefix(segmentReferPrefix)
	if err != nil {
		log.Error("load segments lock from etcd failed", zap.Error(err))
		return nil, err
	}

	for _, value := range values {
		segReferLock := &datapb.SegmentReferenceLock{}
		if err = proto.Unmarshal([]byte(value), segReferLock); err != nil {
			log.Error("unmarshal segment reference lock failed", zap.Error(err))
			return nil, err
		}
		if _, ok := segReferManager.segmentsLock[segReferLock.TaskID]; !ok {
			segReferManager.segmentsLock[segReferLock.TaskID] = map[UniqueID]*datapb.SegmentReferenceLock{}
		}
		segReferManager.segmentsLock[segReferLock.TaskID][segReferLock.NodeID] = segReferLock
		for _, segID := range segReferLock.SegmentIDs {
			segReferManager.segmentReferCnt[segID]++
		}
	}

	err = segReferManager.recoverySegReferManager(onlineIDs)
	if err != nil {
		log.Error("recovery segment reference manager failed", zap.Error(err))
		return nil, err
	}

	log.Info("create new segment reference manager successfully")
	return segReferManager, nil
}

func generateLocKey(taskID, nodeID UniqueID) string {
	return path.Join(segmentReferPrefix, strconv.FormatInt(taskID, 10), strconv.FormatInt(nodeID, 10))
}

// AddSegmentsLock adds a reference lock on segments to ensure the segments does not compaction during the reference period.
func (srm *SegmentReferenceManager) AddSegmentsLock(taskID int64, segIDs []UniqueID, nodeID UniqueID) error {
	srm.lock.Lock()
	defer srm.lock.Unlock()
	log.Info("add reference lock on segments", zap.Int64s("segIDs", segIDs), zap.Int64("nodeID", nodeID))

	segReferLock := &datapb.SegmentReferenceLock{
		TaskID:     taskID,
		NodeID:     nodeID,
		SegmentIDs: segIDs,
	}
	value, err := proto.Marshal(segReferLock)
	if err != nil {
		log.Error("AddSegmentsLock marshal failed", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID),
			zap.Int64s("segIDs", segIDs), zap.Error(err))
		return err
	}
	if err = srm.etcdKV.Save(generateLocKey(taskID, nodeID), string(value)); err != nil {
		log.Error("AddSegmentsLock save segment lock to etcd failed", zap.Int64("taskID", taskID),
			zap.Int64("nodeID", nodeID), zap.Int64s("segIDs", segIDs), zap.Error(err))
		return err
	}
	if _, ok := srm.segmentsLock[taskID]; !ok {
		srm.segmentsLock[taskID] = map[UniqueID]*datapb.SegmentReferenceLock{}
	}
	srm.segmentsLock[taskID][nodeID] = segReferLock
	for _, segID := range segIDs {
		srm.segmentReferCnt[segID]++
	}
	log.Info("add reference lock on segments successfully", zap.Int64s("segIDs", segIDs), zap.Int64("nodeID", nodeID))
	return nil
}

func (srm *SegmentReferenceManager) ReleaseSegmentsLock(taskID int64, nodeID UniqueID) error {
	srm.lock.Lock()
	defer srm.lock.Unlock()

	log.Info("release reference lock by taskID", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
	if _, ok := srm.segmentsLock[taskID]; !ok {
		log.Warn("taskID has no reference lock on segment", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
		return nil
	}

	if _, ok := srm.segmentsLock[taskID][nodeID]; !ok {
		log.Warn("taskID has no reference lock on segment with the nodeID", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
		return nil
	}

	if err := srm.etcdKV.Remove(generateLocKey(taskID, nodeID)); err != nil {
		log.Error("remove reference lock paths by taskID failed", zap.Int64("taskID", taskID),
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	for _, segID := range srm.segmentsLock[taskID][nodeID].SegmentIDs {
		srm.segmentReferCnt[segID]--
		if srm.segmentReferCnt[segID] <= 0 {
			delete(srm.segmentReferCnt, segID)
		}
	}

	delete(srm.segmentsLock[taskID], nodeID)
	if len(srm.segmentsLock[taskID]) == 0 {
		delete(srm.segmentsLock, taskID)
	}
	log.Info("release reference lock by taskID successfully", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
	return nil
}

func (srm *SegmentReferenceManager) ReleaseSegmentsLockByNodeID(nodeID UniqueID) error {
	srm.lock.Lock()
	defer srm.lock.Unlock()

	log.Info("release reference lock on segments by node", zap.Int64("nodeID", nodeID))
	for taskID, segReferLock := range srm.segmentsLock {
		if _, ok := segReferLock[nodeID]; !ok {
			continue
		}
		// The reason for not using MultiRemove is to prevent too many keys.
		if err := srm.etcdKV.Remove(generateLocKey(taskID, nodeID)); err != nil {
			log.Warn("remove reference lock path by taskID failed, need to retry", zap.Int64("nodeID", nodeID),
				zap.Int64("taskID", taskID), zap.Error(err))
			return err
		}
		for _, segID := range segReferLock[nodeID].SegmentIDs {
			srm.segmentReferCnt[segID]--
			if srm.segmentReferCnt[segID] <= 0 {
				delete(srm.segmentReferCnt, segID)
			}
		}
		delete(srm.segmentsLock[taskID], nodeID)
		if len(srm.segmentsLock[taskID]) == 0 {
			delete(srm.segmentsLock, taskID)
		}
	}

	log.Info("release reference lock on segments by node successfully", zap.Int64("nodeID", nodeID))
	return nil
}

func (srm *SegmentReferenceManager) recoverySegReferManager(nodeIDs []UniqueID) error {
	log.Info("recovery reference lock on segments by online nodes", zap.Int64s("online nodeIDs", nodeIDs))
	onlineIDs := make(map[UniqueID]struct{})
	for _, nodeID := range nodeIDs {
		onlineIDs[nodeID] = struct{}{}
	}
	offlineIDs := make(map[UniqueID]struct{})
	for _, segLock := range srm.segmentsLock {
		for nodeID := range segLock {
			if _, ok := onlineIDs[nodeID]; !ok {
				offlineIDs[nodeID] = struct{}{}
			}
		}
	}
	for nodeID := range offlineIDs {
		if err := srm.ReleaseSegmentsLockByNodeID(nodeID); err != nil {
			log.Error("remove reference lock on segments by offline node failed",
				zap.Int64("offline nodeID", nodeID), zap.Error(err))
			return err
		}
	}
	log.Info("recovery reference lock on segments by online nodes successfully", zap.Int64s("online nodeIDs", nodeIDs),
		zap.Any("offline nodeIDs", offlineIDs))
	return nil
}

func (srm *SegmentReferenceManager) HasSegmentLock(segID UniqueID) bool {
	srm.lock.RLock()
	defer srm.lock.RUnlock()

	if _, ok := srm.segmentReferCnt[segID]; !ok {
		return false
	}
	return true
}
