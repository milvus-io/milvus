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
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
)

type SegmentLock struct {
	segmentID UniqueID
	nodeID    UniqueID
	locKey    string
}

type SegmentReferenceManager struct {
	etcdKV kv.BaseKV

	segmentsLock map[UniqueID][]*SegmentLock
	lock         sync.RWMutex
}

func parseLockKey(key string) (segID UniqueID, nodeID UniqueID, err error) {
	ss := strings.Split(key, "/")
	// segment lock key consists of at least "meta/segmentRefer/nodeID/segID"
	if len(ss) < 4 {
		return 0, 0, fmt.Errorf("segment lock key is invalid with %s", key)
	}
	segID, err = strconv.ParseInt(ss[len(ss)-1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	nodeID, err = strconv.ParseInt(ss[len(ss)-2], 10, 64)
	return segID, nodeID, err
}

func NewSegmentReferenceManager(etcdKV kv.BaseKV, onlineIDs []UniqueID) (*SegmentReferenceManager, error) {
	log.Info("New segment reference manager")
	segReferManager := &SegmentReferenceManager{
		etcdKV:       etcdKV,
		segmentsLock: make(map[UniqueID][]*SegmentLock),
	}
	keys, _, err := segReferManager.etcdKV.LoadWithPrefix(segmentReferPrefix)
	if err != nil {
		log.Error("load segments lock from etcd failed", zap.Error(err))
		return nil, err
	}

	for _, key := range keys {
		segID, nodeID, err := parseLockKey(key)
		if err != nil {
			log.Error("parse segment lock key failed", zap.String("lock key", key), zap.Error(err))
			return nil, err
		}
		segLock := &SegmentLock{
			segmentID: segID,
			nodeID:    nodeID,
			locKey:    key,
		}
		segReferManager.segmentsLock[segID] = append(segReferManager.segmentsLock[segID], segLock)
	}

	err = segReferManager.recoverySegReferManager(onlineIDs)
	if err != nil {
		log.Error("Recovery segment reference manager failed", zap.Error(err))
		return nil, err
	}

	log.Info("New segment reference manager successfully")
	return segReferManager, nil
}

func (srm *SegmentReferenceManager) AddSegmentsLock(segIDs []UniqueID, nodeID UniqueID) error {
	srm.lock.Lock()
	defer srm.lock.Unlock()
	log.Info("Add reference lock on segments", zap.Int64s("segIDs", segIDs), zap.Int64("nodeID", nodeID))
	locKVs := make(map[string]string)
	segID2SegmentLock := make(map[UniqueID][]*SegmentLock)
	for _, segID := range segIDs {
		locKey := path.Join(segmentReferPrefix, strconv.FormatInt(nodeID, 10), strconv.FormatInt(segID, 10))
		locKVs[locKey] = strconv.FormatInt(nodeID, 10)
		segLock := &SegmentLock{
			segmentID: segID,
			nodeID:    nodeID,
			locKey:    locKey,
		}
		segID2SegmentLock[segID] = append(segID2SegmentLock[segID], segLock)
	}
	if err := srm.etcdKV.MultiSave(locKVs); err != nil {
		log.Error("AddSegmentsLock save  segment lock to etcd failed", zap.Int64s("segIDs", segIDs), zap.Error(err))
		return err
	}
	for segID, segLocks := range segID2SegmentLock {
		srm.segmentsLock[segID] = append(srm.segmentsLock[segID], segLocks...)
	}
	log.Info("Add reference lock on segments successfully", zap.Int64s("segIDs", segIDs), zap.Int64("nodeID", nodeID))
	return nil
}

func (srm *SegmentReferenceManager) ReleaseSegmentsLock(segIDs []UniqueID, nodeID UniqueID) error {
	srm.lock.Lock()
	defer srm.lock.Unlock()

	log.Info("Release reference lock on segments", zap.Int64s("segIDs", segIDs), zap.Int64("nodeID", nodeID))
	locKeys := make([]string, 0)
	for _, segID := range segIDs {
		for _, segLock := range srm.segmentsLock[segID] {
			if segLock.nodeID == nodeID {
				locKeys = append(locKeys, segLock.locKey)
			}
		}
	}
	if err := srm.etcdKV.MultiRemove(locKeys); err != nil {
		log.Error("Remove reference lock paths on segments failed", zap.Int64s("segIDs", segIDs),
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	for _, segID := range segIDs {
		if _, ok := srm.segmentsLock[segID]; !ok {
			continue
		}
		for i := 0; i < len(srm.segmentsLock[segID]); i++ {
			segLock := srm.segmentsLock[segID][i]
			if segLock.nodeID == nodeID {
				srm.segmentsLock[segID] = append(srm.segmentsLock[segID][:i], srm.segmentsLock[segID][i+1:]...)
				i--
			}
		}
		if len(srm.segmentsLock[segID]) == 0 {
			delete(srm.segmentsLock, segID)
		}
	}
	log.Info("Release reference lock on segments successfully", zap.Int64s("segIDs", segIDs), zap.Int64("nodeID", nodeID))
	return nil
}

func (srm *SegmentReferenceManager) ReleaseSegmentsLockByNodeID(nodeID UniqueID) error {
	srm.lock.Lock()
	defer srm.lock.Unlock()

	log.Info("Release reference lock on segments by node", zap.Int64("nodeID", nodeID))
	locKeys := make([]string, 0)
	for segID := range srm.segmentsLock {
		for _, segLock := range srm.segmentsLock[segID] {
			if segLock.nodeID == nodeID {
				locKeys = append(locKeys, segLock.locKey)
			}
		}
	}
	if err := srm.etcdKV.MultiRemove(locKeys); err != nil {
		log.Error("Remove reference lock paths on segments by node failed",
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	for segID := range srm.segmentsLock {
		for i := 0; i < len(srm.segmentsLock[segID]); i++ {
			segLock := srm.segmentsLock[segID][i]
			if segLock.nodeID == nodeID {
				srm.segmentsLock[segID] = append(srm.segmentsLock[segID][:i], srm.segmentsLock[segID][i+1:]...)
				i--
			}
		}
		if len(srm.segmentsLock[segID]) == 0 {
			delete(srm.segmentsLock, segID)
		}
	}
	log.Info("Release reference lock on segments by node successfully", zap.Int64("nodeID", nodeID))
	return nil
}

func (srm *SegmentReferenceManager) recoverySegReferManager(nodeIDs []UniqueID) error {
	log.Info("Recovery reference lock on segments by online nodes", zap.Int64s("online nodeIDs", nodeIDs))
	offlineIDs := make(map[UniqueID]struct{})
	for segID := range srm.segmentsLock {
		for _, segLock := range srm.segmentsLock[segID] {
			alive := false
			for _, nodeID := range nodeIDs {
				if segLock.nodeID == nodeID {
					alive = true
					break
				}
			}
			if !alive {
				offlineIDs[segLock.nodeID] = struct{}{}
			}
		}
	}
	for nodeID := range offlineIDs {
		if err := srm.ReleaseSegmentsLockByNodeID(nodeID); err != nil {
			log.Error("Remove reference lock on segments by offline node failed",
				zap.Int64("offline nodeID", nodeID), zap.Error(err))
			return err
		}
	}
	log.Info("Recovery reference lock on segments by online nodes successfully", zap.Int64s("online nodeIDs", nodeIDs),
		zap.Any("offline nodeIDs", offlineIDs))
	return nil
}

func (srm *SegmentReferenceManager) HasSegmentLock(segID UniqueID) bool {
	srm.lock.RLock()
	defer srm.lock.RUnlock()

	_, ok := srm.segmentsLock[segID]
	return ok
}

func (srm *SegmentReferenceManager) GetHasReferLockSegmentIDs() []UniqueID {
	srm.lock.RLock()
	defer srm.lock.RUnlock()

	segIDs := make([]UniqueID, 0)
	for segID := range srm.segmentsLock {
		segIDs = append(segIDs, segID)
	}
	return segIDs
}
