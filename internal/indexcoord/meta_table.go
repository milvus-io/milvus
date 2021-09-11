// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexcoord

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/util/retry"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type Meta struct {
	indexMeta *indexpb.IndexMeta
	revision  int64
}

type metaTable struct {
	client            *etcdkv.EtcdKV    // client of a reliable kv service, i.e. etcd client
	indexBuildID2Meta map[UniqueID]Meta // index build id to index meta

	lock sync.RWMutex
}

func NewMetaTable(kv *etcdkv.EtcdKV) (*metaTable, error) {
	mt := &metaTable{
		client: kv,
		lock:   sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return mt, nil
}

func (mt *metaTable) reloadFromKV() error {
	mt.indexBuildID2Meta = make(map[UniqueID]Meta)
	key := "indexes"
	log.Debug("IndexCoord metaTable LoadWithPrefix ", zap.String("prefix", key))

	_, values, versions, err := mt.client.LoadWithPrefix2(key)
	if err != nil {
		return err
	}

	for i := 0; i < len(values); i++ {
		indexMeta := indexpb.IndexMeta{}
		err = proto.UnmarshalText(values[i], &indexMeta)
		if err != nil {
			return fmt.Errorf("IndexCoord metaTable reloadFromKV UnmarshalText indexpb.IndexMeta err:%w", err)
		}

		meta := &Meta{
			indexMeta: &indexMeta,
			revision:  versions[i],
		}
		mt.indexBuildID2Meta[indexMeta.IndexBuildID] = *meta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *Meta) error {
	value := proto.MarshalTextString(meta.indexMeta)

	key := "indexes/" + strconv.FormatInt(meta.indexMeta.IndexBuildID, 10)
	err := mt.client.CompareVersionAndSwap(key, meta.revision, value)
	log.Debug("IndexCoord metaTable saveIndexMeta ", zap.String("key", key), zap.Error(err))
	if err != nil {
		return err
	}
	meta.revision = meta.revision + 1
	mt.indexBuildID2Meta[meta.indexMeta.IndexBuildID] = *meta
	log.Debug("IndexCoord metaTable saveIndexMeta success", zap.Any("meta.revision", meta.revision))

	return nil
}

func (mt *metaTable) reloadMeta(indexBuildID UniqueID) (*Meta, error) {
	key := "indexes/" + strconv.FormatInt(indexBuildID, 10)

	_, values, version, err := mt.client.LoadWithPrefix2(key)
	log.Debug("IndexCoord reloadMeta mt.client.LoadWithPrefix2", zap.Any("indexBuildID", indexBuildID), zap.Error(err))
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		log.Error("IndexCoord reload Meta", zap.Any("indexBuildID", indexBuildID), zap.Error(errors.New("meta doesn't exist in KV")))
		return nil, errors.New("meta doesn't exist in KV")
	}
	im := &indexpb.IndexMeta{}
	err = proto.UnmarshalText(values[0], im)
	if err != nil {
		return nil, err
	}
	//if im.State == commonpb.IndexState_Finished {
	//	return nil, nil
	//}
	m := &Meta{
		revision:  version[0],
		indexMeta: im,
	}

	return m, nil
}

func (mt *metaTable) AddIndex(indexBuildID UniqueID, req *indexpb.BuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	_, ok := mt.indexBuildID2Meta[indexBuildID]
	log.Debug("IndexCoord metaTable AddIndex", zap.Any("indexBuildID", indexBuildID), zap.Any(" index already exist", ok))
	if ok {
		return fmt.Errorf("index already exists with ID = %d", indexBuildID)
	}
	meta := &Meta{
		indexMeta: &indexpb.IndexMeta{
			State:        commonpb.IndexState_Unissued,
			IndexBuildID: indexBuildID,
			Req:          req,
			NodeID:       0,
			Version:      0,
		},
		revision: 0,
	}
	return mt.saveIndexMeta(meta)
}

func (mt *metaTable) BuildIndex(indexBuildID UniqueID, nodeID int64) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord metaTable BuildIndex")

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		log.Debug("IndexCoord metaTable BuildIndex index not exists", zap.Any("indexBuildID", indexBuildID))
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	//if meta.indexMeta.State != commonpb.IndexState_Unissued {
	//	return fmt.Errorf("can not set lease key, index with ID = %d state is %d", indexBuildID, meta.indexMeta.State)
	//}

	meta.indexMeta.NodeID = nodeID
	meta.indexMeta.State = commonpb.IndexState_InProgress

	err := mt.saveIndexMeta(&meta)
	if err != nil {
		fn := func() error {
			m, err := mt.reloadMeta(meta.indexMeta.IndexBuildID)
			if m == nil {
				return err
			}
			m.indexMeta.NodeID = nodeID
			return mt.saveIndexMeta(m)
		}
		err2 := retry.Do(context.TODO(), fn, retry.Attempts(5))
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (mt *metaTable) UpdateVersion(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord metaTable update UpdateVersion", zap.Any("IndexBuildId", indexBuildID))
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		log.Debug("IndexCoord metaTable update UpdateVersion indexBuildID not exists", zap.Any("IndexBuildId", indexBuildID))
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	//if meta.indexMeta.State != commonpb.IndexState_Unissued {
	//	return fmt.Errorf("can not set lease key, index with ID = %d state is %d", indexBuildID, meta.indexMeta.State)
	//}

	meta.indexMeta.Version = meta.indexMeta.Version + 1
	log.Debug("IndexCoord metaTable update UpdateVersion", zap.Any("IndexBuildId", indexBuildID),
		zap.Any("Version", meta.indexMeta.Version))

	err := mt.saveIndexMeta(&meta)
	if err != nil {
		fn := func() error {
			m, err := mt.reloadMeta(meta.indexMeta.IndexBuildID)
			if m == nil {
				return err
			}
			m.indexMeta.Version = m.indexMeta.Version + 1

			return mt.saveIndexMeta(m)
		}
		err2 := retry.Do(context.TODO(), fn, retry.Attempts(5))
		return err2
	}

	return nil
}

func (mt *metaTable) MarkIndexAsDeleted(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("IndexCoord metaTable MarkIndexAsDeleted ", zap.Int64("indexID", indexID))

	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.Req.IndexID == indexID && !meta.indexMeta.MarkDeleted {
			meta.indexMeta.MarkDeleted = true
			if err := mt.saveIndexMeta(&meta); err != nil {
				log.Debug("IndexCoord metaTable MarkIndexAsDeleted saveIndexMeta failed", zap.Error(err))
				fn := func() error {
					m, err := mt.reloadMeta(meta.indexMeta.IndexBuildID)
					if m == nil {
						return err
					}

					m.indexMeta.MarkDeleted = true
					return mt.saveIndexMeta(m)
				}
				err2 := retry.Do(context.TODO(), fn, retry.Attempts(5))
				if err2 != nil {
					return err2
				}
			}
		}
	}

	return nil
}

func (mt *metaTable) GetIndexStates(indexBuildIDs []UniqueID) []*indexpb.IndexInfo {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	var indexStates []*indexpb.IndexInfo
	for _, id := range indexBuildIDs {
		state := &indexpb.IndexInfo{
			IndexBuildID: id,
		}
		meta, ok := mt.indexBuildID2Meta[id]
		if !ok {
			state.Reason = fmt.Sprintf("index %d not exists", id)
		} else if meta.indexMeta.MarkDeleted {
			state.Reason = fmt.Sprintf("index %d has been deleted", id)
		} else {
			state.State = meta.indexMeta.State
			state.IndexID = meta.indexMeta.Req.IndexID
			state.IndexName = meta.indexMeta.Req.IndexName
			state.Reason = meta.indexMeta.FailReason
		}
		indexStates = append(indexStates, state)
	}
	return indexStates
}

func (mt *metaTable) GetIndexFilePathInfo(indexBuildID UniqueID) (*indexpb.IndexFilePathInfo, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	ret := &indexpb.IndexFilePathInfo{
		IndexBuildID: indexBuildID,
	}
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return nil, fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}
	if meta.indexMeta.MarkDeleted {
		return nil, fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}
	ret.IndexFilePaths = meta.indexMeta.IndexFilePaths
	return ret, nil
}

func (mt *metaTable) DeleteIndex(indexBuildID UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	delete(mt.indexBuildID2Meta, indexBuildID)
	key := "indexes/" + strconv.FormatInt(indexBuildID, 10)

	err := mt.client.Remove(key)
	log.Debug("IndexCoord metaTable DeleteIndex", zap.Error(err))
}

func (mt *metaTable) UpdateRecycleState(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	log.Debug("IndexCoord metaTable UpdateRecycleState", zap.Any("indexBuildID", indexBuildID),
		zap.Any("exists", ok))
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	if meta.indexMeta.Recycled {
		return nil
	}

	meta.indexMeta.Recycled = true
	if err := mt.saveIndexMeta(&meta); err != nil {
		fn := func() error {
			m, err := mt.reloadMeta(meta.indexMeta.IndexBuildID)
			if m == nil {
				return err
			}

			m.indexMeta.Recycled = true
			return mt.saveIndexMeta(m)
		}
		err2 := retry.Do(context.TODO(), fn, retry.Attempts(5))
		if err2 != nil {
			meta.indexMeta.Recycled = false
			log.Debug("IndexCoord metaTable UpdateRecycleState failed", zap.Error(err2))
			return err2
		}
	}

	return nil
}

func (mt *metaTable) GetUnusedIndexFiles(limit int) []Meta {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	var metas []Meta
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.State == commonpb.IndexState_Finished && (meta.indexMeta.MarkDeleted || !meta.indexMeta.Recycled) {
			metas = append(metas, meta)
		}
		if len(metas) >= limit {
			return metas
		}
	}

	return metas
}

func (mt *metaTable) GetUnassignedTasks(onlineNodeIDs []int64) []Meta {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	var metas []Meta

	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.State == commonpb.IndexState_Unissued {
			metas = append(metas, meta)
			continue
		}
		if meta.indexMeta.State == commonpb.IndexState_Finished || meta.indexMeta.State == commonpb.IndexState_Failed {
			continue
		}
		alive := false
		for _, serverID := range onlineNodeIDs {
			if meta.indexMeta.NodeID == serverID {
				alive = true
				break
			}
		}
		if !alive {
			metas = append(metas, meta)
		}
	}

	return metas
}

func (mt *metaTable) HasSameReq(req *indexpb.BuildIndexRequest) (bool, UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.Req.IndexID != req.IndexID {
			continue
		}
		if meta.indexMeta.Req.IndexName != req.IndexName {
			continue
		}
		if len(meta.indexMeta.Req.DataPaths) != len(req.DataPaths) {
			continue
		}
		notEq := false
		for i := range meta.indexMeta.Req.DataPaths {
			if meta.indexMeta.Req.DataPaths[i] != req.DataPaths[i] {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		if len(meta.indexMeta.Req.TypeParams) != len(req.TypeParams) {
			continue
		}
		notEq = false
		for i := range meta.indexMeta.Req.TypeParams {
			if meta.indexMeta.Req.TypeParams[i].Key != req.TypeParams[i].Key {
				notEq = true
				break
			}
			if meta.indexMeta.Req.TypeParams[i].Value != req.TypeParams[i].Value {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		if len(meta.indexMeta.Req.IndexParams) != len(req.IndexParams) {
			continue
		}
		notEq = false
		for i := range meta.indexMeta.Req.IndexParams {
			if meta.indexMeta.Req.IndexParams[i].Key != req.IndexParams[i].Key {
				notEq = true
				break
			}
			if meta.indexMeta.Req.IndexParams[i].Value != req.IndexParams[i].Value {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		return true, meta.indexMeta.IndexBuildID
	}

	return false, -1
}

func (mt *metaTable) LoadMetaFromETCD(indexBuildID int64, revision int64) bool {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	log.Debug("IndexCoord metaTable LoadMetaFromETCD", zap.Any("indexBuildID", indexBuildID),
		zap.Any("revision", revision), zap.Any("ok", ok))
	if ok {
		log.Debug("IndexCoord metaTable LoadMetaFromETCD",
			zap.Any("meta.revision", meta.revision),
			zap.Any("revision", revision))

		if meta.revision >= revision {
			return false
		}
	} else {
		log.Debug("Index not exist", zap.Int64("IndexBuildID", indexBuildID))
		return false
	}

	m, err := mt.reloadMeta(indexBuildID)
	if m == nil {
		log.Debug("IndexCoord metaTable reloadMeta failed", zap.Error(err))
		return false
	}

	mt.indexBuildID2Meta[indexBuildID] = *m
	log.Debug("IndexCoord LoadMetaFromETCD success", zap.Any("IndexMeta", m))

	return true
}

func (mt *metaTable) GetNodeTaskStats() map[UniqueID]int {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	log.Debug("IndexCoord MetaTable GetPriorityForNodeID")
	nodePriority := make(map[UniqueID]int)
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.State == commonpb.IndexState_InProgress {
			nodePriority[meta.indexMeta.NodeID]++
		}
	}
	return nodePriority
}
