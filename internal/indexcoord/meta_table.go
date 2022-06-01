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

package indexcoord

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/retry"
)

// Meta is used to record the state of the index.
// revision: The number of times IndexMeta has been changed in etcd. It's the same as Event.Kv.Version in etcd.
// indexMeta: A structure that records the state of the index defined by proto.
type Meta struct {
	indexMeta *indexpb.IndexMeta
	revision  int64
}

// metaTable records the mapping of IndexBuildID to Meta.
type metaTable struct {
	client            *etcdkv.EtcdKV    // client of a reliable kv service, i.e. etcd client
	indexBuildID2Meta map[UniqueID]Meta // index build id to index meta

	lock sync.RWMutex
}

// NewMetaTable is used to create a new meta table.
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

// reloadFromKV reloads the index meta from ETCD.
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
		err = proto.Unmarshal([]byte(values[i]), &indexMeta)
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

// saveIndexMeta saves the index meta to ETCD.
// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *Meta) error {
	value, err := proto.Marshal(meta.indexMeta)
	if err != nil {
		return err
	}
	key := path.Join(indexFilePrefix, strconv.FormatInt(meta.indexMeta.IndexBuildID, 10))
	err = mt.client.CompareVersionAndSwap(key, meta.revision, string(value))
	log.Debug("IndexCoord metaTable saveIndexMeta ", zap.String("key", key), zap.Error(err))
	if err != nil {
		return err
	}
	meta.revision = meta.revision + 1
	mt.indexBuildID2Meta[meta.indexMeta.IndexBuildID] = *meta
	log.Debug("IndexCoord metaTable saveIndexMeta success", zap.Any("meta.revision", meta.revision))

	return nil
}

// reloadMeta reloads the index meta corresponding indexBuildID from ETCD.
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
	err = proto.Unmarshal([]byte(values[0]), im)
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

// AddIndex adds the index meta corresponding the indexBuildID to meta table.
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
	metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Inc()
	return mt.saveIndexMeta(meta)
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (mt *metaTable) BuildIndex(indexBuildID UniqueID, nodeID int64) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord metaTable BuildIndex")

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		log.Error("IndexCoord metaTable BuildIndex index not exists", zap.Any("indexBuildID", indexBuildID))
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	//if meta.indexMeta.State != commonpb.IndexState_Unissued {
	//	return fmt.Errorf("can not set lease key, index with ID = %d state is %d", indexBuildID, meta.indexMeta.State)
	//}

	if meta.indexMeta.State == commonpb.IndexState_Finished || meta.indexMeta.State == commonpb.IndexState_Failed {
		log.Debug("This index task has been finished", zap.Int64("indexBuildID", indexBuildID),
			zap.Any("index state", meta.indexMeta.State))
		return nil
	}
	meta.indexMeta.NodeID = nodeID
	meta.indexMeta.State = commonpb.IndexState_InProgress
	metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Dec()
	metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.InProgressIndexTaskLabel).Inc()

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

// UpdateVersion updates the version of the index meta, whenever the task is built once, the version will be updated once.
func (mt *metaTable) UpdateVersion(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord metaTable update UpdateVersion", zap.Any("IndexBuildId", indexBuildID))
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		log.Warn("IndexCoord metaTable update UpdateVersion indexBuildID not exists", zap.Any("IndexBuildId", indexBuildID))
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

// MarkIndexAsDeleted will mark the corresponding index as deleted, and recycleUnusedIndexFiles will recycle these tasks.
func (mt *metaTable) MarkIndexAsDeleted(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("IndexCoord metaTable MarkIndexAsDeleted ", zap.Int64("indexID", indexID))

	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.Req.IndexID == indexID && !meta.indexMeta.MarkDeleted {
			meta.indexMeta.MarkDeleted = true
			// marshal inside
			/* #nosec G601 */
			if err := mt.saveIndexMeta(&meta); err != nil {
				log.Error("IndexCoord metaTable MarkIndexAsDeleted saveIndexMeta failed", zap.Error(err))
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

// GetIndexStates gets the index states from meta table.
func (mt *metaTable) GetIndexStates(indexBuildIDs []UniqueID) []*indexpb.IndexInfo {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord get index states from meta table", zap.Int64s("indexBuildIDs", indexBuildIDs))
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

// GetIndexFilePathInfo gets the index file paths from meta table.
func (mt *metaTable) GetIndexFilePathInfo(indexBuildID UniqueID) (*indexpb.IndexFilePathInfo, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord get index file path from meta table", zap.Int64("indexBuildID", indexBuildID))
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
	if meta.indexMeta.State != commonpb.IndexState_Finished {
		return nil, fmt.Errorf("index not finished with ID = %d", indexBuildID)
	}
	ret.IndexFilePaths = meta.indexMeta.IndexFilePaths
	ret.SerializedSize = meta.indexMeta.GetSerializeSize()

	log.Debug("IndexCoord get index file path successfully", zap.Int64("indexBuildID", indexBuildID),
		zap.Int("index files num", len(ret.IndexFilePaths)))
	return ret, nil
}

// DeleteIndex delete the index meta from meta table.
func (mt *metaTable) DeleteIndex(indexBuildID UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	delete(mt.indexBuildID2Meta, indexBuildID)
	key := "indexes/" + strconv.FormatInt(indexBuildID, 10)

	if err := mt.client.Remove(key); err != nil {
		log.Error("IndexCoord delete index meta from etcd failed", zap.Error(err))
	}
	log.Debug("IndexCoord delete index meta successfully", zap.Int64("indexBuildID", indexBuildID))
}

// UpdateRecycleState update the recycle state corresponding the indexBuildID,
// when the recycle state is true, means the index files has been recycled with lower version.
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
			log.Error("IndexCoord metaTable UpdateRecycleState failed", zap.Error(err2))
			return err2
		}
	}

	return nil
}

// GetUnusedIndexFiles get the index files with lower version or corresponding the indexBuildIDs which has been deleted.
func (mt *metaTable) GetUnusedIndexFiles(limit int) []Meta {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	var metas []Meta
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.State == commonpb.IndexState_Finished && (meta.indexMeta.MarkDeleted || !meta.indexMeta.Recycled) {
			metas = append(metas, Meta{indexMeta: proto.Clone(meta.indexMeta).(*indexpb.IndexMeta), revision: meta.revision})
		}
		if len(metas) >= limit {
			return metas
		}
	}

	return metas
}

// GetUnassignedTasks get the unassigned tasks.
func (mt *metaTable) GetUnassignedTasks(onlineNodeIDs []int64) []Meta {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	var metas []Meta

	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.State == commonpb.IndexState_Unissued {
			metas = append(metas, Meta{indexMeta: proto.Clone(meta.indexMeta).(*indexpb.IndexMeta), revision: meta.revision})
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
			log.Info("Reassign because node no longer alive", zap.Any("onlineID", onlineNodeIDs), zap.Int64("nodeID", meta.indexMeta.NodeID))
			metas = append(metas, Meta{indexMeta: proto.Clone(meta.indexMeta).(*indexpb.IndexMeta), revision: meta.revision})
		}
	}
	return metas
}

// HasSameReq determine whether there are same indexing tasks.
func (mt *metaTable) HasSameReq(req *indexpb.BuildIndexRequest) (bool, UniqueID) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	for _, meta := range mt.indexBuildID2Meta {
		if req.GetSegmentID() != meta.indexMeta.Req.GetSegmentID() {
			continue
		}
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

	return false, 0
}

// LoadMetaFromETCD load the meta of specified indexBuildID from ETCD.
// If the version of meta in memory is greater equal to the version in ETCD, no need to reload.
func (mt *metaTable) LoadMetaFromETCD(indexBuildID int64, revision int64) bool {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	log.Debug("IndexCoord metaTable LoadMetaFromETCD", zap.Int64("indexBuildID", indexBuildID),
		zap.Int64("revision", revision), zap.Bool("ok", ok))
	if ok {
		log.Debug("IndexCoord metaTable LoadMetaFromETCD",
			zap.Int64("meta.revision", meta.revision),
			zap.Int64("revision", revision))

		if meta.revision >= revision {
			return false
		}
	} else {
		log.Error("Index not exist", zap.Int64("IndexBuildID", indexBuildID))
		return false
	}

	m, err := mt.reloadMeta(indexBuildID)
	if m == nil {
		log.Error("IndexCoord metaTable reloadMeta failed", zap.Error(err))
		return false
	}

	mt.indexBuildID2Meta[indexBuildID] = *m
	log.Debug("IndexCoord LoadMetaFromETCD success", zap.Any("IndexMeta", m))

	return true
}

// GetNodeTaskStats get task stats of IndexNode.
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

// GetIndexMetaByIndexBuildID get the index meta of the specified indexBuildID.
func (mt *metaTable) GetIndexMetaByIndexBuildID(indexBuildID UniqueID) *indexpb.IndexMeta {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	log.Debug("IndexCoord MetaTable GetIndexMeta", zap.Int64("IndexBuildID", indexBuildID))
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		log.Error("IndexCoord MetaTable GetIndexMeta not exist", zap.Int64("IndexBuildID", indexBuildID))
		return nil
	}
	return proto.Clone(meta.indexMeta).(*indexpb.IndexMeta)
}
