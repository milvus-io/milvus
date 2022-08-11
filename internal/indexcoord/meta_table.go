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

	"github.com/milvus-io/milvus/internal/kv"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/retry"
)

// Meta is used to record the state of the index.
// revision: The number of times IndexMeta has been changed in etcd. It's the same as Event.Kv.Version in etcd.
// indexMeta: A structure that records the state of the index defined by proto.
type Meta struct {
	indexMeta   *indexpb.IndexMeta
	etcdVersion int64
}

// metaTable records the mapping of IndexBuildID to Meta.
type metaTable struct {
	client            kv.MetaKv          // client of a reliable kv service, i.e. etcd client
	indexBuildID2Meta map[UniqueID]*Meta // index build id to index meta

	etcdRevision int64

	lock sync.RWMutex
}

// NewMetaTable is used to create a new meta table.
func NewMetaTable(kv kv.MetaKv) (*metaTable, error) {
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
	mt.indexBuildID2Meta = make(map[UniqueID]*Meta)
	key := indexFilePrefix
	log.Debug("IndexCoord metaTable LoadWithPrefix ", zap.String("prefix", key))

	_, values, versions, revision, err := mt.client.LoadWithRevisionAndVersions(key)
	if err != nil {
		return err
	}

	mt.etcdRevision = revision

	for i := 0; i < len(values); i++ {
		indexMeta := indexpb.IndexMeta{}
		err = proto.Unmarshal([]byte(values[i]), &indexMeta)
		if err != nil {
			return fmt.Errorf("IndexCoord metaTable reloadFromKV UnmarshalText indexpb.IndexMeta err:%w", err)
		}

		meta := &Meta{
			indexMeta:   &indexMeta,
			etcdVersion: versions[i],
		}
		mt.indexBuildID2Meta[indexMeta.IndexBuildID] = meta
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
	success, err := mt.client.CompareVersionAndSwap(key, meta.etcdVersion, string(value))
	if err != nil {
		log.Warn("failed to save index meta in etcd", zap.Int64("buildID", meta.indexMeta.IndexBuildID), zap.Error(err))
		return err
	}
	if !success {
		log.Warn("failed to save index meta in etcd because version compare failure", zap.Int64("buildID", meta.indexMeta.IndexBuildID), zap.Any("index", meta.indexMeta))
		return ErrCompareVersion
	}
	meta.etcdVersion = meta.etcdVersion + 1
	mt.indexBuildID2Meta[meta.indexMeta.IndexBuildID] = meta
	log.Info("IndexCoord metaTable saveIndexMeta success", zap.Int64("buildID", meta.indexMeta.IndexBuildID), zap.Int64("meta.revision", meta.etcdVersion))
	return nil
}

// reloadMeta reloads the index meta corresponding indexBuildID from ETCD.
func (mt *metaTable) reloadMeta(indexBuildID UniqueID) (*Meta, error) {
	key := path.Join(indexFilePrefix, strconv.FormatInt(indexBuildID, 10))
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
	m := &Meta{
		etcdVersion: version[0],
		indexMeta:   im,
	}

	log.Debug("reload meta from etcd success", zap.Int64("buildID", indexBuildID), zap.Any("indexMeta", im))
	return m, nil
}

func (mt *metaTable) GetAllIndexMeta() map[int64]*indexpb.IndexMeta {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	metas := map[int64]*indexpb.IndexMeta{}
	for build, meta := range mt.indexBuildID2Meta {
		metas[build] = proto.Clone(meta.indexMeta).(*indexpb.IndexMeta)
	}

	return metas
}

// AddIndex adds the index meta corresponding the indexBuildID to meta table.
func (mt *metaTable) AddIndex(indexBuildID UniqueID, req *indexpb.BuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	_, ok := mt.indexBuildID2Meta[indexBuildID]
	log.Debug("IndexCoord metaTable AddIndex", zap.Int64("indexBuildID", indexBuildID), zap.Bool(" index already exist", ok))
	if ok {
		log.Info("index already exists", zap.Int64("buildID", indexBuildID), zap.Int64("indexID", req.IndexID))
		return nil
	}
	meta := &Meta{
		indexMeta: &indexpb.IndexMeta{
			State:        commonpb.IndexState_Unissued,
			IndexBuildID: indexBuildID,
			Req:          req,
			NodeID:       0,
			IndexVersion: 0,
		},
		etcdVersion: 0,
	}
	metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Inc()
	if err := mt.saveIndexMeta(meta); err != nil {
		// no need to reload, no reason to compare version fail
		log.Error("IndexCoord metaTable save index meta failed", zap.Int64("buildID", indexBuildID),
			zap.Int64("indexID", req.IndexID), zap.Error(err))
		return err
	}
	log.Info("IndexCoord metaTable AddIndex success", zap.Int64("buildID", indexBuildID))
	return nil
}

func (mt *metaTable) updateMeta(buildID UniqueID, updateFunc func(m *Meta) error) error {
	meta, ok := mt.indexBuildID2Meta[buildID]
	if !ok {
		log.Error("IndexCoord metaTable updateMeta index not exists", zap.Any("indexBuildID", buildID))
		return fmt.Errorf("index not exists with ID = %d", buildID)
	}
	clonedMeta := &Meta{
		indexMeta:   proto.Clone(meta.indexMeta).(*indexpb.IndexMeta),
		etcdVersion: meta.etcdVersion,
	}
	if err := updateFunc(clonedMeta); err != nil {
		if !errors.Is(err, ErrCompareVersion) {
			log.Error("IndexCoord metaTable updateMeta fail", zap.Int64("buildID", buildID), zap.Error(err))
			return err
		}
		fn := func() error {
			m, err := mt.reloadMeta(meta.indexMeta.IndexBuildID)
			if err != nil {
				return err
			}
			return updateFunc(m)
		}
		err2 := retry.Do(context.TODO(), fn, retry.Attempts(5))
		if err2 != nil {
			return err2
		}
	}
	return nil
}

func (mt *metaTable) ResetNodeID(buildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Info("IndexCoord metaTable ResetNodeID", zap.Int64("buildID", buildID))
	updateFunc := func(m *Meta) error {
		m.indexMeta.NodeID = 0
		return mt.saveIndexMeta(m)
	}

	if err := mt.updateMeta(buildID, updateFunc); err != nil {
		log.Error("IndexCoord metaTable ResetNodeID fail", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}
	log.Info("reset index meta nodeID success", zap.Int64("buildID", buildID))
	return nil
}

func (mt *metaTable) ResetMeta(buildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Info("IndexCoord metaTable ResetMeta", zap.Int64("buildID", buildID))
	updateFunc := func(m *Meta) error {
		m.indexMeta.NodeID = 0
		m.indexMeta.State = commonpb.IndexState_Unissued
		return mt.saveIndexMeta(m)
	}

	if err := mt.updateMeta(buildID, updateFunc); err != nil {
		log.Error("IndexCoord metaTable ResetMeta fail", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}
	log.Info("reset index meta success", zap.Int64("buildID", buildID))
	return nil
}

func (mt *metaTable) GetMeta(buildID UniqueID) (*Meta, bool) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	meta, ok := mt.indexBuildID2Meta[buildID]
	if ok {
		return &Meta{indexMeta: proto.Clone(meta.indexMeta).(*indexpb.IndexMeta), etcdVersion: meta.etcdVersion}, ok
	}

	return nil, ok
}

func (mt *metaTable) canIndex(buildID int64) bool {
	meta := mt.indexBuildID2Meta[buildID]
	if meta.indexMeta.MarkDeleted {
		log.Debug("Index has been deleted", zap.Int64("buildID", buildID))
		return false
	}

	if meta.indexMeta.NodeID != 0 {
		log.Debug("IndexCoord metaTable BuildIndex, but indexMeta's NodeID is not zero",
			zap.Int64("buildID", buildID), zap.Int64("nodeID", meta.indexMeta.NodeID))
		return false
	}
	if meta.indexMeta.State != commonpb.IndexState_Unissued {
		log.Debug("IndexCoord metaTable BuildIndex, but indexMeta's state is not unissued",
			zap.Int64("buildID", buildID), zap.String("state", meta.indexMeta.State.String()))
		return false
	}
	return true
}

// UpdateVersion updates the version and nodeID of the index meta, whenever the task is built once, the version will be updated once.
func (mt *metaTable) UpdateVersion(indexBuildID UniqueID, nodeID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Info("IndexCoord metaTable UpdateVersion", zap.Int64("IndexBuildId", indexBuildID))
	updateFunc := func(m *Meta) error {
		if !mt.canIndex(indexBuildID) {
			return fmt.Errorf("it's no necessary to build index with ID = %d", indexBuildID)
		}
		m.indexMeta.NodeID = nodeID
		m.indexMeta.IndexVersion++
		return mt.saveIndexMeta(m)
	}
	if err := mt.updateMeta(indexBuildID, updateFunc); err != nil {
		return err
	}
	log.Info("IndexCoord metaTable UpdateVersion success", zap.Int64("IndexBuildId", indexBuildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (mt *metaTable) BuildIndex(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexCoord metaTable BuildIndex")

	updateFunc := func(m *Meta) error {
		if m.indexMeta.MarkDeleted {
			log.Warn("index has been marked deleted, no need to build index", zap.Int64("indexBuildID", indexBuildID))
			return nil
		}
		if m.indexMeta.State == commonpb.IndexState_Finished || m.indexMeta.State == commonpb.IndexState_Failed {
			log.Warn("index has been finished, no need to set InProgress state", zap.Int64("indexBuildID", indexBuildID),
				zap.String("state", m.indexMeta.State.String()))
			return nil
		}
		m.indexMeta.State = commonpb.IndexState_InProgress

		err := mt.saveIndexMeta(m)
		if err != nil {
			log.Error("IndexCoord metaTable BuildIndex fail", zap.Int64("buildID", indexBuildID), zap.Error(err))
			return err
		}
		metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Dec()
		metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.InProgressIndexTaskLabel).Inc()
		log.Info("IndexCoord metaTable BuildIndex success", zap.Int64("buildID", indexBuildID),
			zap.String("state", m.indexMeta.State.String()))
		return nil
	}
	return mt.updateMeta(indexBuildID, updateFunc)
}

func (mt *metaTable) GetMetasByNodeID(nodeID UniqueID) []Meta {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	metas := make([]Meta, 0)
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.MarkDeleted {
			continue
		}
		if nodeID == meta.indexMeta.NodeID {
			metas = append(metas, Meta{indexMeta: proto.Clone(meta.indexMeta).(*indexpb.IndexMeta), etcdVersion: meta.etcdVersion})
		}
	}
	return metas
}

// MarkIndexAsDeleted will mark the corresponding index as deleted, and recycleUnusedIndexFiles will recycle these tasks.
func (mt *metaTable) MarkIndexAsDeleted(indexID UniqueID) ([]UniqueID, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Info("IndexCoord metaTable MarkIndexAsDeleted ", zap.Int64("indexID", indexID))
	deletedBuildIDs := make([]UniqueID, 0)

	updateFunc := func(m *Meta) error {
		m.indexMeta.MarkDeleted = true
		log.Debug("IndexCoord metaTable MarkIndexAsDeleted ", zap.Int64("indexID", indexID),
			zap.Int64("buildID", m.indexMeta.IndexBuildID))
		return mt.saveIndexMeta(m)
	}

	for buildID, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.Req.IndexID == indexID {
			deletedBuildIDs = append(deletedBuildIDs, buildID)
			if meta.indexMeta.MarkDeleted {
				continue
			}
			if err := mt.updateMeta(buildID, updateFunc); err != nil {
				log.Error("IndexCoord metaTable mark index as deleted failed", zap.Int64("buildID", buildID))
				return nil, err
			}
		}
	}
	log.Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("indexID", indexID))
	return deletedBuildIDs, nil
}

func (mt *metaTable) MarkIndexAsDeletedByBuildIDs(buildIDs []UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("IndexCoord metaTable MarkIndexAsDeletedByBuildIDs", zap.Int64s("buildIDs", buildIDs))
	updateFunc := func(m *Meta) error {
		if m.indexMeta.MarkDeleted {
			return nil
		}
		m.indexMeta.MarkDeleted = true
		log.Debug("IndexCoord metaTable MarkIndexAsDeletedByBuildIDs ",
			zap.Int64("buildID", m.indexMeta.IndexBuildID))
		return mt.saveIndexMeta(m)
	}

	for _, buildID := range buildIDs {
		if _, ok := mt.indexBuildID2Meta[buildID]; !ok {
			continue
		}
		if err := mt.updateMeta(buildID, updateFunc); err != nil {
			log.Error("IndexCoord metaTable MarkIndexAsDeletedByBuildIDs fail", zap.Int64("buildID", buildID),
				zap.Error(err))
			return err
		}
	}

	log.Info("IndexCoord metaTable MarkIndexAsDeletedByBuildIDs success", zap.Int64s("buildIDs", buildIDs))
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
	if meta.indexMeta.State != commonpb.IndexState_Finished && meta.indexMeta.State != commonpb.IndexState_Failed {
		return nil, fmt.Errorf("index not finished with ID = %d", indexBuildID)
	}
	ret.IndexFilePaths = meta.indexMeta.IndexFilePaths
	ret.SerializedSize = meta.indexMeta.GetSerializeSize()

	log.Debug("IndexCoord get index file path successfully", zap.Int64("indexBuildID", indexBuildID),
		zap.Int("index files num", len(ret.IndexFilePaths)))
	return ret, nil
}

// DeleteIndex delete the index meta from meta table.
func (mt *metaTable) DeleteIndex(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	key := path.Join(indexFilePrefix, strconv.FormatInt(indexBuildID, 10))
	if err := mt.client.Remove(key); err != nil {
		log.Error("IndexCoord delete index meta from etcd failed", zap.Error(err))
		return err
	}
	delete(mt.indexBuildID2Meta, indexBuildID)
	log.Debug("IndexCoord delete index meta successfully", zap.Int64("indexBuildID", indexBuildID))
	return nil
}

func (mt *metaTable) GetBuildID2IndexFiles() map[UniqueID][]string {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	buildID2IndexFiles := make(map[UniqueID][]string)

	for buildID, meta := range mt.indexBuildID2Meta {
		buildID2IndexFiles[buildID] = append(buildID2IndexFiles[buildID], meta.indexMeta.IndexFilePaths...)
	}

	return buildID2IndexFiles
}

func (mt *metaTable) GetDeletedMetas() []*indexpb.IndexMeta {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	var metas []*indexpb.IndexMeta
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.MarkDeleted {
			metas = append(metas, proto.Clone(meta.indexMeta).(*indexpb.IndexMeta))
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
		if meta.indexMeta.MarkDeleted {
			continue
		}
		return true, meta.indexMeta.IndexBuildID
	}

	return false, 0
}

// NeedUpdateMeta update the meta of specified indexBuildID.
// If the version of meta in memory is greater equal to the version in put event, no need to update.
func (mt *metaTable) NeedUpdateMeta(m *Meta) bool {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexBuildID2Meta[m.indexMeta.IndexBuildID]
	if !ok {
		log.Warn("index is not exist, Might have been cleaned up meta", zap.Int64("buildID", m.indexMeta.IndexBuildID))
		return false
	}
	log.Info("IndexCoord metaTable NeedUpdateMeta", zap.Int64("indexBuildID", m.indexMeta.IndexBuildID),
		zap.Int64("meta.revision", meta.etcdVersion), zap.Int64("update revision", m.etcdVersion))

	if meta.etcdVersion < m.etcdVersion {
		mt.indexBuildID2Meta[m.indexMeta.IndexBuildID] = m
		return true
	}
	return false
}

func (mt *metaTable) HasBuildID(buildID UniqueID) bool {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	_, ok := mt.indexBuildID2Meta[buildID]
	return ok
}
