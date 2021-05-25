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

package indexservice

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

const (
	RequestTimeout = 10 * time.Second
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
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))

	resp, err := mt.client.LoadWithPrefix2(key)
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		indexMeta := indexpb.IndexMeta{}
		err = proto.UnmarshalText(string(kv.Value), &indexMeta)
		if err != nil {
			return fmt.Errorf("IndexService metaTable reloadFromKV UnmarshalText indexpb.IndexMeta err:%w", err)
		}
		meta := &Meta{
			indexMeta: &indexMeta,
			revision:  kv.Version,
		}
		mt.indexBuildID2Meta[indexMeta.IndexBuildID] = *meta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *Meta) error {
	value := proto.MarshalTextString(meta.indexMeta)

	key := "indexes/" + strconv.FormatInt(meta.indexMeta.IndexBuildID, 10)
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))

	_, err := mt.client.Put(key, value)
	if err != nil {
		return err
	}

	resp, err := mt.client.LoadWithPrefix2(key)
	if err != nil {
		return err
	}
	meta.revision = resp.Kvs[0].Version
	mt.indexBuildID2Meta[meta.indexMeta.IndexBuildID] = *meta

	return nil
}

func (mt *metaTable) AddIndex(indexBuildID UniqueID, req *indexpb.BuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("indexservice add index ...")
	_, ok := mt.indexBuildID2Meta[indexBuildID]
	if ok {
		return fmt.Errorf("index already exists with ID = %d", indexBuildID)
	}
	meta := &Meta{
		indexMeta: &indexpb.IndexMeta{
			State:        commonpb.IndexState_Unissued,
			IndexBuildID: indexBuildID,
			Req:          req,
			NodeServerID: 0,
			Version:      0,
		},
	}
	return mt.saveIndexMeta(meta)
}

func (mt *metaTable) BuildIndex(indexBuildID UniqueID, serverID int64) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexService update index state")

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	if meta.indexMeta.State != commonpb.IndexState_Unissued {
		return fmt.Errorf("can not set lease key, index with ID = %d state is %d", indexBuildID, meta.indexMeta.State)
	}
	meta.indexMeta.NodeServerID = serverID
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) UpdateVersion(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexService update index version")

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	meta.indexMeta.Version = meta.indexMeta.Version + 1
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) MarkIndexAsDeleted(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("indexservice", zap.Int64("mark index is deleted", indexID))

	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.Req.IndexID == indexID {
			meta.indexMeta.MarkDeleted = true
			if err := mt.saveIndexMeta(&meta); err != nil {
				log.Debug("IndexService", zap.Any("Meta table mark deleted err", err.Error()))
			}
		}
	}

	return nil
}

func (mt *metaTable) GetIndexState(indexBuildID UniqueID) (*indexpb.IndexInfo, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	ret := &indexpb.IndexInfo{
		IndexBuildID: indexBuildID,
	}
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return ret, fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}
	if meta.indexMeta.MarkDeleted {
		return ret, fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}
	ret.IndexID = meta.indexMeta.Req.IndexID
	ret.IndexName = meta.indexMeta.Req.IndexName
	ret.Reason = meta.indexMeta.FailReason
	ret.State = meta.indexMeta.State
	return ret, nil
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
	if err != nil {
		log.Debug("IndexService", zap.Any("Delete IndexMeta in etcd error", err))
	}
}

func (mt *metaTable) updateRecycleState(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	meta.indexMeta.Recycled = true
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) getUnusedIndexFiles(limit int) []Meta {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	var metas []Meta
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.State == commonpb.IndexState_Finished && !meta.indexMeta.Recycled {
			metas = append(metas, meta)
		}
		if len(metas) >= limit {
			return metas
		}
	}

	return metas
}

func (mt *metaTable) getIndexMeta(indexBuildID UniqueID) Meta {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		log.Debug("IndexService", zap.Any("Meta table does not have the meta with indexBuildID", indexBuildID))
	}

	return meta
}

func compare2Array(arr1, arr2 interface{}) bool {
	p1, ok := arr1.([]*commonpb.KeyValuePair)
	if ok {
		p2, ok1 := arr2.([]*commonpb.KeyValuePair)
		if ok1 {
			for _, param1 := range p1 {
				sameParams := false
				for _, param2 := range p2 {
					if param1.Key == param2.Key && param1.Value == param2.Value {
						sameParams = true
					}
				}
				if !sameParams {
					return false
				}
			}
			return true
		}
		log.Error("indexservice", zap.Any("type error", "arr2 type should be commonpb.KeyValuePair"))
		return false
	}
	v1, ok2 := arr1.([]string)
	if ok2 {
		v2, ok3 := arr2.([]string)
		if ok3 {
			for _, s1 := range v1 {
				sameParams := false
				for _, s2 := range v2 {
					if s1 == s2 {
						sameParams = true
					}
				}
				if !sameParams {
					return false
				}
			}
			return true
		}
		log.Error("indexservice", zap.Any("type error", "arr2 type should be string array"))
		return false
	}
	log.Error("indexservice", zap.Any("type error", "param type should be commonpb.KeyValuePair or string array"))
	return false
}

func (mt *metaTable) hasSameReq(req *indexpb.BuildIndexRequest) (bool, UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

LOOP:
	for _, meta := range mt.indexBuildID2Meta {
		if meta.indexMeta.Req.IndexID == req.IndexID {
			if len(meta.indexMeta.Req.DataPaths) != len(req.DataPaths) {
				goto LOOP
			}
			if len(meta.indexMeta.Req.IndexParams) == len(req.IndexParams) &&
				compare2Array(meta.indexMeta.Req.DataPaths, req.DataPaths) {
				if !compare2Array(meta.indexMeta.Req.IndexParams, req.IndexParams) ||
					!compare2Array(meta.indexMeta.Req.TypeParams, req.TypeParams) {
					goto LOOP
				}
				return true, meta.indexMeta.IndexBuildID
			}
		}
	}
	return false, -1
}

func (mt *metaTable) loadMetaFromETCD(indexBuildID int64, revision int64) bool {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if ok {
		if meta.revision >= revision {
			return false
		}
	}

	key := "indexes/" + strconv.FormatInt(indexBuildID, 10)
	resp, err := mt.client.LoadWithPrefix2(key)
	if err != nil {
		log.Debug("IndexService", zap.Any("Load meta from etcd error", err))
		return false
	}
	value := string(resp.Kvs[0].Value)
	indexMeta := &indexpb.IndexMeta{}
	err = proto.UnmarshalText(value, indexMeta)
	if err != nil {
		log.Debug("IndexService", zap.Any("Unmarshal error", err))
		return false
	}

	log.Debug("IndexService", zap.Any("IndexMeta", indexMeta))
	mt.indexBuildID2Meta[indexBuildID] = Meta{
		indexMeta: indexMeta,
		revision:  resp.Kvs[0].Version,
	}

	return true
}

type nodeTasks struct {
	nodeID2Tasks map[int64][]UniqueID
}

func NewNodeTasks() *nodeTasks {
	return &nodeTasks{
		nodeID2Tasks: map[int64][]UniqueID{},
	}
}

func (nt *nodeTasks) getTasksByLeaseKey(serverID int64) []UniqueID {
	indexBuildIDs, ok := nt.nodeID2Tasks[serverID]
	if !ok {
		return nil
	}
	return indexBuildIDs
}

func (nt *nodeTasks) assignTask(serverID int64, indexBuildID UniqueID) {
	indexBuildIDs, ok := nt.nodeID2Tasks[serverID]
	if !ok {
		var IDs []UniqueID
		IDs = append(IDs, indexBuildID)
		nt.nodeID2Tasks[serverID] = IDs
		return
	}
	indexBuildIDs = append(indexBuildIDs, indexBuildID)
	nt.nodeID2Tasks[serverID] = indexBuildIDs
}

func (nt *nodeTasks) finishTask(indexBuildID UniqueID) {
	for serverID := range nt.nodeID2Tasks {
		for i, buildID := range nt.nodeID2Tasks[serverID] {
			if buildID == indexBuildID {
				nt.nodeID2Tasks[serverID] = append(nt.nodeID2Tasks[serverID][:i], nt.nodeID2Tasks[serverID][:i+1]...)
			}
		}
	}
}

func (nt *nodeTasks) delete(serverID int64) {
	delete(nt.nodeID2Tasks, serverID)
}
