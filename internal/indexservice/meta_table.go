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

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type metaTable struct {
	client            kv.TxnKV                       // client of a reliable kv service, i.e. etcd client
	indexBuildID2Meta map[UniqueID]indexpb.IndexMeta // index build id to index meta

	lock sync.RWMutex
}

func NewMetaTable(kv kv.TxnKV) (*metaTable, error) {
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
	mt.indexBuildID2Meta = make(map[UniqueID]indexpb.IndexMeta)

	_, values, err := mt.client.LoadWithPrefix("indexes")
	if err != nil {
		return err
	}

	for _, value := range values {
		indexMeta := indexpb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMeta)
		if err != nil {
			return fmt.Errorf("IndexService metaTable reloadFromKV UnmarshalText indexpb.IndexMeta err:%w", err)
		}
		mt.indexBuildID2Meta[indexMeta.IndexBuildID] = indexMeta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *indexpb.IndexMeta) error {
	value := proto.MarshalTextString(meta)

	mt.indexBuildID2Meta[meta.IndexBuildID] = *meta

	return mt.client.Save("/indexes/"+strconv.FormatInt(meta.IndexBuildID, 10), value)
}

func (mt *metaTable) AddIndex(indexBuildID UniqueID, req *indexpb.BuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("indexservice add index ...")
	_, ok := mt.indexBuildID2Meta[indexBuildID]
	if ok {
		return fmt.Errorf("index already exists with ID = %d", indexBuildID)
	}
	meta := &indexpb.IndexMeta{
		State:        commonpb.IndexState_Unissued,
		IndexBuildID: indexBuildID,
		Req:          req,
		LeaseKey:     "",
		Version:      0,
	}
	return mt.saveIndexMeta(meta)
}

func (mt *metaTable) BuildIndex(indexBuildID UniqueID, leaseKey string) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexService update index state")

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	if meta.State != commonpb.IndexState_Unissued {
		return fmt.Errorf("can not update index state, index with ID = %d state is %d", indexBuildID, meta.State)
	}
	meta.State = commonpb.IndexState_InProgress
	meta.LeaseKey = leaseKey
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) UpdateVersion(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Debug("IndexService update index state")

	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	meta.Version = meta.Version + 1
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) MarkIndexAsDeleted(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("indexservice", zap.Int64("mark index is deleted", indexID))

	for indexBuildID, meta := range mt.indexBuildID2Meta {
		if meta.Req.IndexID == indexID {
			meta.MarkDeleted = true
			mt.indexBuildID2Meta[indexBuildID] = meta
		}
	}

	return nil
}

func (mt *metaTable) NotifyBuildIndex(nty *indexpb.NotifyBuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("indexservice", zap.Int64("notify build index", nty.IndexBuildID))
	indexBuildID := nty.IndexBuildID
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}

	if nty.Status.ErrorCode != commonpb.ErrorCode_Success {
		meta.State = commonpb.IndexState_Failed
		meta.FailReason = nty.Status.Reason
	} else {
		meta.State = commonpb.IndexState_Finished
		meta.IndexFilePaths = nty.IndexFilePaths
	}

	return mt.saveIndexMeta(&meta)
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
	if meta.MarkDeleted {
		return ret, fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}
	ret.IndexID = meta.Req.IndexID
	ret.IndexName = meta.Req.IndexName
	ret.Reason = meta.FailReason
	ret.State = meta.State
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
	if meta.MarkDeleted {
		return nil, fmt.Errorf("index not exists with ID = %d", indexBuildID)
	}
	ret.IndexFilePaths = meta.IndexFilePaths
	return ret, nil
}

func (mt *metaTable) DeleteIndex(indexBuildID UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	delete(mt.indexBuildID2Meta, indexBuildID)
}

func (mt *metaTable) getMarkDeleted(limit int) []indexpb.IndexMeta {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("IndexService get mark deleted meta")

	var indexMetas []indexpb.IndexMeta
	for _, meta := range mt.indexBuildID2Meta {
		if meta.MarkDeleted && meta.State == commonpb.IndexState_Finished {
			indexMetas = append(indexMetas, meta)
		}
		if len(indexMetas) >= limit {
			return indexMetas
		}
	}

	return indexMetas
}

func (mt *metaTable) getToAssignedTasks() []indexpb.IndexMeta {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("IndexService get unissued tasks")

	var tasks []indexpb.IndexMeta
	for _, meta := range mt.indexBuildID2Meta {
		// TODO: lease Key not in IndexService watched lease keys
		if meta.LeaseKey == "" {
			tasks = append(tasks, meta)
		}
	}

	return tasks
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
LOOP:
	for _, meta := range mt.indexBuildID2Meta {
		if meta.Req.IndexID == req.IndexID {
			if len(meta.Req.DataPaths) != len(req.DataPaths) {
				goto LOOP
			}
			if len(meta.Req.IndexParams) == len(req.IndexParams) && compare2Array(meta.Req.DataPaths, req.DataPaths) {
				if !compare2Array(meta.Req.IndexParams, req.IndexParams) || !compare2Array(meta.Req.TypeParams, req.TypeParams) {
					goto LOOP
				}
				return true, meta.IndexBuildID
			}
		}
	}
	return false, -1
}

type nodeTasks struct {
	nodeID2Tasks map[string][]indexpb.IndexMeta
}

func (nt *nodeTasks) getTasksByLeaseKey(leaseKey string) ([]UniqueID, error) {
	var taskIndexBuildIDs []UniqueID
	tasks, ok := nt.nodeID2Tasks[leaseKey]
	if !ok {
		return nil, fmt.Errorf("lease key not exists with %s", leaseKey)
	}
	for _, task := range tasks {
		taskIndexBuildIDs = append(taskIndexBuildIDs, task.IndexBuildID)
	}

	return taskIndexBuildIDs, nil
}
