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
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

const (
	RequestTimeout = 10 * time.Second
)

type Meta struct {
	indexMeta *indexpb.IndexMeta
	reversion int64
}

type metaTable struct {
	client            *clientv3.Client // client of a reliable kv service, i.e. etcd client
	rootPath          string
	indexBuildID2Meta map[UniqueID]Meta // index build id to index meta

	lock sync.RWMutex
}

func NewMetaTable(kv *clientv3.Client, rootPath string) (*metaTable, error) {
	mt := &metaTable{
		client:   kv,
		rootPath: rootPath,
		lock:     sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return mt, nil
}

func (mt *metaTable) reloadFromKV() error {
	mt.indexBuildID2Meta = make(map[UniqueID]Meta)
	key := path.Join(mt.rootPath, "indexes")
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := mt.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return err
	}
	values := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		values = append(values, string(kv.Value))
	}

	for _, value := range values {
		indexMeta := indexpb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMeta)
		if err != nil {
			return fmt.Errorf("IndexService metaTable reloadFromKV UnmarshalText indexpb.IndexMeta err:%w", err)
		}
		meta := &Meta{
			indexMeta: &indexMeta,
			reversion: resp.Header.Revision,
		}
		mt.indexBuildID2Meta[indexMeta.IndexBuildID] = *meta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *Meta) error {
	value := proto.MarshalTextString(meta.indexMeta)

	key := path.Join(mt.rootPath, "/indexes/"+strconv.FormatInt(meta.indexMeta.IndexBuildID, 10))
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := mt.client.Put(ctx, key, value)
	if err != nil {
		return err
	}
	meta.reversion = resp.Header.Revision
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
			LeaseKey:     "",
			Version:      0,
		},
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

	if meta.indexMeta.State != commonpb.IndexState_Unissued {
		return fmt.Errorf("can not set lease key, index with ID = %d state is %d", indexBuildID, meta.indexMeta.State)
	}
	meta.indexMeta.LeaseKey = leaseKey
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

	log.Debug("IndexService get mark deleted meta")

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

func (mt *metaTable) getToAssignedTasks() []Meta {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Debug("IndexService get unissued tasks")

	var tasks []Meta
	for _, meta := range mt.indexBuildID2Meta {
		// TODO: lease Key not in IndexService watched lease keys
		if meta.indexMeta.LeaseKey == "" {
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

type nodeTasks struct {
	nodeID2Tasks map[string][]UniqueID
}

func (nt *nodeTasks) getTasksByLeaseKey(leaseKey string) []UniqueID {
	indexBuildIDs, ok := nt.nodeID2Tasks[leaseKey]
	if !ok {
		return nil
	}
	return indexBuildIDs
}
