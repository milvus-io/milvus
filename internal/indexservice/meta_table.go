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
	"log"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
)

type metaTable struct {
	client            kv.TxnBase                     // client of a reliable kv service, i.e. etcd client
	indexBuildID2Meta map[UniqueID]indexpb.IndexMeta // index build id to index meta

	lock sync.RWMutex
}

func NewMetaTable(kv kv.TxnBase) (*metaTable, error) {
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
			return err
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
	_, ok := mt.indexBuildID2Meta[indexBuildID]
	if ok {
		return errors.Errorf("index already exists with ID = " + strconv.FormatInt(indexBuildID, 10))
	}
	meta := &indexpb.IndexMeta{
		State:        commonpb.IndexState_UNISSUED,
		IndexBuildID: indexBuildID,
		Req:          req,
	}
	return mt.saveIndexMeta(meta)
}

func (mt *metaTable) MarkIndexAsDeleted(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	exist := false
	for indexBuildID, meta := range mt.indexBuildID2Meta {
		if meta.Req.IndexID == indexID {
			meta.State = commonpb.IndexState_DELETED
			mt.indexBuildID2Meta[indexBuildID] = meta
			exist = true
		}
	}

	if !exist {
		return errors.New("index not exists")
	}

	return nil
}

func (mt *metaTable) NotifyBuildIndex(nty *indexpb.BuildIndexNotification) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	indexBuildID := nty.IndexBuildID
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexBuildID, 10))
	}
	if meta.State == commonpb.IndexState_DELETED {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexBuildID, 10))
	}

	if nty.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		meta.State = commonpb.IndexState_FAILED
		meta.FailReason = nty.Status.Reason
	} else {
		meta.State = commonpb.IndexState_FINISHED
		meta.IndexFilePaths = nty.IndexFilePaths
	}

	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) GetIndexState(indexBuildID UniqueID) (*indexpb.IndexInfo, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	ret := &indexpb.IndexInfo{
		IndexBuildID: indexBuildID,
		State:        commonpb.IndexState_NONE,
	}
	meta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return ret, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexBuildID, 10))
	}
	if meta.State == commonpb.IndexState_DELETED {
		return ret, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexBuildID, 10))
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
		return nil, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexBuildID, 10))
	}
	if meta.State == commonpb.IndexState_DELETED {
		return nil, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexBuildID, 10))
	}
	ret.IndexFilePaths = meta.IndexFilePaths
	return ret, nil
}

func (mt *metaTable) removeIndexFile(indexID UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	for _, meta := range mt.indexBuildID2Meta {
		if meta.Req.IndexID == indexID {
			err := mt.client.MultiRemove(meta.IndexFilePaths)
			if err != nil {
				log.Println("remove index file err: ", err)
			}
		}
	}
}

func (mt *metaTable) removeMeta(indexID UniqueID) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	indexBuildIDToRemove := make([]UniqueID, 0)
	for indexBuildID, meta := range mt.indexBuildID2Meta {
		if meta.Req.IndexID == indexID {
			indexBuildIDToRemove = append(indexBuildIDToRemove, indexBuildID)
		}
	}

	for _, indexBuildID := range indexBuildIDToRemove {
		delete(mt.indexBuildID2Meta, indexBuildID)
	}
}

func (mt *metaTable) DeleteIndex(indexBuildID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	indexMeta, ok := mt.indexBuildID2Meta[indexBuildID]
	if !ok {
		return errors.Errorf("can't find index. id = " + strconv.FormatInt(indexBuildID, 10))
	}
	fmt.Print(indexMeta)

	return nil
}
