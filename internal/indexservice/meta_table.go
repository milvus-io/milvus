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

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
)

type metaTable struct {
	client       kv.TxnBase                // client of a reliable kv service, i.e. etcd client
	indexID2Meta map[UniqueID]pb.IndexMeta // index id to index meta

	nodeID2Address map[int64]*commonpb.Address

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

	mt.nodeID2Address = make(map[int64]*commonpb.Address)
	return mt, nil
}

func (mt *metaTable) reloadFromKV() error {
	mt.indexID2Meta = make(map[UniqueID]pb.IndexMeta)

	_, values, err := mt.client.LoadWithPrefix("indexes")
	if err != nil {
		return err
	}

	for _, value := range values {
		indexMeta := pb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMeta)
		if err != nil {
			return err
		}
		mt.indexID2Meta[indexMeta.IndexID] = indexMeta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *pb.IndexMeta) error {
	value := proto.MarshalTextString(meta)

	mt.indexID2Meta[meta.IndexID] = *meta

	return mt.client.Save("/indexes/"+strconv.FormatInt(meta.IndexID, 10), value)
}

func (mt *metaTable) AddIndex(indexID UniqueID, req *pb.BuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	_, ok := mt.indexID2Meta[indexID]
	if ok {
		return errors.Errorf("index already exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta := &pb.IndexMeta{
		State:   commonpb.IndexState_INPROGRESS,
		IndexID: indexID,
		Req:     req,
	}
	return mt.saveIndexMeta(meta)
}

func (mt *metaTable) UpdateIndexStatus(indexID UniqueID, status commonpb.IndexState) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.State = status
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) UpdateIndexEnqueTime(indexID UniqueID, t time.Time) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.EnqueTime = t.UnixNano()
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) UpdateIndexScheduleTime(indexID UniqueID, t time.Time) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.ScheduleTime = t.UnixNano()
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) NotifyBuildIndex(indexID UniqueID, dataPaths []string, state commonpb.IndexState) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}

	meta.State = state
	meta.IndexFilePaths = dataPaths
	return mt.saveIndexMeta(&meta)
}

func (mt *metaTable) GetIndexFilePaths(indexID UniqueID) ([]string, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	return meta.IndexFilePaths, nil
}

func (mt *metaTable) DeleteIndex(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	indexMeta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("can't find index. id = " + strconv.FormatInt(indexID, 10))
	}
	fmt.Print(indexMeta)

	return nil
}
