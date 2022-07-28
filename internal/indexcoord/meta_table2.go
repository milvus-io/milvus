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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	catalog "github.com/milvus-io/milvus/internal/metastore/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

// metaTable maintains index-related information
type metaTable struct {
	catalog          catalog.Catalog
	indexLock        sync.RWMutex
	segmentIndexLock sync.RWMutex

	// collectionIndexes records which indexes are on the collection
	collectionIndexes map[UniqueID]map[UniqueID]*model.Index
	// segmentIndexes records which indexes are on the segment
	segmentIndexes map[UniqueID]map[UniqueID]*model.SegmentIndex
	// buildID2Meta records the meta information of the segment
	buildID2SegmentIndex map[UniqueID]*model.SegmentIndex
}

// NewMetaTable is used to create a new meta table.
func NewMetaTable(kv kv.MetaKv) (*metaTable, error) {
	mt := &metaTable{
		catalog: catalog.Catalog{
			Txn: kv,
		},
		indexLock:        sync.RWMutex{},
		segmentIndexLock: sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return mt, nil
}

// reloadFromKV reloads the index meta from ETCD.
func (mt *metaTable) reloadFromKV() error {
	mt.collectionIndexes = make(map[UniqueID]map[UniqueID]*model.Index)
	mt.segmentIndexes = make(map[UniqueID]map[UniqueID]*model.SegmentIndex)
	mt.buildID2SegmentIndex = make(map[UniqueID]*model.SegmentIndex)

	// load field indexes
	log.Info("IndexCoord metaTable reloadFromKV load indexes")
	fieldIndexes, err := mt.catalog.ListIndexes()
	if err != nil {
		log.Error("IndexCoord metaTable reloadFromKV load field indexes fail", zap.Error(err))
		return err
	}
	segmentIndxes, err := mt.catalog.ListSegmentIndexes()
	if err != nil {
		log.Error("IndexCoord metaTable reloadFromKV load segment indexes fail", zap.Error(err))
		return err
	}

	for indexID, fieldIndex := range fieldIndexes {
		mt.collectionIndexes[fieldIndex.CollectionID][indexID] = fieldIndex
	}
	for buildID, segIdx := range segmentIndxes {
		mt.segmentIndexes[segIdx.SegmentID][segIdx.IndexID] = segIdx
		mt.buildID2SegmentIndex[buildID] = segIdx
	}

	log.Info("IndexCoord metaTable reloadFromKV success")
	return nil
}

func (mt *metaTable) saveFieldIndexMeta(index *model.Index) error {
	err := mt.catalog.CreateIndex(index)
	if err != nil {
		log.Error("failed to save index meta in etcd", zap.Int64("buildID", index.CollectionID),
			zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.Error(err))
		return err
	}

	mt.collectionIndexes[index.CollectionID][index.IndexID] = index
	return nil
}

// saveSegmentIndexMeta saves the index meta to ETCD.
// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveSegmentIndexMeta(segIdx *model.SegmentIndex) error {
	err := mt.catalog.CreateSegmentIndex(segIdx)
	if err != nil {
		log.Error("failed to save index meta in etcd", zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
		return err
	}

	mt.segmentIndexes[segIdx.SegmentID][segIdx.IndexID] = segIdx
	mt.buildID2SegmentIndex[segIdx.BuildID] = segIdx
	log.Info("IndexCoord metaTable saveIndexMeta success", zap.Int64("buildID", segIdx.BuildID))
	return nil
}

func (mt *metaTable) SetIndex(indexID int64, req *indexpb.BuildIndexRequest, createTs uint64) {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	index := &model.Index{
		CollectionID: req.CollectionID,
		FieldID:      req.FieldID,
		IndexID:      indexID,
		IndexName:    req.IndexName,
		TypeParams:   req.TypeParams,
		IndexParams:  req.IndexParams,
		CreateTime:   createTs,
	}
	mt.collectionIndexes[req.CollectionID][indexID] = index
}

func (mt *metaTable) CreateIndex(indexID int64, req *indexpb.BuildIndexRequest, createTs uint64) error {
	mt.indexLock.Lock()
	defer mt.indexLock.RLock()
	log.Info("IndexCoord metaTable CreateIndex", zap.Int64("collectionID", req.CollectionID),
		zap.Int64("fieldID", req.FieldID), zap.Int64("indexID", indexID), zap.String("indexName", req.IndexName))

	index := &model.Index{
		CollectionID: req.CollectionID,
		FieldID:      req.FieldID,
		IndexID:      indexID,
		IndexName:    req.IndexName,
		TypeParams:   req.TypeParams,
		IndexParams:  req.IndexParams,
		CreateTime:   createTs,
	}
	if err := mt.saveFieldIndexMeta(index); err != nil {
		log.Error("IndexCoord metaTable CreateIndex save meta fail", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("fieldID", req.FieldID), zap.Int64("indexID", indexID),
			zap.String("indexName", req.IndexName), zap.Error(err))
		return err
	}
	log.Info("IndexCoord metaTable CreateIndex success", zap.Int64("collectionID", req.CollectionID),
		zap.Int64("fieldID", req.FieldID), zap.Int64("indexID", indexID), zap.String("indexName", req.IndexName))
	return nil
}

// AddIndex adds the index meta corresponding the indexBuildID to meta table.
func (mt *metaTable) AddIndex(segIndex *model.SegmentIndex) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	buildID := segIndex.BuildID
	log.Info("IndexCoord metaTable AddIndex", zap.Int64("buildID", buildID))

	_, ok := mt.buildID2SegmentIndex[buildID]
	if ok {
		log.Info("index already exists", zap.Int64("buildID", buildID), zap.Int64("indexID", segIndex.IndexID))
		return nil
	}
	segIndex.State = commonpb.IndexState_Unissued

	metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Inc()
	if err := mt.saveSegmentIndexMeta(segIndex); err != nil {
		// no need to reload, no reason to compare version fail
		log.Error("IndexCoord metaTable save index meta failed", zap.Int64("buildID", buildID),
			zap.Int64("indexID", segIndex.IndexID), zap.Error(err))
		return err
	}
	log.Info("IndexCoord metaTable AddIndex success", zap.Int64("buildID", buildID))
	return nil
}

func (mt *metaTable) BuildIndexes(segmentInfo *datapb.SegmentInfo) []*model.SegmentIndex {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	segmentIndexes := make([]*model.SegmentIndex, 0)
	if segmentInfo.NumOfRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex {
		return segmentIndexes
	}

	for indexID := range mt.collectionIndexes[segmentInfo.CollectionID] {
		segmentIndexes = append(segmentIndexes, &model.SegmentIndex{
			Segment: model.Segment{
				CollectionID: segmentInfo.CollectionID,
				PartitionID:  segmentInfo.PartitionID,
				SegmentID:    segmentInfo.ID,
			},
			IndexID: indexID,
		})
	}
	return segmentIndexes
}

// HasSameReq determine whether there are same indexing tasks.
func (mt *metaTable) HasSameReq(req *indexpb.BuildIndexRequest) bool {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	for _, fieldIndex := range mt.collectionIndexes[req.CollectionID] {
		if fieldIndex.IsDeleted {
			continue
		}
		if fieldIndex.FieldID != req.FieldID {
			continue
		}
		if fieldIndex.IndexName != req.IndexName {
			continue
		}

		if len(fieldIndex.TypeParams) != len(req.TypeParams) {
			continue
		}
		notEq := false
		for _, param1 := range fieldIndex.TypeParams {
			exist := false
			for _, param2 := range req.TypeParams {
				if param2.Key == param1.Key && param2.Value == param1.Value {
					exist = true
				}
			}
			if !exist {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		if len(fieldIndex.IndexParams) != len(req.IndexParams) {
			continue
		}
		for _, param1 := range fieldIndex.IndexParams {
			exist := false
			for _, param2 := range req.IndexParams {
				if param2.Key == param1.Key && param2.Value == param1.Value {
					exist = true
				}
			}
			if !exist {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		log.Debug("IndexCoord has same index", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("fieldID", req.FieldID), zap.String("indexName", req.IndexName),
			zap.Int64("indexID", fieldIndex.IndexID))
		return true
	}

	return false
}

func (mt *metaTable) CheckBuiltIndex(segIdx *model.SegmentIndex) (bool, UniqueID) {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	if _, ok := mt.segmentIndexes[segIdx.SegmentID]; !ok {
		return false, 0
	}

	if index, ok := mt.segmentIndexes[segIdx.SegmentID][segIdx.IndexID]; ok {
		return true, index.BuildID
	}

	return false, 0
}
