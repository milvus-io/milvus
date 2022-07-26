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
	"path"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"go.uber.org/zap"
)

// metaTable maintains index-related information
type metaTable struct {
	kvClient         kv.MetaKv
	indexLock        sync.RWMutex
	segmentIndexLock sync.RWMutex

	// collectionIndexes records which indexes are on the collection
	collectionIndexes map[UniqueID]map[UniqueID]*indexpb.FieldIndex
	// segmentIndexes records which indexes are on the segment
	segmentIndexes map[UniqueID]map[UniqueID]*indexpb.SegmentIndex
	// buildID2Meta records the meta information of the segment
	buildID2SegmentIndex map[UniqueID]*indexpb.SegmentIndex
}

// NewMetaTable is used to create a new meta table.
func NewMetaTable(kv kv.MetaKv) (*metaTable, error) {
	mt := &metaTable{
		kvClient:         kv,
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
	mt.collectionIndexes = make(map[UniqueID]map[UniqueID]*indexpb.FieldIndex)
	mt.segmentIndexes = make(map[UniqueID]map[UniqueID]*indexpb.SegmentIndex)
	mt.buildID2SegmentIndex = make(map[UniqueID]*indexpb.SegmentIndex)

	// load field indexes
	log.Info("IndexCoord metaTable reloadFromKV load field indexes", zap.String("prefix", fieldIndexPrefix))
	_, values, err := mt.kvClient.LoadWithPrefix(fieldIndexPrefix)
	if err != nil {
		log.Error("IndexCoord metaTable reloadFromKV load field indexes fail", zap.String("prefix", fieldIndexPrefix), zap.Error(err))
		return err
	}

	for _, value := range values {
		fieldIndex := &indexpb.FieldIndex{}
		err = proto.Unmarshal([]byte(value), fieldIndex)
		if err != nil {
			log.Error("IndexCoord metaTable reloadFromKV unmarshal field index fail", zap.Error(err))
			return err
		}
		mt.collectionIndexes[fieldIndex.CollectionID][fieldIndex.IndexID] = fieldIndex
	}

	// load segment indexes
	log.Info("IndexCoord metaTable reloadFromKV load segment indexes", zap.String("prefix", segmentIndexPrefix))
	_, values, err = mt.kvClient.LoadWithPrefix(segmentIndexPrefix)
	if err != nil {
		log.Error("IndexCoord metaTable reloadFromKV load segment indexes fail", zap.String("prefix", fieldIndexPrefix), zap.Error(err))
		return err
	}

	for _, value := range values {
		segmentIndex := &indexpb.SegmentIndex{}
		err = proto.Unmarshal([]byte(value), segmentIndex)
		if err != nil {
			log.Error("IndexCoord metaTable reloadFromKV unmarshal segment index fail", zap.Error(err))
			return err
		}
		mt.segmentIndexes[segmentIndex.SegmentID][segmentIndex.IndexID] = segmentIndex
		mt.buildID2SegmentIndex[segmentIndex.BuildID] = segmentIndex
	}
	log.Info("IndexCoord metaTable reloadFromKV success")
	return nil
}

func (mt *metaTable) saveFieldIndexMeta(index *indexpb.FieldIndex) error {
	value, err := proto.Marshal(index)
	if err != nil {
		return err
	}

	key := path.Join(fieldIndexPrefix, strconv.FormatInt(index.CollectionID, 10),
		strconv.FormatInt(index.IndexID, 10))

	err = mt.kvClient.Save(key, string(value))
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
func (mt *metaTable) saveSegmentIndexMeta(segIdx *indexpb.SegmentIndex) error {
	value, err := proto.Marshal(segIdx)
	if err != nil {
		return err
	}
	key := path.Join(segmentIndexPrefix, strconv.FormatInt(segIdx.BuildID, 10))
	err = mt.kvClient.Save(key, string(value))
	if err != nil {
		log.Error("failed to save index meta in etcd", zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
		return err
	}

	mt.segmentIndexes[segIdx.SegmentID][segIdx.IndexID] = segIdx
	mt.buildID2SegmentIndex[segIdx.BuildID] = segIdx
	log.Info("IndexCoord metaTable saveIndexMeta success", zap.Int64("buildID", segIdx.BuildID))
	return nil
}

func (mt *metaTable) CreateIndex(indexID int64, req *indexpb.BuildIndexRequest, createTs uint64) error {
	mt.indexLock.Lock()
	defer mt.indexLock.RLock()
	log.Info("IndexCoord metaTable CreateIndex", zap.Int64("collectionID", req.CollectionID),
		zap.Int64("fieldID", req.FieldID), zap.Int64("indexID", indexID), zap.String("indexName", req.IndexName))

	index := &indexpb.FieldIndex{
		CollectionID: req.CollectionID,
		FieldID:      req.FieldID,
		IndexID:      indexID,
		IndexName:    req.IndexName,
		TypeParams:   req.TypeParams,
		IndexParams:  req.IndexParams,
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
func (mt *metaTable) AddIndex(segIndex *indexpb.SegmentIndex) error {
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

func (mt *metaTable) ConstructIndexes(segmentInfo *datapb.SegmentInfo) []*indexpb.SegmentIndex {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	segmentIndexes := make([]*indexpb.SegmentIndex, 0)
	if segmentInfo.NumOfRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex {
		return segmentIndexes
	}

	for indexID := range mt.collectionIndexes[segmentInfo.CollectionID] {
		segmentIndexes = append(segmentIndexes, &indexpb.SegmentIndex{
			CollectionID: segmentInfo.CollectionID,
			PartitionID:  segmentInfo.PartitionID,
			SegmentID:    segmentInfo.ID,
			IndexID:      indexID,
		})
	}
	return segmentIndexes
}

// HasSameReq determine whether there are same indexing tasks.
func (mt *metaTable) HasSameReq(req *indexpb.BuildIndexRequest) bool {

}

func (mt *metaTable) AlreadyBuiltIndex(segIdx *indexpb.SegmentIndex) (bool, UniqueID) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	if _, ok := mt.segmentIndexes[segIdx.GetSegmentID()]; !ok {
		return false, 0
	}

	if index, ok := mt.segmentIndexes[segIdx.GetSegmentID()][segIdx.GetIndexID()]; ok {
		return true, index.BuildID
	}

	return false, 0
}
