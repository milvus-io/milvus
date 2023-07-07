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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
)

func TestMeta_CanCreateIndex(t *testing.T) {
	var (
		collID = UniqueID(1)
		//partID     = UniqueID(2)
		indexID    = UniqueID(10)
		fieldID    = UniqueID(100)
		indexName  = "_default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "FLAT",
			},
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("CreateIndex",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	m := &meta{
		RWMutex:              sync.RWMutex{},
		ctx:                  context.Background(),
		catalog:              catalog,
		collections:          nil,
		segments:             nil,
		channelCPs:           nil,
		chunkManager:         nil,
		indexes:              map[UniqueID]map[UniqueID]*model.Index{},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{},
	}

	req := &indexpb.CreateIndexRequest{
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		Timestamp:       0,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	}

	t.Run("can create index", func(t *testing.T) {
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
		index := &model.Index{
			TenantID:        "",
			CollectionID:    collID,
			FieldID:         fieldID,
			IndexID:         indexID,
			IndexName:       indexName,
			IsDeleted:       false,
			CreateTime:      0,
			TypeParams:      typeParams,
			IndexParams:     indexParams,
			IsAutoIndex:     false,
			UserIndexParams: nil,
		}

		err = m.CreateIndex(index)
		assert.NoError(t, err)

		tmpIndexID, err = m.CanCreateIndex(req)
		assert.NoError(t, err)
		assert.Equal(t, indexID, tmpIndexID)
	})

	t.Run("params not consistent", func(t *testing.T) {
		req.TypeParams = append(req.TypeParams, &commonpb.KeyValuePair{Key: "primary_key", Value: "false"})
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "64"}}
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.TypeParams = typeParams
		req.IndexParams = append(req.IndexParams, &commonpb.KeyValuePair{Key: "metrics_type", Value: "L2"})
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = indexParams
		req.FieldID++
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
	})

	t.Run("multiple indexes", func(t *testing.T) {
		req.IndexName = "_default_idx_2"
		req.FieldID = fieldID
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
	})

	t.Run("index has been deleted", func(t *testing.T) {
		m.indexes[collID][indexID].IsDeleted = true
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
	})
}

func TestMeta_HasSameReq(t *testing.T) {
	var (
		collID = UniqueID(1)
		//partID     = UniqueID(2)
		indexID    = UniqueID(10)
		fieldID    = UniqueID(100)
		indexName  = "_default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "FLAT",
			},
		}
	)
	m := &meta{
		RWMutex:              sync.RWMutex{},
		ctx:                  context.Background(),
		catalog:              catalogmocks.NewDataCoordCatalog(t),
		collections:          nil,
		segments:             nil,
		channelCPs:           nil,
		chunkManager:         nil,
		indexes:              map[UniqueID]map[UniqueID]*model.Index{},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{},
	}

	req := &indexpb.CreateIndexRequest{
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		Timestamp:       0,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	}

	t.Run("no indexes", func(t *testing.T) {
		has, _ := m.HasSameReq(req)
		assert.False(t, has)
	})

	t.Run("has same req", func(t *testing.T) {
		m.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      10,
				TypeParams:      typeParams,
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		}
		has, _ := m.HasSameReq(req)
		assert.True(t, has)
	})

	t.Run("params not consistent", func(t *testing.T) {
		req.TypeParams = []*commonpb.KeyValuePair{{}}
		has, _ := m.HasSameReq(req)
		assert.False(t, has)
	})

	t.Run("index has been deleted", func(t *testing.T) {
		m.indexes[collID][indexID].IsDeleted = true
		has, _ := m.HasSameReq(req)
		assert.False(t, has)
	})
}

func TestMeta_CreateIndex(t *testing.T) {
	index := &model.Index{
		TenantID:     "",
		CollectionID: 1,
		FieldID:      2,
		IndexID:      3,
		IndexName:    "_default_idx",
		IsDeleted:    false,
		CreateTime:   12,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "FLAT",
			},
		},
		IsAutoIndex:     false,
		UserIndexParams: nil,
	}

	t.Run("success", func(t *testing.T) {
		sc := catalogmocks.NewDataCoordCatalog(t)
		sc.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(nil)

		m := &meta{
			RWMutex:              sync.RWMutex{},
			ctx:                  context.Background(),
			catalog:              sc,
			indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
			buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
		}

		err := m.CreateIndex(index)
		assert.NoError(t, err)
	})

	t.Run("save fail", func(t *testing.T) {
		ec := catalogmocks.NewDataCoordCatalog(t)
		ec.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))

		m := &meta{
			RWMutex:              sync.RWMutex{},
			ctx:                  context.Background(),
			catalog:              ec,
			indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
			buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
		}

		err := m.CreateIndex(index)
		assert.Error(t, err)
	})
}

func TestMeta_AddSegmentIndex(t *testing.T) {
	sc := catalogmocks.NewDataCoordCatalog(t)
	sc.On("CreateSegmentIndex",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	ec := catalogmocks.NewDataCoordCatalog(t)
	ec.On("CreateSegmentIndex",
		mock.Anything,
		mock.Anything,
	).Return(errors.New("fail"))

	m := &meta{
		RWMutex:              sync.RWMutex{},
		ctx:                  context.Background(),
		catalog:              ec,
		indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo:     nil,
					segmentIndexes:  map[UniqueID]*model.SegmentIndex{},
					currRows:        0,
					allocations:     nil,
					lastFlushTime:   time.Time{},
					isCompacting:    false,
					lastWrittenTime: time.Time{},
				},
			},
		},
	}

	segmentIndex := &model.SegmentIndex{
		SegmentID:     1,
		CollectionID:  2,
		PartitionID:   3,
		NumRows:       10240,
		IndexID:       4,
		BuildID:       5,
		NodeID:        6,
		IndexVersion:  0,
		IndexState:    0,
		FailReason:    "",
		IsDeleted:     false,
		CreateTime:    12,
		IndexFileKeys: nil,
		IndexSize:     0,
		WriteHandoff:  false,
	}

	t.Run("save meta fail", func(t *testing.T) {
		err := m.AddSegmentIndex(segmentIndex)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m.catalog = sc
		err := m.AddSegmentIndex(segmentIndex)
		assert.NoError(t, err)
	})
}

func TestMeta_GetIndexIDByName(t *testing.T) {
	var (
		collID = UniqueID(1)
		//partID     = UniqueID(2)
		indexID    = UniqueID(10)
		fieldID    = UniqueID(100)
		indexName  = "_default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "FLAT",
			},
		}
	)
	metakv := mockkv.NewMetaKv(t)
	metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil).Maybe()
	m := &meta{
		RWMutex:              sync.RWMutex{},
		ctx:                  context.Background(),
		catalog:              &datacoord.Catalog{MetaKv: metakv},
		indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
	}

	t.Run("no indexes", func(t *testing.T) {
		indexID2CreateTS := m.GetIndexIDByName(collID, indexName)
		assert.Equal(t, 0, len(indexID2CreateTS))
	})

	t.Run("success", func(t *testing.T) {
		m.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      12,
				TypeParams:      typeParams,
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		}

		indexID2CreateTS := m.GetIndexIDByName(collID, indexName)
		assert.Contains(t, indexID2CreateTS, indexID)
	})

}

func TestMeta_GetSegmentIndexState(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		indexID    = UniqueID(10)
		fieldID    = UniqueID(100)
		segID      = UniqueID(1000)
		buildID    = UniqueID(10000)
		indexName  = "_default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "FLAT",
			},
		}
	)
	metakv := mockkv.NewMetaKv(t)
	metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil).Maybe()
	m := &meta{
		RWMutex:              sync.RWMutex{},
		ctx:                  context.Background(),
		catalog:              &datacoord.Catalog{MetaKv: metakv},
		indexes:              map[UniqueID]map[UniqueID]*model.Index{},
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo:     nil,
					segmentIndexes:  map[UniqueID]*model.SegmentIndex{},
					currRows:        0,
					allocations:     nil,
					lastFlushTime:   time.Time{},
					isCompacting:    false,
					lastWrittenTime: time.Time{},
				},
			},
		},
	}

	t.Run("segment has no index", func(t *testing.T) {
		state := m.GetSegmentIndexState(collID, segID)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.state)
	})

	t.Run("meta not saved yet", func(t *testing.T) {
		m.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      12,
				TypeParams:      typeParams,
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		}
		state := m.GetSegmentIndexState(collID, segID)
		assert.Equal(t, commonpb.IndexState_Unissued, state.state)
	})

	t.Run("segment not exist", func(t *testing.T) {
		state := m.GetSegmentIndexState(collID, segID+1)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.state)
	})

	t.Run("unissued", func(t *testing.T) {
		m.segments.SetSegmentIndex(segID, &model.SegmentIndex{
			SegmentID:     segID,
			CollectionID:  collID,
			PartitionID:   partID,
			NumRows:       10250,
			IndexID:       indexID,
			BuildID:       buildID,
			NodeID:        1,
			IndexVersion:  0,
			IndexState:    commonpb.IndexState_Unissued,
			FailReason:    "",
			IsDeleted:     false,
			CreateTime:    12,
			IndexFileKeys: nil,
			IndexSize:     0,
			WriteHandoff:  false,
		})

		state := m.GetSegmentIndexState(collID, segID)
		assert.Equal(t, commonpb.IndexState_Unissued, state.state)
	})

	t.Run("finish", func(t *testing.T) {
		m.segments.SetSegmentIndex(segID, &model.SegmentIndex{
			SegmentID:     segID,
			CollectionID:  collID,
			PartitionID:   partID,
			NumRows:       10250,
			IndexID:       indexID,
			BuildID:       buildID,
			NodeID:        1,
			IndexVersion:  0,
			IndexState:    commonpb.IndexState_Finished,
			FailReason:    "",
			IsDeleted:     false,
			CreateTime:    12,
			IndexFileKeys: nil,
			IndexSize:     0,
			WriteHandoff:  false,
		})

		state := m.GetSegmentIndexState(collID, segID)
		assert.Equal(t, commonpb.IndexState_Finished, state.state)
	})
}

func TestMeta_GetSegmentIndexStateOnField(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		indexID    = UniqueID(10)
		fieldID    = UniqueID(100)
		segID      = UniqueID(1000)
		buildID    = UniqueID(10000)
		indexName  = "_default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "FLAT",
			},
		}
	)
	m := &meta{
		RWMutex:     sync.RWMutex{},
		ctx:         context.Background(),
		catalog:     nil,
		collections: nil,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1025,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        nodeID,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: nil,
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
			},
		},
		channelCPs:   nil,
		chunkManager: nil,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      10,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1025,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: nil,
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		state := m.GetSegmentIndexStateOnField(collID, segID, fieldID)
		assert.Equal(t, commonpb.IndexState_Finished, state.state)
	})

	t.Run("no index on field", func(t *testing.T) {
		state := m.GetSegmentIndexStateOnField(collID, segID, fieldID+1)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.state)
	})

	t.Run("no index", func(t *testing.T) {
		state := m.GetSegmentIndexStateOnField(collID+1, segID, fieldID+1)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.state)
	})

	t.Run("segment not exist", func(t *testing.T) {
		state := m.GetSegmentIndexStateOnField(collID, segID+1, fieldID)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.state)
	})
}

func TestMeta_MarkIndexAsDeleted(t *testing.T) {
	sc := catalogmocks.NewDataCoordCatalog(t)
	sc.On("AlterIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	ec := catalogmocks.NewDataCoordCatalog(t)
	ec.On("AlterIndexes",
		mock.Anything,
		mock.Anything,
	).Return(errors.New("fail"))

	m := &meta{
		catalog: sc,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      10,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				indexID + 1: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 1,
					IndexID:         indexID + 1,
					IndexName:       "_default_idx_102",
					IsDeleted:       true,
					CreateTime:      1,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("fail", func(t *testing.T) {
		m.catalog = ec
		err := m.MarkIndexAsDeleted(collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m.catalog = sc
		err := m.MarkIndexAsDeleted(collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.NoError(t, err)

		err = m.MarkIndexAsDeleted(collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.NoError(t, err)

		err = m.MarkIndexAsDeleted(collID+1, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.NoError(t, err)
	})
}

func TestMeta_GetSegmentIndexes(t *testing.T) {
	m := createMetaTable(&datacoord.Catalog{MetaKv: mocks.NewMetaKv(t)})

	t.Run("success", func(t *testing.T) {
		segIndexes := m.GetSegmentIndexes(segID)
		assert.Equal(t, 1, len(segIndexes))
	})

	t.Run("segment not exist", func(t *testing.T) {
		segIndexes := m.GetSegmentIndexes(segID + 100)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist", func(t *testing.T) {
		m = &meta{
			RWMutex: sync.RWMutex{},
			segments: &SegmentsInfo{
				segments: map[UniqueID]*SegmentInfo{
					segID: {
						SegmentInfo: &datapb.SegmentInfo{
							ID:           segID,
							CollectionID: collID,
							PartitionID:  partID,
							NumOfRows:    0,
							State:        commonpb.SegmentState_Flushed,
						},
					},
				},
			},
			indexes:              nil,
			buildID2SegmentIndex: nil,
		}

		segIndexes := m.GetSegmentIndexes(segID)
		assert.Equal(t, 0, len(segIndexes))
	})
}

func TestMeta_GetFieldIDByIndexID(t *testing.T) {
	m := &meta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		fID := m.GetFieldIDByIndexID(collID, indexID)
		assert.Equal(t, fieldID, fID)
	})

	t.Run("fail", func(t *testing.T) {
		fID := m.GetFieldIDByIndexID(collID, indexID+1)
		assert.Equal(t, UniqueID(0), fID)
	})
}

func TestMeta_GetIndexNameByID(t *testing.T) {
	m := &meta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		iName := m.GetIndexNameByID(collID, indexID)
		assert.Equal(t, indexName, iName)
	})

	t.Run("fail", func(t *testing.T) {
		iName := m.GetIndexNameByID(collID, indexID+1)
		assert.Equal(t, "", iName)
	})
}

func TestMeta_GetTypeParams(t *testing.T) {
	m := &meta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   0,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   common.IndexTypeKey,
							Value: "HNSW",
						},
					},
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		tp := m.GetTypeParams(collID, indexID)
		assert.Equal(t, 1, len(tp))
	})

	t.Run("not exist", func(t *testing.T) {
		tp := m.GetTypeParams(collID, indexID+1)
		assert.Equal(t, 0, len(tp))

		tp = m.GetTypeParams(collID+1, indexID)
		assert.Equal(t, 0, len(tp))
	})
}

func TestMeta_GetIndexParams(t *testing.T) {
	m := &meta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   0,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   common.IndexTypeKey,
							Value: "HNSW",
						},
					},
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		ip := m.GetIndexParams(collID, indexID)
		assert.Equal(t, 1, len(ip))
	})

	t.Run("not exist", func(t *testing.T) {
		ip := m.GetIndexParams(collID, indexID+1)
		assert.Equal(t, 0, len(ip))

		ip = m.GetIndexParams(collID+1, indexID)
		assert.Equal(t, 0, len(ip))
	})
}

func TestMeta_GetIndexJob(t *testing.T) {
	m := &meta{
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1025,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: nil,
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}

	t.Run("exist", func(t *testing.T) {
		segIndex, exist := m.GetIndexJob(buildID)
		assert.True(t, exist)
		assert.NotNil(t, segIndex)
	})

	t.Run("not exist", func(t *testing.T) {
		segIndex, exist := m.GetIndexJob(buildID + 1)
		assert.False(t, exist)
		assert.Nil(t, segIndex)
	})
}

func TestMeta_IsIndexExist(t *testing.T) {
	m := &meta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				indexID + 1: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID + 1,
					IndexName:       "index2",
					IsDeleted:       true,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("exist", func(t *testing.T) {
		exist := m.IsIndexExist(collID, indexID)
		assert.True(t, exist)
	})

	t.Run("not exist", func(t *testing.T) {
		exist := m.IsIndexExist(collID, indexID+1)
		assert.False(t, exist)

		exist = m.IsIndexExist(collID, indexID+2)
		assert.False(t, exist)

		exist = m.IsIndexExist(collID+1, indexID)
		assert.False(t, exist)
	})
}

func updateSegmentIndexMeta(t *testing.T) *meta {
	sc := catalogmocks.NewDataCoordCatalog(t)
	sc.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	return &meta{
		catalog: sc,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1025,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1025,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        0,
							IndexVersion:  0,
							IndexState:    commonpb.IndexState_Unissued,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    0,
							IndexFileKeys: nil,
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
			},
		},
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1025,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: nil,
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}
}

func TestMeta_UpdateVersion(t *testing.T) {
	m := updateSegmentIndexMeta(t)
	ec := catalogmocks.NewDataCoordCatalog(t)
	ec.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(errors.New("fail"))

	t.Run("success", func(t *testing.T) {
		err := m.UpdateVersion(buildID, nodeID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		m.catalog = ec
		err := m.UpdateVersion(buildID, nodeID)
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.UpdateVersion(buildID+1, nodeID)
		assert.Error(t, err)
	})
}

func TestMeta_FinishTask(t *testing.T) {
	m := updateSegmentIndexMeta(t)

	t.Run("success", func(t *testing.T) {
		err := m.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: 1024,
			FailReason:     "",
		})
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("failed")).Maybe()
		m.catalog = &datacoord.Catalog{
			MetaKv: metakv,
		}
		err := m.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: 1024,
			FailReason:     "",
		})
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        buildID + 1,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: 1024,
			FailReason:     "",
		})
		assert.NoError(t, err)
	})
}

func TestMeta_BuildIndex(t *testing.T) {
	m := updateSegmentIndexMeta(t)
	ec := catalogmocks.NewDataCoordCatalog(t)
	ec.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(errors.New("fail"))

	t.Run("success", func(t *testing.T) {
		err := m.BuildIndex(buildID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		m.catalog = ec
		err := m.BuildIndex(buildID)
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.BuildIndex(buildID + 1)
		assert.Error(t, err)
	})
}

func TestMeta_GetHasUnindexTaskSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1025,
						State:         commonpb.SegmentState_Flushed,
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 1,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1025,
						State:         commonpb.SegmentState_Growing,
					},
				},
				segID + 2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 2,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1025,
						State:         commonpb.SegmentState_Dropped,
					},
				},
			},
		},
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
	}

	t.Run("normal", func(t *testing.T) {
		segments := m.GetHasUnindexTaskSegments()
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)
	})
}

// see also: https://github.com/milvus-io/milvus/issues/21660
func TestUpdateSegmentIndexNotExists(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{},
		},
		indexes:              map[UniqueID]map[UniqueID]*model.Index{},
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
	}

	assert.NotPanics(t, func() {
		m.updateSegmentIndex(&model.SegmentIndex{
			SegmentID: 1,
			IndexID:   2,
		})
	})
}
