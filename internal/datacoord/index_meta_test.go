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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/common"
)

func TestReloadFromKV(t *testing.T) {
	t.Run("ListIndexes_fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, errors.New("mock"))
		_, err := newIndexMeta(context.TODO(), catalog)
		assert.Error(t, err)
	})

	t.Run("ListSegmentIndexes_fails", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, errors.New("mock"))

		_, err := newIndexMeta(context.TODO(), catalog)
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{
			{
				CollectionID: 1,
				IndexID:      1,
				IndexName:    "dix",
				CreateTime:   1,
			},
		}, nil)

		catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return([]*model.SegmentIndex{
			{
				SegmentID: 1,
				IndexID:   1,
			},
		}, nil)

		meta, err := newIndexMeta(context.TODO(), catalog)
		assert.NoError(t, err)
		assert.NotNil(t, meta)
	})
}

func TestMeta_ScalarAutoIndex(t *testing.T) {
	var (
		collID      = UniqueID(1)
		indexID     = UniqueID(10)
		fieldID     = UniqueID(100)
		indexName   = "_default_idx"
		typeParams  = []*commonpb.KeyValuePair{}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "HYBRID",
			},
		}
		userIndexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: common.AutoIndexName,
			},
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	m := newSegmentIndexMeta(catalog)

	req := &indexpb.CreateIndexRequest{
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		Timestamp:       0,
		IsAutoIndex:     true,
		UserIndexParams: userIndexParams,
	}

	t.Run("user index params consistent", func(t *testing.T) {
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
				UserIndexParams: userIndexParams,
			},
		}
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.NoError(t, err)
		assert.Equal(t, int64(indexID), tmpIndexID)
	})

	t.Run("user index params not consistent", func(t *testing.T) {
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
				UserIndexParams: userIndexParams,
			},
		}
		req.UserIndexParams = append(req.UserIndexParams, &commonpb.KeyValuePair{Key: "bitmap_cardinality_limit", Value: "1000"})
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.UserIndexParams = append(req.UserIndexParams, &commonpb.KeyValuePair{Key: "bitmap_cardinality_limit", Value: "500"})
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
	})

	req = &indexpb.CreateIndexRequest{
		CollectionID: collID,
		FieldID:      fieldID,
		IndexName:    indexName,
		TypeParams:   typeParams,
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "HYBRID",
			}},
		Timestamp:       0,
		IsAutoIndex:     true,
		UserIndexParams: userIndexParams,
	}

	t.Run("index param rewrite", func(t *testing.T) {
		m.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:     "",
				CollectionID: collID,
				FieldID:      fieldID,
				IndexID:      indexID,
				IndexName:    indexName,
				IsDeleted:    false,
				CreateTime:   10,
				TypeParams:   typeParams,
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "INVERTED",
					},
				},
				IsAutoIndex:     false,
				UserIndexParams: userIndexParams,
			},
		}
		tmpIndexID, err := m.CanCreateIndex(req)
		assert.NoError(t, err)
		assert.Equal(t, int64(indexID), tmpIndexID)
		newIndexParams := req.GetIndexParams()
		assert.Equal(t, len(newIndexParams), 1)
		assert.Equal(t, newIndexParams[0].Key, common.IndexTypeKey)
		assert.Equal(t, newIndexParams[0].Value, "INVERTED")
	})

}

func TestMeta_CanCreateIndex(t *testing.T) {
	var (
		collID = UniqueID(1)
		// partID     = UniqueID(2)
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
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
		}
		userIndexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: common.AutoIndexName,
			},
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("CreateIndex",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	m := newSegmentIndexMeta(catalog)

	req := &indexpb.CreateIndexRequest{
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		Timestamp:       0,
		IsAutoIndex:     false,
		UserIndexParams: userIndexParams,
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
			UserIndexParams: userIndexParams,
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
		req.UserIndexParams = append(indexParams, &commonpb.KeyValuePair{Key: "metrics_type", Value: "L2"})
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
		req.UserIndexParams = req.IndexParams
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserIndexParams = req.IndexParams
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		// when we use autoindex, it is possible autoindex changes default metric type
		// if user does not specify metric type, we should follow the very first autoindex config
		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserIndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "AUTOINDEX"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserAutoindexMetricTypeSpecified = false
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.NoError(t, err)
		assert.Equal(t, indexID, tmpIndexID)
		// req should follow the meta
		assert.Equal(t, "L2", req.GetUserIndexParams()[1].Value)
		assert.Equal(t, "L2", req.GetIndexParams()[1].Value)

		// if autoindex specify metric type, so the index param change is from user, return error
		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserIndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "AUTOINDEX"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserAutoindexMetricTypeSpecified = true
		tmpIndexID, err = m.CanCreateIndex(req)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = indexParams
		req.UserIndexParams = indexParams
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
		// partID     = UniqueID(2)
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

	m := newSegmentIndexMeta(catalogmocks.NewDataCoordCatalog(t))

	req := &indexpb.CreateIndexRequest{
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		Timestamp:       0,
		IsAutoIndex:     false,
		UserIndexParams: indexParams,
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
				UserIndexParams: indexParams,
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

func newSegmentIndexMeta(catalog metastore.DataCoordCatalog) *indexMeta {
	return &indexMeta{
		RWMutex:              sync.RWMutex{},
		ctx:                  context.Background(),
		catalog:              catalog,
		indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
		segmentIndexes:       make(map[UniqueID]map[UniqueID]*model.SegmentIndex),
	}
}

func TestMeta_CreateIndex(t *testing.T) {
	indexParams := []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: "FLAT",
		},
	}
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
		IndexParams:     indexParams,
		IsAutoIndex:     false,
		UserIndexParams: indexParams,
	}

	t.Run("success", func(t *testing.T) {
		sc := catalogmocks.NewDataCoordCatalog(t)
		sc.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(nil)

		m := newSegmentIndexMeta(sc)
		err := m.CreateIndex(index)
		assert.NoError(t, err)
	})

	t.Run("save fail", func(t *testing.T) {
		ec := catalogmocks.NewDataCoordCatalog(t)
		ec.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))

		m := newSegmentIndexMeta(ec)
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

	m := newSegmentIndexMeta(ec)
	m.segmentIndexes = map[UniqueID]map[UniqueID]*model.SegmentIndex{
		1: make(map[UniqueID]*model.SegmentIndex, 0),
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
		// partID     = UniqueID(2)
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

	m := newSegmentIndexMeta(&datacoord.Catalog{MetaKv: metakv})
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
				UserIndexParams: indexParams,
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

	m := newSegmentIndexMeta(&datacoord.Catalog{MetaKv: metakv})
	m.segmentIndexes = map[UniqueID]map[UniqueID]*model.SegmentIndex{
		segID: make(map[UniqueID]*model.SegmentIndex, 0),
	}

	t.Run("collection has no index", func(t *testing.T) {
		state := m.GetSegmentIndexState(collID, segID, indexID)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.GetState())
		assert.Contains(t, state.GetFailReason(), "collection not exist with ID")
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
				UserIndexParams: indexParams,
			},
		}
		state := m.GetSegmentIndexState(collID, segID, indexID)
		assert.Equal(t, commonpb.IndexState_Unissued, state.GetState())
	})

	t.Run("segment not exist", func(t *testing.T) {
		state := m.GetSegmentIndexState(collID, segID+1, indexID)
		assert.Equal(t, commonpb.IndexState_Unissued, state.GetState())
		assert.Contains(t, state.FailReason, "segment index not exist with ID")
	})

	t.Run("unissued", func(t *testing.T) {
		m.updateSegmentIndex(&model.SegmentIndex{
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
		})

		state := m.GetSegmentIndexState(collID, segID, indexID)
		assert.Equal(t, commonpb.IndexState_Unissued, state.GetState())
	})

	t.Run("finish", func(t *testing.T) {
		m.updateSegmentIndex(&model.SegmentIndex{
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
		})

		state := m.GetSegmentIndexState(collID, segID, indexID)
		assert.Equal(t, commonpb.IndexState_Finished, state.GetState())
	})
}

func TestMeta_GetIndexedSegment(t *testing.T) {
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

	m := newSegmentIndexMeta(nil)
	m.segmentIndexes = map[UniqueID]map[UniqueID]*model.SegmentIndex{
		segID: {
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
			},
		},
	}
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
				UserIndexParams: indexParams,
			},
		},
	}
	m.buildID2SegmentIndex = map[UniqueID]*model.SegmentIndex{
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
		},
	}

	t.Run("success", func(t *testing.T) {
		segments := m.GetIndexedSegments(collID, []int64{segID}, []int64{fieldID})
		assert.Len(t, segments, 1)
	})

	t.Run("no index on field", func(t *testing.T) {
		segments := m.GetIndexedSegments(collID, []int64{segID}, []int64{fieldID + 1})
		assert.Len(t, segments, 0)
	})

	t.Run("no index", func(t *testing.T) {
		segments := m.GetIndexedSegments(collID+1, []int64{segID}, []int64{fieldID})
		assert.Len(t, segments, 0)
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

	m := newSegmentIndexMeta(sc)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
	catalog := &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}
	m := createMeta(catalog, withIndexMeta(createIndexMeta(catalog)))

	t.Run("success", func(t *testing.T) {
		segIndexes := m.indexMeta.getSegmentIndexes(segID)
		assert.Equal(t, 1, len(segIndexes))
	})

	t.Run("segment not exist", func(t *testing.T) {
		segIndexes := m.indexMeta.getSegmentIndexes(segID + 100)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist- segment index empty", func(t *testing.T) {
		m := newSegmentIndexMeta(nil)
		segIndexes := m.GetSegmentIndexes(collID, segID)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist- field index empty", func(t *testing.T) {
		m := newSegmentIndexMeta(nil)
		m.segmentIndexes = map[UniqueID]map[UniqueID]*model.SegmentIndex{
			1: {
				1: &model.SegmentIndex{},
			},
		}
		segIndexes := m.GetSegmentIndexes(collID, 1)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("index exists", func(t *testing.T) {
		m := &indexMeta{
			segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
				segID: {
					indexID: &model.SegmentIndex{
						CollectionID: collID,
						SegmentID:    segID,
						IndexID:      indexID,
						IndexState:   commonpb.IndexState_Finished,
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
		segIndexes := m.GetSegmentIndexes(collID, segID)
		assert.Equal(t, 1, len(segIndexes))

		segIdx, ok := segIndexes[indexID]
		assert.True(t, ok)
		assert.NotNil(t, segIdx)
	})
}

func TestMeta_GetFieldIDByIndexID(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
	m := newSegmentIndexMeta(nil)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
	indexParams := []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: "HNSW",
		},
	}

	m := newSegmentIndexMeta(nil)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: indexParams,
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
	indexParams := []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: "HNSW",
		},
	}

	m := newSegmentIndexMeta(nil)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: indexParams,
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
	m := newSegmentIndexMeta(nil)
	m.buildID2SegmentIndex = map[UniqueID]*model.SegmentIndex{
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
	m := newSegmentIndexMeta(nil)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
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

func updateSegmentIndexMeta(t *testing.T) *indexMeta {
	sc := catalogmocks.NewDataCoordCatalog(t)
	sc.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	return &indexMeta{
		catalog: sc,
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
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
		err := m.UpdateVersion(buildID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		m.catalog = ec
		err := m.UpdateVersion(buildID)
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.UpdateVersion(buildID + 1)
		assert.Error(t, err)
	})
}

func TestMeta_FinishTask(t *testing.T) {
	m := updateSegmentIndexMeta(t)

	t.Run("success", func(t *testing.T) {
		err := m.FinishTask(&workerpb.IndexTaskInfo{
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
		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: 1024,
			FailReason:     "",
		})
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.FinishTask(&workerpb.IndexTaskInfo{
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
		err := m.BuildIndex(buildID, nodeID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		m.catalog = ec
		err := m.BuildIndex(buildID, nodeID)
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.BuildIndex(buildID+1, nodeID)
		assert.Error(t, err)
	})
}

// see also: https://github.com/milvus-io/milvus/issues/21660
func TestUpdateSegmentIndexNotExists(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	assert.NotPanics(t, func() {
		m.updateSegmentIndex(&model.SegmentIndex{
			SegmentID: 1,
			IndexID:   2,
		})
	})

	assert.Equal(t, 1, len(m.segmentIndexes))
	segmentIdx := m.segmentIndexes[1]
	assert.Equal(t, 1, len(segmentIdx))
	_, ok := segmentIdx[2]
	assert.True(t, ok)
}

func TestMeta_DeleteTask_Error(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	t.Run("segment index not found", func(t *testing.T) {
		err := m.DeleteTask(buildID)
		assert.NoError(t, err)
	})

	t.Run("segment update failed", func(t *testing.T) {
		ec := catalogmocks.NewDataCoordCatalog(t)
		ec.On("AlterSegmentIndexes",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		m.catalog = ec

		m.buildID2SegmentIndex[buildID] = &model.SegmentIndex{
			SegmentID:    segID,
			PartitionID:  partID,
			CollectionID: collID,
		}

		err := m.DeleteTask(buildID)
		assert.Error(t, err)
	})
}

func TestMeta_GetFieldIndexes(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
		collID: {
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       true,
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
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
			indexID + 2: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID + 2,
				IndexID:         indexID + 2,
				IndexName:       indexName + "2",
				IsDeleted:       false,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		},
	}

	indexes := m.GetFieldIndexes(collID, fieldID, "")
	assert.Equal(t, 1, len(indexes))
	assert.Equal(t, indexName, indexes[0].IndexName)
}

func TestRemoveIndex(t *testing.T) {
	t.Run("drop index fail", func(t *testing.T) {
		expectedErr := errors.New("error")
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().
			DropIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(expectedErr)

		m := newSegmentIndexMeta(catalog)
		err := m.RemoveIndex(collID, indexID)
		assert.Error(t, err)
		assert.EqualError(t, err, "error")
	})

	t.Run("remove index ok", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().
			DropIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		m := &indexMeta{
			catalog: catalog,
			indexes: map[int64]map[int64]*model.Index{
				collID: {
					indexID: &model.Index{},
				},
			},
		}

		err := m.RemoveIndex(collID, indexID)
		assert.NoError(t, err)
		assert.Equal(t, len(m.indexes), 0)
	})
}

func TestRemoveSegmentIndex(t *testing.T) {
	t.Run("drop segment index fail", func(t *testing.T) {
		expectedErr := errors.New("error")
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().
			DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(expectedErr)

		m := newSegmentIndexMeta(catalog)
		err := m.RemoveSegmentIndex(0, 0, 0, 0, 0)

		assert.Error(t, err)
		assert.EqualError(t, err, "error")
	})

	t.Run("remove segment index ok", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().
			DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		m := &indexMeta{
			catalog: catalog,
			segmentIndexes: map[int64]map[int64]*model.SegmentIndex{
				segID: {
					indexID: &model.SegmentIndex{},
				},
			},
			buildID2SegmentIndex: map[int64]*model.SegmentIndex{
				buildID: {},
			},
		}

		err := m.RemoveSegmentIndex(collID, partID, segID, indexID, buildID)
		assert.NoError(t, err)

		assert.Equal(t, len(m.segmentIndexes), 0)
		assert.Equal(t, len(m.buildID2SegmentIndex), 0)
	})
}

func TestIndexMeta_GetUnindexedSegments(t *testing.T) {
	catalog := &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}
	m := createMeta(catalog, withIndexMeta(createIndexMeta(catalog)))

	// normal case
	segmentIDs := make([]int64, 0, 11)
	for i := 0; i <= 10; i++ {
		segmentIDs = append(segmentIDs, segID+int64(i))
	}
	unindexed := m.indexMeta.GetUnindexedSegments(collID, segmentIDs)
	assert.Equal(t, 8, len(unindexed))

	// no index
	unindexed = m.indexMeta.GetUnindexedSegments(collID+1, segmentIDs)
	assert.Equal(t, 0, len(unindexed))
}
