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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestReloadFromKV(t *testing.T) {
	t.Run("ListIndexes_fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, errors.New("mock"))
		catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		_, err := newIndexMeta(context.TODO(), catalog, []int64{0})
		assert.Error(t, err)
	})

	t.Run("ListSegmentIndexes_fails", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, errors.New("mock"))

		_, err := newIndexMeta(context.TODO(), catalog, []int64{0})
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

		catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{
			{
				SegmentID: 1,
				IndexID:   1,
			},
		}, nil)

		meta, err := newIndexMeta(context.TODO(), catalog, []int64{0})
		assert.NoError(t, err)
		assert.NotNil(t, meta)
	})

	// Reload must only count active indexes (non-deleted) in the gauge.
	t.Run("reload gauge skips deleted indexes", func(t *testing.T) {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		var (
			collID       = int64(100)
			aliveIndexID = int64(1)
			deadIndexID  = int64(2)
		)
		collIDLabel := fmt.Sprintf("%d", collID)
		var aliveSize uint64 = 5000
		var deadSize uint64 = 3000

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{
			{CollectionID: collID, IndexID: aliveIndexID, IndexName: "alive", IsDeleted: false},
			{CollectionID: collID, IndexID: deadIndexID, IndexName: "dead", IsDeleted: true},
		}, nil)
		catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return([]*model.SegmentIndex{
			{SegmentID: 10, CollectionID: collID, IndexID: aliveIndexID, BuildID: 100, IndexSerializedSize: aliveSize},
			{SegmentID: 20, CollectionID: collID, IndexID: deadIndexID, BuildID: 200, IndexSerializedSize: deadSize},
		}, nil)

		meta, err := newIndexMeta(context.TODO(), catalog, []int64{collID})
		assert.NoError(t, err)
		assert.NotNil(t, meta)

		// The gauge goroutine is async; wait for it to finish.
		assert.Eventually(t, func() bool {
			val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
			return val == float64(aliveSize)
		}, time.Second, 10*time.Millisecond,
			"reload gauge must only count the alive index (size=%d), not the deleted one (size=%d)", aliveSize, deadSize)
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
		tmpIndexID, err := m.CanCreateIndex(req, false)
		assert.ErrorIs(t, err, errIndexOperationIgnored)
		assert.Zero(t, tmpIndexID)
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
		tmpIndexID, err := m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.UserIndexParams = append(req.UserIndexParams, &commonpb.KeyValuePair{Key: "bitmap_cardinality_limit", Value: "500"})
		tmpIndexID, err = m.CanCreateIndex(req, false)
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
			},
		},
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
		tmpIndexID, err := m.CanCreateIndex(req, false)
		assert.ErrorIs(t, err, errIndexOperationIgnored)
		assert.Zero(t, tmpIndexID)
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
	indexModel := &model.Index{
		CollectionID:    req.CollectionID,
		FieldID:         req.FieldID,
		IndexID:         indexID,
		IndexName:       req.IndexName,
		IsDeleted:       false,
		CreateTime:      req.Timestamp,
		TypeParams:      req.TypeParams,
		IndexParams:     req.IndexParams,
		IsAutoIndex:     req.IsAutoIndex,
		UserIndexParams: userIndexParams,
	}

	t.Run("can create index", func(t *testing.T) {
		tmpIndexID, err := m.CanCreateIndex(req, false)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		err = m.CreateIndex(context.TODO(), indexModel)
		assert.NoError(t, err)

		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.ErrorIs(t, err, errIndexOperationIgnored)
		assert.Zero(t, tmpIndexID)
	})

	t.Run("params not consistent", func(t *testing.T) {
		req.TypeParams = append(req.TypeParams, &commonpb.KeyValuePair{Key: "primary_key", Value: "false"})
		tmpIndexID, err := m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "64"}}
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.TypeParams = typeParams
		req.UserIndexParams = append(indexParams, &commonpb.KeyValuePair{Key: "metrics_type", Value: "L2"})
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
		req.UserIndexParams = req.IndexParams
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserIndexParams = req.IndexParams
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		// when we use autoindex, it is possible autoindex changes default metric type
		// if user does not specify metric type, we should follow the very first autoindex config
		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserIndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "AUTOINDEX"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserAutoindexMetricTypeSpecified = false
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.ErrorIs(t, err, errIndexOperationIgnored)
		assert.Zero(t, tmpIndexID)
		// req should follow the meta
		assert.Equal(t, "L2", req.GetUserIndexParams()[1].Value)
		assert.Equal(t, "L2", req.GetIndexParams()[1].Value)

		// if autoindex specify metric type, so the index param change is from user, return error
		req.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserIndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "AUTOINDEX"}, {Key: common.MetricTypeKey, Value: "COSINE"}}
		req.UserAutoindexMetricTypeSpecified = true
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)

		req.IndexParams = indexParams
		req.UserIndexParams = indexParams
		req.FieldID++
		tmpIndexID, err = m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
	})

	t.Run("multiple indexes", func(t *testing.T) {
		req.IndexName = "_default_idx_2"
		req.FieldID = fieldID
		tmpIndexID, err := m.CanCreateIndex(req, false)
		assert.Error(t, err)
		assert.Equal(t, int64(0), tmpIndexID)
	})

	t.Run("index has been deleted", func(t *testing.T) {
		m.indexes[collID][indexID].IsDeleted = true
		tmpIndexID, err := m.CanCreateIndex(req, false)
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
			{
				Key:   common.MmapEnabledKey,
				Value: "true",
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
		keyLock:          lock.NewKeyLock[UniqueID](),
		ctx:              context.Background(),
		catalog:          catalog,
		indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
}

func TestMeta_CreateIndex(t *testing.T) {
	indexName := "default_idx"
	indexParams := []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: "FLAT",
		},
	}

	typeParams := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "128",
		},
	}

	req := &indexpb.CreateIndexRequest{
		CollectionID:    1,
		FieldID:         2,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		Timestamp:       12,
		IsAutoIndex:     false,
		UserIndexParams: indexParams,
	}
	allocatedID := UniqueID(3)
	indexModel := &model.Index{
		CollectionID:    req.CollectionID,
		FieldID:         req.FieldID,
		IndexID:         allocatedID,
		IndexName:       req.IndexName,
		TypeParams:      req.TypeParams,
		IndexParams:     req.IndexParams,
		CreateTime:      req.Timestamp,
		IsAutoIndex:     req.IsAutoIndex,
		UserIndexParams: req.UserIndexParams,
	}

	t.Run("success", func(t *testing.T) {
		sc := catalogmocks.NewDataCoordCatalog(t)
		sc.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(nil)

		m := newSegmentIndexMeta(sc)
		err := m.CreateIndex(context.TODO(), indexModel)
		assert.NoError(t, err)
	})

	t.Run("save fail", func(t *testing.T) {
		ec := catalogmocks.NewDataCoordCatalog(t)
		ec.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))

		m := newSegmentIndexMeta(ec)
		indexModel.IndexID = 4
		err := m.CreateIndex(context.TODO(), indexModel)
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
	m.segmentIndexes = typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	m.segmentIndexes.Insert(1, typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]())

	segmentIndex := &model.SegmentIndex{
		SegmentID:           1,
		CollectionID:        2,
		PartitionID:         3,
		NumRows:             10240,
		IndexID:             4,
		BuildID:             5,
		NodeID:              6,
		IndexVersion:        0,
		IndexState:          0,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      12,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	}

	t.Run("save meta fail", func(t *testing.T) {
		err := m.AddSegmentIndex(context.TODO(), segmentIndex)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m.catalog = sc
		err := m.AddSegmentIndex(context.TODO(), segmentIndex)
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
	metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()

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
	metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()

	m := newSegmentIndexMeta(&datacoord.Catalog{MetaKv: metakv})
	m.segmentIndexes = typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	m.segmentIndexes.Insert(1, typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]())

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
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			NumRows:             10250,
			IndexID:             indexID,
			BuildID:             buildID,
			NodeID:              1,
			IndexVersion:        0,
			IndexState:          commonpb.IndexState_Unissued,
			FailReason:          "",
			IsDeleted:           false,
			CreatedUTCTime:      12,
			IndexFileKeys:       nil,
			IndexSerializedSize: 0,
		})

		state := m.GetSegmentIndexState(collID, segID, indexID)
		assert.Equal(t, commonpb.IndexState_Unissued, state.GetState())
	})

	t.Run("finish", func(t *testing.T) {
		m.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			NumRows:             10250,
			IndexID:             indexID,
			BuildID:             buildID,
			NodeID:              1,
			IndexVersion:        0,
			IndexState:          commonpb.IndexState_Finished,
			FailReason:          "",
			IsDeleted:           false,
			CreatedUTCTime:      12,
			IndexFileKeys:       nil,
			IndexSerializedSize: 0,
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
		nodeID     = UniqueID(1)
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
	segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxes.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})

	m.segmentIndexes.Insert(segID, segIdxes)
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

	m.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      10,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})

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

	t.Run("with deleted index entries", func(t *testing.T) {
		// Simulate drop+create index cycles: add deleted index entries for the same field.
		// Previously, deleted entries inflated len(targetIndices), causing the indexed check
		// to always fail (indexedFields=1 != len(targetIndices)=N).
		deletedIndexID1 := indexID + 100
		deletedIndexID2 := indexID + 200
		m.indexes[collID][deletedIndexID1] = &model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      deletedIndexID1,
			IndexName:    "old_idx_1",
			IsDeleted:    true,
		}
		m.indexes[collID][deletedIndexID2] = &model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      deletedIndexID2,
			IndexName:    "old_idx_2",
			IsDeleted:    true,
		}

		segments := m.GetIndexedSegments(collID, []int64{segID}, []int64{fieldID})
		assert.Len(t, segments, 1, "segment should be indexed even with deleted index entries")

		// Cleanup
		delete(m.indexes[collID], deletedIndexID1)
		delete(m.indexes[collID], deletedIndexID2)
	})
}

func TestMeta_MarkIndexAsDeleted(t *testing.T) {
	var (
		collID    = UniqueID(1)
		fieldID   = UniqueID(2)
		indexID   = UniqueID(100)
		indexName = "default_idx"
	)
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
		err := m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m.catalog = sc
		err := m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.NoError(t, err)

		err = m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.NoError(t, err)

		err = m.MarkIndexAsDeleted(context.TODO(), collID+1, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.NoError(t, err)
	})
}

func TestMeta_GetSegmentIndexes(t *testing.T) {
	catalog := &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}
	m := createMeta(catalog, withIndexMeta(createIndexMeta(catalog)))

	t.Run("success", func(t *testing.T) {
		segIndexes := m.indexMeta.GetSegmentIndexes(collID, segID)
		assert.Equal(t, 1, len(segIndexes))
	})

	t.Run("segment not exist", func(t *testing.T) {
		segIndexes := m.indexMeta.GetSegmentIndexes(collID, segID+100)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist- segment index empty", func(t *testing.T) {
		m := newSegmentIndexMeta(nil)
		segIndexes := m.GetSegmentIndexes(collID, segID)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist- field index empty", func(t *testing.T) {
		m := newSegmentIndexMeta(nil)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, &model.SegmentIndex{})
		m.segmentIndexes.Insert(segID, segIdxes)

		segIndexes := m.GetSegmentIndexes(collID, 1)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("index exists", func(t *testing.T) {
		m := &indexMeta{
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, &model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID,
			IndexState:   commonpb.IndexState_Finished,
		})
		m.segmentIndexes.Insert(segID, segIdxes)
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
	m.segmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})

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

	indexBuildInfo := newSegmentIndexBuildInfo()
	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})

	m := &indexMeta{
		catalog:        sc,
		keyLock:        lock.NewKeyLock[UniqueID](),
		segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
		segmentBuildInfo: indexBuildInfo,
	}
	segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxes.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})
	m.segmentIndexes.Insert(segID, segIdxes)
	return m
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
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
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

// see also: https://github.com/milvus-io/milvus/issues/21660
func TestUpdateSegmentIndexNotExists(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	assert.NotPanics(t, func() {
		m.updateSegmentIndex(&model.SegmentIndex{
			SegmentID: 1,
			IndexID:   2,
		})
	})

	assert.Equal(t, 1, m.segmentIndexes.Len())
	segmentIdx, ok := m.segmentIndexes.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 1, segmentIdx.Len())
	_, ok = segmentIdx.Get(2)
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

		m.segmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      buildID,
			SegmentID:    segID,
			PartitionID:  partID,
			CollectionID: collID,
		})

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
		err := m.RemoveIndex(context.TODO(), collID, indexID)
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

		err := m.RemoveIndex(context.TODO(), collID, indexID)
		assert.NoError(t, err)
		assert.Equal(t, len(m.indexes), 0)
	})
}

func TestRemoveSegmentIndex(t *testing.T) {
	t.Run("drop no exist segment index", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		m := newSegmentIndexMeta(catalog)
		err := m.RemoveSegmentIndex(context.TODO(), 0)

		assert.NoError(t, err)
	})

	t.Run("drop segment index fail", func(t *testing.T) {
		expectedErr := errors.New("error")
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().
			DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(expectedErr)
		m := newSegmentIndexMeta(catalog)
		err := m.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID:    3,
			CollectionID: 1,
			PartitionID:  2,
			NumRows:      1024,
			IndexID:      1,
			BuildID:      4,
		})
		assert.NoError(t, err)

		err = m.RemoveSegmentIndex(context.TODO(), 4)
		assert.Error(t, err)
		assert.EqualError(t, err, "error")
	})

	t.Run("remove segment index ok", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().
			DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		m := newSegmentIndexMeta(catalog)
		err := m.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID:    3,
			CollectionID: 1,
			PartitionID:  2,
			NumRows:      1024,
			IndexID:      1,
			BuildID:      4,
		})
		assert.NoError(t, err)

		err = m.RemoveSegmentIndex(context.TODO(), 4)
		assert.NoError(t, err)

		assert.Equal(t, 0, m.segmentIndexes.Len())
		assert.Equal(t, len(m.segmentBuildInfo.List()), 0)
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

func TestBuildIndexTaskStatsJSON(t *testing.T) {
	im := &indexMeta{segmentBuildInfo: newSegmentIndexBuildInfo()}
	si1 := &model.SegmentIndex{
		BuildID:             1,
		CollectionID:        100,
		SegmentID:           1000,
		IndexID:             10,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IndexSerializedSize: 1024,
		IndexVersion:        1,
		CreatedUTCTime:      uint64(time.Now().Unix()),
	}
	si2 := &model.SegmentIndex{
		BuildID:             2,
		CollectionID:        101,
		SegmentID:           1001,
		IndexID:             11,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IndexSerializedSize: 2048,
		IndexVersion:        1,
		CreatedUTCTime:      uint64(time.Now().Unix()),
	}

	actualJSON := im.TaskStatsJSON()
	assert.Equal(t, "[]", actualJSON)

	im.segmentBuildInfo.Add(si1)
	im.segmentBuildInfo.Add(si2)

	assert.Equal(t, 2, len(im.segmentBuildInfo.List()))
	ret1, ok := im.segmentBuildInfo.Get(si1.BuildID)
	assert.True(t, ok)
	assert.EqualValues(t, si1, ret1)

	expectedTasks := []*metricsinfo.IndexTaskStats{
		newIndexTaskStats(si1),
		newIndexTaskStats(si2),
	}
	expectedJSON, err := json.Marshal(expectedTasks)
	assert.NoError(t, err)

	actualJSON = im.TaskStatsJSON()
	assert.JSONEq(t, string(expectedJSON), actualJSON)

	im.segmentBuildInfo.Remove(si1.BuildID)
	assert.Equal(t, 1, len(im.segmentBuildInfo.List()))
}

func TestSegmentBuildInfo_AddForRecovery(t *testing.T) {
	t.Run("lru not full, all states inserted", func(t *testing.T) {
		info := newSegmentIndexBuildInfo()
		finished := &model.SegmentIndex{BuildID: 1, IndexState: commonpb.IndexState_Finished}
		inProgress := &model.SegmentIndex{BuildID: 2, IndexState: commonpb.IndexState_InProgress}

		info.AddForRecovery(finished)
		info.AddForRecovery(inProgress)

		assert.Equal(t, 2, len(info.List()))
		assert.Equal(t, 2, len(info.GetTaskStats()))
	})

	t.Run("lru full, finished tasks skipped", func(t *testing.T) {
		info := newSegmentIndexBuildInfo()
		// Fill the LRU to capacity with in-progress tasks.
		for i := int64(0); i < taskStatsLRUCapacity; i++ {
			info.AddForRecovery(&model.SegmentIndex{BuildID: i, IndexState: commonpb.IndexState_InProgress})
		}
		assert.Equal(t, taskStatsLRUCapacity, info.taskStats.Len())

		// A finished task should be skipped when LRU is full.
		finished := &model.SegmentIndex{BuildID: taskStatsLRUCapacity + 1, IndexState: commonpb.IndexState_Finished}
		info.AddForRecovery(finished)

		_, ok := info.Get(finished.BuildID)
		assert.True(t, ok, "buildID2SegmentIndex should still contain the entry")
		assert.Equal(t, taskStatsLRUCapacity, info.taskStats.Len(), "LRU size should not grow")
	})

	t.Run("lru full, unfinished tasks still inserted", func(t *testing.T) {
		info := newSegmentIndexBuildInfo()
		for i := int64(0); i < taskStatsLRUCapacity; i++ {
			info.AddForRecovery(&model.SegmentIndex{BuildID: i, IndexState: commonpb.IndexState_InProgress})
		}
		assert.Equal(t, taskStatsLRUCapacity, info.taskStats.Len())

		// An unfinished task should still be inserted (evicting the oldest).
		unissued := &model.SegmentIndex{BuildID: taskStatsLRUCapacity + 1, IndexState: commonpb.IndexState_Unissued}
		info.AddForRecovery(unissued)

		_, ok := info.Get(unissued.BuildID)
		assert.True(t, ok)
		// LRU size stays at capacity because the oldest entry was evicted.
		assert.Equal(t, taskStatsLRUCapacity, info.taskStats.Len())
	})
}

func TestMeta_GetIndexJSON(t *testing.T) {
	m := &indexMeta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			1: {
				1: &model.Index{
					CollectionID: 1,
					FieldID:      1,
					IndexID:      1,
					IndexName:    "index1",
					IsDeleted:    false,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "param1",
							Value: "value1",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "param1",
							Value: "value1",
						},
					},
					IsAutoIndex: true,
					UserIndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "param1",
							Value: "value1",
						},
					},
				},
			},
		},
	}

	actualJSON := m.GetIndexJSON(0)
	var actualIndex []*metricsinfo.Index
	err := json.Unmarshal([]byte(actualJSON), &actualIndex)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), actualIndex[0].CollectionID)
	assert.Equal(t, int64(1), actualIndex[0].FieldID)
	assert.Equal(t, int64(1), actualIndex[0].IndexID)
	assert.Equal(t, map[string]string{"param1": "value1"}, actualIndex[0].IndexParams)
	assert.Equal(t, map[string]string{"param1": "value1"}, actualIndex[0].UserIndexParams)
}

func TestMeta_GetSegmentIndexStatus(t *testing.T) {
	var (
		collID  = UniqueID(1)
		partID  = UniqueID(2)
		indexID = UniqueID(10)
		fieldID = UniqueID(100)
		segID   = UniqueID(1000)
		buildID = UniqueID(10000)
	)

	m := &indexMeta{}
	m.indexes = map[UniqueID]map[UniqueID]*model.Index{
		collID: {
			indexID: {
				CollectionID: collID,
				FieldID:      fieldID,
				IndexID:      indexID,
				IndexName:    "test_index",
				IsDeleted:    false,
			},
		},
	}
	m.segmentIndexes = typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10250,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              1,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      12,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})
	m.segmentIndexes.Insert(segID, segIdx)

	t.Run("index exists", func(t *testing.T) {
		isIndexed, segmentIndexes := m.GetSegmentIndexedFields(collID, segID)
		assert.True(t, isIndexed)
		assert.Len(t, segmentIndexes, 1)
		assert.Equal(t, indexID, segmentIndexes[0].IndexID)
		assert.Equal(t, buildID, segmentIndexes[0].BuildID)
	})

	t.Run("index does not exist", func(t *testing.T) {
		isIndexed, segmentIndexes := m.GetSegmentIndexedFields(collID+1, segID)
		assert.False(t, isIndexed)
		assert.Empty(t, segmentIndexes)
	})

	t.Run("segment does not exist", func(t *testing.T) {
		isIndexed, segmentIndexes := m.GetSegmentIndexedFields(collID, segID+1)
		assert.False(t, isIndexed)
		assert.Empty(t, segmentIndexes)
	})
}

func TestCheckParams(t *testing.T) {
	t.Run("same params without warmup", func(t *testing.T) {
		fieldIndex := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		req := &indexpb.CreateIndexRequest{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		assert.True(t, checkParams(fieldIndex, req))
	})

	t.Run("same params with warmup in request only", func(t *testing.T) {
		// This test verifies the fix for the idempotency bug
		// When index is created, WarmupKey is removed from stored TypeParams
		// But when checking idempotency, WarmupKey should also be ignored in request
		fieldIndex := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
				// Note: WarmupKey is NOT in stored TypeParams because it was removed during CreateIndex
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		req := &indexpb.CreateIndexRequest{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
				{Key: common.WarmupKey, Value: "sync"}, // WarmupKey in request should be ignored
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		// Before fix: this would return false because len(metaTypeParams) != len(reqTypeParams)
		// After fix: this should return true because WarmupKey is filtered from both
		assert.True(t, checkParams(fieldIndex, req))
	})

	t.Run("same params with warmup in different order", func(t *testing.T) {
		fieldIndex := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		req := &indexpb.CreateIndexRequest{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: "sync"},
				{Key: common.DimKey, Value: "128"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "HNSW"},
				{Key: common.MetricTypeKey, Value: "L2"},
			},
		}
		assert.True(t, checkParams(fieldIndex, req))
	})

	t.Run("different params", func(t *testing.T) {
		fieldIndex := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		req := &indexpb.CreateIndexRequest{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "256"}, // Different dimension
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		assert.False(t, checkParams(fieldIndex, req))
	})

	t.Run("mmap enabled should be ignored", func(t *testing.T) {
		fieldIndex := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		req := &indexpb.CreateIndexRequest{
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
				{Key: common.MmapEnabledKey, Value: "true"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: "L2"},
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		assert.True(t, checkParams(fieldIndex, req))
	})
}

// TestStoredIndexFilesSizeMetric exercises the stored_index_files_size gauge
// invariants (issue #49024).
func TestStoredIndexFilesSizeMetric(t *testing.T) {
	var (
		collID  = UniqueID(1)
		partID  = UniqueID(2)
		indexID = UniqueID(10)
		segID   = UniqueID(1000)
		buildID = UniqueID(10000)
	)
	collIDLabel := fmt.Sprintf("%d", collID)
	var serializedSize uint64 = 4096

	// helper: builds a fresh indexMeta with one segment index (size=0, InProgress).
	setup := func(t *testing.T) *indexMeta {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		indexBuildInfo := newSegmentIndexBuildInfo()
		indexBuildInfo.Add(&model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			NumRows:             1025,
			IndexID:             indexID,
			BuildID:             buildID,
			IndexState:          commonpb.IndexState_InProgress,
			IndexSerializedSize: 0,
		})

		m := &indexMeta{
			ctx:              context.Background(),
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: indexBuildInfo,
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			IndexID:      indexID,
			BuildID:      buildID,
		})
		m.segmentIndexes.Insert(segID, segIdxes)
		return m
	}

	// FinishTask must add the real serialized size (not 0 from stale pointer).
	t.Run("FinishTask adds real serialized size", func(t *testing.T) {
		m := setup(t)

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(serializedSize), val,
			"metric should equal serialized size after FinishTask")
	})

	// Segment dropped (e.g. compaction) while the index definition is still
	// alive. RemoveSegmentIndex is the only place the size gets reclaimed.
	t.Run("RemoveSegmentIndex subtracts when index alive (segment drop)", func(t *testing.T) {
		m := setup(t)

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		err = m.RemoveSegmentIndex(context.TODO(), buildID)
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val,
			"RemoveSegmentIndex must subtract when index definition is still alive")
	})

	// Retry idempotency: calling FinishTask twice with the same size must not
	// double-count the metric.
	t.Run("FinishTask retry is idempotent", func(t *testing.T) {
		m := setup(t)

		for i := 0; i < 2; i++ {
			err := m.FinishTask(&workerpb.IndexTaskInfo{
				BuildID:        buildID,
				State:          commonpb.IndexState_Finished,
				IndexFileKeys:  []string{"file1", "file2"},
				SerializedSize: serializedSize,
			})
			assert.NoError(t, err)
		}

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(serializedSize), val,
			"metric must equal serialized size exactly once, not doubled")
	})

	// Index version upgrade: FinishTask called again with a different size
	// should adjust the metric by the delta.
	t.Run("FinishTask with new size adjusts metric by delta", func(t *testing.T) {
		m := setup(t)

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		var newSize uint64 = 8192
		err = m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2", "file3"},
			SerializedSize: newSize,
		})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(newSize), val,
			"metric should reflect the updated size after version upgrade")
	})

	// Index rebuild with a smaller size: the delta is negative, Gauge.Add
	// accepts negative values so this must decrease the metric, not panic.
	t.Run("FinishTask with smaller size decreases metric", func(t *testing.T) {
		m := setup(t)

		var largeSize uint64 = 8192
		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2", "file3"},
			SerializedSize: largeSize,
		})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(largeSize), val)

		var smallerSize uint64 = 2048
		err = m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1"},
			SerializedSize: smallerSize,
		})
		assert.NoError(t, err)

		val = testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(smallerSize), val,
			"metric should decrease when new index is smaller")
	})

	// DropCollection followed by GC RemoveSegmentIndex must not recreate a
	// negative metric. CleanupDataCoordWithCollectionID deletes the time
	// series, and the collection is removed from m.indexes, so
	// RemoveSegmentIndex skips the gauge subtraction.
	t.Run("DropCollection then GC Remove must not go negative", func(t *testing.T) {
		m := setup(t)

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(serializedSize), val)

		// Simulate DropCollection: metric time series is deleted entirely,
		// and collection index meta is removed.
		metrics.CleanupDataCoordWithCollectionID(collID)
		delete(m.indexes, collID)

		val = testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val)

		// GC removes the segment index. RemoveSegmentIndex sees the
		// collection is gone from m.indexes, so it skips gauge subtraction.
		err = m.RemoveSegmentIndex(context.TODO(), buildID)
		assert.NoError(t, err)

		val = testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val,
			"metric must not go negative after GC on a dropped collection")
	})

	// MarkIndexAsDeleted subtracts gauge immediately — alive index only.
	t.Run("MarkIndexAsDeleted subtracts gauge immediately", func(t *testing.T) {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil)

		m := &indexMeta{
			ctx:              context.Background(),
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  partID,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_InProgress,
		}
		m.segmentBuildInfo.Add(segIdx)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, segIdx)
		m.segmentIndexes.Insert(segID, segIdxes)

		// Build finishes — gauge goes up.
		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(serializedSize), val)

		// Drop the index — gauge goes to 0 immediately.
		err = m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID})
		assert.NoError(t, err)

		val = testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val,
			"gauge must be 0 immediately after MarkIndexAsDeleted")
	})

	// After MarkIndexAsDeleted subtracts, GC's RemoveSegmentIndex must not
	// change the gauge (GC never touches it).
	t.Run("GC after MarkIndexAsDeleted is gauge-neutral", func(t *testing.T) {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		m := &indexMeta{
			ctx:              context.Background(),
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  partID,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_InProgress,
		}
		m.segmentBuildInfo.Add(segIdx)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, segIdx)
		m.segmentIndexes.Insert(segID, segIdxes)

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		// Drop the index — gauge goes to 0.
		err = m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val)

		// GC removes the segment index — gauge must stay at 0, not go negative.
		err = m.RemoveSegmentIndex(context.TODO(), buildID)
		assert.NoError(t, err)

		val = testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val,
			"gauge must remain 0 after GC removes segment index post-drop")
	})

	// copy_segment_task inserts segment indexes that already arrived in Finished
	// state with non-zero IndexSerializedSize. FinishTask will not be called for
	// them, so AddSegmentIndex must count them into the gauge itself.
	t.Run("AddSegmentIndex in Finished state counts preloaded size", func(t *testing.T) {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)

		m := &indexMeta{
			ctx:              context.Background(),
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		err := m.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			IndexID:             indexID,
			BuildID:             buildID,
			IndexState:          commonpb.IndexState_Finished,
			IndexSerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(serializedSize), val,
			"AddSegmentIndex must count preloaded Finished indexes")
	})

	// Preloaded Finished index dropped via index deletion — AddSegmentIndex adds
	// the size, MarkIndexAsDeleted subtracts it, net zero.
	t.Run("AddSegmentIndex preloaded then MarkIndexAsDeleted nets zero", func(t *testing.T) {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil)

		m := &indexMeta{
			ctx:              context.Background(),
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		err := m.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			IndexID:             indexID,
			BuildID:             buildID,
			IndexState:          commonpb.IndexState_Finished,
			IndexSerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		err = m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val,
			"MarkIndexAsDeleted must subtract preloaded Finished bytes")
	})

	// FinishTask after MarkIndexAsDeleted must not re-add bytes for a dropped index.
	// Regression: FinishTask and MarkIndexAsDeleted use different locks (keyLock vs
	// fieldIndexLock), so a late FinishTask could race and add delta for a deleted index.
	t.Run("FinishTask after MarkIndexAsDeleted does not re-add gauge", func(t *testing.T) {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil)

		m := &indexMeta{
			ctx:              context.Background(),
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  partID,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_InProgress,
		}
		m.segmentBuildInfo.Add(segIdx)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, segIdx)
		m.segmentIndexes.Insert(segID, segIdxes)

		// Drop the index BEFORE the build finishes — gauge stays 0.
		err := m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID})
		assert.NoError(t, err)

		val := testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val)

		// Late FinishTask arrives — must NOT add to gauge because the index is deleted.
		err = m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: serializedSize,
		})
		assert.NoError(t, err)

		val = testutil.ToFloat64(metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", collIDLabel))
		assert.Equal(t, float64(0), val,
			"FinishTask after index drop must not re-add bytes to gauge")
	})
}

func TestIndexMeta_UpdateIndexRowsProgressMetrics(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	m := newSegmentIndexMeta(catalog)

	const (
		testCollID  = int64(200)
		testIndexID = int64(10)
	)
	const testIndexName = "vector_idx"

	// Register a live index so IsIndexExist returns true.
	m.indexes[testCollID] = map[UniqueID]*model.Index{
		testIndexID: {
			CollectionID: testCollID,
			IndexID:      testIndexID,
			IndexName:    testIndexName,
		},
	}

	t.Run("empty build info emits no metrics", func(t *testing.T) {
		metrics.IndexRowsProgress.Reset()
		m.updateIndexRowsProgressMetrics()
		assert.Equal(t, 0, testutil.CollectAndCount(metrics.IndexRowsProgress))
	})

	t.Run("finished and pending segments are counted correctly", func(t *testing.T) {
		metrics.IndexRowsProgress.Reset()

		m.segmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      1,
			CollectionID: testCollID,
			IndexID:      testIndexID,
			NumRows:      1000,
			IndexState:   commonpb.IndexState_Finished,
		})
		m.segmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      2,
			CollectionID: testCollID,
			IndexID:      testIndexID,
			NumRows:      400,
			IndexState:   commonpb.IndexState_InProgress,
		})
		m.segmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      3,
			CollectionID: testCollID,
			IndexID:      testIndexID,
			NumRows:      100,
			IndexState:   commonpb.IndexState_Unissued,
		})

		m.updateIndexRowsProgressMetrics()

		collIDStr := fmt.Sprint(testCollID)
		assert.Equal(t, float64(1500), testutil.ToFloat64(metrics.IndexRowsProgress.WithLabelValues(collIDStr, testIndexName, "total_rows")))
		assert.Equal(t, float64(1000), testutil.ToFloat64(metrics.IndexRowsProgress.WithLabelValues(collIDStr, testIndexName, "indexed_rows")))
		assert.Equal(t, float64(500), testutil.ToFloat64(metrics.IndexRowsProgress.WithLabelValues(collIDStr, testIndexName, "pending_index_rows")))

		metrics.IndexRowsProgress.Reset()
	})

	t.Run("deleted segment indexes are skipped", func(t *testing.T) {
		metrics.IndexRowsProgress.Reset()
		m2 := newSegmentIndexMeta(catalog)
		m2.indexes[testCollID] = map[UniqueID]*model.Index{
			testIndexID: {CollectionID: testCollID, IndexID: testIndexID, IndexName: testIndexName},
		}
		m2.segmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      4,
			CollectionID: testCollID,
			IndexID:      testIndexID,
			NumRows:      999,
			IndexState:   commonpb.IndexState_Finished,
			IsDeleted:    true,
		})
		m2.updateIndexRowsProgressMetrics()
		assert.Equal(t, 0, testutil.CollectAndCount(metrics.IndexRowsProgress))
		metrics.IndexRowsProgress.Reset()
	})

	t.Run("segment for unknown index is skipped", func(t *testing.T) {
		metrics.IndexRowsProgress.Reset()
		m3 := newSegmentIndexMeta(catalog)
		// no indexes registered — IsIndexExist will return false
		m3.segmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      5,
			CollectionID: testCollID,
			IndexID:      testIndexID,
			NumRows:      500,
			IndexState:   commonpb.IndexState_Finished,
		})
		m3.updateIndexRowsProgressMetrics()
		assert.Equal(t, 0, testutil.CollectAndCount(metrics.IndexRowsProgress))
		metrics.IndexRowsProgress.Reset()
	})
}
