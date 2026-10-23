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
package index

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/json"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReloadFromKV(t *testing.T) {
	t.Run("ListIndexes_fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, errors.New("mock"))
		catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		_, err := NewIndexMeta(context.TODO(), catalog, []int64{0})
		assert.Error(t, err)
	})

	t.Run("ListSegmentIndexes_fails", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, errors.New("mock"))

		_, err := NewIndexMeta(context.TODO(), catalog, []int64{0})
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

		mt, err := NewIndexMeta(context.TODO(), catalog, []int64{0})
		assert.NoError(t, err)
		assert.NotNil(t, mt)
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

		mt, err := NewIndexMeta(context.TODO(), catalog, []int64{collID})
		assert.NoError(t, err)
		assert.NotNil(t, mt)

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
	m := NewSegmentIndexMeta(catalog)

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
		m.Indexes[collID] = map[UniqueID]*model.Index{
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
		assert.ErrorIs(t, err, ErrIndexOperationIgnored)
		assert.Zero(t, tmpIndexID)
	})

	t.Run("user index params not consistent", func(t *testing.T) {
		m.Indexes[collID] = map[UniqueID]*model.Index{
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
		m.Indexes[collID] = map[UniqueID]*model.Index{
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
		assert.ErrorIs(t, err, ErrIndexOperationIgnored)
		assert.Zero(t, tmpIndexID)
		newIndexParams := req.GetIndexParams()
		assert.Equal(t, len(newIndexParams), 1)
		assert.Equal(t, newIndexParams[0].Key, common.IndexTypeKey)
		assert.Equal(t, newIndexParams[0].Value, "INVERTED")
	})

	t.Run("ngram index param rewrite", func(t *testing.T) {
		m.Indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				CollectionID: collID,
				FieldID:      fieldID,
				IndexID:      indexID,
				IndexName:    indexName,
				CreateTime:   10,
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "NGRAM"},
				},
				UserIndexParams: userIndexParams,
			},
		}
		ngramReq := &indexpb.CreateIndexRequest{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "HYBRID"},
			},
			IsAutoIndex:     true,
			UserIndexParams: userIndexParams,
		}

		indexID, err := m.CanCreateIndex(ngramReq, false)
		assert.ErrorIs(t, err, ErrIndexOperationIgnored)
		assert.Zero(t, indexID)
		require.Len(t, ngramReq.GetIndexParams(), 1)
		assert.Equal(t, "NGRAM", ngramReq.GetIndexParams()[0].GetValue())
	})
}

// TestMeta_CanCreateIndex_BoundAutoIndexEquivalence: the add-function-field DDL
// persists an AutoIndex-resolved bound index in the create_index convention
// (UserIndexParams = AUTOINDEX + final metric); a later create_index(AUTOINDEX)
// on the same field must dedupe as idempotent instead of hitting the
// one-distinct-index-per-field conflict.
func TestMeta_CanCreateIndex_BoundAutoIndexEquivalence(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("CreateIndex", mock.Anything, mock.Anything).Return(nil)
	m := NewSegmentIndexMeta(catalog)

	resolvedIndexParams := []*commonpb.KeyValuePair{
		{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"},
		{Key: common.MetricTypeKey, Value: "BM25"},
		{Key: "bm25_k1", Value: "1.2"},
		{Key: "bm25_b", Value: "0.75"},
		{Key: "bm25_avgdl", Value: "100"},
	}
	autoIndexUserParams := []*commonpb.KeyValuePair{
		{Key: common.IndexTypeKey, Value: common.AutoIndexName},
		{Key: common.MetricTypeKey, Value: "BM25"},
	}
	// Shape persisted by rootcoord prepareBoundFieldIndex on the AutoIndex path.
	boundIndex := &model.Index{
		CollectionID:    UniqueID(1),
		FieldID:         UniqueID(101),
		IndexID:         UniqueID(10),
		IndexName:       "sparse",
		IndexParams:     resolvedIndexParams,
		UserIndexParams: autoIndexUserParams,
		IsAutoIndex:     false,
	}
	err := m.CreateIndex(context.TODO(), boundIndex)
	assert.NoError(t, err)

	// What proxy sends for a later create_index(AUTOINDEX, metric_type=BM25).
	req := &indexpb.CreateIndexRequest{
		CollectionID:                     UniqueID(1),
		FieldID:                          UniqueID(101),
		IndexName:                        "sparse",
		IndexParams:                      resolvedIndexParams,
		UserIndexParams:                  autoIndexUserParams,
		IsAutoIndex:                      false,
		UserAutoindexMetricTypeSpecified: true,
	}
	tmpIndexID, err := m.CanCreateIndex(req, false)
	assert.ErrorIs(t, err, ErrIndexOperationIgnored)
	assert.Zero(t, tmpIndexID)

	// A different explicit index on the same field remains a conflict.
	conflictReq := &indexpb.CreateIndexRequest{
		CollectionID: UniqueID(1),
		FieldID:      UniqueID(101),
		IndexName:    "sparse",
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_WAND"},
			{Key: common.MetricTypeKey, Value: "IP"},
		},
		UserIndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_WAND"},
			{Key: common.MetricTypeKey, Value: "IP"},
		},
	}
	tmpIndexID, err = m.CanCreateIndex(conflictReq, false)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrIndexOperationIgnored)
	assert.Zero(t, tmpIndexID)
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

	m := NewSegmentIndexMeta(catalog)

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
		assert.ErrorIs(t, err, ErrIndexOperationIgnored)
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
		assert.ErrorIs(t, err, ErrIndexOperationIgnored)
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
		m.Indexes[collID][indexID].IsDeleted = true
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

	m := NewSegmentIndexMeta(catalogmocks.NewDataCoordCatalog(t))

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
		m.Indexes[collID] = map[UniqueID]*model.Index{
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
		m.Indexes[collID][indexID].IsDeleted = true
		has, _ := m.HasSameReq(req)
		assert.False(t, has)
	})
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

		m := NewSegmentIndexMeta(sc)
		err := m.CreateIndex(context.TODO(), indexModel)
		assert.NoError(t, err)
	})

	t.Run("save fail", func(t *testing.T) {
		ec := catalogmocks.NewDataCoordCatalog(t)
		ec.On("CreateIndex",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))

		m := NewSegmentIndexMeta(ec)
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

	m := NewSegmentIndexMeta(ec)
	m.SegmentIndexes = typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	m.SegmentIndexes.Insert(1, typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]())

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
		m.Catalog = sc
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

	m := NewSegmentIndexMeta(&datacoord.Catalog{MetaKv: metakv})
	t.Run("no indexes", func(t *testing.T) {
		indexID2CreateTS := m.GetIndexIDByName(collID, indexName)
		assert.Equal(t, 0, len(indexID2CreateTS))
	})

	t.Run("success", func(t *testing.T) {
		m.Indexes[collID] = map[UniqueID]*model.Index{
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

	m := NewSegmentIndexMeta(&datacoord.Catalog{MetaKv: metakv})
	m.SegmentIndexes = typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	m.SegmentIndexes.Insert(1, typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]())

	t.Run("collection has no index", func(t *testing.T) {
		state := m.GetSegmentIndexState(collID, segID, indexID)
		assert.Equal(t, commonpb.IndexState_IndexStateNone, state.GetState())
		assert.Contains(t, state.GetFailReason(), "collection not exist with ID")
	})

	t.Run("meta not saved yet", func(t *testing.T) {
		m.Indexes[collID] = map[UniqueID]*model.Index{
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

func TestMeta_GetSegmentsIndexStatesUsesScalarVersionForNGRAM(t *testing.T) {
	const (
		collID  = UniqueID(1)
		indexID = UniqueID(10)
		segID   = UniqueID(1000)
	)

	segmentIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	indexesForSegment := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	indexesForSegment.Insert(indexID, &model.SegmentIndex{
		SegmentID:                 segID,
		IndexID:                   indexID,
		IndexType:                 "NGRAM",
		CurrentIndexVersion:       11,
		CurrentScalarIndexVersion: 7,
		IndexState:                commonpb.IndexState_Finished,
	})
	segmentIndexes.Insert(segID, indexesForSegment)

	m := &IndexMeta{
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					CollectionID: collID,
					IndexID:      indexID,
					IndexName:    "ngram_idx",
				},
			},
		},
		SegmentIndexes: segmentIndexes,
	}

	states := m.GetSegmentsIndexStates(collID, []UniqueID{segID})
	require.Contains(t, states, segID)
	require.Contains(t, states[segID], indexID)
	assert.Equal(t, int32(7), states[segID][indexID].GetIndexVersion())
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

	m := NewSegmentIndexMeta(nil)
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

	m.SegmentIndexes.Insert(segID, segIdxes)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
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

	m.SegmentBuildInfo.Add(&model.SegmentIndex{
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
		m.Indexes[collID][deletedIndexID1] = &model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      deletedIndexID1,
			IndexName:    "old_idx_1",
			IsDeleted:    true,
		}
		m.Indexes[collID][deletedIndexID2] = &model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      deletedIndexID2,
			IndexName:    "old_idx_2",
			IsDeleted:    true,
		}

		segments := m.GetIndexedSegments(collID, []int64{segID}, []int64{fieldID})
		assert.Len(t, segments, 1, "segment should be indexed even with deleted index entries")

		// Cleanup
		delete(m.Indexes[collID], deletedIndexID1)
		delete(m.Indexes[collID], deletedIndexID2)
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

	m := NewSegmentIndexMeta(sc)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
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
		m.Catalog = ec
		err := m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{indexID, indexID + 1, indexID + 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m.Catalog = sc
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
	im := createIndexMeta(catalog)

	t.Run("success", func(t *testing.T) {
		segIndexes := im.GetSegmentIndexes(meta.CollID, meta.SegID)
		assert.Equal(t, 1, len(segIndexes))
	})

	t.Run("segment not exist", func(t *testing.T) {
		segIndexes := im.GetSegmentIndexes(meta.CollID, meta.SegID+100)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist- segment index empty", func(t *testing.T) {
		m := NewSegmentIndexMeta(nil)
		segIndexes := m.GetSegmentIndexes(meta.CollID, meta.SegID)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("no index exist- field index empty", func(t *testing.T) {
		m := NewSegmentIndexMeta(nil)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(meta.IndexID, &model.SegmentIndex{})
		m.SegmentIndexes.Insert(meta.SegID, segIdxes)

		segIndexes := m.GetSegmentIndexes(meta.CollID, 1)
		assert.Equal(t, 0, len(segIndexes))
	})

	t.Run("index exists", func(t *testing.T) {
		m := &IndexMeta{
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				meta.CollID: {
					meta.IndexID: {
						TenantID:        "",
						CollectionID:    meta.CollID,
						FieldID:         meta.FieldID,
						IndexID:         meta.IndexID,
						IndexName:       meta.IndexName,
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
		segIdxes.Insert(meta.IndexID, &model.SegmentIndex{
			CollectionID: meta.CollID,
			SegmentID:    meta.SegID,
			IndexID:      meta.IndexID,
			IndexState:   commonpb.IndexState_Finished,
		})
		m.SegmentIndexes.Insert(meta.SegID, segIdxes)
		segIndexes := m.GetSegmentIndexes(meta.CollID, meta.SegID)
		assert.Equal(t, 1, len(segIndexes))

		segIdx, ok := segIndexes[meta.IndexID]
		assert.True(t, ok)
		assert.NotNil(t, segIdx)
	})
}

func TestMeta_GetAllSegmentIndexes(t *testing.T) {
	assert.Nil(t, (&IndexMeta{}).GetAllSegmentIndexes(meta.SegID))

	m := &IndexMeta{
		SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			meta.CollID: {
				meta.IndexID: {
					CollectionID: meta.CollID,
					FieldID:      meta.FieldID,
					IndexID:      meta.IndexID,
					IndexName:    meta.IndexName,
					IsDeleted:    true,
				},
			},
		},
	}
	segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxes.Insert(meta.IndexID, &model.SegmentIndex{
		CollectionID: meta.CollID,
		SegmentID:    meta.SegID,
		IndexID:      meta.IndexID,
		BuildID:      meta.BuildID,
		IndexState:   commonpb.IndexState_Finished,
	})
	m.SegmentIndexes.Insert(meta.SegID, segIdxes)

	assert.Empty(t, m.GetSegmentIndexes(meta.CollID, meta.SegID))
	segIndexes := m.GetAllSegmentIndexes(meta.SegID)
	require.Len(t, segIndexes, 1)
	assert.Equal(t, meta.BuildID, segIndexes[0].BuildID)
	origin, ok := segIdxes.Get(meta.IndexID)
	require.True(t, ok)
	assert.False(t, origin == segIndexes[0])
}

func TestMeta_GetFieldIDByIndexID(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
		meta.CollID: {
			meta.IndexID: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID,
				IndexID:         meta.IndexID,
				IndexName:       meta.IndexName,
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
		fID := m.GetFieldIDByIndexID(meta.CollID, meta.IndexID)
		assert.Equal(t, meta.FieldID, fID)
	})

	t.Run("fail", func(t *testing.T) {
		fID := m.GetFieldIDByIndexID(meta.CollID, meta.IndexID+1)
		assert.Equal(t, UniqueID(0), fID)
	})
}

func TestMeta_GetIndexNameByID(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
		meta.CollID: {
			meta.IndexID: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID,
				IndexID:         meta.IndexID,
				IndexName:       meta.IndexName,
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
		iName := m.GetIndexNameByID(meta.CollID, meta.IndexID)
		assert.Equal(t, meta.IndexName, iName)
	})

	t.Run("fail", func(t *testing.T) {
		iName := m.GetIndexNameByID(meta.CollID, meta.IndexID+1)
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

	m := NewSegmentIndexMeta(nil)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
		meta.CollID: {
			meta.IndexID: {
				TenantID:     "",
				CollectionID: meta.CollID,
				FieldID:      meta.FieldID,
				IndexID:      meta.IndexID,
				IndexName:    meta.IndexName,
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
		tp := m.GetTypeParams(meta.CollID, meta.IndexID)
		assert.Equal(t, 1, len(tp))
	})

	t.Run("not exist", func(t *testing.T) {
		tp := m.GetTypeParams(meta.CollID, meta.IndexID+1)
		assert.Equal(t, 0, len(tp))

		tp = m.GetTypeParams(meta.CollID+1, meta.IndexID)
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

	m := NewSegmentIndexMeta(nil)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
		meta.CollID: {
			meta.IndexID: {
				TenantID:     "",
				CollectionID: meta.CollID,
				FieldID:      meta.FieldID,
				IndexID:      meta.IndexID,
				IndexName:    meta.IndexName,
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
		ip := m.GetIndexParams(meta.CollID, meta.IndexID)
		assert.Equal(t, 1, len(ip))
	})

	t.Run("not exist", func(t *testing.T) {
		ip := m.GetIndexParams(meta.CollID, meta.IndexID+1)
		assert.Equal(t, 0, len(ip))

		ip = m.GetIndexParams(meta.CollID+1, meta.IndexID)
		assert.Equal(t, 0, len(ip))
	})
}

func TestMeta_GetIndexJob(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	m.SegmentBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1025,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
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
		segIndex, exist := m.GetIndexJob(meta.BuildID)
		assert.True(t, exist)
		assert.NotNil(t, segIndex)
	})

	t.Run("not exist", func(t *testing.T) {
		segIndex, exist := m.GetIndexJob(meta.BuildID + 1)
		assert.False(t, exist)
		assert.Nil(t, segIndex)
	})
}

func TestMeta_IsIndexExist(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
		meta.CollID: {
			meta.IndexID: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID,
				IndexID:         meta.IndexID,
				IndexName:       meta.IndexName,
				IsDeleted:       false,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
			meta.IndexID + 1: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID,
				IndexID:         meta.IndexID + 1,
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
		exist := m.IsIndexExist(meta.CollID, meta.IndexID)
		assert.True(t, exist)
	})

	t.Run("not exist", func(t *testing.T) {
		exist := m.IsIndexExist(meta.CollID, meta.IndexID+1)
		assert.False(t, exist)

		exist = m.IsIndexExist(meta.CollID, meta.IndexID+2)
		assert.False(t, exist)

		exist = m.IsIndexExist(meta.CollID+1, meta.IndexID)
		assert.False(t, exist)
	})
}

func updateSegmentIndexMeta(t *testing.T) *IndexMeta {
	sc := catalogmocks.NewDataCoordCatalog(t)
	sc.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	indexBuildInfo := NewSegmentIndexBuildInfo()
	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1025,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})

	m := &IndexMeta{
		Catalog:        sc,
		KeyLock:        lock.NewKeyLock[UniqueID](),
		SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			meta.CollID: {
				meta.IndexID: {
					TenantID:        "",
					CollectionID:    meta.CollID,
					FieldID:         meta.FieldID,
					IndexID:         meta.IndexID,
					IndexName:       meta.IndexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		SegmentBuildInfo: indexBuildInfo,
	}
	segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxes.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1025,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	})
	m.SegmentIndexes.Insert(meta.SegID, segIdxes)
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
		err := m.UpdateVersion(meta.BuildID, meta.NodeID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		m.Catalog = ec
		err := m.UpdateVersion(meta.BuildID, meta.NodeID)
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.UpdateVersion(meta.BuildID+1, meta.NodeID)
		assert.Error(t, err)
	})
}

func TestMeta_FinishTask(t *testing.T) {
	m := updateSegmentIndexMeta(t)

	t.Run("success", func(t *testing.T) {
		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        meta.BuildID,
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
		m.Catalog = &datacoord.Catalog{
			MetaKv: metakv,
		}
		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        meta.BuildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: 1024,
			FailReason:     "",
		})
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:        meta.BuildID + 1,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file1", "file2"},
			SerializedSize: 1024,
			FailReason:     "",
		})
		assert.NoError(t, err)
	})
}

func TestMeta_FinishTaskIndexStorePathVersion(t *testing.T) {
	setup := func(t *testing.T, requested indexpb.IndexStorePathVersion, indexFileKeys []string) *IndexMeta {
		t.Helper()

		sc := catalogmocks.NewDataCoordCatalog(t)
		sc.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()

		segIdx := &model.SegmentIndex{
			SegmentID:             meta.SegID,
			CollectionID:          meta.CollID,
			PartitionID:           meta.PartID,
			NumRows:               1025,
			IndexID:               meta.IndexID,
			BuildID:               meta.BuildID,
			IndexState:            commonpb.IndexState_InProgress,
			IndexFileKeys:         indexFileKeys,
			IndexStorePathVersion: requested,
		}

		indexBuildInfo := NewSegmentIndexBuildInfo()
		indexBuildInfo.Add(model.CloneSegmentIndex(segIdx))

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          sc,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			SegmentBuildInfo: indexBuildInfo,
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				meta.CollID: {
					meta.IndexID: {
						CollectionID: meta.CollID,
						IndexID:      meta.IndexID,
					},
				},
			},
		}

		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(meta.IndexID, model.CloneSegmentIndex(segIdx))
		m.SegmentIndexes.Insert(meta.SegID, segIdxes)
		return m
	}

	t.Run("actual lower than requested persists actual version", func(t *testing.T) {
		m := setup(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, []string{"old-file"})

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:               meta.BuildID,
			State:                 commonpb.IndexState_Finished,
			IndexFileKeys:         []string{"file1", "file2"},
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		})
		assert.NoError(t, err)

		segIdx, ok := m.SegmentBuildInfo.Get(meta.BuildID)
		assert.True(t, ok)
		assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, segIdx.IndexStorePathVersion)
		assert.Equal(t, []string{"file1", "file2"}, segIdx.IndexFileKeys)
	})

	t.Run("actual higher than requested returns error and leaves metadata unchanged", func(t *testing.T) {
		m := setup(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, []string{"old-file"})

		err := m.FinishTask(&workerpb.IndexTaskInfo{
			BuildID:               meta.BuildID,
			State:                 commonpb.IndexState_Finished,
			IndexFileKeys:         []string{"file1", "file2"},
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		})
		assert.Error(t, err)

		segIdx, ok := m.SegmentBuildInfo.Get(meta.BuildID)
		assert.True(t, ok)
		assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, segIdx.IndexStorePathVersion)
		assert.Equal(t, []string{"old-file"}, segIdx.IndexFileKeys)
		assert.Equal(t, commonpb.IndexState_InProgress, segIdx.IndexState)
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
		err := m.BuildIndex(meta.BuildID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		m.Catalog = ec
		err := m.BuildIndex(meta.BuildID)
		assert.Error(t, err)
	})

	t.Run("not exist", func(t *testing.T) {
		err := m.BuildIndex(meta.BuildID + 1)
		assert.Error(t, err)
	})
}

// see also: https://github.com/milvus-io/milvus/issues/21660
func TestUpdateSegmentIndexNotExists(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	assert.NotPanics(t, func() {
		m.updateSegmentIndex(&model.SegmentIndex{
			SegmentID: 1,
			IndexID:   2,
		})
	})

	assert.Equal(t, 1, m.SegmentIndexes.Len())
	segmentIdx, ok := m.SegmentIndexes.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 1, segmentIdx.Len())
	_, ok = segmentIdx.Get(2)
	assert.True(t, ok)
}

func TestMeta_DeleteTask_Error(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	t.Run("segment index not found", func(t *testing.T) {
		err := m.DeleteTask(meta.BuildID)
		assert.NoError(t, err)
	})

	t.Run("segment update failed", func(t *testing.T) {
		ec := catalogmocks.NewDataCoordCatalog(t)
		ec.On("AlterSegmentIndexes",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		m.Catalog = ec

		m.SegmentBuildInfo.Add(&model.SegmentIndex{
			BuildID:      meta.BuildID,
			SegmentID:    meta.SegID,
			PartitionID:  meta.PartID,
			CollectionID: meta.CollID,
		})

		err := m.DeleteTask(meta.BuildID)
		assert.Error(t, err)
	})
}

func TestMeta_GetFieldIndexes(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
		meta.CollID: {
			meta.IndexID: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID,
				IndexID:         meta.IndexID,
				IndexName:       meta.IndexName,
				IsDeleted:       true,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
			meta.IndexID + 1: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID,
				IndexID:         meta.IndexID + 1,
				IndexName:       meta.IndexName,
				IsDeleted:       false,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
			meta.IndexID + 2: {
				TenantID:        "",
				CollectionID:    meta.CollID,
				FieldID:         meta.FieldID + 2,
				IndexID:         meta.IndexID + 2,
				IndexName:       meta.IndexName + "2",
				IsDeleted:       false,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		},
	}

	indexes := m.GetFieldIndexes(meta.CollID, meta.FieldID, "")
	assert.Equal(t, 1, len(indexes))
	assert.Equal(t, meta.IndexName, indexes[0].IndexName)
}

func TestRemoveIndex(t *testing.T) {
	t.Run("drop index fail", func(t *testing.T) {
		expectedErr := errors.New("error")
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().
			DropIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(expectedErr)

		m := NewSegmentIndexMeta(catalog)
		err := m.RemoveIndex(context.TODO(), meta.CollID, meta.IndexID)
		assert.Error(t, err)
		assert.EqualError(t, err, "error")
	})

	t.Run("remove index ok", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().
			DropIndex(mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		m := &IndexMeta{
			Catalog: catalog,
			Indexes: map[int64]map[int64]*model.Index{
				meta.CollID: {
					meta.IndexID: &model.Index{},
				},
			},
		}

		err := m.RemoveIndex(context.TODO(), meta.CollID, meta.IndexID)
		assert.NoError(t, err)
		assert.Equal(t, len(m.Indexes), 0)
	})
}

func TestRemoveSegmentIndex(t *testing.T) {
	t.Run("drop no exist segment index", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		m := NewSegmentIndexMeta(catalog)
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
		m := NewSegmentIndexMeta(catalog)
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

		m := NewSegmentIndexMeta(catalog)
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

		assert.Equal(t, 0, m.SegmentIndexes.Len())
		assert.Equal(t, len(m.SegmentBuildInfo.List()), 0)
	})
}

func TestIndexMeta_GetUnindexedSegments(t *testing.T) {
	catalog := &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}
	im := createIndexMeta(catalog)

	// normal case
	segmentIDs := make([]int64, 0, 11)
	for i := 0; i <= 10; i++ {
		segmentIDs = append(segmentIDs, meta.SegID+int64(i))
	}
	unindexed := im.GetUnindexedSegments(meta.CollID, segmentIDs)
	assert.Equal(t, 8, len(unindexed))

	// no index
	unindexed = im.GetUnindexedSegments(meta.CollID+1, segmentIDs)
	assert.Equal(t, 0, len(unindexed))
}

func TestBuildIndexTaskStatsJSON(t *testing.T) {
	im := &IndexMeta{SegmentBuildInfo: NewSegmentIndexBuildInfo()}
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

	im.SegmentBuildInfo.Add(si1)
	im.SegmentBuildInfo.Add(si2)

	assert.Equal(t, 2, len(im.SegmentBuildInfo.List()))
	ret1, ok := im.SegmentBuildInfo.Get(si1.BuildID)
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

	im.SegmentBuildInfo.Remove(si1.BuildID)
	assert.Equal(t, 1, len(im.SegmentBuildInfo.List()))
}

func TestSegmentBuildInfo_AddForRecovery(t *testing.T) {
	t.Run("lru not full, all states inserted", func(t *testing.T) {
		info := NewSegmentIndexBuildInfo()
		finished := &model.SegmentIndex{BuildID: 1, IndexState: commonpb.IndexState_Finished}
		inProgress := &model.SegmentIndex{BuildID: 2, IndexState: commonpb.IndexState_InProgress}

		info.AddForRecovery(finished)
		info.AddForRecovery(inProgress)

		assert.Equal(t, 2, len(info.List()))
		assert.Equal(t, 2, len(info.GetTaskStats()))
	})

	t.Run("lru full, finished tasks skipped", func(t *testing.T) {
		info := NewSegmentIndexBuildInfo()
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
		info := NewSegmentIndexBuildInfo()
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
	m := &IndexMeta{
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
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

	m := &IndexMeta{}
	m.Indexes = map[UniqueID]map[UniqueID]*model.Index{
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
	m.SegmentIndexes = typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
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
	m.SegmentIndexes.Insert(segID, segIdx)

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
	setup := func(t *testing.T) *IndexMeta {
		metrics.DataCoordStoredIndexFilesSize.Reset()

		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		indexBuildInfo := NewSegmentIndexBuildInfo()
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

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          catalog,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentBuildInfo: indexBuildInfo,
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			IndexID:      indexID,
			BuildID:      buildID,
		})
		m.SegmentIndexes.Insert(segID, segIdxes)
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
		delete(m.Indexes, collID)

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

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          catalog,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentBuildInfo: NewSegmentIndexBuildInfo(),
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  partID,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_InProgress,
		}
		m.SegmentBuildInfo.Add(segIdx)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, segIdx)
		m.SegmentIndexes.Insert(segID, segIdxes)

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

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          catalog,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentBuildInfo: NewSegmentIndexBuildInfo(),
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  partID,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_InProgress,
		}
		m.SegmentBuildInfo.Add(segIdx)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, segIdx)
		m.SegmentIndexes.Insert(segID, segIdxes)

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

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          catalog,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentBuildInfo: NewSegmentIndexBuildInfo(),
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          catalog,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentBuildInfo: NewSegmentIndexBuildInfo(),
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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

		m := &IndexMeta{
			Ctx:              context.Background(),
			Catalog:          catalog,
			KeyLock:          lock.NewKeyLock[UniqueID](),
			SegmentBuildInfo: NewSegmentIndexBuildInfo(),
			Indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {indexID: {CollectionID: collID, IndexID: indexID}},
			},
			SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		}

		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: collID,
			PartitionID:  partID,
			IndexID:      indexID,
			BuildID:      buildID,
			IndexState:   commonpb.IndexState_InProgress,
		}
		m.SegmentBuildInfo.Add(segIdx)
		segIdxes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdxes.Insert(indexID, segIdx)
		m.SegmentIndexes.Insert(segID, segIdxes)

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

// The feature switch controls completed-artifact publication only. Task
// lifecycle states remain durable in etcd in both modes.
func TestSegmentIndexTaskStatesAlwaysPersistToEtcd(t *testing.T) {
	assert.False(t, paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.GetAsBool())

	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("manifest=%t", enabled), func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, enabled)
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
			catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Times(2)
			m := &IndexMeta{
				Ctx:              context.TODO(),
				Catalog:          catalog,
				Indexes:          map[UniqueID]map[UniqueID]*model.Index{},
				KeyLock:          lock.NewKeyLock[UniqueID](),
				SegmentBuildInfo: NewSegmentIndexBuildInfo(),
				SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			}
			require.NoError(t, m.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
				CollectionID: 100, PartitionID: 10, SegmentID: 6002,
				IndexID: 600, BuildID: 6200,
			}))
			require.NoError(t, m.BuildIndex(6200))
			require.NoError(t, m.UpdateIndexState(6200, commonpb.IndexState_Failed, "failed"))
		})
	}
}
