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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// setAllowVectorIndexDropOnLoaded sets the datacoord admission switch for the
// test and restores the default afterwards.
func setAllowVectorIndexDropOnLoaded(t *testing.T, allow bool) {
	t.Helper()
	key := Params.DataCoordCfg.IndexAllowVectorIndexDropOnLoadedCollection.Key
	if allow {
		Params.Save(key, "true")
	} else {
		Params.Save(key, "false")
	}
	t.Cleanup(func() { Params.Reset(key) })
}

// newLoadedVectorIndexServer builds a datacoord whose one collection is loaded
// and carries one vector index, and the request that drops that index.
func newLoadedVectorIndexServer(t *testing.T) (*Server, *indexpb.DropIndexRequest) {
	const (
		collID    = UniqueID(1)
		fieldID   = UniqueID(10)
		indexID   = UniqueID(100)
		indexName = "vector_idx"
	)
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()
	b := broker.NewMockBroker(t)
	b.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:         merr.Success(),
		DbName:         "test_db",
		CollectionName: "test_collection",
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: fieldID, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		},
	}, nil)
	s := &Server{
		meta: &meta{
			catalog: catalog,
			indexMeta: &indexMeta{
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						indexID: {
							CollectionID: collID,
							FieldID:      fieldID,
							IndexID:      indexID,
							IndexName:    indexName,
							TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
							IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "IVF_FLAT"}},
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},
			segments: NewSegmentsInfo(),
		},
		broker:          b,
		allocator:       newMockAllocator(t),
		notifyIndexChan: make(chan UniqueID, 1),
	}
	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:        merr.Success(),
		CollectionIDs: []int64{collID},
	}, nil)
	s.mixCoord = mixCoord
	RegisterDDLCallbacks(s)
	s.stateCode.Store(commonpb.StateCode_Healthy)
	return s, &indexpb.DropIndexRequest{CollectionID: collID, IndexName: indexName}
}

func TestDropVectorIndexOnLoadedIsRefusedByDefault(t *testing.T) {
	initStreamingSystem(t)
	assert.False(t, Params.DataCoordCfg.IndexAllowVectorIndexDropOnLoadedCollection.GetAsBool())
	s, req := newLoadedVectorIndexServer(t)
	status, err := s.DropIndex(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, merr.Ok(status))
	assert.Contains(t, status.GetReason(), "vector index cannot be dropped on loaded collection")
	assert.NotEmpty(t, s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName()),
		"a refused drop must leave the index in place")
}

func TestDropVectorIndexOnLoadedIsRefusedWhenConfiguredOff(t *testing.T) {
	initStreamingSystem(t)
	setAllowVectorIndexDropOnLoaded(t, false)
	s, req := newLoadedVectorIndexServer(t)
	status, err := s.DropIndex(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, merr.Ok(status))
	assert.NotEmpty(t, s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName()))
}

func TestDropVectorIndexOnLoadedProceedsWhenConfiguredOn(t *testing.T) {
	initStreamingSystem(t)
	setAllowVectorIndexDropOnLoaded(t, true)
	s, req := newLoadedVectorIndexServer(t)
	status, err := s.DropIndex(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, merr.Ok(status), "with the switch on the drop must not be refused")
	assert.Empty(t, s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName()),
		"an allowed drop must actually remove the index, not just skip the refusal")
}
