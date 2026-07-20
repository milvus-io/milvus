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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestIndexMetaVersionStamping(t *testing.T) {
	collID := UniqueID(1)
	newIndexModel := func(indexID UniqueID) *model.Index {
		return &model.Index{
			CollectionID: collID,
			FieldID:      100 + indexID,
			IndexID:      indexID,
			IndexName:    "idx",
		}
	}

	t.Run("mutations stamp monotonic versions", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("CreateIndex", mock.Anything, mock.Anything).Return(nil)
		catalog.On("AlterIndexes", mock.Anything, mock.Anything).Return(nil)

		ts := uint64(100)
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).RunAndReturn(func(ctx context.Context) (uint64, error) {
			ts += 100
			return ts, nil
		})

		m := newSegmentIndexMeta(catalog)
		m.allocator = alloc

		require.NoError(t, m.CreateIndex(context.TODO(), newIndexModel(1)))
		infos, version := m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Len(t, infos, 1)
		assert.Equal(t, uint64(200), version)

		require.NoError(t, m.AlterIndex(context.TODO(), newIndexModel(1)))
		_, version = m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Equal(t, uint64(300), version)

		require.NoError(t, m.MarkIndexAsDeleted(context.TODO(), collID, []UniqueID{1}))
		infos, version = m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Empty(t, infos)
		assert.Equal(t, uint64(400), version)
	})

	t.Run("allocation failure aborts before catalog write", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(0, errors.New("tso unavailable"))

		m := newSegmentIndexMeta(catalog)
		m.allocator = alloc

		assert.Error(t, m.CreateIndex(context.TODO(), newIndexModel(1)))
		catalog.AssertNotCalled(t, "CreateIndex", mock.Anything, mock.Anything)
		_, version := m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Zero(t, version)
	})

	t.Run("catalog failure does not bump version", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("CreateIndex", mock.Anything, mock.Anything).Return(errors.New("etcd down"))
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(100, nil)

		m := newSegmentIndexMeta(catalog)
		m.allocator = alloc

		assert.Error(t, m.CreateIndex(context.TODO(), newIndexModel(1)))
		_, version := m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Zero(t, version)
	})

	t.Run("nil allocator keeps versions unstamped", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("CreateIndex", mock.Anything, mock.Anything).Return(nil)

		m := newSegmentIndexMeta(catalog)

		require.NoError(t, m.CreateIndex(context.TODO(), newIndexModel(1)))
		infos, version := m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Len(t, infos, 1)
		assert.Zero(t, version)
	})

	t.Run("untouched collection reports base version", func(t *testing.T) {
		m := newSegmentIndexMeta(catalogmocks.NewDataCoordCatalog(t))
		m.baseIndexMetaVersion = 55

		infos, version := m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Empty(t, infos)
		assert.Equal(t, uint64(55), version)
	})

	t.Run("reload stamps base version", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("ListIndexes", mock.Anything).Return([]*model.Index{newIndexModel(1)}, nil)
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(77, nil)

		m, err := newIndexMeta(context.TODO(), catalog, nil, alloc)
		require.NoError(t, err)
		assert.Equal(t, uint64(77), m.baseIndexMetaVersion)
		infos, version := m.GetIndexesForCollectionWithVersion(collID, "")
		assert.Len(t, infos, 1)
		assert.Equal(t, uint64(77), version)
	})

	t.Run("reload allocation failure fails construction", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("ListIndexes", mock.Anything).Return([]*model.Index{}, nil)
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(0, errors.New("tso unavailable"))

		_, err := newIndexMeta(context.TODO(), catalog, nil, alloc)
		assert.Error(t, err)
	})
}
