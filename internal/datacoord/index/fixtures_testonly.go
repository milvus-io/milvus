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

//go:build test

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func NewTestVersionManager() IndexEngineVersionManager {
	return &VersionManagerImpl{}
}

func NewSegmentIndexMeta(catalog metastore.DataCoordCatalog) *IndexMeta {
	return &IndexMeta{
		KeyLock:          lock.NewKeyLock[UniqueID](),
		ctx:              context.Background(),
		Catalog:          catalog,
		Indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
}

// createTestIndexMeta creates an indexMeta with pre-registered index definitions for testing.
// If catalog is nil, a default mock catalog (CreateSegmentIndex returns nil) is used.
func CreateTestIndexMeta(t *testing.T, collectionID int64, indexes map[int64]*model.Index, catalog ...metastore.DataCoordCatalog) *IndexMeta {
	var cat metastore.DataCoordCatalog
	if len(catalog) > 0 && catalog[0] != nil {
		cat = catalog[0]
	} else {
		mockCat := catalogmocks.NewDataCoordCatalog(t)
		mockCat.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
		cat = mockCat
	}

	im := &IndexMeta{
		ctx:              context.Background(),
		Catalog:          cat,
		KeyLock:          lock.NewKeyLock[UniqueID](),
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{collectionID: indexes},
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	return im
}

func NewTestIndexMeta(collID, segID, indexID int64, indexType string, segIdx *model.SegmentIndex) *IndexMeta {
	im := NewSegmentIndexMeta(nil)
	im.Indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			IndexID:      indexID,
			IndexName:    "test_idx",
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: indexType},
			},
		},
	}
	segIdxMap := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxMap.Insert(indexID, segIdx)
	im.SegmentIndexes.Insert(segID, segIdxMap)
	return im
}

// indexTestMetas holds the domain metas that index-layer tests build together with a meta.
type IndexTestMetas struct {
	IndexMeta *IndexMeta
}

// loadIndexTestMetas returns a DomainMetaLoader that builds only the index meta into out and returns it as the index catalog.
func loadIndexTestMetas(catalog metastore.DataCoordCatalog, out *IndexTestMetas) meta.DomainMetaLoader {
	return meta.ScheduleTestDomainLoads(func(ctx context.Context, collectionIDs []int64) (meta.SegmentIndexCatalog, meta.CompactionProtection, error) {
		var err error
		out.IndexMeta, err = NewIndexMeta(ctx, catalog, collectionIDs)
		if err != nil {
			return nil, nil, err
		}
		return out.IndexMeta, nil, nil
	})
}

// withIndexTestMetas returns a factory of loaders that build only the index meta into out.
func withIndexTestMetas(out *IndexTestMetas) func(metastore.DataCoordCatalog, storage.ChunkManager) meta.DomainMetaLoader {
	return func(catalog metastore.DataCoordCatalog, _ storage.ChunkManager) meta.DomainMetaLoader {
		return loadIndexTestMetas(catalog, out)
	}
}

// newMemoryMetaWithIndex builds a meta over an in-memory catalog together with an index meta over the same catalog.
func NewMemoryMetaWithIndex(t *testing.T) (*meta.Meta, *IndexTestMetas, error) {
	dm := &IndexTestMetas{}
	m, err := meta.NewMemoryMetaWithLoader(t, withIndexTestMetas(dm))
	return m, dm, err
}

const restartPartID = UniqueID(30)

func IndexMetaFromTestMeta(m *meta.Meta) *IndexMeta {
	if m == nil || m.IndexCatalog == nil {
		return nil
	}
	return m.IndexCatalog.(*IndexMeta)
}
