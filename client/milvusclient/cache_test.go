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

package milvusclient

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestCollectionCacheWriteTs(t *testing.T) {
	cache := NewCollectionCache(nil)

	t.Run("monotonic_update", func(t *testing.T) {
		cache.UpdateWriteTs("coll_a", 10)
		// an older timestamp never overwrites the cached one
		cache.UpdateWriteTs("coll_a", 5)
		if got := cache.GetWriteTs("coll_a"); got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
		cache.UpdateWriteTs("coll_a", 20)
		if got := cache.GetWriteTs("coll_a"); got != 20 {
			t.Fatalf("expected 20, got %d", got)
		}
		// zero timestamps are ignored
		cache.UpdateWriteTs("coll_a", 0)
		if got := cache.GetWriteTs("coll_a"); got != 20 {
			t.Fatalf("expected 20, got %d", got)
		}
		// unknown collection returns 0
		if got := cache.GetWriteTs("coll_b"); got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
	})

	t.Run("concurrent_updates", func(t *testing.T) {
		cache := NewCollectionCache(nil)
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(n uint64) {
				defer wg.Done()
				cache.UpdateWriteTs("coll_a", n)
			}(uint64(1000 - i))
		}
		wg.Wait()
		if got := cache.GetWriteTs("coll_a"); got != 1000 {
			t.Fatalf("expected 1000, got %d", got)
		}
	})

	t.Run("reset", func(t *testing.T) {
		cache.UpdateWriteTs("coll_a", 10)
		cache.Reset()
		if got := cache.GetWriteTs("coll_a"); got != 0 {
			t.Fatalf("expected 0 after reset, got %d", got)
		}
	})

	t.Run("evict_keeps_write_ts", func(t *testing.T) {
		cache.UpdateWriteTs("coll_a", 10)
		cache.Evict("coll_a")
		if got := cache.GetWriteTs("coll_a"); got != 10 {
			t.Fatalf("expected 10 after schema evict, got %d", got)
		}
	})

	t.Run("evict_write_ts", func(t *testing.T) {
		cache.UpdateWriteTs("coll_a", 10)
		cache.EvictWriteTs("coll_a")
		if got := cache.GetWriteTs("coll_a"); got != 0 {
			t.Fatalf("expected 0 after write-ts evict, got %d", got)
		}
	})
}

type CacheSuite struct {
	MockSuiteBase

	schema *entity.Schema
}

func (s *CacheSuite) SetupSuite() {
	s.MockSuiteBase.SetupSuite()
	s.schema = entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))
}

func (s *CacheSuite) newVector() entity.FloatVector {
	return entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))
}

func (s *CacheSuite) searchResults() *milvuspb.SearchResults {
	return &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1}},
				},
			},
			Scores: []float32{0.9},
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{1}),
			},
		},
	}
}

// customizedSearchOption is a SearchOption that pre-sets the request
// guarantee_timestamp, emulating a caller-provided Customized timestamp.
type customizedSearchOption struct {
	base *searchOption
	ts   uint64
}

func (o *customizedSearchOption) Request() (*milvuspb.SearchRequest, error) {
	req, err := o.base.Request()
	if err != nil {
		return nil, err
	}
	req.GuaranteeTimestamp = o.ts
	return req, nil
}

func (s *CacheSuite) TestCustomizedPreservesCallerTs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)

	opt := &customizedSearchOption{
		base: NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClCustomized),
		ts:   123,
	}

	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.EqualValues(123, sr.GetGuaranteeTimestamp())
		return s.searchResults(), nil
	}).Once()

	_, err := s.client.Search(ctx, opt)
	s.NoError(err)
}

func (s *CacheSuite) TestSearchGuaranteeTimestamp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)

	cases := []struct {
		name     string
		option   SearchOption
		expected uint64
	}{
		{"default", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}), eventuallyTs},
		{"strong", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClStrong), 0},
		{"session", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClSession), eventuallyTs},
		{"bounded", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClBounded), boundedTs},
		{"customized", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClCustomized), eventuallyTs},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				s.Equal(tc.expected, sr.GetGuaranteeTimestamp())
				return s.searchResults(), nil
			}).Once()

			_, err := s.client.Search(ctx, tc.option)
			s.NoError(err)
		})
	}
}

func (s *CacheSuite) TestSearchGuaranteeTimestampWithCachedTs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)
	s.client.collCache.UpdateWriteTs(collectionName, 42)

	cases := []struct {
		name     string
		option   SearchOption
		expected uint64
	}{
		{"default", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}), 42},
		{"session", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClSession), 42},
		{"strong", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClStrong), 0},
		{"bounded", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClBounded), boundedTs},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				s.Equal(tc.expected, sr.GetGuaranteeTimestamp())
				return s.searchResults(), nil
			}).Once()

			_, err := s.client.Search(ctx, tc.option)
			s.NoError(err)
		})
	}
}

func (s *CacheSuite) TestQueryGuaranteeTimestamp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)
	s.client.collCache.UpdateWriteTs(collectionName, 42)

	cases := []struct {
		name     string
		option   QueryOption
		expected uint64
	}{
		{"default", NewQueryOption(collectionName), 42},
		{"session", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClSession), 42},
		{"strong", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClStrong), 0},
		{"bounded", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClBounded), boundedTs},
		{"customized", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClCustomized), eventuallyTs},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
				s.Equal(tc.expected, qr.GetGuaranteeTimestamp())
				return &milvuspb.QueryResults{Status: merr.Success()}, nil
			}).Once()

			_, err := s.client.Query(ctx, tc.option)
			s.NoError(err)
		})
	}
}

func (s *CacheSuite) TestHybridSearchGuaranteeTimestamp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)
	s.client.collCache.UpdateWriteTs(collectionName, 42)

	annRequest := NewAnnRequest("Vector", 1, s.newVector())

	cases := []struct {
		name     string
		option   HybridSearchOption
		expected uint64
	}{
		{"default", NewHybridSearchOption(collectionName, 1, annRequest), 42},
		{"session", NewHybridSearchOption(collectionName, 1, annRequest).WithConsistencyLevel(entity.ClSession), 42},
		{"strong", NewHybridSearchOption(collectionName, 1, annRequest).WithConsistencyLevel(entity.ClStrong), 0},
		{"bounded", NewHybridSearchOption(collectionName, 1, annRequest).WithConsistencyLevel(entity.ClBounded), boundedTs},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.mock.EXPECT().HybridSearch(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, hsr *milvuspb.HybridSearchRequest) (*milvuspb.SearchResults, error) {
				s.Equal(tc.expected, hsr.GetGuaranteeTimestamp())
				return s.searchResults(), nil
			}).Once()

			_, err := s.client.HybridSearch(ctx, tc.option)
			s.NoError(err)
		})
	}
}

func (s *CacheSuite) TestMutationFeedsCollectionTs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)

	s.Run("insert", func() {
		s.mock.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
			Status:    merr.Success(),
			InsertCnt: 1,
			Timestamp: 100,
			IDs: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1}},
				},
			},
		}, nil).Once()

		_, err := s.client.Insert(ctx, NewColumnBasedInsertOption(collectionName).
			WithInt64Column("ID", []int64{1}).
			WithFloatVectorColumn("Vector", 128, [][]float32{s.newVector()}))
		s.NoError(err)
		s.EqualValues(100, s.client.collCache.GetWriteTs(collectionName))
	})

	s.Run("upsert", func() {
		s.mock.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
			Status:    merr.Success(),
			UpsertCnt: 1,
			Timestamp: 200,
			IDs: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1}},
				},
			},
		}, nil).Once()

		_, err := s.client.Upsert(ctx, NewColumnBasedInsertOption(collectionName).
			WithInt64Column("ID", []int64{1}).
			WithFloatVectorColumn("Vector", 128, [][]float32{s.newVector()}))
		s.NoError(err)
		s.EqualValues(200, s.client.collCache.GetWriteTs(collectionName))
	})

	s.Run("delete", func() {
		s.mock.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
			Status:    merr.Success(),
			DeleteCnt: 1,
			Timestamp: 300,
			IDs: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1}},
				},
			},
		}, nil).Once()

		_, err := s.client.Delete(ctx, NewDeleteOption(collectionName).WithInt64IDs("ID", []int64{1}))
		s.NoError(err)
		s.EqualValues(300, s.client.collCache.GetWriteTs(collectionName))
	})
}

func (s *CacheSuite) TestSchemaCacheInvalidation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("drop_collection", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.DropCollection(ctx, NewDropCollectionOption(collName)))
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
	})

	s.Run("rename_collection", func() {
		oldName := fmt.Sprintf("coll_%s", s.randString(6))
		newName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(oldName, s.schema)
		s.mock.EXPECT().RenameCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.RenameCollection(ctx, NewRenameCollectionOption(oldName, newName)))
		_, ok := s.client.collCache.collections.Get(oldName)
		s.False(ok)
	})

	s.Run("alter_collection_properties", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterCollectionProperties(ctx, NewAlterCollectionPropertiesOption(collName).WithProperty("key", "value")))
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
	})

	s.Run("drop_collection_properties", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.DropCollectionProperties(ctx, NewDropCollectionPropertiesOption(collName, "key")))
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
	})

	s.Run("alter_field_property", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollectionField(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterCollectionFieldProperty(ctx, NewAlterCollectionFieldPropertiesOption(collName, "ID").WithProperty("key", "value")))
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
	})

	s.Run("add_field", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AddCollectionField(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		field := entity.NewField().WithName("extra").WithDataType(entity.FieldTypeInt64)
		s.NoError(s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field)))
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
	})

	s.Run("schema_mutation_keeps_write_ts", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(collName, 42)
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterCollectionProperties(ctx, NewAlterCollectionPropertiesOption(collName).WithProperty("key", "value")))
		// schema is evicted, write timestamp is preserved for session consistency
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
		s.EqualValues(42, s.client.collCache.GetWriteTs(collName))
	})

	s.Run("drop_clears_write_ts", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(collName, 42)
		s.mock.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.DropCollection(ctx, NewDropCollectionOption(collName)))
		_, ok := s.client.collCache.collections.Get(collName)
		s.False(ok)
		s.EqualValues(0, s.client.collCache.GetWriteTs(collName))
	})

	s.Run("create_index", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		// the cached collection meta carries no index info, so create index
		// must not evict the schema cache
		task, err := s.client.CreateIndex(ctx, NewCreateIndexOption(collName, "Vector", index.NewHNSWIndex(entity.L2, 32, 128)))
		s.NoError(err)
		s.NotNil(task)
		_, ok := s.client.collCache.collections.Get(collName)
		s.True(ok)
	})
}

func TestCacheFunc(t *testing.T) {
	suite.Run(t, new(CacheSuite))
}
