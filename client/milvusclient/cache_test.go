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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func TestCollectionCacheWriteTs(t *testing.T) {
	cache := newCollectionCache(nil)
	keyA := newCollectionCacheKey("default", "coll_a")
	keyB := newCollectionCacheKey("default", "coll_b")

	t.Run("monotonic_update", func(t *testing.T) {
		cache.UpdateWriteTs(keyA, 10)
		// an older timestamp never overwrites the cached one
		cache.UpdateWriteTs(keyA, 5)
		if got := cache.GetWriteTs(keyA); got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
		cache.UpdateWriteTs(keyA, 20)
		if got := cache.GetWriteTs(keyA); got != 20 {
			t.Fatalf("expected 20, got %d", got)
		}
		// zero timestamps are ignored
		cache.UpdateWriteTs(keyA, 0)
		if got := cache.GetWriteTs(keyA); got != 20 {
			t.Fatalf("expected 20, got %d", got)
		}
		// unknown collection returns 0
		if got := cache.GetWriteTs(keyB); got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
	})

	t.Run("db_isolation", func(t *testing.T) {
		// same collection name in different databases does not collide
		cache := newCollectionCache(nil)
		otherDBKey := newCollectionCacheKey("other_db", "coll_a")
		cache.UpdateWriteTs(keyA, 10)
		cache.UpdateWriteTs(otherDBKey, 99)
		if got := cache.GetWriteTs(keyA); got != 10 {
			t.Fatalf("expected 10 for default db, got %d", got)
		}
		if got := cache.GetWriteTs(otherDBKey); got != 99 {
			t.Fatalf("expected 99 for other db, got %d", got)
		}
	})

	t.Run("concurrent_updates", func(t *testing.T) {
		cache := newCollectionCache(nil)
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(n uint64) {
				defer wg.Done()
				cache.UpdateWriteTs(keyA, n)
			}(uint64(1000 - i))
		}
		wg.Wait()
		if got := cache.GetWriteTs(keyA); got != 1000 {
			t.Fatalf("expected 1000, got %d", got)
		}
	})

	t.Run("reset", func(t *testing.T) {
		cache.UpdateWriteTs(keyA, 10)
		cache.Reset()
		if got := cache.GetWriteTs(keyA); got != 0 {
			t.Fatalf("expected 0 after reset, got %d", got)
		}
	})

	t.Run("evict_keeps_write_ts", func(t *testing.T) {
		cache.UpdateWriteTs(keyA, 10)
		cache.Evict(keyA)
		if got := cache.GetWriteTs(keyA); got != 10 {
			t.Fatalf("expected 10 after schema evict, got %d", got)
		}
	})

	t.Run("evict_write_ts", func(t *testing.T) {
		cache.UpdateWriteTs(keyA, 10)
		cache.EvictWriteTs(keyA)
		if got := cache.GetWriteTs(keyA); got != 0 {
			t.Fatalf("expected 0 after write-ts evict, got %d", got)
		}
	})

	t.Run("copy_write_ts", func(t *testing.T) {
		cache := newCollectionCache(nil)
		cache.UpdateWriteTs(keyA, 10)
		aliasKey := newCollectionCacheKey("default", "alias_a")
		// copy to alias keeps the source and seeds the target monotonically
		cache.CopyWriteTs(keyA, aliasKey)
		if got := cache.GetWriteTs(keyA); got != 10 {
			t.Fatalf("expected 10 retained on source, got %d", got)
		}
		if got := cache.GetWriteTs(aliasKey); got != 10 {
			t.Fatalf("expected 10 copied to alias, got %d", got)
		}
		// a newer timestamp on the target is not overwritten
		cache.UpdateWriteTs(aliasKey, 30)
		cache.CopyWriteTs(keyA, aliasKey)
		if got := cache.GetWriteTs(aliasKey); got != 30 {
			t.Fatalf("expected 30 kept on alias, got %d", got)
		}
		// copying when the source has no timestamp is a no-op
		cache.CopyWriteTs(keyB, aliasKey)
		if got := cache.GetWriteTs(aliasKey); got != 30 {
			t.Fatalf("expected 30 unchanged after empty-copy, got %d", got)
		}
	})

	t.Run("invalidate_db", func(t *testing.T) {
		cache := newCollectionCache(nil)
		keyDefault := newCollectionCacheKey("default", "coll_a")
		keyOther := newCollectionCacheKey("other_db", "coll_a")
		cache.collections.Insert(keyDefault, &entity.Collection{Name: "coll_a"})
		cache.collections.Insert(keyOther, &entity.Collection{Name: "coll_a"})
		cache.UpdateWriteTs(keyDefault, 10)
		cache.UpdateWriteTs(keyOther, 20)
		cache.InvalidateDb("default")
		if _, ok := cache.collections.Get(keyDefault); ok {
			t.Fatalf("expected default db schema evicted")
		}
		if _, ok := cache.collections.Get(keyOther); !ok {
			t.Fatalf("expected other db schema kept")
		}
		if got := cache.GetWriteTs(keyDefault); got != 0 {
			t.Fatalf("expected 0 for default db ts, got %d", got)
		}
		if got := cache.GetWriteTs(keyOther); got != 20 {
			t.Fatalf("expected 20 for other db ts, got %d", got)
		}
	})

	t.Run("rename_migrates_write_ts", func(t *testing.T) {
		cache.UpdateWriteTs(keyA, 10)
		keyC := newCollectionCacheKey("default", "coll_c")
		cache.RenameWriteTs(keyA, keyC)
		if got := cache.GetWriteTs(keyA); got != 0 {
			t.Fatalf("expected 0 for old name after rename, got %d", got)
		}
		if got := cache.GetWriteTs(keyC); got != 10 {
			t.Fatalf("expected 10 for new name after rename, got %d", got)
		}
		// rename without a cached timestamp is a no-op
		cache.RenameWriteTs(keyB, keyC)
		if got := cache.GetWriteTs(keyC); got != 10 {
			t.Fatalf("expected 10 unchanged for unknown rename, got %d", got)
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

func (s *CacheSuite) TestCustomizedPreservesCallerTs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)

	opt := NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).
		WithConsistencyLevel(entity.ClCustomized).
		WithGuaranteeTimestamp(123)

	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.EqualValues(123, sr.GetGuaranteeTimestamp())
		return s.searchResults(), nil
	}).Once()

	_, err := s.client.Search(ctx, opt)
	s.NoError(err)
}

// TestEventuallyPreservesCallerTs mirrors pymilvus setdefault semantics:
// like Customized, Eventually keeps a caller-provided guarantee_timestamp.
func (s *CacheSuite) TestEventuallyPreservesCallerTs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collectionName, s.schema)

	opt := NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).
		WithConsistencyLevel(entity.ClEventually).
		WithGuaranteeTimestamp(456)

	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(c context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.EqualValues(456, sr.GetGuaranteeTimestamp())
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
		{"eventually", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClEventually), eventuallyTs},
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
	s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collectionName), 42)

	cases := []struct {
		name     string
		option   SearchOption
		expected uint64
	}{
		{"default", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}), 42},
		{"session", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClSession), 42},
		{"strong", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClStrong), 0},
		{"bounded", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClBounded), boundedTs},
		{"eventually", NewSearchOption(collectionName, 1, []entity.Vector{s.newVector()}).WithConsistencyLevel(entity.ClEventually), eventuallyTs},
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
	s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collectionName), 42)

	cases := []struct {
		name     string
		option   QueryOption
		expected uint64
	}{
		{"default", NewQueryOption(collectionName), 42},
		{"session", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClSession), 42},
		{"strong", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClStrong), 0},
		{"bounded", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClBounded), boundedTs},
		{"eventually", NewQueryOption(collectionName).WithConsistencyLevel(entity.ClEventually), eventuallyTs},
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
	s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collectionName), 42)

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
		{"eventually", NewHybridSearchOption(collectionName, 1, annRequest).WithConsistencyLevel(entity.ClEventually), eventuallyTs},
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
		s.EqualValues(100, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collectionName)))
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
		s.EqualValues(200, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collectionName)))
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
		s.EqualValues(300, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collectionName)))
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
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
	})

	s.Run("rename_collection", func() {
		oldName := fmt.Sprintf("coll_%s", s.randString(6))
		newName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(oldName, s.schema)
		s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), oldName), 42)
		s.mock.EXPECT().RenameCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.RenameCollection(ctx, NewRenameCollectionOption(oldName, newName)))
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), oldName))
		s.False(ok)
		// the write timestamp is migrated to the new name, not dropped
		s.EqualValues(0, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), oldName)))
		s.EqualValues(42, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), newName)))
	})

	s.Run("alter_collection_properties", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterCollectionProperties(ctx, NewAlterCollectionPropertiesOption(collName).WithProperty("key", "value")))
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
	})

	s.Run("drop_collection_properties", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.DropCollectionProperties(ctx, NewDropCollectionPropertiesOption(collName, "key")))
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
	})

	s.Run("alter_field_property", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollectionField(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterCollectionFieldProperty(ctx, NewAlterCollectionFieldPropertiesOption(collName, "ID").WithProperty("key", "value")))
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
	})

	s.Run("add_field", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).Return(&milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Success(),
		}, nil).Once()

		field := entity.NewField().WithName("extra").WithDataType(entity.FieldTypeInt64)
		s.NoError(s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field)))
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
	})

	s.Run("add_collection_struct_field", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName), 42)
		s.mock.EXPECT().AddCollectionStructField(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		structSchema := entity.NewStructSchema().
			WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
			WithField(entity.NewField().WithName("emb").WithDataType(entity.FieldTypeFloatVector).WithDim(8))
		field := entity.NewField().
			WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(16).
			WithNullable(true).
			WithStructSchema(structSchema)

		s.NoError(s.client.AddCollectionStructField(ctx, NewAddCollectionStructFieldOption(collName, field)))
		// schema is evicted, write timestamp is preserved
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
		s.EqualValues(42, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName)))
	})

	s.Run("schema_mutation_keeps_write_ts", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName), 42)
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterCollectionProperties(ctx, NewAlterCollectionPropertiesOption(collName).WithProperty("key", "value")))
		// schema is evicted, write timestamp is preserved for session consistency
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
		s.EqualValues(42, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName)))
	})

	s.Run("drop_clears_write_ts", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName), 42)
		s.mock.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.DropCollection(ctx, NewDropCollectionOption(collName)))
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.False(ok)
		s.EqualValues(0, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName)))
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
		_, ok := s.client.collCache.collections.Get(newCollectionCacheKey(s.client.getCurrentDB(), collName))
		s.True(ok)
	})
}

// TestAliasTsCache verifies the alias cache behavior mirrors java/cpp:
// create/alter alias copies the collection write timestamp to the alias and
// evicts the alias schema; drop alias clears the alias timestamp and schema.
func (s *CacheSuite) TestAliasTsCache() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("create_alias", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		aliasName := fmt.Sprintf("alias_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName), 42)
		s.mock.EXPECT().CreateAlias(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.CreateAlias(ctx, NewCreateAliasOption(collName, aliasName)))
		// the alias shares the collection write timestamp
		s.EqualValues(42, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), aliasName)))
	})

	s.Run("alter_alias", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		aliasName := fmt.Sprintf("alias_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.client.collCache.UpdateWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), collName), 42)
		s.mock.EXPECT().AlterAlias(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.AlterAlias(ctx, NewAlterAliasOption(aliasName, collName)))
		s.EqualValues(42, s.client.collCache.GetWriteTs(newCollectionCacheKey(s.client.getCurrentDB(), aliasName)))
	})

	s.Run("drop_alias", func() {
		aliasName := fmt.Sprintf("alias_%s", s.randString(6))
		aliasKey := newCollectionCacheKey(s.client.getCurrentDB(), aliasName)
		// seed a stale alias schema and timestamp
		s.client.collCache.collections.Insert(aliasKey, &entity.Collection{Name: aliasName, Schema: s.schema})
		s.client.collCache.UpdateWriteTs(aliasKey, 42)
		s.mock.EXPECT().DropAlias(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		s.NoError(s.client.DropAlias(ctx, NewDropAliasOption(aliasName)))
		// the alias no longer resolves to any collection
		_, ok := s.client.collCache.collections.Get(aliasKey)
		s.False(ok)
		s.EqualValues(0, s.client.collCache.GetWriteTs(aliasKey))
	})
}

// TestDropDatabaseInvalidatesCache verifies DropDatabase clears all cached
// schemas and write timestamps of the dropped database, keeping other databases.
func (s *CacheSuite) TestDropDatabaseInvalidatesCache() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := fmt.Sprintf("db_%s", s.randString(6))
	keyA := newCollectionCacheKey(dbName, "coll_a")
	keyB := newCollectionCacheKey(dbName, "coll_b")
	otherKey := newCollectionCacheKey("default", "coll_a")
	s.client.collCache.collections.Insert(keyA, &entity.Collection{Name: "coll_a", Schema: s.schema})
	s.client.collCache.collections.Insert(keyB, &entity.Collection{Name: "coll_b", Schema: s.schema})
	s.client.collCache.collections.Insert(otherKey, &entity.Collection{Name: "coll_a", Schema: s.schema})
	s.client.collCache.UpdateWriteTs(keyA, 42)
	s.client.collCache.UpdateWriteTs(otherKey, 7)
	s.mock.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	s.NoError(s.client.DropDatabase(ctx, NewDropDatabaseOption(dbName)))
	_, ok := s.client.collCache.collections.Get(keyA)
	s.False(ok)
	_, ok = s.client.collCache.collections.Get(keyB)
	s.False(ok)
	_, ok = s.client.collCache.collections.Get(otherKey)
	s.True(ok)
	s.EqualValues(0, s.client.collCache.GetWriteTs(keyA))
	s.EqualValues(7, s.client.collCache.GetWriteTs(otherKey))
}

// TestUseDatabaseKeepsCache verifies that switching the database does not reset
// the collection cache: entries are keyed by (database, collection), so the old
// database's entries remain isolated and usable, mirroring the java/cpp SDKs.
func (s *CacheSuite) TestUseDatabaseKeepsCache() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := fmt.Sprintf("db_%s", s.randString(6))
	keyA := newCollectionCacheKey(s.client.getCurrentDB(), "coll_a")
	s.client.collCache.collections.Insert(keyA, &entity.Collection{Name: "coll_a", Schema: s.schema})
	s.client.collCache.UpdateWriteTs(keyA, 42)

	s.NoError(s.client.UseDatabase(ctx, NewUseDatabaseOption(dbName)))

	_, ok := s.client.collCache.collections.Get(keyA)
	s.True(ok)
	s.EqualValues(42, s.client.collCache.GetWriteTs(keyA))
}

func TestCacheFunc(t *testing.T) {
	suite.Run(t, new(CacheSuite))
}
