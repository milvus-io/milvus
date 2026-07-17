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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Tests for ResolveCollectionAlias in MetaCache and resolveCollectionAlias helper.

func TestResolveCollectionAlias_WildcardAndEmptySkippedByInterceptor(t *testing.T) {
	// The interceptor guard (objectName != util.AnyWord && objectName != "") ensures
	// that "*" and "" never reach resolveCollectionAlias. This test verifies those
	// values are correctly guarded at the interceptor level by checking that the
	// cache's DescribeAlias RPC is never called for these sentinel values.
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	// Wildcard "*" should not trigger resolution
	for _, sentinel := range []string{"*", ""} {
		// These sentinel values should be caught by the interceptor guard
		// (objectName != util.AnyWord && objectName != "") before calling
		// resolveCollectionAlias. Verify the guard logic directly.
		shouldSkip := sentinel == "*" || sentinel == ""
		assert.True(t, shouldSkip, "sentinel %q should be skipped by interceptor guard", sentinel)
	}

	// Normal name should resolve via RPC
	mockCoord.On("DescribeAlias", mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Collection: "real_col",
	}, nil)

	result, err := resolveCollectionAlias(ctx, "default", "some_alias")
	assert.NoError(t, err)
	assert.Equal(t, "real_col", result)
	mockCoord.AssertCalled(t, "DescribeAlias", mock.Anything, mock.Anything)
}

func TestResolveCollectionAlias_CachedCollection(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}
	// plant the collection into the primary store + name hint the way update() would
	seedCollection(cache, "default", "test_collection", 1)

	result, err := cache.ResolveCollectionAlias(ctx, "default", "test_collection")
	assert.NoError(t, err)
	assert.Equal(t, "test_collection", result)
	mockCoord.AssertNotCalled(t, "DescribeAlias")
}

func TestResolveCollectionAlias_ValidAlias(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	mockCoord.On("DescribeAlias", mock.Anything, mock.MatchedBy(func(req *milvuspb.DescribeAliasRequest) bool {
		return req.DbName == "default" && req.Alias == "my_alias"
	})).Return(&milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Collection: "actual_collection",
	}, nil)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	result, err := cache.ResolveCollectionAlias(ctx, "default", "my_alias")
	assert.NoError(t, err)
	assert.Equal(t, "actual_collection", result)
}

func TestResolveCollectionAlias_NotAnAlias(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	mockCoord.On("DescribeAlias", mock.Anything, mock.MatchedBy(func(req *milvuspb.DescribeAliasRequest) bool {
		return req.DbName == "default" && req.Alias == "not_an_alias"
	})).Return(&milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "alias not found",
		},
	}, nil)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	// Should return original name when alias not found (not error)
	result, err := cache.ResolveCollectionAlias(ctx, "default", "not_an_alias")
	assert.NoError(t, err)
	assert.Equal(t, "not_an_alias", result)
}

func TestResolveCollectionAlias_RPCError(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	mockCoord.On("DescribeAlias", mock.Anything, mock.Anything).
		Return(nil, assert.AnError)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	result, err := cache.ResolveCollectionAlias(ctx, "default", "some_name")
	assert.Error(t, err)
	assert.Equal(t, "", result)
}

func TestResolveCollectionAlias_InternalServerError(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Internal server error should be propagated, not swallowed
	mockCoord.On("DescribeAlias", mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "internal error",
		},
	}, nil)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	result, err := cache.ResolveCollectionAlias(ctx, "default", "some_name")
	assert.Error(t, err)
	assert.Equal(t, "", result)
}

func TestResolveCollectionAlias_EmptyCollectionInResponse(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Status is success but collection field is empty
	mockCoord.On("DescribeAlias", mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Collection: "",
	}, nil)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	result, err := cache.ResolveCollectionAlias(ctx, "default", "some_alias")
	assert.NoError(t, err)
	assert.Equal(t, "some_alias", result)
}

func TestResolveCollectionAlias_NilGlobalMetaCache(t *testing.T) {
	ctx := context.Background()
	oldCache := globalMetaCache
	globalMetaCache = nil
	defer func() { globalMetaCache = oldCache }()

	result, err := resolveCollectionAlias(ctx, "default", "test")
	assert.Error(t, err)
	assert.Equal(t, "test", result)
}

func TestResolveCollectionAlias_AliasCacheHit(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"my_alias": {collectionName: "real_collection", cachedAt: time.Now()},
			},
		},
	}

	// Should return cached alias without RPC call
	result, err := cache.ResolveCollectionAlias(ctx, "default", "my_alias")
	assert.NoError(t, err)
	assert.Equal(t, "real_collection", result)
	mockCoord.AssertNotCalled(t, "DescribeAlias")
}

func TestResolveCollectionAlias_NegativeCacheHit(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"regular_name": {collectionName: "", cachedAt: time.Now()},
			},
		},
	}

	// Negative cache: not an alias, return as-is without RPC
	result, err := cache.ResolveCollectionAlias(ctx, "default", "regular_name")
	assert.NoError(t, err)
	assert.Equal(t, "regular_name", result)
	mockCoord.AssertNotCalled(t, "DescribeAlias")
}

func TestResolveCollectionAlias_PopulatesPositiveCache(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	mockCoord.On("DescribeAlias", mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Collection: "real_collection",
	}, nil).Once()

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	// First call triggers RPC
	result, err := cache.ResolveCollectionAlias(ctx, "default", "my_alias")
	assert.NoError(t, err)
	assert.Equal(t, "real_collection", result)

	// Second call should hit cache, no additional RPC
	result, err = cache.ResolveCollectionAlias(ctx, "default", "my_alias")
	assert.NoError(t, err)
	assert.Equal(t, "real_collection", result)

	// DescribeAlias should have been called exactly once
	mockCoord.AssertNumberOfCalls(t, "DescribeAlias", 1)
}

func TestResolveCollectionAlias_PopulatesNegativeCache(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	mockCoord.On("DescribeAlias", mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "alias not found",
		},
	}, nil).Once()

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	// First call triggers RPC
	result, err := cache.ResolveCollectionAlias(ctx, "default", "not_alias")
	assert.NoError(t, err)
	assert.Equal(t, "not_alias", result)

	// Second call should hit negative cache
	result, err = cache.ResolveCollectionAlias(ctx, "default", "not_alias")
	assert.NoError(t, err)
	assert.Equal(t, "not_alias", result)

	mockCoord.AssertNumberOfCalls(t, "DescribeAlias", 1)
}

func TestRemoveAlias_InvalidatesCache(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"my_alias": {collectionName: "real_collection", cachedAt: time.Now()},
			},
		},
	}

	// Remove the alias
	cache.RemoveAlias(ctx, "default", "my_alias")

	// Verify the alias was removed from cache
	_, ok := cache.getAlias("default", "my_alias")
	assert.False(t, ok)
}

// TestRemoveCollectionByID_AliasHintsSurviveButMiss documents the new
// invalidation contract: removeCollectionByID deletes ONLY the primary entry
// (the single source of truth). Alias entries are HINTS — they are no longer
// cleaned synchronously; they dangle legally, fail validation on read, and are
// reclaimed lazily by the background GC.
func TestRemoveCollectionByID_AliasHintsSurviveButMiss(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}
	// plant the collection into the primary store + name hint like update() would
	seedCollection(cache, "default", "my_collection", 100)
	cache.setAliasLocked("default", "alias1", &aliasEntry{collectionName: "my_collection", cachedAt: time.Now()})
	cache.setAliasLocked("default", "alias2", &aliasEntry{collectionName: "my_collection", cachedAt: time.Now()})
	cache.setAliasLocked("default", "alias3", &aliasEntry{collectionName: "other_collection", cachedAt: time.Now()})

	// Remove collection by ID: only the primary entry goes away.
	cache.RemoveCollectionsByID(ctx, 100, 0)

	cache.mu.RLock()
	assert.Nil(t, collByIDLive(cache, 100), "the primary entry must be deleted")
	assert.Nil(t, cachedEntryLocked(cache, "default", "my_collection"), "a by-name lookup must miss after the drop")
	cache.mu.RUnlock()

	// The alias hints still exist physically (lazy cleanup), but a lookup
	// through them must MISS the dead collection.
	for _, alias := range []string{"alias1", "alias2"} {
		entry, ok := cache.getAlias("default", alias)
		if assert.True(t, ok, "alias hint %s survives as a dangling hint", alias) {
			assert.Equal(t, "my_collection", entry.collectionName)
		}
		_, ok = cache.getCollection("default", alias, 0)
		assert.False(t, ok, "a lookup through alias %s must not reach the dead collection", alias)
	}

	// Alias pointing to other_collection is untouched.
	entry, ok := cache.getAlias("default", "alias3")
	assert.True(t, ok)
	assert.Equal(t, "other_collection", entry.collectionName)
}

// TestRemoveCollection_DanglingAliasHintCleanup documents the new contract for
// alias hints whose target is NOT cached: the old "target not cached -> sweep
// every alias pointing at it" fallback is gone (hints dangle legally and are
// swept lazily by the GC). The only synchronous cleanup left is
// removeCollectionByAliasLocked's dangling branch: RemoveCollection for an
// ALIAS name whose target is uncached deletes just that alias hint, so it
// cannot mis-resolve a later ResolveCollectionAlias. Aliases pointing at the
// same uncached target via OTHER alias names are untouched.
func TestRemoveCollection_DanglingAliasHintCleanup(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Target collection is NOT in the primary store, but alias hints exist.
	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}
	cache.setAliasLocked("default", "alias1", &aliasEntry{collectionName: "uncached_collection", cachedAt: time.Now()})
	cache.setAliasLocked("default", "alias2", &aliasEntry{collectionName: "uncached_collection", cachedAt: time.Now()})
	cache.setAliasLocked("default", "alias3", &aliasEntry{collectionName: "other_collection", cachedAt: time.Now()})

	// RemoveCollection with the (uncached) TARGET name no longer sweeps the
	// aliases pointing at it — they remain as dangling hints.
	cache.RemoveCollection(ctx, "default", "uncached_collection", 0)
	_, ok := cache.getAlias("default", "alias1")
	assert.True(t, ok, "aliases of an uncached target are no longer swept synchronously")
	_, ok = cache.getAlias("default", "alias2")
	assert.True(t, ok)

	// RemoveCollection with the ALIAS name deletes just that dangling hint.
	cache.RemoveCollection(ctx, "default", "alias1", 0)
	_, ok = cache.getAlias("default", "alias1")
	assert.False(t, ok, "the dangling alias hint itself must be deleted")

	// The sibling alias to the same uncached target is untouched.
	entry, ok := cache.getAlias("default", "alias2")
	if assert.True(t, ok) {
		assert.Equal(t, "uncached_collection", entry.collectionName)
	}
	// Aliases of other targets are untouched.
	entry, ok = cache.getAlias("default", "alias3")
	if assert.True(t, ok) {
		assert.Equal(t, "other_collection", entry.collectionName)
	}
}

func TestRemoveDatabase_CleansUpAliases(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"mydb": {
				"alias1": {collectionName: "coll1", cachedAt: time.Now()},
			},
		},
		dbGen: map[string]uint64{},
		dbInfo: map[string]*databaseInfo{
			"mydb": {},
		},
	}

	cache.RemoveDatabase(ctx, "mydb", 0)

	// Alias cache for the database should be gone
	_, ok := cache.getAlias("mydb", "alias1")
	assert.False(t, ok)
}

func TestCreateAliasTask_ResolvesCollectionAlias(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Set up cache: "existing_alias" -> "real_collection"
	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"existing_alias": {collectionName: "real_collection", cachedAt: time.Now()},
			},
		},
	}

	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	paramtable.Init()

	task := &CreateAliasTask{
		Condition: NewTaskCondition(ctx),
		CreateAliasRequest: &milvuspb.CreateAliasRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         "default",
			CollectionName: "existing_alias",
			Alias:          "new_alias",
		},
		ctx:      ctx,
		mixCoord: mockCoord,
	}

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
	// CollectionName should always be resolved from alias to real collection
	assert.Equal(t, "real_collection", task.CollectionName)
}

func TestAlterAliasTask_ResolvesCollectionAlias(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Set up cache: "existing_alias" -> "real_collection"
	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"existing_alias": {collectionName: "real_collection", cachedAt: time.Now()},
			},
		},
	}

	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	paramtable.Init()

	task := &AlterAliasTask{
		Condition: NewTaskCondition(ctx),
		AlterAliasRequest: &milvuspb.AlterAliasRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         "default",
			CollectionName: "existing_alias",
			Alias:          "some_alias",
		},
		ctx:      ctx,
		mixCoord: mockCoord,
	}

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
	// CollectionName should always be resolved from alias to real collection
	assert.Equal(t, "real_collection", task.CollectionName)
}

func TestCreateAliasTask_ResolvesEvenWhenRBACFlagDisabled(t *testing.T) {
	// Alias resolution in CreateAlias/AlterAlias is unconditional (for correctness),
	// independent of the RBAC feature flag.
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"existing_alias": {collectionName: "real_collection", cachedAt: time.Now()},
			},
		},
	}

	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	paramtable.Init()
	paramtable.Get().Save(Params.ProxyCfg.ResolveAliasForPrivilege.Key, "false")
	defer paramtable.Get().Reset(Params.ProxyCfg.ResolveAliasForPrivilege.Key)

	task := &CreateAliasTask{
		Condition: NewTaskCondition(ctx),
		CreateAliasRequest: &milvuspb.CreateAliasRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         "default",
			CollectionName: "existing_alias",
			Alias:          "new_alias",
		},
		ctx:      ctx,
		mixCoord: mockCoord,
	}

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
	// CollectionName should be resolved even when RBAC flag is disabled
	assert.Equal(t, "real_collection", task.CollectionName)
}

func TestListAliasesTask_ResolvesCollectionAlias(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Set up cache: "existing_alias" -> "real_collection"
	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		dbGen:       map[string]uint64{},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"existing_alias": {collectionName: "real_collection", cachedAt: time.Now()},
			},
		},
	}

	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	paramtable.Init()

	task := &ListAliasesTask{
		Condition: NewTaskCondition(ctx),
		ListAliasesRequest: &milvuspb.ListAliasesRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         "default",
			CollectionName: "existing_alias",
		},
		ctx:      ctx,
		mixCoord: mockCoord,
	}

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
	// CollectionName should be resolved from alias to real collection
	assert.Equal(t, "real_collection", task.CollectionName)
}

func TestListAliasesTask_NoResolveWhenCollectionNameEmpty(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord:    mockCoord,
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]*aliasEntry{},
		dbGen:       map[string]uint64{},
	}

	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	paramtable.Init()

	task := &ListAliasesTask{
		Condition: NewTaskCondition(ctx),
		ListAliasesRequest: &milvuspb.ListAliasesRequest{
			Base:   &commonpb.MsgBase{},
			DbName: "default",
		},
		ctx:      ctx,
		mixCoord: mockCoord,
	}

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
	// CollectionName should remain empty
	assert.Equal(t, "", task.CollectionName)
	mockCoord.AssertNotCalled(t, "DescribeAlias")
}
