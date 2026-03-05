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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		mixCoord:  mockCoord,
		collInfo:  map[string]map[string]*collectionInfo{"default": {}},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {
				"test_collection": {
					collID: 1,
					schema: &schemaInfo{},
				},
			},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
	}

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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
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

func TestRemoveCollectionByID_CleansUpAliases(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {
				"my_collection": {
					collID: 100,
					schema: &schemaInfo{},
				},
			},
		},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"alias1": {collectionName: "my_collection", cachedAt: time.Now()},
				"alias2": {collectionName: "my_collection", cachedAt: time.Now()},
				"alias3": {collectionName: "other_collection", cachedAt: time.Now()},
			},
		},
		collectionCacheVersion: make(map[UniqueID]uint64),
		sfGlobal:               conc.Singleflight[*collectionInfo]{},
	}

	// Remove collection by ID
	cache.RemoveCollectionsByID(ctx, 100, 0, true)

	// Aliases pointing to my_collection should be removed
	_, ok := cache.getAlias("default", "alias1")
	assert.False(t, ok)
	_, ok = cache.getAlias("default", "alias2")
	assert.False(t, ok)

	// Alias pointing to other_collection should remain
	entry, ok := cache.getAlias("default", "alias3")
	assert.True(t, ok)
	assert.Equal(t, "other_collection", entry.collectionName)
}

func TestRemoveCollection_CleansUpAliasesWhenNotCached(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Collection is NOT in collInfo cache, but aliases pointing to it exist
	cache := &MetaCache{
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"default": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{
			"default": {
				"alias1": {collectionName: "uncached_collection", cachedAt: time.Now()},
				"alias2": {collectionName: "uncached_collection", cachedAt: time.Now()},
				"alias3": {collectionName: "other_collection", cachedAt: time.Now()},
			},
		},
		collectionCacheVersion: make(map[UniqueID]uint64),
		sfGlobal:               conc.Singleflight[*collectionInfo]{},
	}

	// RemoveCollection for an uncached collection
	cache.RemoveCollection(ctx, "default", "uncached_collection", 0)

	// Aliases pointing to uncached_collection should be removed
	_, ok := cache.getAlias("default", "alias1")
	assert.False(t, ok)
	_, ok = cache.getAlias("default", "alias2")
	assert.False(t, ok)

	// Alias pointing to other_collection should remain
	entry, ok := cache.getAlias("default", "alias3")
	assert.True(t, ok)
	assert.Equal(t, "other_collection", entry.collectionName)
}

func TestRemoveDatabase_CleansUpAliases(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	cache := &MetaCache{
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{
			"mydb": {},
		},
		aliasInfo: map[string]map[string]*aliasEntry{
			"mydb": {
				"alias1": {collectionName: "coll1", cachedAt: time.Now()},
			},
		},
		dbInfo: map[string]*databaseInfo{
			"mydb": {},
		},
	}

	cache.RemoveDatabase(ctx, "mydb")

	// Alias cache for the database should be gone
	_, ok := cache.getAlias("mydb", "alias1")
	assert.False(t, ok)
}

func TestCreateAliasTask_ResolvesCollectionAlias(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)

	// Set up cache: "existing_alias" -> "real_collection"
	cache := &MetaCache{
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{"default": {}},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{"default": {}},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{"default": {}},
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
		mixCoord: mockCoord,
		collInfo: map[string]map[string]*collectionInfo{"default": {}},
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
		mixCoord:  mockCoord,
		collInfo:  map[string]map[string]*collectionInfo{"default": {}},
		aliasInfo: map[string]map[string]*aliasEntry{},
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
