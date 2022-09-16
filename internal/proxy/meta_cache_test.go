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
	"errors"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type MockRootCoordClientInterface struct {
	types.RootCoord
	Error       bool
	AccessCount int

	listPolicy func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)
}

type MockQueryCoordClientInterface struct {
	types.QueryCoord
	Error       bool
	AccessCount int
}

func (m *MockRootCoordClientInterface) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	if in.CollectionName == "collection1" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:         []typeutil.UniqueID{1, 2},
			CreatedTimestamps:    []uint64{100, 200},
			CreatedUtcTimestamps: []uint64{100, 200},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "collection2" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:         []typeutil.UniqueID{3, 4},
			CreatedTimestamps:    []uint64{201, 202},
			CreatedUtcTimestamps: []uint64{201, 202},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:         []typeutil.UniqueID{5, 6},
			CreatedTimestamps:    []uint64{201},
			CreatedUtcTimestamps: []uint64{201},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		PartitionIDs:         []typeutil.UniqueID{},
		CreatedTimestamps:    []uint64{},
		CreatedUtcTimestamps: []uint64{},
		PartitionNames:       []string{},
	}, nil
}

func (m *MockRootCoordClientInterface) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.AccessCount++
	if in.CollectionName == "collection1" {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(1),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
		}, nil
	}
	if in.CollectionName == "collection2" {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(2),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(3),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
		}, nil
	}

	err := fmt.Errorf("can't find collection: " + in.CollectionName)
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "describe collection failed: " + err.Error(),
		},
		Schema: nil,
	}, nil
}

func (m *MockRootCoordClientInterface) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.AccessCount++
	if req.Username == "mockUser" {
		encryptedPassword, _ := crypto.PasswordEncrypt("mockPass")
		return &rootcoordpb.GetCredentialResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Username: "mockUser",
			Password: encryptedPassword,
		}, nil
	}

	err := fmt.Errorf("can't find credential: " + req.Username)
	return nil, err
}

func (m *MockRootCoordClientInterface) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}

	return &milvuspb.ListCredUsersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Usernames: []string{"mockUser"},
	}, nil
}

func (m *MockRootCoordClientInterface) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	if m.listPolicy != nil {
		return m.listPolicy(ctx, in)
	}
	return &internalpb.ListPolicyResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (m *MockQueryCoordClientInterface) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.AccessCount++
	rsp := &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs:       []UniqueID{1, 2},
		InMemoryPercentages: []int64{100, 50},
	}
	return rsp, nil
}

//Simulate the cache path and the
func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "collection1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	assert.Equal(t, rootCoord.AccessCount, 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Equal(t, rootCoord.AccessCount, 1)
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})
	id, err = globalMetaCache.GetCollectionID(ctx, "collection2")
	assert.Equal(t, rootCoord.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection2")
	assert.Equal(t, rootCoord.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})

	// test to get from cache, this should trigger root request
	id, err = globalMetaCache.GetCollectionID(ctx, "collection1")
	assert.Equal(t, rootCoord.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Equal(t, rootCoord.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})

}

func TestMetaCache_GetCollectionFailure(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.Nil(t, err)
	rootCoord.Error = true

	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.NotNil(t, err)
	assert.Nil(t, schema)

	rootCoord.Error = false

	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})

	rootCoord.Error = true
	// should be cached with no error
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})
}

func TestMetaCache_GetNonExistCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "collection3")
	assert.NotNil(t, err)
	assert.Equal(t, id, int64(0))
	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection3")
	assert.NotNil(t, err)
	assert.Nil(t, schema)
}

func TestMetaCache_GetPartitionID(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetPartitionID(ctx, "collection1", "par1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection1", "par2")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection2", "par1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(3))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection2", "par2")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(4))
}

func TestMetaCache_GetPartitionError(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.Nil(t, err)

	// Test the case where ShowPartitionsResponse is not aligned
	id, err := globalMetaCache.GetPartitionID(ctx, "errorCollection", "par1")
	assert.NotNil(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))

	partitions, err2 := globalMetaCache.GetPartitions(ctx, "errorCollection")
	assert.NotNil(t, err2)
	log.Debug(err.Error())
	assert.Equal(t, len(partitions), 0)

	// Test non existed tables
	id, err = globalMetaCache.GetPartitionID(ctx, "nonExisted", "par1")
	assert.NotNil(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))

	// Test non existed partition
	id, err = globalMetaCache.GetPartitionID(ctx, "collection1", "par3")
	assert.NotNil(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))
}

func TestMetaCache_GetShards(t *testing.T) {
	var (
		ctx            = context.Background()
		collectionName = "collection1"
	)

	rootCoord := &MockRootCoordClientInterface{}
	qc := NewQueryCoordMock()
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, qc, shardMgr)
	require.Nil(t, err)

	qc.Init()
	qc.Start()
	defer qc.Stop()

	t.Run("No collection in meta cache", func(t *testing.T) {
		shards, err := globalMetaCache.GetShards(ctx, true, "non-exists")
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info invalid shardLeaders", func(t *testing.T) {
		qc.validShardLeaders = false
		shards, err := globalMetaCache.GetShards(ctx, false, collectionName)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info", func(t *testing.T) {
		qc.validShardLeaders = true
		shards, err := globalMetaCache.GetShards(ctx, true, collectionName)
		assert.NoError(t, err)
		assert.NotEmpty(t, shards)
		assert.Equal(t, 1, len(shards))
		assert.Equal(t, 3, len(shards["channel-1"]))

		// get from cache
		qc.validShardLeaders = false
		shards, err = globalMetaCache.GetShards(ctx, true, collectionName)

		assert.NoError(t, err)
		assert.NotEmpty(t, shards)
		assert.Equal(t, 1, len(shards))
		assert.Equal(t, 3, len(shards["channel-1"]))
	})
}

func TestMetaCache_ClearShards(t *testing.T) {
	var (
		ctx            = context.TODO()
		collectionName = "collection1"
	)

	rootCoord := &MockRootCoordClientInterface{}
	qc := NewQueryCoordMock()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, qc, mgr)
	require.Nil(t, err)

	qc.Init()
	qc.Start()
	defer qc.Stop()

	t.Run("Clear with no collection info", func(t *testing.T) {
		globalMetaCache.ClearShards("collection_not_exist")
	})

	t.Run("Clear valid collection empty cache", func(t *testing.T) {
		globalMetaCache.ClearShards(collectionName)
	})

	t.Run("Clear valid collection valid cache", func(t *testing.T) {

		qc.validShardLeaders = true
		shards, err := globalMetaCache.GetShards(ctx, true, collectionName)
		require.NoError(t, err)
		require.NotEmpty(t, shards)
		require.Equal(t, 1, len(shards))
		require.Equal(t, 3, len(shards["channel-1"]))

		globalMetaCache.ClearShards(collectionName)

		qc.validShardLeaders = false
		shards, err = globalMetaCache.GetShards(ctx, true, collectionName)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})
}

func TestMetaCache_PolicyInfo(t *testing.T) {
	client := &MockRootCoordClientInterface{}
	qc := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()

	t.Run("InitMetaCache", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, fmt.Errorf("mock error")
		}
		err := InitMetaCache(context.Background(), client, qc, mgr)
		assert.NotNil(t, err)

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
			}, nil
		}
		err = InitMetaCache(context.Background(), client, qc, mgr)
		assert.Nil(t, err)
	})

	t.Run("GetPrivilegeInfo", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2")},
			}, nil
		}
		err := InitMetaCache(context.Background(), client, qc, mgr)
		assert.Nil(t, err)
		policyInfos := globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))
		roles := globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 2, len(roles))
	})

	t.Run("GetPrivilegeInfo", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2")},
			}, nil
		}
		err := InitMetaCache(context.Background(), client, qc, mgr)
		assert.Nil(t, err)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: "policyX"})
		assert.Nil(t, err)
		policyInfos := globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 4, len(policyInfos))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRevokePrivilege, OpKey: "policyX"})
		assert.Nil(t, err)
		policyInfos = globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.Nil(t, err)
		roles := globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 3, len(roles))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.Nil(t, err)
		roles = globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 2, len(roles))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: ""})
		assert.NotNil(t, err)
		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: 100, OpKey: "policyX"})
		assert.NotNil(t, err)
	})
}

func TestMetaCache_LoadCache(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.Nil(t, err)

	t.Run("test IsCollectionLoaded", func(t *testing.T) {
		info, err := globalMetaCache.GetCollectionInfo(ctx, "collection1")
		assert.NoError(t, err)
		assert.True(t, info.isLoaded)
		// no collectionInfo of collection1, should access RootCoord
		assert.Equal(t, rootCoord.AccessCount, 1)
		// not loaded, should access QueryCoord
		assert.Equal(t, queryCoord.AccessCount, 1)

		info, err = globalMetaCache.GetCollectionInfo(ctx, "collection1")
		assert.NoError(t, err)
		assert.True(t, info.isLoaded)
		// shouldn't access QueryCoord or RootCoord again
		assert.Equal(t, rootCoord.AccessCount, 1)
		assert.Equal(t, queryCoord.AccessCount, 1)

		// test collection2 not fully loaded
		info, err = globalMetaCache.GetCollectionInfo(ctx, "collection2")
		assert.NoError(t, err)
		assert.False(t, info.isLoaded)
		// no collectionInfo of collection2, should access RootCoord
		assert.Equal(t, rootCoord.AccessCount, 2)
		// not loaded, should access QueryCoord
		assert.Equal(t, queryCoord.AccessCount, 2)
	})

	t.Run("test RemoveCollectionLoadCache", func(t *testing.T) {
		globalMetaCache.RemoveCollection(ctx, "collection1")
		info, err := globalMetaCache.GetCollectionInfo(ctx, "collection1")
		assert.NoError(t, err)
		assert.True(t, info.isLoaded)
		// should access QueryCoord
		assert.Equal(t, queryCoord.AccessCount, 3)
	})
}

func TestMetaCache_RemoveCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &MockQueryCoordClientInterface{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.Nil(t, err)

	info, err := globalMetaCache.GetCollectionInfo(ctx, "collection1")
	assert.NoError(t, err)
	assert.True(t, info.isLoaded)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.AccessCount, 1)

	info, err = globalMetaCache.GetCollectionInfo(ctx, "collection1")
	assert.NoError(t, err)
	assert.True(t, info.isLoaded)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.AccessCount, 1)

	globalMetaCache.RemoveCollection(ctx, "collection1")
	// no collectionInfo of collection2, should access RootCoord
	info, err = globalMetaCache.GetCollectionInfo(ctx, "collection1")
	assert.NoError(t, err)
	assert.True(t, info.isLoaded)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.AccessCount, 2)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1))
	// no collectionInfo of collection2, should access RootCoord
	info, err = globalMetaCache.GetCollectionInfo(ctx, "collection1")
	assert.NoError(t, err)
	assert.True(t, info.isLoaded)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.AccessCount, 3)
}
