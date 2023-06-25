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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	uatomic "go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var dbName = GetCurDBNameFromContextOrDefault(context.Background())

type MockRootCoordClientInterface struct {
	types.RootCoord
	Error       bool
	AccessCount int32

	listPolicy func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)
}

func (m *MockRootCoordClientInterface) IncAccessCount() {
	atomic.AddInt32(&m.AccessCount, 1)
}

func (m *MockRootCoordClientInterface) GetAccessCount() int {
	ret := atomic.LoadInt32(&m.AccessCount)
	return int(ret)
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
	m.IncAccessCount()
	if in.CollectionName == "collection1" || in.CollectionID == 1 {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(1),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
				Name:   "collection1",
			},
			DbName: dbName,
		}, nil
	}
	if in.CollectionName == "collection2" || in.CollectionID == 2 {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(2),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
				Name:   "collection2",
			},
			DbName: dbName,
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
			DbName: dbName,
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
	m.IncAccessCount()
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

// Simulate the cache path and the
func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
	id, err = globalMetaCache.GetCollectionID(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection2",
	})

	// test to get from cache, this should trigger root request
	id, err = globalMetaCache.GetCollectionID(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})

}

func TestMetaCache_GetCollectionName(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	db, collection, err := globalMetaCache.GetDatabaseAndCollectionName(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, db, dbName)
	assert.Equal(t, collection, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
	_, collection, err = globalMetaCache.GetDatabaseAndCollectionName(ctx, 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection2",
	})

	// test to get from cache, this should trigger root request
	_, collection, err = globalMetaCache.GetDatabaseAndCollectionName(ctx, 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
}

func TestMetaCache_GetCollectionFailure(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)
	rootCoord.Error = true

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Error(t, err)
	assert.Nil(t, schema)

	rootCoord.Error = false

	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})

	rootCoord.Error = true
	// should be cached with no error
	assert.NoError(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
}

func TestMetaCache_GetNonExistCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection3")
	assert.Error(t, err)
	assert.Equal(t, id, int64(0))
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection3")
	assert.Error(t, err)
	assert.Nil(t, schema)
}

func TestMetaCache_GetPartitionID(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par2")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection2", "par1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(3))
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection2", "par2")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(4))
}

func TestMetaCache_ConcurrentTest1(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	cnt := 100
	getCollectionCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			//GetCollectionSchema will never fail
			schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
			assert.NoError(t, err)
			assert.Equal(t, schema, &schemapb.CollectionSchema{
				AutoID: true,
				Fields: []*schemapb.FieldSchema{},
				Name:   "collection1",
			})
			time.Sleep(10 * time.Millisecond)
		}
	}

	getPartitionCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			//GetPartitions may fail
			globalMetaCache.GetPartitions(ctx, dbName, "collection1")
			time.Sleep(10 * time.Millisecond)
		}
	}

	invalidCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			//periodically invalid collection cache
			globalMetaCache.RemoveCollection(ctx, dbName, "collection1")
			time.Sleep(10 * time.Millisecond)
		}
	}

	wg.Add(1)
	go getCollectionCacheFunc(&wg)

	wg.Add(1)
	go invalidCacheFunc(&wg)

	wg.Add(1)
	go getPartitionCacheFunc(&wg)
	wg.Wait()
}

func TestMetaCache_GetPartitionError(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	// Test the case where ShowPartitionsResponse is not aligned
	id, err := globalMetaCache.GetPartitionID(ctx, dbName, "errorCollection", "par1")
	assert.Error(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))

	partitions, err2 := globalMetaCache.GetPartitions(ctx, dbName, "errorCollection")
	assert.NotNil(t, err2)
	log.Debug(err.Error())
	assert.Equal(t, len(partitions), 0)

	// Test non existed tables
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "nonExisted", "par1")
	assert.Error(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))

	// Test non existed partition
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par3")
	assert.Error(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))
}

func TestMetaCache_GetShards(t *testing.T) {
	var (
		ctx            = context.Background()
		collectionName = "collection1"
	)

	rootCoord := &MockRootCoordClientInterface{}
	qc := getQueryCoord()
	qc.EXPECT().Init().Return(nil)
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, qc, shardMgr)
	require.Nil(t, err)

	qc.Init()
	qc.Start()
	defer qc.Stop()

	t.Run("No collection in meta cache", func(t *testing.T) {
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, "non-exists")
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info invalid shardLeaders", func(t *testing.T) {
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "not implemented",
			},
		}, nil).Times(1)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		shards, err := globalMetaCache.GetShards(ctx, false, dbName, collectionName)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info", func(t *testing.T) {
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, collectionName)
		assert.NoError(t, err)
		assert.NotEmpty(t, shards)
		assert.Equal(t, 1, len(shards))
		assert.Equal(t, 3, len(shards["channel-1"]))

		// get from cache
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "not implemented",
			},
		}, nil)
		shards, err = globalMetaCache.GetShards(ctx, true, dbName, collectionName)

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
	qc := getQueryCoord()
	qc.EXPECT().Init().Return(nil)
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, qc, mgr)
	require.Nil(t, err)

	qc.Init()
	qc.Start()
	defer qc.Stop()

	t.Run("Clear with no collection info", func(t *testing.T) {
		globalMetaCache.DeprecateShardCache(dbName, "collection_not_exist")
	})

	t.Run("Clear valid collection empty cache", func(t *testing.T) {
		globalMetaCache.DeprecateShardCache(dbName, collectionName)
	})

	t.Run("Clear valid collection valid cache", func(t *testing.T) {

		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil).Times(1)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, collectionName)
		require.NoError(t, err)
		require.NotEmpty(t, shards)
		require.Equal(t, 1, len(shards))
		require.Equal(t, 3, len(shards["channel-1"]))

		globalMetaCache.DeprecateShardCache(dbName, collectionName)

		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "not implemented",
			},
		}, nil)
		shards, err = globalMetaCache.GetShards(ctx, true, dbName, collectionName)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})
}

func TestMetaCache_PolicyInfo(t *testing.T) {
	client := &MockRootCoordClientInterface{}
	qc := &types.MockQueryCoord{}
	mgr := newShardClientMgr()

	t.Run("InitMetaCache", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, fmt.Errorf("mock error")
		}
		err := InitMetaCache(context.Background(), client, qc, mgr)
		assert.Error(t, err)

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
			}, nil
		}
		err = InitMetaCache(context.Background(), client, qc, mgr)
		assert.NoError(t, err)
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
		assert.NoError(t, err)
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
		assert.NoError(t, err)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos := globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 4, len(policyInfos))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRevokePrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos = globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles := globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 3, len(roles))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles = globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 2, len(roles))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: ""})
		assert.Error(t, err)
		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: 100, OpKey: "policyX"})
		assert.Error(t, err)
	})
}

func TestMetaCache_RemoveCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)

	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs:       []UniqueID{1, 2},
		InMemoryPercentages: []int64{100, 50},
	}, nil)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1")
	assert.NoError(t, err)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1")
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	globalMetaCache.RemoveCollection(ctx, dbName, "collection1")
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1")
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 2)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1))
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1")
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 3)
}

func TestMetaCache_ExpireShardLeaderCache(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.ProxyCfg.ShardLeaderCacheInterval.Key, "1")

	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &types.MockQueryCoord{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)

	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs:       []UniqueID{1},
		InMemoryPercentages: []int64{100},
	}, nil)
	queryCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil)
	nodeInfos, err := globalMetaCache.GetShards(ctx, true, dbName, "collection1")
	assert.NoError(t, err)
	assert.Len(t, nodeInfos["channel-1"], 3)

	queryCoord.ExpectedCalls = nil
	queryCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001"},
			},
		},
	}, nil)

	assert.Eventually(t, func() bool {
		nodeInfos, err := globalMetaCache.GetShards(ctx, true, dbName, "collection1")
		assert.NoError(t, err)
		return len(nodeInfos["channel-1"]) == 2
	}, 3*time.Second, 1*time.Second)

	queryCoord.ExpectedCalls = nil
	queryCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil)

	assert.Eventually(t, func() bool {
		nodeInfos, err := globalMetaCache.GetShards(ctx, true, dbName, "collection1")
		assert.NoError(t, err)
		return len(nodeInfos["channel-1"]) == 3
	}, 3*time.Second, 1*time.Second)

	queryCoord.ExpectedCalls = nil
	queryCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
			{
				ChannelName: "channel-2",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil)

	assert.Eventually(t, func() bool {
		nodeInfos, err := globalMetaCache.GetShards(ctx, true, dbName, "collection1")
		assert.NoError(t, err)
		return len(nodeInfos["channel-1"]) == 3 && len(nodeInfos["channel-2"]) == 3
	}, 3*time.Second, 1*time.Second)
}

func TestGlobalMetaCache_ShuffleShardLeaders(t *testing.T) {
	shards := map[string][]nodeInfo{
		"channel-1": {
			{
				nodeID:  1,
				address: "localhost:9000",
			},
			{
				nodeID:  2,
				address: "localhost:9000",
			},
			{
				nodeID:  3,
				address: "localhost:9000",
			},
		},
	}
	sl := &shardLeaders{
		deprecated:   uatomic.NewBool(false),
		idx:          uatomic.NewInt64(5),
		shardLeaders: shards,
	}

	reader := sl.GetReader()
	result := reader.Shuffle()
	assert.Len(t, result["channel-1"], 3)
	assert.Equal(t, int64(1), result["channel-1"][0].nodeID)

	reader = sl.GetReader()
	result = reader.Shuffle()
	assert.Len(t, result["channel-1"], 3)
	assert.Equal(t, int64(2), result["channel-1"][0].nodeID)

	reader = sl.GetReader()
	result = reader.Shuffle()
	assert.Len(t, result["channel-1"], 3)
	assert.Equal(t, int64(3), result["channel-1"][0].nodeID)
}
