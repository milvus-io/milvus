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
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var dbName = GetCurDBNameFromContextOrDefault(context.Background())

type MockRootCoordClientInterface struct {
	types.RootCoordClient
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

func (m *MockRootCoordClientInterface) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	if in.CollectionName == "collection1" || in.CollectionID == 1 {
		return &milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{1, 2},
			CreatedTimestamps:    []uint64{100, 200},
			CreatedUtcTimestamps: []uint64{100, 200},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "collection2" || in.CollectionID == 2 {
		return &milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{3, 4},
			CreatedTimestamps:    []uint64{201, 202},
			CreatedUtcTimestamps: []uint64{201, 202},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
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

func (m *MockRootCoordClientInterface) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.IncAccessCount()
	if in.CollectionName == "collection1" || in.CollectionID == 1 {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
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
			Status:       merr.Success(),
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
			Status:       merr.Success(),
			CollectionID: typeutil.UniqueID(3),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
			DbName: dbName,
		}, nil
	}

	err := merr.WrapErrCollectionNotFound(in.CollectionName)
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Status(err),
		Schema: nil,
	}, nil
}

func (m *MockRootCoordClientInterface) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.IncAccessCount()
	if req.Username == "mockUser" {
		encryptedPassword, _ := crypto.PasswordEncrypt("mockPass")
		return &rootcoordpb.GetCredentialResponse{
			Status:   merr.Success(),
			Username: "mockUser",
			Password: encryptedPassword,
		}, nil
	}

	err := fmt.Errorf("can't find credential: " + req.Username)
	return nil, err
}

func (m *MockRootCoordClientInterface) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}

	return &milvuspb.ListCredUsersResponse{
		Status:    merr.Success(),
		Usernames: []string{"mockUser"},
	}, nil
}

func (m *MockRootCoordClientInterface) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	if m.listPolicy != nil {
		return m.listPolicy(ctx, in)
	}
	return &internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil
}

// Simulate the cache path and the
func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}

	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()

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
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
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
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
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
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
}

func TestMetaCache_GetBasicCollectionInfo(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}

	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	// should be no data race.
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
		assert.NoError(t, err)
		assert.Equal(t, info.collID, int64(1))
		_ = info.consistencyLevel
		_ = info.createdTimestamp
		_ = info.createdUtcTimestamp
	}()
	go func() {
		defer wg.Done()
		info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
		assert.NoError(t, err)
		assert.Equal(t, info.collID, int64(1))
		_ = info.consistencyLevel
		_ = info.createdTimestamp
		_ = info.createdUtcTimestamp
	}()
	wg.Wait()
}

func TestMetaCache_GetCollectionName(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	collection, err := globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
	collection, err = globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection2",
	})

	// test to get from cache, this should trigger root request
	collection, err = globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
}

func TestMetaCache_GetCollectionFailure(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
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
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})

	rootCoord.Error = true
	// should be cached with no error
	assert.NoError(t, err)
	assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
		Name:   "collection1",
	})
}

func TestMetaCache_GetNonExistCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
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
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
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
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	cnt := 100
	getCollectionCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			// GetCollectionSchema will never fail
			schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
			assert.NoError(t, err)
			assert.Equal(t, schema.CollectionSchema, &schemapb.CollectionSchema{
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
			// GetPartitions may fail
			globalMetaCache.GetPartitions(ctx, dbName, "collection1")
			time.Sleep(10 * time.Millisecond)
		}
	}

	invalidCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			// periodically invalid collection cache
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
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, mgr)
	assert.NoError(t, err)

	// Test the case where ShowPartitionsResponse is not aligned
	id, err := globalMetaCache.GetPartitionID(ctx, dbName, "errorCollection", "par1")
	assert.Error(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))

	partitions, err2 := globalMetaCache.GetPartitions(ctx, dbName, "errorCollection")
	assert.NotNil(t, err2)
	assert.Equal(t, len(partitions), 0)

	// Test non existed tables
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "nonExisted", "par1")
	assert.Error(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))

	// Test non existed partition
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par3")
	assert.Error(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))
}

func TestMetaCache_GetShards(t *testing.T) {
	var (
		ctx            = context.Background()
		collectionName = "collection1"
		collectionID   = int64(1)
	)

	rootCoord := &MockRootCoordClientInterface{}
	qc := getQueryCoordClient()
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, qc, shardMgr)
	require.Nil(t, err)

	t.Run("No collection in meta cache", func(t *testing.T) {
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, "non-exists", 0)
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
		shards, err := globalMetaCache.GetShards(ctx, false, dbName, collectionName, collectionID)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info", func(t *testing.T) {
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: merr.Success(),
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
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)
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
		shards, err = globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)

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
		collectionID   = int64(1)
	)

	rootCoord := &MockRootCoordClientInterface{}
	qc := getQueryCoordClient()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, qc, mgr)
	require.Nil(t, err)

	t.Run("Clear with no collection info", func(t *testing.T) {
		globalMetaCache.DeprecateShardCache(dbName, "collection_not_exist")
	})

	t.Run("Clear valid collection empty cache", func(t *testing.T) {
		globalMetaCache.DeprecateShardCache(dbName, collectionName)
	})

	t.Run("Clear valid collection valid cache", func(t *testing.T) {
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: merr.Success(),
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
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)
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
		shards, err = globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})
}

func TestMetaCache_PolicyInfo(t *testing.T) {
	client := &MockRootCoordClientInterface{}
	qc := &mocks.MockQueryCoordClient{}
	mgr := newShardClientMgr()

	t.Run("InitMetaCache", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, fmt.Errorf("mock error")
		}
		err := InitMetaCache(context.Background(), client, qc, mgr)
		assert.Error(t, err)

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
			}, nil
		}
		err = InitMetaCache(context.Background(), client, qc, mgr)
		assert.NoError(t, err)
	})

	t.Run("GetPrivilegeInfo", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
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
				Status:      merr.Success(),
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

	t.Run("Delete user or drop role", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role2", "Collection", "collection1", "read", "default"),
					"policy2",
					"policy3",
				},
				UserRoles: []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2"), funcutil.EncodeUserRoleCache("foo2", "role3")},
			}, nil
		}
		err := InitMetaCache(context.Background(), client, qc, mgr)
		assert.NoError(t, err)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDeleteUser, OpKey: "foo"})
		assert.NoError(t, err)

		roles := globalMetaCache.GetUserRole("foo")
		assert.Len(t, roles, 0)

		roles = globalMetaCache.GetUserRole("foo2")
		assert.Len(t, roles, 2)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDropRole, OpKey: "role2"})
		assert.NoError(t, err)
		roles = globalMetaCache.GetUserRole("foo2")
		assert.Len(t, roles, 1)
		assert.Equal(t, "role3", roles[0])

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2"), funcutil.EncodeUserRoleCache("foo2", "role3")},
			}, nil
		}
		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh})
		assert.NoError(t, err)
		roles = globalMetaCache.GetUserRole("foo")
		assert.Len(t, roles, 2)
	})
}

func TestMetaCache_RemoveCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)

	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:              merr.Success(),
		CollectionIDs:       []UniqueID{1, 2},
		InMemoryPercentages: []int64{100, 50},
	}, nil)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	globalMetaCache.RemoveCollection(ctx, dbName, "collection1")
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 2)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1))
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 3)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1))
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.GetAccessCount(), 4)
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

func TestMetaCache_Database(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)
	assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	_, err = GetCachedCollectionSchema(ctx, dbName, "collection1")
	assert.NoError(t, err)
	assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), true)
	assert.Equal(t, CheckDatabase(ctx, dbName), true)
}

func TestGetDatabaseInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockRootCoordClient(t)
		queryCoord := &mocks.MockQueryCoordClient{}
		shardMgr := newShardClientMgr()
		cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)

		rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{
			Status: merr.Success(),
			DbID:   1,
			DbName: "default",
		}, nil).Once()
		{
			dbInfo, err := cache.GetDatabaseInfo(ctx, "default")
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(1), dbInfo.dbID)
		}

		{
			dbInfo, err := cache.GetDatabaseInfo(ctx, "default")
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(1), dbInfo.dbID)
		}
	})

	t.Run("error", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockRootCoordClient(t)
		queryCoord := &mocks.MockQueryCoordClient{}
		shardMgr := newShardClientMgr()
		cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)

		rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{
			Status: merr.Status(errors.New("mock error: describe database")),
		}, nil).Once()
		_, err = cache.GetDatabaseInfo(ctx, "default")
		assert.Error(t, err)
	})
}

func TestMetaCache_AllocID(t *testing.T) {
	ctx := context.Background()
	queryCoord := &mocks.MockQueryCoordClient{}
	shardMgr := newShardClientMgr()

	t.Run("success", func(t *testing.T) {
		rootCoord := mocks.NewMockRootCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(nil),
			ID:     11198,
			Count:  10,
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.NoError(t, err)
		assert.Equal(t, id, int64(11198))
	})

	t.Run("error", func(t *testing.T) {
		rootCoord := mocks.NewMockRootCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(nil),
		}, fmt.Errorf("mock error"))
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})

	t.Run("failed", func(t *testing.T) {
		rootCoord := mocks.NewMockRootCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(fmt.Errorf("mock failed")),
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})
}

func TestGlobalMetaCache_UpdateDBInfo(t *testing.T) {
	rootCoord := mocks.NewMockRootCoordClient(t)
	queryCoord := mocks.NewMockQueryCoordClient(t)
	shardMgr := newShardClientMgr()
	ctx := context.Background()

	cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)

	t.Run("fail to list db", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Code:      500,
			},
		}, nil).Once()
		err := cache.updateDBInfo(ctx)
		assert.Error(t, err)
	})

	t.Run("fail to list collection", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			DbNames: []string{"db1"},
		}, nil).Once()
		rootCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Code:      500,
			},
		}, nil).Once()
		err := cache.updateDBInfo(ctx)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			DbNames: []string{"db1"},
		}, nil).Once()
		rootCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionNames: []string{"collection1"},
			CollectionIds:   []int64{1},
		}, nil).Once()
		err := cache.updateDBInfo(ctx)
		assert.NoError(t, err)
		assert.Len(t, cache.dbCollectionInfo, 1)
		assert.Len(t, cache.dbCollectionInfo["db1"], 1)
		assert.Equal(t, "collection1", cache.dbCollectionInfo["db1"][1])
	})
}

func TestGlobalMetaCache_GetCollectionNamesByID(t *testing.T) {
	rootCoord := mocks.NewMockRootCoordClient(t)
	queryCoord := mocks.NewMockQueryCoordClient(t)
	shardMgr := newShardClientMgr()
	ctx := context.Background()

	t.Run("fail to update db info", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Code:      500,
			},
		}, nil).Once()

		cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)

		_, _, err = cache.GetCollectionNamesByID(ctx, []int64{1})
		assert.Error(t, err)
	})

	t.Run("not found collection", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			DbNames: []string{"db1"},
		}, nil).Once()
		rootCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionNames: []string{"collection1"},
			CollectionIds:   []int64{1},
		}, nil).Once()

		cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)
		_, _, err = cache.GetCollectionNamesByID(ctx, []int64{2})
		assert.Error(t, err)
	})

	t.Run("not found collection 2", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			DbNames: []string{"db1"},
		}, nil).Once()
		rootCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionNames: []string{"collection1"},
			CollectionIds:   []int64{1},
		}, nil).Once()

		cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)
		_, _, err = cache.GetCollectionNamesByID(ctx, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		rootCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			DbNames: []string{"db1"},
		}, nil).Once()
		rootCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionNames: []string{"collection1", "collection2"},
			CollectionIds:   []int64{1, 2},
		}, nil).Once()

		cache, err := NewMetaCache(rootCoord, queryCoord, shardMgr)
		assert.NoError(t, err)
		dbNames, collectionNames, err := cache.GetCollectionNamesByID(ctx, []int64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, []string{"collection1", "collection2"}, collectionNames)
		assert.Equal(t, []string{"db1", "db1"}, dbNames)
	})
}

func TestMetaCache_InvalidateShardLeaderCache(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.ProxyCfg.ShardLeaderCacheInterval.Key, "1")

	ctx := context.Background()
	rootCoord := &MockRootCoordClientInterface{}
	queryCoord := &mocks.MockQueryCoordClient{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)

	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:              merr.Success(),
		CollectionIDs:       []UniqueID{1},
		InMemoryPercentages: []int64{100},
	}, nil)

	called := uatomic.NewInt32(0)
	queryCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context,
		gslr *querypb.GetShardLeadersRequest, co ...grpc.CallOption,
	) (*querypb.GetShardLeadersResponse, error) {
		called.Inc()
		return &querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil
	})
	nodeInfos, err := globalMetaCache.GetShards(ctx, true, dbName, "collection1", 1)
	assert.NoError(t, err)
	assert.Len(t, nodeInfos["channel-1"], 3)
	assert.Equal(t, called.Load(), int32(1))

	globalMetaCache.GetShards(ctx, true, dbName, "collection1", 1)
	assert.Equal(t, called.Load(), int32(1))

	globalMetaCache.InvalidateShardLeaderCache([]int64{1})
	nodeInfos, err = globalMetaCache.GetShards(ctx, true, dbName, "collection1", 1)
	assert.NoError(t, err)
	assert.Len(t, nodeInfos["channel-1"], 3)
	assert.Equal(t, called.Load(), int32(2))
}

func TestSchemaInfo_GetLoadFieldIDs(t *testing.T) {
	type testCase struct {
		tag              string
		schema           *schemapb.CollectionSchema
		loadFields       []string
		skipDynamicField bool
		expectResult     []int64
		expectErr        bool
	}

	rowIDField := &schemapb.FieldSchema{
		FieldID:  common.RowIDField,
		Name:     common.RowIDFieldName,
		DataType: schemapb.DataType_Int64,
	}
	timestampField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	pkField := &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	scalarField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
	}
	scalarFieldSkipLoad := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.FieldSkipLoadKey, Value: "true"},
		},
	}
	partitionKeyField := &schemapb.FieldSchema{
		FieldID:        common.StartOfUserFieldID + 2,
		Name:           "part_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	vectorField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 3,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "768"},
		},
	}
	dynamicField := &schemapb.FieldSchema{
		FieldID:   common.StartOfUserFieldID + 4,
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}

	testCases := []testCase{
		{
			tag: "default",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 1, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4},
			expectErr:        false,
		},
		{
			tag: "default_from_schema",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarFieldSkipLoad,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4},
			expectErr:        false,
		},
		{
			tag: "load_fields",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       []string{"pk", "part_key", "vector"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4},
			expectErr:        false,
		},
		{
			tag: "load_fields_skip_dynamic",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       []string{"pk", "part_key", "vector"},
			skipDynamicField: true,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3},
			expectErr:        false,
		},
		{
			tag: "pk_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       []string{"part_key", "vector"},
			skipDynamicField: true,
			expectErr:        true,
		},
		{
			tag: "part_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       []string{"pk", "vector"},
			skipDynamicField: true,
			expectErr:        true,
		},
		{
			tag: "vector_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			loadFields:       []string{"pk", "part_key"},
			skipDynamicField: true,
			expectErr:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			info := newSchemaInfo(tc.schema)

			result, err := info.GetLoadFieldIDs(tc.loadFields, tc.skipDynamicField)
			if tc.expectErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.expectResult, result)
		})
	}
}
