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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var dbName = GetCurDBNameFromContextOrDefault(context.Background())

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric, ok := observer.(prometheus.Metric)
	require.True(t, ok)
	pb := &dto.Metric{}
	require.NoError(t, metric.Write(pb))
	return pb.GetHistogram().GetSampleCount()
}

func TestMetaCacheFillGateMetrics(t *testing.T) {
	cache := &MetaCache{}
	nodeID := paramtable.GetStringNodeID()
	readObserver := metrics.ProxyMetaCacheLockWaitLatency.WithLabelValues(nodeID, "read", metaCacheOpCollectionFill)
	writeObserver := metrics.ProxyMetaCacheLockWaitLatency.WithLabelValues(nodeID, "write", metaCacheOpCollectionInvalidate)
	invalidationObserver := metrics.ProxyMetaCacheInvalidationLatency.WithLabelValues(nodeID, metaCacheOpCollectionInvalidate)

	readBefore := histogramSampleCount(t, readObserver)
	readUnlock := cache.lockFillRead(metaCacheOpCollectionFill)
	readUnlock()
	assert.Equal(t, readBefore+1, histogramSampleCount(t, readObserver))

	writeBefore := histogramSampleCount(t, writeObserver)
	writeUnlock := cache.lockFillWrite(metaCacheOpCollectionInvalidate)
	writeUnlock()
	assert.Equal(t, writeBefore+1, histogramSampleCount(t, writeObserver))

	invalidationBefore := histogramSampleCount(t, invalidationObserver)
	observeMetaCacheInvalidation(metaCacheOpCollectionInvalidate)()
	assert.Equal(t, invalidationBefore+1, histogramSampleCount(t, invalidationObserver))
}

func TestMetaCacheGlobalFillGateBlocksAcrossDatabases(t *testing.T) {
	cache := &MetaCache{
		collections: make(map[UniqueID]*collectionInfo),
		nameIdx:     make(map[string]map[string]UniqueID),
		aliasInfo:   make(map[string]map[string]string),
	}
	seedCollection(cache, "db-c", "cached", 1)

	// DB-A has a slow fill in progress. Holding the shared gate directly keeps
	// the test independent of coordinator timing while exercising the same gate
	// used by collection, partition, and database fills.
	releaseDBAFill := cache.lockFillRead(metaCacheOpCollectionFill)

	// DB-B queues an invalidation writer behind DB-A's fill and deliberately
	// holds the writer gate once acquired so DB-C's later fill can be observed.
	writerAcquired := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		unlock := cache.lockFillWrite(metaCacheOpCollectionInvalidate)
		close(writerAcquired)
		<-releaseWriter
		unlock()
	}()

	// TryRLock succeeds while only readers exist and fails once an RWMutex
	// writer is queued. This gives a deterministic queued-writer gate without a
	// scheduler-dependent sleep.
	require.Eventually(t, func() bool {
		if cache.fillMu.TryRLock() {
			cache.fillMu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "DB-B invalidation did not queue behind DB-A fill")

	// A DB-C cache hit uses only m.mu and must remain available even while the
	// global fill writer is queued.
	hitDone := make(chan struct{})
	go func() {
		defer close(hitDone)
		entry, ok := cache.getCollection("db-c", "cached", 0)
		assert.True(t, ok)
		assert.Equal(t, UniqueID(1), entry.collID)
	}()
	select {
	case <-hitDone:
	case <-time.After(time.Second):
		t.Fatal("DB-C cache hit was blocked by the global fill gate")
	}

	// A new DB-C fill arrives after DB-B's writer is queued. Go's RWMutex gives
	// the queued writer priority, so this unrelated database is blocked too.
	dbcFillAcquired := make(chan struct{})
	dbcFillDone := make(chan struct{})
	go func() {
		defer close(dbcFillDone)
		unlock := cache.lockFillRead(metaCacheOpCollectionFill)
		close(dbcFillAcquired)
		unlock()
	}()
	select {
	case <-dbcFillAcquired:
		t.Fatal("DB-C fill bypassed the queued DB-B invalidation writer")
	case <-time.After(20 * time.Millisecond):
	}

	releaseDBAFill()
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("DB-B invalidation did not acquire after DB-A fill released")
	}
	select {
	case <-dbcFillAcquired:
		t.Fatal("DB-C fill acquired while DB-B invalidation held the writer gate")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseWriter)
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("DB-B invalidation did not release the writer gate")
	}
	select {
	case <-dbcFillDone:
	case <-time.After(time.Second):
		t.Fatal("DB-C fill did not proceed after the invalidation released")
	}
}

type MockMixCoordClientInterface struct {
	types.MixCoordClient
	Error       bool
	AccessCount int32

	listPolicy            func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)
	showLoadCollections   func(ctx context.Context, in *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)
	getShardLeaders       func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error)
	listResourceGroups    func(ctx context.Context, in *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error)
	describeResourceGroup func(ctx context.Context, in *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error)
}

func EqualSchema(t *testing.T, expect, actual *schemapb.CollectionSchema) {
	assert.Equal(t, expect.AutoID, actual.AutoID)
	assert.Equal(t, expect.Description, actual.Description)
	assert.Equal(t, expect.Name, actual.Name)
	assert.Equal(t, expect.EnableDynamicField, actual.EnableDynamicField)
	assert.Equal(t, len(expect.Fields), len(actual.Fields))
	for i := range expect.Fields {
		assert.Equal(t, expect.Fields[i], actual.Fields[i])
	}
	assert.Equal(t, len(expect.Functions), len(actual.Functions))
	for i := range expect.Functions {
		assert.Equal(t, expect.Functions[i], actual.Functions[i])
	}
	assert.Equal(t, len(expect.Properties), len(actual.Properties))
	for i := range expect.Properties {
		assert.Equal(t, expect.Properties[i], actual.Properties[i])
	}
}

func (m *MockMixCoordClientInterface) IncAccessCount() {
	atomic.AddInt32(&m.AccessCount, 1)
}

func (m *MockMixCoordClientInterface) GetAccessCount() int {
	ret := atomic.LoadInt32(&m.AccessCount)
	return int(ret)
}

func (m *MockMixCoordClientInterface) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	if in.CollectionName == "collection1" || in.CollectionName == "collection1_alias" || in.CollectionID == 1 {
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

func (m *MockMixCoordClientInterface) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.IncAccessCount()
	if in.CollectionName == "collection1" || in.CollectionName == "collection1_alias" || in.CollectionID == 1 {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: typeutil.UniqueID(1),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
				Name:   "collection1",
			},
			Aliases:     []string{"collection1_alias"},
			DbName:      dbName,
			RequestTime: 100,
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
			DbName:      dbName,
			RequestTime: 100,
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: typeutil.UniqueID(3),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
			DbName:      dbName,
			RequestTime: 100,
		}, nil
	}

	err := merr.WrapErrCollectionNotFound(in.CollectionName)
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Status(err),
		Schema: nil,
	}, nil
}

func (m *MockMixCoordClientInterface) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
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

	err := fmt.Errorf("can't find credential: %s", req.Username)
	return nil, err
}

func (m *MockMixCoordClientInterface) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}

	return &milvuspb.ListCredUsersResponse{
		Status:    merr.Success(),
		Usernames: []string{"mockUser"},
	}, nil
}

func (m *MockMixCoordClientInterface) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	if m.listPolicy != nil {
		return m.listPolicy(ctx, in)
	}
	return &internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil
}

func (c *MockMixCoordClientInterface) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	panic("implement me")
}

// GetTimeTickChannel get timetick channel name
func (c *MockMixCoordClientInterface) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

// GetStatisticsChannel just define a channel, not used currently
func (c *MockMixCoordClientInterface) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	panic("implement me")
}

// ShowConfigurations gets specified configurations para of RootCoord
func (c *MockMixCoordClientInterface) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	panic("implement me")
}

// CreateCollection create collection
func (c *MockMixCoordClientInterface) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DropCollection drop collection
func (c *MockMixCoordClientInterface) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// HasCollection check collection existence
func (c *MockMixCoordClientInterface) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	panic("implement me")
}

// CreatePartition create partition
func (c *MockMixCoordClientInterface) AddCollectionField(ctx context.Context, in *milvuspb.AddCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AddCollectionStructField(ctx context.Context, in *milvuspb.AddCollectionStructFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// describeCollectionInternal return collection info
func (c *MockMixCoordClientInterface) describeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	panic("implement me")
}

// ShowCollections list all collection names
func (c *MockMixCoordClientInterface) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: merr.Success(),
	}, nil
}

// ShowCollectionIDs returns all collection IDs.
func (c *MockMixCoordClientInterface) ShowCollectionIDs(ctx context.Context, in *rootcoordpb.ShowCollectionIDsRequest, opts ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// CreatePartition create partition
func (c *MockMixCoordClientInterface) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DropPartition drop partition
func (c *MockMixCoordClientInterface) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// HasPartition check partition existence
func (c *MockMixCoordClientInterface) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	panic("implement me")
}

// showPartitionsInternal list all partitions in collection
func (c *MockMixCoordClientInterface) showPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	panic("implement me")
}

// AllocTimestamp global timestamp allocator
func (c *MockMixCoordClientInterface) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	panic("implement me")
}

// AllocID global ID allocator
func (c *MockMixCoordClientInterface) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	panic("implement me")
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *MockMixCoordClientInterface) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ShowSegments list all segments
func (c *MockMixCoordClientInterface) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	panic("implement me")
}

// GetVChannels returns all vchannels belonging to the pchannel.
func (c *MockMixCoordClientInterface) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*rootcoordpb.GetPChannelInfoResponse, error) {
	panic("implement me")
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *MockMixCoordClientInterface) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// CreateAlias create collection alias
func (c *MockMixCoordClientInterface) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DropAlias drop collection alias
func (c *MockMixCoordClientInterface) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// AlterAlias alter collection alias
func (c *MockMixCoordClientInterface) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DescribeAlias describe alias
func (c *MockMixCoordClientInterface) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	return &milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "alias not found",
		},
	}, nil
}

// ListAliases list all aliases of db or collection
func (c *MockMixCoordClientInterface) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterRole(ctx context.Context, req *milvuspb.AlterRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DescribeDatabase(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// Flush flushes a collection's data
func (c *MockMixCoordClientInterface) Flush(ctx context.Context, req *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	panic("implement me")
}

// AssignSegmentID applies allocations for specified Coolection/Partition and related Channel Name(Virtial Channel)
//
// ctx is the context to control request deadline and cancellation
// req contains the requester's info(id and role) and the list of Assignment Request,
// which contains the specified collection, partitaion id, the related VChannel Name and row count it needs
//
// response struct `AssignSegmentIDResponse` contains the assignment result for each request
// error is returned only when some communication issue occurs
// if some error occurs in the process of `AssignSegmentID`, it will be recorded and returned in `Status` field of response
//
// `AssignSegmentID` will applies current configured allocation policies for each request
// if the VChannel is newly used, `WatchDmlChannels` will be invoked to notify a `DataNode`(selected by policy) to watch it
// if there is anything make the allocation impossible, the response will not contain the corresponding result
func (c *MockMixCoordClientInterface) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AllocSegment(ctx context.Context, in *datapb.AllocSegmentRequest, opts ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
	panic("implement me")
}

// GetSegmentStates requests segment state information
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment id to query
//
// response struct `GetSegmentStatesResponse` contains the list of each state query result
//
//	when the segment is not found, the state entry will has the field `Status`  to identify failure
//	otherwise the Segment State and Start position information will be returned
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

// GetInsertBinlogPaths requests binlog paths for specified segment
//
// ctx is the context to control request deadline and cancellation
// req contains the segment id to query
//
// response struct `GetInsertBinlogPathsResponse` contains the fields list
//
//	and corresponding binlog path list
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

// GetCollectionStatistics requests collection statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection id to query
//
// response struct `GetCollectionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

// GetPartitionStatistics requests partition statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection and partition id to query
//
// response struct `GetPartitionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

// GetSegmentInfoChannel DEPRECATED
// legacy api to get SegmentInfo Channel name
func (c *MockMixCoordClientInterface) GetSegmentInfoChannel(ctx context.Context, _ *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

// GetSegmentInfo requests segment info
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment ids to query
//
// response struct `GetSegmentInfoResponse` contains the list of segment info
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

// SaveBinlogPaths updates segments binlogs(including insert binlogs, stats logs and delta logs)
//
//	and related message stream positions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response status contains the status/error code and failing reason if any
// error is returned only when some communication issue occurs
//
// there is a constraint that the `SaveBinlogPaths` requests of same segment shall be passed in sequence
//
//		the root reason is each `SaveBinlogPaths` will overwrite the checkpoint position
//	 if the constraint is broken, the checkpoint position will not be monotonically increasing and the integrity will be compromised
func (c *MockMixCoordClientInterface) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// GetRecoveryInfo request segment recovery info of collection/partition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

// GetRecoveryInfoV2 request segment recovery info of collection/partitions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partitions id to query
//
// response struct `GetRecoveryInfoResponseV2` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	panic("implement me")
}

// GetChannelRecoveryInfo returns the corresponding vchannel info.
func (c *MockMixCoordClientInterface) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
	panic("implement me")
}

// GetFlushedSegments returns flushed segment list of requested collection/parition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
//	when partition is lesser or equal to 0, all flushed segments of collection will be returned
//
// response struct `GetFlushedSegmentsResponse` contains flushed segment id list
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

// GetSegmentsByStates returns segment list of requested collection/partition and segment states
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id and segment states to query
// when partition is lesser or equal to 0, all segments of collection will be returned
//
// response struct `GetSegmentsByStatesResponse` contains segment id list
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

// ManualCompaction triggers a compaction for a collection
func (c *MockMixCoordClientInterface) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	panic("implement me")
}

// GetCompactionState gets the state of a compaction
func (c *MockMixCoordClientInterface) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	panic("implement me")
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (c *MockMixCoordClientInterface) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	panic("implement me")
}

// WatchChannels notifies DataCoord to watch vchannels of a collection
func (c *MockMixCoordClientInterface) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	panic("implement me")
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (c *MockMixCoordClientInterface) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	panic("implement me")
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (c *MockMixCoordClientInterface) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	panic("implement me")
}

// DropVirtualChannel drops virtual channel in datacoord.
func (c *MockMixCoordClientInterface) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	panic("implement me")
}

// SetSegmentState sets the state of a given segment.
func (c *MockMixCoordClientInterface) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	panic("implement me")
}

// UpdateSegmentStatistics is the client side caller of UpdateSegmentStatistics.
func (c *MockMixCoordClientInterface) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (c *MockMixCoordClientInterface) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// BroadcastAlteredCollection is the DataCoord client side code for BroadcastAlteredCollection call.
func (c *MockMixCoordClientInterface) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	panic("implement me")
}

// CreateIndex sends the build index request to IndexCoord.
func (c *MockMixCoordClientInterface) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// AlterIndex sends the alter index request to IndexCoord.
func (c *MockMixCoordClientInterface) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// GetIndexState gets the index states from IndexCoord.
func (c *MockMixCoordClientInterface) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	panic("implement me")
}

// GetSegmentIndexState gets the index states from IndexCoord.
func (c *MockMixCoordClientInterface) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	panic("implement me")
}

// GetIndexInfos gets the index file paths from IndexCoord.
func (c *MockMixCoordClientInterface) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	panic("implement me")
}

// DescribeIndex describe the index info of the collection.
func (c *MockMixCoordClientInterface) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	panic("implement me")
}

// GetIndexStatistics get the statistics of the index.
func (c *MockMixCoordClientInterface) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	panic("implement me")
}

// GetIndexBuildProgress describe the progress of the index.
func (c *MockMixCoordClientInterface) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	panic("implement me")
}

// DropIndex sends the drop index request to IndexCoord.
func (c *MockMixCoordClientInterface) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GcControl(ctx context.Context, req *datapb.GcControlRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest, opts ...grpc.CallOption) (*indexpb.ListIndexesResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	if c.showLoadCollections != nil {
		return c.showLoadCollections(ctx, req)
	}
	return &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

// LoadCollection loads the data of the specified collections in the QueryCoord.
func (c *MockMixCoordClientInterface) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ReleaseCollection release the data of the specified collections in the QueryCoord.
func (c *MockMixCoordClientInterface) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ShowPartitions shows the partitions in the QueryCoord.
func (c *MockMixCoordClientInterface) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	panic("implement me")
}

// LoadPartitions loads the data of the specified partitions in the QueryCoord.
func (c *MockMixCoordClientInterface) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ReleasePartitions release the data of the specified partitions in the QueryCoord.
func (c *MockMixCoordClientInterface) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// SyncNewCreatedPartition notifies QueryCoord to sync new created partition if collection is loaded.
func (c *MockMixCoordClientInterface) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// GetPartitionStates gets the states of the specified partition.
func (c *MockMixCoordClientInterface) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	panic("implement me")
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (c *MockMixCoordClientInterface) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

// LoadBalance migrate the sealed segments on the source node to the dst nodes.
func (c *MockMixCoordClientInterface) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ShowConfigurations gets specified configurations para of QueryCoord
// func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
// 	req = typeutil.Clone(req)
// 	commonpbutil.UpdateMsgBase(
// 		req.GetBase(),
// 		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
// 	)
// 	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ShowConfigurationsResponse, error) {
// 		return client.ShowConfigurations(ctx, req)
// 	})
// }

// GetReplicas gets the replicas of a certain collection.
func (c *MockMixCoordClientInterface) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	panic("implement me")
}

// GetShardLeaders gets the shard leaders of a certain collection.
func (c *MockMixCoordClientInterface) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
	if c.getShardLeaders != nil {
		return c.getShardLeaders(ctx, req)
	}
	return &querypb.GetShardLeadersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
	}, nil
}

func (c *MockMixCoordClientInterface) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error) {
	if c.describeResourceGroup != nil {
		return c.describeResourceGroup(ctx, req)
	}
	return &querypb.DescribeResourceGroupResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (c *MockMixCoordClientInterface) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *MockMixCoordClientInterface) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error) {
	if c.listResourceGroups != nil {
		return c.listResourceGroups(ctx, req)
	}

	return &milvuspb.ListResourceGroupsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (c *MockMixCoordClientInterface) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest, opts ...grpc.CallOption) (*querypb.CheckBalanceStatusResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) Close() error {
	panic("implement me")
}

// Simulate the cache path and the
func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
	id, err = globalMetaCache.GetCollectionID(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection2",
	})

	// test to get from cache, this should trigger root request
	id, err = globalMetaCache.GetCollectionID(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
}

func TestMetaCache_GetCollectionByAliasHitsCache(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection1_alias")
	assert.NoError(t, err)
	assert.Equal(t, typeutil.UniqueID(1), id)
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1_alias")
	assert.NoError(t, err)
	assert.Equal(t, "collection1", schema.GetName())
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1_alias", 0)
	assert.NoError(t, err)
	assert.Equal(t, typeutil.UniqueID(1), info.collID)
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	metaCache := globalMetaCache.(*MetaCache)
	metaCache.mu.RLock()
	defer metaCache.mu.RUnlock()
	_, aliasCachedAsCollection := metaCache.nameIdx[dbName]["collection1_alias"]
	assert.False(t, aliasCachedAsCollection, "the alias must not be registered as a collection-name hint")
	assert.Equal(t, "collection1", metaCache.aliasInfo[dbName]["collection1_alias"])
}

func TestMetaCache_GetBasicCollectionInfo(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	err := InitMetaCache(ctx, rootCoord)
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

func TestMetaCacheGetCollectionWithUpdate(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil)
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)
	t.Run("update with name", func(t *testing.T) {
		rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
			Schema: &schemapb.CollectionSchema{
				Name: "bar",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: 1,
						Name:    "p",
					},
					{
						FieldID: 100,
						Name:    "pk",
					},
				},
			},
			ShardsNum:            1,
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1"},
			VirtualChannelNames:  []string{"by-dev-rootcoord-dml_1_1v0"},
		}, nil).Once()
		c, err := globalMetaCache.GetCollectionInfo(ctx, "foo", "bar", 1)
		assert.NoError(t, err)
		assert.Equal(t, c.collID, int64(1))
		assert.Equal(t, c.schema.Name, "bar")
	})

	t.Run("update with name", func(t *testing.T) {
		rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
			Schema: &schemapb.CollectionSchema{
				Name: "bar",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: 1,
						Name:    "p",
					},
					{
						FieldID: 100,
						Name:    "pk",
					},
				},
			},
			ShardsNum:            1,
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1"},
			VirtualChannelNames:  []string{"by-dev-rootcoord-dml_1_1v0"},
		}, nil).Once()
		c, err := globalMetaCache.GetCollectionInfo(ctx, "foo", "hoo", 0)
		assert.NoError(t, err)
		assert.Equal(t, c.collID, int64(1))
		assert.Equal(t, c.schema.Name, "bar")
	})
}

func TestMetaCache_InitCache(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil).Once()
		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
	})

	t.Run("failed to list policy", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(
			&internalpb.ListPolicyResponse{Status: merr.Status(errors.New("mock list policy error"))},
			nil).Once()
		err := InitMetaCache(ctx, rootCoord)
		assert.Error(t, err)
	})

	t.Run("rpc error", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(
			nil, errors.New("mock list policy rpc errorr")).Once()
		err := InitMetaCache(ctx, rootCoord)
		assert.Error(t, err)
	})
}

// TestMetaCache_ByIDIndexRealDBBucket verifies that by-id lookups are served
// from the cluster-wide id index and that fills land under the collection's
// actual database: same-name collections in different databases no longer
// evict each other, and a by-id fill is shared with later by-name lookups.
func TestMetaCache_ByIDIndexRealDBBucket(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		resp := &milvuspb.DescribeCollectionResponse{Status: merr.Success(), RequestTime: 100}
		switch {
		case req.GetCollectionID() == 101 || (req.GetDbName() == "db1" && req.GetCollectionName() == "foo"):
			resp.CollectionID = 101
			resp.DbName = "db1"
			resp.DbId = 1
			resp.Schema = &schemapb.CollectionSchema{Name: "foo"}
		case req.GetCollectionID() == 202 || (req.GetDbName() == "db2" && req.GetCollectionName() == "foo"):
			resp.CollectionID = 202
			resp.DbName = "db2"
			resp.DbId = 2
			resp.Schema = &schemapb.CollectionSchema{Name: "foo"}
		default:
			return nil, merr.WrapErrCollectionNotFound(req.GetCollectionName())
		}
		return resp, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	// two same-name collections in different databases, both filled by id only
	name, err := cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	name, err = cache.GetCollectionName(ctx, "", 202)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// repeated by-id lookups hit the id index; the entries do not evict each
	// other even though the names collide
	for i := 0; i < 3; i++ {
		_, err = cache.GetCollectionName(ctx, "", 101)
		assert.NoError(t, err)
		_, err = cache.GetCollectionName(ctx, "", 202)
		assert.NoError(t, err)
	}
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// entries live under their real databases; no "" bucket exists
	cache.mu.RLock()
	assert.Nil(t, cache.nameIdx[""])
	assert.NotNil(t, cachedEntryLocked(cache, "db1", "foo"))
	assert.NotNil(t, cachedEntryLocked(cache, "db2", "foo"))
	assert.Same(t, cachedEntryLocked(cache, "db1", "foo"), collByIDLive(cache, 101))
	assert.Same(t, cachedEntryLocked(cache, "db2", "foo"), collByIDLive(cache, 202))
	cache.mu.RUnlock()

	// a by-name lookup reuses the by-id fill, no extra RPC
	id, err := cache.GetCollectionID(ctx, "db1", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(101), id)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// removal by id drops the index entry, next by-id lookup goes remote
	cache.RemoveCollectionsByID(ctx, 101)
	name, err = cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(3), atomic.LoadInt32(&describeCount))

	// dropping a database drops its entries from the index too
	cache.RemoveDatabase(ctx, "db2")
	cache.mu.RLock()
	assert.Nil(t, collByIDLive(cache, 202))
	assert.NotNil(t, collByIDLive(cache, 101))
	cache.mu.RUnlock()
}

// TestMetaCache_ByIDEmptyDBNameNotCached verifies that when an id-only describe
// comes back without a real db name (an older rootcoord during a rolling
// upgrade), the entry is served UNCACHED: its database is unknown, so no
// database DDL could ever invalidate a cached copy (it would escape every
// db generation). Repeat lookups re-describe until the rootcoords are upgraded
// -- correctness over an upgrade-window cache hit.
func TestMetaCache_ByIDEmptyDBNameNotCached(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		// older rootcoord: resolves the name but does not report the real db
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 101,
			DbName:       "",
			Schema:       &schemapb.CollectionSchema{Name: "foo"},
			RequestTime:  100,
		}, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	// the id-only lookup resolves the name from the response
	name, err := cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount))

	// repeat by-id lookups RE-DESCRIBE: nothing was cached (the entry's db is
	// unknown, so a cached copy could never be invalidated by database DDL)
	name, err = cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// nothing in the primary store, no name hint under any guessed bucket
	cache.mu.RLock()
	assert.Nil(t, collByIDLive(cache, 101))
	assert.Nil(t, cachedEntryLocked(cache, "", "foo"))
	assert.Nil(t, cachedEntryLocked(cache, "default", "foo"))
	cache.mu.RUnlock()

	// a later default.foo BY-NAME lookup issues its own describe (cross-db
	// mis-hit protection preserved)
	_, err = cache.GetCollectionID(ctx, "default", "foo")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&describeCount))
}

// TestMetaCache_ByIDDefaultedRequestDBNameNotCached covers the case where the
// request db is not empty but was defaulted by database_interceptor: an
// external id-only lookup has its empty DbName filled with "default" before it
// reaches the cache. That db is NOT authoritative for an id lookup, and the
// describe response carries no real db either -- so the entry is served
// UNCACHED (its true database is unknown; a cached copy could never be
// invalidated by database DDL, and a name hint under default.<name> could
// mis-hit a collection of another database).
func TestMetaCache_ByIDDefaultedRequestDBNameNotCached(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		// older rootcoord resolves the name but does not report the real db; the
		// collection actually lives in another database (unknown to the proxy)
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 101,
			DbName:       "",
			Schema:       &schemapb.CollectionSchema{Name: "foo"},
			RequestTime:  100,
		}, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	// id-only lookup, but the request db arrives as the interceptor-defaulted
	// "default" (not "") — the name still resolves from the response
	name, err := cache.GetCollectionName(ctx, "default", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount))

	// repeat by-id lookups RE-DESCRIBE: nothing was cached
	name, err = cache.GetCollectionName(ctx, "default", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// nothing in the primary, and the defaulted request db planted no name hint
	cache.mu.RLock()
	assert.Nil(t, collByIDLive(cache, 101))
	assert.Nil(t, cachedEntryLocked(cache, "default", "foo"))
	cache.mu.RUnlock()

	// a later default.foo BY-NAME lookup issues its own describe (it cannot
	// silently mis-hit a collection of another database)
	_, err = cache.GetCollectionID(ctx, "default", "foo")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&describeCount))
}

// TestMetaCache_GetCollectionInfoByIDCacheHit verifies GetCollectionInfo serves
// an id-only lookup straight from the cluster-wide by-id index as a cache HIT,
// instead of probing id 0 (an unconditional miss) and routing through
// UpdateByID with a spurious cache-miss metric.
func TestMetaCache_GetCollectionInfoByIDCacheHit(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
			DbName:       "db1",
			DbId:         3,
			Schema:       &schemapb.CollectionSchema{Name: "foo"},
			RequestTime:  100,
		}, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	hitCounter := metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), "GetCollectionInfo", metrics.CacheHitLabel)
	hitBefore := testutil.ToFloat64(hitCounter)

	// first id-only lookup primes the by-id index (one describe, recorded as a miss)
	info, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, "foo", info.schema.GetName())

	// a second id-only lookup must be served from the by-id index as a HIT, with
	// no additional describe and no cache-miss metric
	info, err = cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), info.collID)

	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount), "cached id-only lookup should not re-describe")
	assert.Equal(t, float64(1), testutil.ToFloat64(hitCounter)-hitBefore,
		"id-only GetCollectionInfo should record exactly one cache hit, not a miss")
}

func TestMetaCache_EmptyDBNameSharesDefaultEntry(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "", "collection1")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	// explicit default db, by-id with empty db, and by-id with an unrelated db
	// (ids are cluster-unique, rootcoord ignores the db for by-id describes)
	// are all served by the same entry without further RPCs
	id, err = globalMetaCache.GetCollectionID(ctx, "default", "collection1")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)
	name, err := globalMetaCache.GetCollectionName(ctx, "", 1)
	assert.NoError(t, err)
	assert.Equal(t, "collection1", name)
	name, err = globalMetaCache.GetCollectionName(ctx, "some_other_db", 1)
	assert.NoError(t, err)
	assert.Equal(t, "collection1", name)
	assert.Equal(t, 1, rootCoord.GetAccessCount())
}

func TestMetaCache_GetCollectionName(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	collection, err := globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
	collection, err = globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection2",
	})

	// test to get from cache, this should trigger root request
	collection, err = globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
}

func TestMetaCache_GetCollectionFailure(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)
	rootCoord.Error = true

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Error(t, err)
	assert.Nil(t, schema)

	rootCoord.Error = false

	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})

	rootCoord.Error = true
	// should be cached with no error
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
}

func TestMetaCache_GetNonExistCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
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
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
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
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	cnt := 100
	getCollectionCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			// GetCollectionSchema will never fail
			schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
			assert.NoError(t, err)
			EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
				AutoID:    true,
				Fields:    []*schemapb.FieldSchema{},
				Functions: []*schemapb.FunctionSchema{},
				Name:      "collection1",
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
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
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

func TestMetaCache_GetShard(t *testing.T) {
	t.Skip("GetShard has been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_ClearShards(t *testing.T) {
	t.Skip("DeprecateShardCache has been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_PolicyInfo(t *testing.T) {
	client := &MockMixCoordClientInterface{}

	t.Run("InitMetaCache", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, errors.New("mock error")
		}
		err := InitMetaCache(context.Background(), client)
		assert.Error(t, err)

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
			}, nil
		}
		err = InitMetaCache(context.Background(), client)
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
		err := InitMetaCache(context.Background(), client)
		assert.NoError(t, err)
		policyInfos := privilege.GetPrivilegeCache().GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))
		roles := privilege.GetPrivilegeCache().GetUserRole("foo")
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
		err := InitMetaCache(context.Background(), client)
		assert.NoError(t, err)

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos := privilege.GetPrivilegeCache().GetPrivilegeInfo(context.Background())
		assert.Equal(t, 4, len(policyInfos))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRevokePrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos = privilege.GetPrivilegeCache().GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles := privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Equal(t, 3, len(roles))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles = privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Equal(t, 2, len(roles))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: ""})
		assert.Error(t, err)
		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: 100, OpKey: "policyX"})
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
		err := InitMetaCache(context.Background(), client)
		assert.NoError(t, err)

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDeleteUser, OpKey: "foo"})
		assert.NoError(t, err)

		roles := privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Len(t, roles, 0)

		roles = privilege.GetPrivilegeCache().GetUserRole("foo2")
		assert.Len(t, roles, 2)

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDropRole, OpKey: "role2"})
		assert.NoError(t, err)
		roles = privilege.GetPrivilegeCache().GetUserRole("foo2")
		assert.Len(t, roles, 1)
		assert.Equal(t, "role3", roles[0])

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2"), funcutil.EncodeUserRoleCache("foo2", "role3")},
			}, nil
		}
		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh})
		assert.NoError(t, err)
		roles = privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Len(t, roles, 2)
	})
}

func TestMetaCache_RemoveCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	rootCoord.showLoadCollections = func(ctx context.Context, in *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status:              merr.Success(),
			CollectionIDs:       []UniqueID{1, 2},
			InMemoryPercentages: []int64{100, 50},
		}, nil
	}

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
	t.Skip("shardLeaders and nodeInfo have been moved to shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_Database(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
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
		rootCoord := mocks.NewMockMixCoordClient(t)
		cache, err := NewMetaCache(rootCoord)
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
		rootCoord := mocks.NewMockMixCoordClient(t)
		cache, err := NewMetaCache(rootCoord)
		assert.NoError(t, err)

		rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{
			Status: merr.Status(errors.New("mock error: describe database")),
		}, nil).Once()
		_, err = cache.GetDatabaseInfo(ctx, "default")
		assert.Error(t, err)
	})
}

func TestMetaCache_RemoveDatabaseInfoKeepsCollectionMetadata(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	var altered atomic.Bool
	rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
			value := "old"
			if altered.Load() {
				value = "new"
			}
			return &rootcoordpb.DescribeDatabaseResponse{
				Status: merr.Success(),
				DbID:   1,
				DbName: "db",
				Properties: []*commonpb.KeyValuePair{
					{Key: "k", Value: value},
				},
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	cache.mu.Lock()
	entry := seedCollection(cache, "db", "foo", 10)
	entry.aliases = []string{"a"}
	cache.setAliasLocked("db", "a", "foo")
	cache.partitionCache[buildPartitionCacheKey(10)] = parsePartitionsInfo([]*partitionInfo{{name: "p", partitionID: 100}}, false)
	cache.mu.Unlock()

	info, err := cache.GetDatabaseInfo(ctx, "db")
	assert.NoError(t, err)
	assert.Equal(t, "old", info.properties[0].GetValue())

	altered.Store(true)
	cache.RemoveDatabaseInfo(ctx, "db")

	cache.mu.RLock()
	_, hasDBInfo := cache.dbInfo["db"]
	collection := collByIDLive(cache, 10)
	nameEntry := cachedEntryLocked(cache, "db", "foo")
	aliasTarget, hasAlias := cache.aliasInfo["db"]["a"]
	_, hasPartitions := cache.partitionCache[buildPartitionCacheKey(10)]
	cache.mu.RUnlock()
	assert.False(t, hasDBInfo)
	assert.Same(t, entry, collection)
	assert.Same(t, entry, nameEntry)
	assert.True(t, hasAlias)
	assert.Equal(t, "foo", aliasTarget)
	assert.True(t, hasPartitions)

	info, err = cache.GetDatabaseInfo(ctx, "db")
	assert.NoError(t, err)
	assert.Equal(t, "new", info.properties[0].GetValue())
}

func TestMetaCache_RemoveDatabaseInfoBlocksStaleResurrection(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	var altered atomic.Bool
	rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
			isPostAlter := altered.Load()
			if !isPostAlter {
				first.Do(func() {
					close(entered)
					<-release
				})
			}
			value := "old"
			if isPostAlter {
				value = "new"
			}
			return &rootcoordpb.DescribeDatabaseResponse{
				Status: merr.Success(),
				DbID:   1,
				DbName: "db",
				Properties: []*commonpb.KeyValuePair{
					{Key: "k", Value: value},
				},
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	fillDone := make(chan *databaseInfo, 1)
	fillErr := make(chan error, 1)
	go func() {
		info, err := cache.GetDatabaseInfo(ctx, "db")
		fillDone <- info
		fillErr <- err
	}()
	<-entered

	invalidationDone := make(chan struct{})
	go func() {
		defer close(invalidationDone)
		cache.RemoveDatabaseInfo(ctx, "db")
	}()
	select {
	case <-invalidationDone:
		t.Fatal("RemoveDatabaseInfo must drain the pre-alter database fill")
	case <-time.After(100 * time.Millisecond):
	}

	altered.Store(true)
	close(release)
	oldInfo := <-fillDone
	assert.NoError(t, <-fillErr)
	assert.Equal(t, "old", oldInfo.properties[0].GetValue())
	<-invalidationDone
	assert.Nil(t, cache.safeGetDBInfo("db"), "the pre-alter write-back must be removed")

	newInfo, err := cache.GetDatabaseInfo(ctx, "db")
	assert.NoError(t, err)
	assert.Equal(t, "new", newInfo.properties[0].GetValue())
}

func TestMetaCache_AllocID(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(nil),
			ID:     11198,
			Count:  10,
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.NoError(t, err)
		assert.Equal(t, id, int64(11198))
	})

	t.Run("error", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(nil),
		}, errors.New("mock error"))
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})

	t.Run("failed", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(errors.New("mock failed")),
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})
}

func TestMetaCache_InvalidateShardLeaderCache(t *testing.T) {
	t.Skip("GetShard and InvalidateShardLeaderCache have been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
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
	clusteringKeyField := &schemapb.FieldSchema{
		FieldID:         common.StartOfUserFieldID + 5,
		Name:            "clustering_key",
		DataType:        schemapb.DataType_Int32,
		IsClusteringKey: true,
	}

	subIntField := &schemapb.FieldSchema{
		FieldID:     common.StartOfUserFieldID + 7,
		Name:        "sub_int",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int32,
	}
	subFloatVectorField := &schemapb.FieldSchema{
		FieldID:     common.StartOfUserFieldID + 8,
		Name:        "sub_float_vector",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "768"},
		},
	}
	structArrayField := &schemapb.StructArrayFieldSchema{
		FieldID: common.StartOfUserFieldID + 6,
		Name:    "struct_array",
		Fields: []*schemapb.FieldSchema{
			subIntField,
			subFloatVectorField,
		},
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
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{},
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
					clusteringKeyField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4, common.StartOfUserFieldID + 5},
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
					clusteringKeyField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"pk", "part_key", "vector", "clustering_key"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4, common.StartOfUserFieldID + 5},
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
				Functions: []*schemapb.FunctionSchema{},
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
				Functions: []*schemapb.FunctionSchema{},
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
				Functions: []*schemapb.FunctionSchema{},
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
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"pk", "part_key"},
			skipDynamicField: true,
			expectErr:        true,
		},
		{
			tag: "clustering_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields: []string{"pk", "part_key", "vector"},
			expectErr:  true,
		},
		{
			tag: "struct_array_field_default",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{
					structArrayField,
				},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{},
			expectErr:        false,
		},
		{
			tag: "load_struct_array_field",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{
					structArrayField,
				},
			},
			loadFields:       []string{"pk", "part_key", "clustering_key", "struct_array"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 5, common.StartOfUserFieldID + 7, common.StartOfUserFieldID + 8},
			expectErr:        false,
		},
		{
			tag: "load_struct_array_field_with_vector",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{
					structArrayField,
				},
			},
			loadFields:       []string{"pk", "part_key", "clustering_key", "vector", "struct_array"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 5, common.StartOfUserFieldID + 7, common.StartOfUserFieldID + 8},
			expectErr:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			info := mustNewSchemaInfo(tc.schema)

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

// TestMetaCache_FillInvalidateOrdering replaces the old version-floor test
// (TestMetaCache_Parallel): cache fills are ordered against invalidations by
// fillMu instead of a per-collection version comparison. A fill whose describe
// RPC is in flight when an invalidation arrives must be DRAINED by the
// invalidation — its (possibly pre-DDL) write-back lands before the eviction
// runs, so the eviction cleans it and a stale snapshot can never outlive the
// DDL that invalidated it.
func TestMetaCache_FillInvalidateOrdering(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			// first call parks in the "RPC in flight" state until released
			once.Do(func() {
				close(entered)
				<-gate
			})
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 111,
				DbName:       dbName,
				Schema:       &schemapb.CollectionSchema{Name: "collection1", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  100, // pre-DDL snapshot
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// start a fill and park it inside the describe RPC
	fillDone := make(chan struct{})
	go func() {
		defer close(fillDone)
		_, err := cache.GetCollectionInfo(ctx, dbName, "collection1", 0)
		assert.NoError(t, err)
	}()
	<-entered

	// the invalidation must WAIT for the in-flight fill
	invalDone := make(chan struct{})
	go func() {
		defer close(invalDone)
		cache.RemoveCollectionsByID(ctx, 111)
	}()
	select {
	case <-invalDone:
		t.Fatal("invalidation must drain the in-flight fill before evicting")
	case <-time.After(100 * time.Millisecond):
	}

	// release the fill: its write-back lands first, then the eviction runs
	close(gate)
	<-fillDone
	<-invalDone

	cache.mu.RLock()
	assert.Nil(t, cachedEntryLocked(cache, dbName, "collection1"), "the drained fill's pre-DDL write-back must not survive the eviction")
	assert.Nil(t, collByIDLive(cache, 111))
	cache.mu.RUnlock()
	assertMetaCacheByIDConsistent(t, cache)
}

func TestMetaCache_GetShardLeaderList(t *testing.T) {
	t.Skip("GetShardLeaderList has been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_GetPartitionInfo_CacheHit(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// the cold fill costs two describes -- one for the collection resolution and
	// one inside the partition-list filler; the second lookup hits both caches
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Times(2)

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info1, err := cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), info1.partitionID)
	assert.Equal(t, "par1", info1.name)

	info2, err := cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, info1, info2)
}

func TestMetaCache_GetPartitionInfo_DefaultPartition(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// two describes on the cold fill: collection resolution + partition-list filler
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Times(2)

	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowPartitionsRequest) bool {
		return len(req.PartitionNames) == 0
	})).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{1},
		PartitionNames:       []string{defaultPartitionName},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info, err := cache.GetPartitionInfo(ctx, "db", "collection", "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.partitionID)
	assert.Equal(t, defaultPartitionName, info.name)
}

func TestMetaCache_GetPartitionInfo_Error(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// two describes: collection resolution + the filler's describe, which
	// precedes the failing ShowPartitions inside the fill
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Times(2)

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(nil, errors.New("connection failed")).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.Error(t, err)
}

func TestMetaCache_GetPartitionInfos_CacheHit(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// one describe for the name resolution (fills the collection cache) and one inside the
	// collection-level partition fill; the second GetPartitionInfos hits both caches
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema: &schemapb.CollectionSchema{
			Name:   "collection",
			Fields: []*schemapb.FieldSchema{},
		},
		RequestTime: 1000,
	}, nil).Times(2)

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100, 101},
		PartitionNames:       []string{"par1", "par2"},
		CreatedTimestamps:    []uint64{1000, 1001},
		CreatedUtcTimestamps: []uint64{1000, 1001},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	infos1, err := cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(infos1.partitionInfos))

	infos2, err := cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	assert.Equal(t, infos1, infos2)
}

func TestMetaCache_RemovePartition(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// Four describes: the initial resolution + the partition-list filler's
	// describe on the cold fill, then both again after the partition DDL staled
	// the collection entry and the partition list.
	describeCalls := 0
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCalls++
			requestTime := uint64(1000)
			if describeCalls > 1 {
				requestTime = 3000
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				// A real rootcoord always reports the collection's db, so this
				// exercises the normal (cached-collection) invalidation path. The
				// older-rootcoord empty-DbName upgrade path is covered by
				// TestMetaCache_RemovePartitionViaAliasInvalidatesRealNameOnEmptyDBName.
				DbName: "db",
				Schema: &schemapb.CollectionSchema{
					Name:   "collection",
					Fields: []*schemapb.FieldSchema{},
				},
				Aliases:     []string{"alias"},
				RequestTime: requestTime,
			}, nil
		}).Times(4)

	// Two fills: the initial one, and the refetch after the partition DDL staled
	// the exact real-name key. The alias lookups resolve to the real name and hit
	// the same entries, so they trigger no extra RPC.
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Times(2)

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	_, err = cache.GetPartitionInfo(ctx, "db", "alias", "par1")
	assert.NoError(t, err)

	cache.RemovePartition(ctx, "db", 1, "alias", "par1")

	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	_, err = cache.GetPartitionInfo(ctx, "db", "alias", "par1")
	assert.NoError(t, err)
}

// TestMetaCache_RemovePartitionViaAliasInvalidatesRealNameOnEmptyDBName is a
// regression test for the rolling-upgrade case: an older rootcoord omits DbName
// from describe responses. A partition DDL issued via an alias must still
// invalidate the partition entries populated through the real name — under the
// id-keyed partition cache this holds because both spellings resolve to the
// same collection id, no matter how the entry was primed.
func TestMetaCache_RemovePartitionViaAliasInvalidatesRealNameOnEmptyDBName(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// older rootcoord: the describe resolves the real name + aliases but reports
	// no db name (for a by-name fill the request db is authoritative instead).
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "",
		Schema: &schemapb.CollectionSchema{
			Name:   "collection",
			Fields: []*schemapb.FieldSchema{},
		},
		Aliases:     []string{"alias"},
		RequestTime: 1000,
	}, nil).Maybe()

	var mu sync.Mutex
	// the fill shows partitions BY the resolved collection id (empty name), so
	// count by id
	showByID := map[int64]int{}
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			mu.Lock()
			showByID[req.GetCollectionID()]++
			mu.Unlock()
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{"par1"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// a data-plane access primed the partition cache via the REAL collection
	// name (keyed by the resolved collection id)
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	// a DropPartition issued via the alias arrives: collectionID is the real id,
	// collectionName is the alias
	cache.RemovePartition(ctx, "db", 1, "alias", "par1")

	// the real-name partition entry must have been invalidated too, so this
	// re-fetches (a second ShowPartitions for "collection") rather than serving
	// the stale cached partition
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, showByID[1],
		"the collection-id partition cache should be invalidated by an alias-based partition DDL even when the collection is uncached (empty DbName)")
}

func TestMetaCache_RemovePartitionInvalidatesCollectionInfo(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	describeCalls := 0
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCalls++
			numPartitions := int64(1)
			requestTime := uint64(1000)
			if describeCalls > 1 {
				numPartitions = 2
				requestTime = 3000
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:        merr.Success(),
				CollectionID:  1,
				DbName:        "db",
				NumPartitions: numPartitions,
				RequestTime:   requestTime,
				Schema: &schemapb.CollectionSchema{
					Name:   "collection",
					Fields: []*schemapb.FieldSchema{},
				},
			}, nil
		}).Times(2)

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info, err := cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.numPartitions)

	cache.RemovePartition(ctx, "db", 1, "collection", "par1")

	info, err = cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), info.numPartitions)
}

// TestMetaCache_RemovePartitionStalesCollectionInfoUnconditionally: with fills
// serialized against invalidations by fillMu, partition DDL no longer compares a
// version floor — it always stales the collection entry. A duplicate or late
// broadcast at worst causes one extra re-describe (over-invalidation), never a
// stale read. (This intentionally replaces the old KeepsCollectionInfoForStale-
// Version behavior, whose floor was removed together with collectionCacheVersion.)
func TestMetaCache_RemovePartitionStalesCollectionInfoUnconditionally(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	describeCalls := 0
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCalls++
			return &milvuspb.DescribeCollectionResponse{
				Status:        merr.Success(),
				CollectionID:  1,
				DbName:        "db",
				NumPartitions: 1,
				RequestTime:   1000,
				Schema: &schemapb.CollectionSchema{
					Name:   "collection",
					Fields: []*schemapb.FieldSchema{},
				},
			}, nil
		}).Times(2)

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info, err := cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.numPartitions)

	// even an older-timestamp partition broadcast evicts the collection entry
	cache.RemovePartition(ctx, "db", 1, "collection", "par1")

	info, err = cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.numPartitions)
	assert.Equal(t, 2, describeCalls, "the entry was staled, so the lookup re-describes")
}

func TestMetaCache_PartitionCache_Concurrent(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// name resolution before keying; concurrent resolvers merge via the
	// collection singleflight, so no fixed count
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Maybe()

	var callCount atomic.Int32
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			callCount.Add(1)
			time.Sleep(50 * time.Millisecond)
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{"par1"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	numGoroutines := 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.GetPartitionInfo(ctx, "db", "collection", "par1")
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(1), callCount.Load(), "Singleflight should merge concurrent requests")
}

func TestMetaCache_GetPartitionInfos_SingleflightKeyIncludesDatabase(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			time.Sleep(50 * time.Millisecond)
			// distinct collections in distinct dbs => distinct cluster-unique ids
			id := int64(1)
			if req.GetDbName() == "db2" {
				id = 2
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       req.GetDbName(),
				Schema: &schemapb.CollectionSchema{
					Name:   req.GetCollectionName(),
					Fields: []*schemapb.FieldSchema{},
				},
				RequestTime: 1000,
			}, nil
		}).Times(4) // per db: one name resolution + one inside the partition fill

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{req.GetDbName() + "_par"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	type result struct {
		db    string
		infos *partitionInfos
		err   error
	}

	results := make(chan result, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for _, db := range []string{"db1", "db2"} {
		wg.Add(1)
		go func(db string) {
			defer wg.Done()
			<-start
			infos, err := cache.GetPartitionInfos(ctx, db, "collection")
			results <- result{
				db:    db,
				infos: infos,
				err:   err,
			}
		}(db)
	}

	close(start)
	wg.Wait()
	close(results)

	for result := range results {
		assert.NoError(t, result.err)
		if assert.NotNil(t, result.infos) && assert.Len(t, result.infos.partitionInfos, 1) {
			assert.Equal(t, result.db+"_par", result.infos.partitionInfos[0].name)
		}
	}
}

// TestMetaCache_GetPartitionInfosNormalizesEmptyDB is a regression test for the
// collection/partition cache asymmetry: the collection cache normalizes an empty
// db to "default", so the partition cache must too. Otherwise an empty-db request
// would key partitions under ""/<coll> while a drop/recreate invalidation keyed
// on the canonical "default" bucket never sweeps them, leaving stale entries. The
// Once() expectations assert both lookups share one normalized cache bucket.
func TestMetaCache_GetPartitionInfosNormalizesEmptyDB(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// two describes on the first call (name resolution + partition fill); the
	// explicit-default call hits the caches, proving both requests share the
	// canonical bucket
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "default",
		Schema: &schemapb.CollectionSchema{
			Name:   "collection",
			Fields: []*schemapb.FieldSchema{},
		},
		RequestTime: 1000,
	}, nil).Times(2)

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// populate the collection-level partition cache via an empty-db request
	infos1, err := cache.GetPartitionInfos(ctx, "", "collection")
	assert.NoError(t, err)
	assert.Len(t, infos1.partitionInfos, 1)

	// the explicit-default request must hit the same canonical bucket with no
	// extra RPC; without normalization it would miss and exceed the Once() mocks
	infos2, err := cache.GetPartitionInfos(ctx, "default", "collection")
	assert.NoError(t, err)
	assert.Equal(t, infos1, infos2)
}

// TestMetaCache_GetPartitionInfoNormalizesEmptyDB is the partition-name-level
// counterpart: GetPartitionInfo must key its cache under the canonical database
// too, so an empty-db and an explicit-default lookup share one entry.
func TestMetaCache_GetPartitionInfoNormalizesEmptyDB(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// two describes on the cold fill (name resolution + partition-list filler);
	// the explicit-default lookup hits the same cached entries
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "default",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Times(2)

	// ShowPartitions must fire exactly once across both lookups when the cache
	// key is normalized.
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100, 101},
		PartitionNames:       []string{"par1", "par2"},
		CreatedTimestamps:    []uint64{1000, 1001},
		CreatedUtcTimestamps: []uint64{1000, 1001},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	p1, err := cache.GetPartitionInfo(ctx, "", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), p1.partitionID)

	// explicit default resolves from the same normalized bucket — no extra RPC
	p2, err := cache.GetPartitionInfo(ctx, "default", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, p1, p2)
}

// getAlias reads an alias hint (test helper; production reads chain through
// getCollection).
func getAlias(cache *MetaCache, database, alias string) (string, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if db, ok := cache.aliasInfo[normalizeDBName(database)]; ok {
		if target, ok := db[alias]; ok {
			return target, true
		}
	}
	return "", false
}

// mustNewSchemaInfo builds a schemaInfo for tests, panicking on the schema-helper
// error that newSchemaInfo now propagates to production. Tests pass valid schemas
// so it never fires; it keeps call sites a single-value expression usable inside
// struct literals.
func mustNewSchemaInfo(schema *schemapb.CollectionSchema) *schemaInfo {
	si, err := newSchemaInfo(schema)
	if err != nil {
		panic(err)
	}
	return si
}

// collByIDLive returns the live primary entry for id (nil when absent —
// liveness is presence). Callers must hold cache.mu (tests take RLock).
func collByIDLive(cache *MetaCache, id UniqueID) *collectionInfo {
	if e, ok := cache.liveLocked(id); ok {
		return e
	}
	return nil
}

// cachedEntryLocked resolves (db, name) exactly like a read would: name hint ->
// primary, validated (id live, name still matches). nil = a lookup would miss.
// Callers must hold cache.mu.
func cachedEntryLocked(cache *MetaCache, db, name string) *collectionInfo {
	db = normalizeDBName(db)
	ids, ok := cache.nameIdx[db]
	if !ok {
		return nil
	}
	id, ok := ids[name]
	if !ok {
		return nil
	}
	e, ok := cache.liveLocked(id)
	if !ok || e.schema.GetName() != name || normalizeDBName(e.dbName) != db {
		return nil
	}
	return e
}

// seedCollection plants a collection into the primary store + name hint the way
// update() would, for tests that build MetaCache literals. The literal must have
// collections/nameIdx maps initialized.
func seedCollection(cache *MetaCache, db, name string, id UniqueID) *collectionInfo {
	info := &collectionInfo{
		collID: id,
		dbName: db,
		schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Name: name}),
	}
	cache.collections[id] = info
	ids, ok := cache.nameIdx[db]
	if !ok {
		ids = make(map[string]UniqueID)
		cache.nameIdx[db] = ids
	}
	ids[name] = id
	return info
}

// assertMetaCacheByIDConsistent verifies the post-unified-cleanup invariant
// set -- "every hint has a live owner" -- in all four directions:
//  1. every primary entry has a name hint (its own, or a same-name recreate's)
//     that targets a LIVE entry, and every alias it declares has a hint;
//  2. every name hint targets a live primary (the name may legally differ --
//     a rename's old-name hint points at the live renamed entry and is
//     rejected by read-time validation -- but a hint to a DEAD id means an
//     eviction bypassed the unified cleanup routine);
//  3. every alias hint chains alias -> name -> live entry that DECLARES the
//     alias (the alias-resurrection bugs were exactly violations of this);
//  4. primary keys equal entry ids.
func assertMetaCacheByIDConsistent(t *testing.T, cache *MetaCache) {
	t.Helper()
	cache.mu.RLock()
	v := metaCacheConsistencyViolations(cache)
	cache.mu.RUnlock()
	assert.Emptyf(t, v, "meta cache consistency violated:\n  %s", strings.Join(v, "\n  "))
}

// metaCacheConsistencyViolations returns every way the cache breaks the
// post-unified-cleanup invariant "every hint has a live owner AND exact
// ownership holds". Pure (no testify) so a test can construct a broken state
// and prove the checker catches it. Caller holds cache.mu.
func metaCacheConsistencyViolations(cache *MetaCache) []string {
	var v []string
	// Forward: every live primary entry owns its hints EXACTLY (not merely
	// "some live entry exists"). Exact ownership is what catches a ghost: if
	// A and B both declare alias "a" while aliasInfo["a"] -> B, then A's "a" is
	// a ghost that a by-id Describe of A would still expose -- A's declared "a"
	// resolves to B's name, not A's.
	for id, e := range cache.collections {
		if e == nil {
			v = append(v, fmt.Sprintf("primary id %d has a nil entry", id))
			continue
		}
		if id != e.collID {
			v = append(v, fmt.Sprintf("primary key %d != entry.collID %d", id, e.collID))
		}
		db := normalizeDBName(e.dbName)
		name := e.schema.GetName()
		if gotID, ok := cache.nameIdx[db][name]; !ok {
			v = append(v, fmt.Sprintf("primary %q/%q (id %d) has no name hint", db, name, id))
		} else if gotID != id {
			v = append(v, fmt.Sprintf("primary %q/%q: name hint points to id %d, not %d", db, name, gotID, id))
		}
		for _, a := range e.aliases {
			if gotName, ok := cache.aliasInfo[db][a]; !ok {
				v = append(v, fmt.Sprintf("declared alias %q of %q/%q (id %d) has no hint", a, db, name, id))
			} else if gotName != name {
				v = append(v, fmt.Sprintf("entry %q/%q (id %d) declares alias %q but aliasInfo[%q]=%q (ghost)", db, name, id, a, a, gotName))
			}
		}
	}
	// Reverse name: every name hint targets a live entry whose db AND real name
	// match exactly.
	for db, ids := range cache.nameIdx {
		for name, id := range ids {
			e, live := cache.collections[id]
			if !live {
				v = append(v, fmt.Sprintf("name hint %q/%q -> id %d dangles to a dead entry (eviction bypassed unified cleanup)", db, name, id))
				continue
			}
			if got := normalizeDBName(e.dbName); got != db {
				v = append(v, fmt.Sprintf("name hint %q/%q -> id %d: entry db is %q", db, name, id, got))
			}
			if got := e.schema.GetName(); got != name {
				v = append(v, fmt.Sprintf("name hint %q/%q -> id %d: entry real name is %q", db, name, id, got))
			}
		}
	}
	// Reverse alias: every alias hint chains to a live entry that DECLARES it.
	for db, aliases := range cache.aliasInfo {
		for alias, target := range aliases {
			id, ok := cache.nameIdx[db][target]
			if !ok {
				v = append(v, fmt.Sprintf("alias hint %q/%q -> %q has no name hint for its target", db, alias, target))
				continue
			}
			e, live := cache.collections[id]
			if !live {
				v = append(v, fmt.Sprintf("alias hint %q/%q chains to dead id %d", db, alias, id))
				continue
			}
			declared := false
			for _, a := range e.aliases {
				if a == alias {
					declared = true
					break
				}
			}
			if !declared {
				v = append(v, fmt.Sprintf("alias hint %q/%q resolves to id %d which does NOT declare it (resurrection risk)", db, alias, id))
			}
		}
	}
	return v
}

// TestMetaCache_RenameOldNameStopsResolving locks in the rename observable:
// after foo -> bar, caching the new name makes the pre-rename name stop
// resolving IMMEDIATELY. update() actively deletes the id's previous name hint
// when it re-fills the same id under a new real name, so the old "foo" hint is
// gone (not lingering); a later "foo" lookup misses and re-describes. The
// rename broadcast then only needs to evict the primary entry by id.
func TestMetaCache_RenameOldNameStopsResolving(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var renamed atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			name := "foo"
			rt := uint64(100)
			if renamed.Load() {
				name = "bar"
				rt = 200
			}
			if req.GetCollectionID() == 1 || req.GetCollectionName() == name {
				return &milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 1,
					DbName:       "db",
					Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
					RequestTime:  rt,
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName())),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	id, err := cache.GetCollectionID(ctx, "db", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	renamed.Store(true)
	id, err = cache.GetCollectionID(ctx, "db", "bar")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	// the old name must no longer resolve (the dangling hint fails validation);
	// note: we deliberately do NOT assert on the internal absence of the hint —
	// stale name resolution is impossible either way
	cache.mu.RLock()
	hasFoo := cachedEntryLocked(cache, "db", "foo") != nil
	hasBar := cachedEntryLocked(cache, "db", "bar") != nil
	cache.mu.RUnlock()
	assert.False(t, hasFoo, "the old name must stop resolving the moment the rename is observed")
	assert.True(t, hasBar)
	assertMetaCacheByIDConsistent(t, cache)

	// PRODUCTION-path check in the pre-broadcast window: a lookup of the old name
	// must go through the real read path, fail the hint validation, re-describe
	// and report not-found -- NOT serve id 1 through the dangling hint. (The
	// helper assertions above re-implement the validation, so only this catches
	// a production regression in getCollection's hint validation.)
	_, err = cache.GetCollectionID(ctx, "db", "foo")
	assert.Error(t, err, "the pre-rename name must not resolve through the dangling hint")

	// the rename broadcast evicts the primary entry by id
	cache.RemoveCollectionsByID(ctx, 1)

	cache.mu.RLock()
	hasFoo = cachedEntryLocked(cache, "db", "foo") != nil
	hasBar = cachedEntryLocked(cache, "db", "bar") != nil
	hasID := collByIDLive(cache, 1) != nil
	cache.mu.RUnlock()
	assert.False(t, hasFoo, "old name must not resolve (no stale read of the pre-rename name)")
	assert.False(t, hasBar, "new name must not resolve either after the eviction")
	assert.False(t, hasID, "the primary entry must be gone")
	assertMetaCacheByIDConsistent(t, cache)

	// real stale-read check: the pre-rename name must not resolve any more -- it
	// re-describes, and since the collection was renamed it is now not found
	// (rather than returning the stale id 1 from a lingering cache entry)
	_, err = cache.GetCollectionID(ctx, "db", "foo")
	assert.Error(t, err, "the pre-rename name must not resolve after invalidation")
	// the new name still resolves correctly
	id, err = cache.GetCollectionID(ctx, "db", "bar")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)
}

// TestMetaCache_CrossDBSameNameRenameRemovesOldHint verifies that observing a
// db1/foo -> db2/foo rename moves the id's name hint to the new database even
// though the collection name did not change. Otherwise a later invalidation of
// the old db1/foo location could follow the stale hint and evict live db2/foo.
func TestMetaCache_CrossDBSameNameRenameRemovesOldHint(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var moved atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			db := "db1"
			if moved.Load() {
				db = "db2"
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				DbName:       db,
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  100,
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	id, err := cache.GetCollectionID(ctx, "db1", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	moved.Store(true)
	id, err = cache.GetCollectionID(ctx, "db2", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	cache.mu.RLock()
	_, oldHint := cache.nameIdx["db1"]["foo"]
	newID, newHint := cache.nameIdx["db2"]["foo"]
	entry := collByIDLive(cache, 1)
	cache.mu.RUnlock()
	assert.False(t, oldHint, "the pre-rename db1/foo hint must be removed")
	assert.True(t, newHint)
	assert.Equal(t, UniqueID(1), newID)
	assert.NotNil(t, entry)
	assert.Equal(t, "db2", entry.dbName)
	assertMetaCacheByIDConsistent(t, cache)

	// A delayed invalidation for the old location must not evict the collection
	// from its new database.
	cache.RemoveCollection(ctx, "db1", "foo")
	cache.mu.RLock()
	entry = collByIDLive(cache, 1)
	newID, newHint = cache.nameIdx["db2"]["foo"]
	cache.mu.RUnlock()
	assert.NotNil(t, entry, "old-db invalidation must not evict live db2/foo")
	assert.True(t, newHint)
	assert.Equal(t, UniqueID(1), newID)
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_AliasRepointResolvesToNewTarget verifies an alias re-point is
// reflected after invalidation: alias -> collA is cached, then AlterAlias points
// it at collB and broadcasts RemoveAlias; the next resolution must re-fill
// through DescribeCollection (rootcoord resolves the alias server-side) and
// return collB, not the stale collA. (Guards the alias-cache correctness the
// RBAC path depends on for its object-name resolution.)
func TestMetaCache_AliasRepointResolvesToNewTarget(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// Exactly two fills: one before the re-point, one after the invalidation.
	// The alias-addressed describe returns the CURRENT target collection.
	var repointed atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			target, id := "collA", int64(1)
			if repointed.Load() {
				target, id = "collB", int64(2)
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: target, Fields: []*schemapb.FieldSchema{}},
				Aliases:      []string{req.GetCollectionName()},
				RequestTime:  100,
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	got, err := cache.ResolveCollectionAlias(ctx, "db", "myalias")
	assert.NoError(t, err)
	assert.Equal(t, "collA", got)

	repointed.Store(true)
	cache.RemoveAlias(ctx, "db", "myalias")

	got, err = cache.ResolveCollectionAlias(ctx, "db", "myalias")
	assert.NoError(t, err)
	assert.Equal(t, "collB", got, "alias must re-resolve to the new target after RemoveAlias")
}

// TestMetaCache_DropCollectionClearsAllCaches verifies a drop makes the
// collection unreachable through every access path. Under the id-primary
// architecture the drop deletes ONLY the primary entry: name/alias hints and
// partition entries survive physically but dangle — hints fail validation on
// read, and partition entries are keyed by the (never reused) collection id, so
// a recreated collection gets a NEW id and refetches everything.
func TestMetaCache_DropCollectionClearsAllCaches(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	// ids are never reused in production: after the drop, the "recreated"
	// collection must be described with a NEW id
	var collID atomic.Int64
	collID.Store(1)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: collID.Load(),
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
				Aliases:      []string{"myalias"},
				RequestTime:  1000,
			}, nil
		}).Maybe()
	var showCount atomic.Int32
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			showCount.Add(1)
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{"par1"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetCollectionID(ctx, "db", "collection")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	target, err := cache.ResolveCollectionAlias(ctx, "db", "myalias")
	assert.NoError(t, err)
	assert.Equal(t, "collection", target)
	assert.Equal(t, int32(1), showCount.Load(), "one merged partition cache: the by-name lookup answers from the list fetched once")

	cache.mu.RLock()
	hasColl := cachedEntryLocked(cache, "db", "collection") != nil
	hasID := collByIDLive(cache, 1) != nil
	aliasTarget, hasAlias := cache.aliasInfo["db"]["myalias"]
	cache.mu.RUnlock()
	assert.True(t, hasColl)
	assert.True(t, hasID)
	assert.True(t, hasAlias)
	assert.Equal(t, "collection", aliasTarget)

	cache.RemoveCollectionsByID(ctx, 1)

	cache.mu.RLock()
	hasColl = cachedEntryLocked(cache, "db", "collection") != nil
	hasID = collByIDLive(cache, 1) != nil
	_, hasAlias = cache.aliasInfo["db"]["myalias"]
	cache.mu.RUnlock()
	assert.False(t, hasColl, "a by-name lookup must miss after the drop")
	assert.False(t, hasID, "the primary entry must be deleted on drop")
	// the alias was declared by the entry, so eviction cleaned its hint
	assert.False(t, hasAlias, "a declared alias hint is cleaned at eviction")
	_, ok := cache.getCollection("db", "myalias", 0)
	assert.False(t, ok, "a lookup through the alias must not reach the dropped collection")
	assertMetaCacheByIDConsistent(t, cache)

	// the recreated collection has a NEW id, so its partition cache misses and
	// re-fetches (the old id's list was reclaimed at eviction)
	collID.Store(2)
	_, err = cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, int32(2), showCount.Load(), "the recreated collection's partition info must be re-fetched, proving the old entry is unreachable")
}

// TestMetaCache_ConcurrentIDPartitionInvalidateConsistency hammers the cache with
// id-only reads, by-name reads, partition reads, and both invalidation paths
// concurrently. Run under -race it flags any data race; the final consistency
// check flags any primary-store/name-hint desync left behind.
func TestMetaCache_ConcurrentIDPartitionInvalidateConsistency(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			if req.GetCollectionName() != "" {
				fmt.Sscanf(req.GetCollectionName(), "coll%d", &id)
			} else {
				id = req.GetCollectionID()
			}
			if id < 1 || id > 5 {
				return &milvuspb.DescribeCollectionResponse{
					Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName())),
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: fmt.Sprintf("coll%d", id), Fields: []*schemapb.FieldSchema{}},
				RequestTime:  1000,
			}, nil
		})
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := int64((i % 5) + 1)
				name := fmt.Sprintf("coll%d", id)
				switch (i + g) % 5 {
				case 0:
					// id-only read must never return another collection's name
					if got, err := cache.GetCollectionName(ctx, "", id); err == nil {
						assert.Equalf(t, name, got, "by-id read returned wrong name for id %d", id)
					}
				case 1:
					// by-name read must never return another collection's id
					if got, err := cache.GetCollectionID(ctx, "db", name); err == nil {
						assert.Equalf(t, id, got, "by-name read returned wrong id for %s", name)
					}
				case 2:
					if infos, err := cache.GetPartitionInfos(ctx, "db", name); err == nil {
						assert.NotNil(t, infos)
					}
				case 3:
					cache.RemovePartition(ctx, "db", id, name, "par1")
				case 4:
					cache.RemoveCollectionsByID(ctx, id)
				}
			}
		}(g)
	}
	wg.Wait()

	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_CrossDBSameNameDropIsolation verifies that dropping a collection
// in one database never disturbs a same-name collection in another database: the
// primary entry, name hint, and partition cache of the untouched db must all
// survive.
func TestMetaCache_CrossDBSameNameDropIsolation(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			switch {
			case req.GetCollectionID() == 1 || (req.GetDbName() == "db1" && req.GetCollectionName() == "foo"):
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 1, DbName: "db1", Schema: &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}}, RequestTime: 100}, nil
			case req.GetCollectionID() == 2 || (req.GetDbName() == "db2" && req.GetCollectionName() == "foo"):
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 2, DbName: "db2", Schema: &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}}, RequestTime: 100}, nil
			default:
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
		})
	var mu sync.Mutex
	showByDB := map[string]int{}
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			mu.Lock()
			showByDB[req.GetDbName()]++
			mu.Unlock()
			return &milvuspb.ShowPartitionsResponse{Status: merr.Success(), PartitionIDs: []int64{100}, PartitionNames: []string{"par1"}, CreatedTimestamps: []uint64{100}, CreatedUtcTimestamps: []uint64{100}}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	for _, db := range []string{"db1", "db2"} {
		_, err = cache.GetCollectionID(ctx, db, "foo")
		assert.NoError(t, err)
		_, err = cache.GetPartitionInfos(ctx, db, "foo")
		assert.NoError(t, err)
	}
	mu.Lock()
	assert.Equal(t, 1, showByDB["db1"])
	assert.Equal(t, 1, showByDB["db2"])
	mu.Unlock()

	// drop only db1/foo
	cache.RemoveCollectionsByID(ctx, 1)

	cache.mu.RLock()
	hasDB1 := cachedEntryLocked(cache, "db1", "foo") != nil
	coll2 := cachedEntryLocked(cache, "db2", "foo")
	live1 := collByIDLive(cache, 1)
	live2 := collByIDLive(cache, 2)
	cache.mu.RUnlock()
	assert.False(t, hasDB1, "dropped db1/foo must be gone")
	assert.Nil(t, live1, "dropped id 1 must leave the primary store")
	assert.NotNil(t, coll2, "db2/foo must be untouched")
	assert.NotNil(t, live2)
	assert.Same(t, coll2, live2)
	assertMetaCacheByIDConsistent(t, cache)

	// db2/foo partitions must still be a cache hit (not swept by db1's drop)
	_, err = cache.GetPartitionInfos(ctx, "db2", "foo")
	assert.NoError(t, err)
	mu.Lock()
	assert.Equal(t, 1, showByDB["db2"], "db2 partitions must not be re-fetched after dropping db1/foo")
	mu.Unlock()
}

// TestMetaCache_EmptyDBSharesDefaultForPartitionInvalidation verifies the
// empty-db/default sharing observable end-to-end: partition entries are keyed
// by the collection id, so an empty-db request and an explicit-default request
// resolve to the same collection and share ONE partition cache entry. After a
// drop, the recreated collection carries a NEW id (ids are never reused), so
// the partition lookup misses and re-fetches — the old id's entries are
// unreachable regardless of which db-name spelling populated them.
func TestMetaCache_EmptyDBSharesDefaultForPartitionInvalidation(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	// ids are never reused in production: after the drop, the "recreated"
	// collection is described with a NEW id
	var collID atomic.Int64
	collID.Store(1)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Success(), CollectionID: collID.Load(), DbName: "default", Schema: &schemapb.CollectionSchema{Name: "coll", Fields: []*schemapb.FieldSchema{}}, RequestTime: 100,
			}, nil
		}).Maybe()
	var showCount atomic.Int32
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			showCount.Add(1)
			return &milvuspb.ShowPartitionsResponse{Status: merr.Success(), PartitionIDs: []int64{100}, PartitionNames: []string{"par1"}, CreatedTimestamps: []uint64{100}, CreatedUtcTimestamps: []uint64{100}}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache the collection (so a later drop can find it) and its partitions via
	// the EMPTY db name; both normalize to "default"
	_, err = cache.GetCollectionID(ctx, "", "coll")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfos(ctx, "", "coll")
	assert.NoError(t, err)
	assert.Equal(t, int32(1), showCount.Load())

	// an explicit "default" request hits the SAME normalized entry (no re-fetch)
	_, err = cache.GetPartitionInfos(ctx, "default", "coll")
	assert.NoError(t, err)
	assert.Equal(t, int32(1), showCount.Load(), "empty-db and default must share one partition cache entry")

	// a drop broadcast evicts the primary entry by id; the recreate gets a new id
	cache.RemoveCollectionsByID(ctx, 1)
	collID.Store(2)

	// the empty-db lookup must now re-fetch: the recreated collection's new id
	// misses the partition cache, and the old id's entry is unreachable
	_, err = cache.GetPartitionInfos(ctx, "", "coll")
	assert.NoError(t, err)
	assert.Equal(t, int32(2), showCount.Load(), "the recreated collection's partition info must be re-fetched under its new id")
}

// TestMetaCache_ConcurrentAliasResolveInvalidate exercises the alias path under
// concurrent resolution and invalidation. Resolution is the collection cache
// with a DescribeCollection fill on miss (rootcoord resolves the alias
// server-side and the fill caches the entry plus its declared alias hint);
// RemoveAlias concurrently drops the hint, forcing re-fills. Run under -race it
// flags any data race or map corruption in the alias path, and asserts
// resolution never returns a garbage value.
func TestMetaCache_ConcurrentAliasResolveInvalidate(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	// the alias flips between two targets, like a concurrent AlterAlias re-point
	var flip atomic.Int64
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			target, id := "collA", int64(1)
			if flip.Add(1)%2 == 0 {
				target, id = "collB", int64(2)
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: target, Fields: []*schemapb.FieldSchema{}},
				Aliases:      []string{req.GetCollectionName()},
				RequestTime:  uint64(flip.Load()),
			}, nil
		}).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	valid := []string{"collA", "collB"}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if i%3 == 0 {
					cache.RemoveAlias(ctx, "db", "myalias")
				} else {
					got, err := cache.ResolveCollectionAlias(ctx, "db", "myalias")
					if err == nil {
						assert.Containsf(t, valid, got, "alias resolved to a corrupted value %q", got)
					}
				}
			}
		}()
	}
	wg.Wait()
}

// TestMetaCache_DropAliasInvalidatesCanonicalCollectionAndByIDIndex is a
// regression test: an alias DDL broadcasts only the alias name with
// CollectionID=0, so the proxy sees RemoveCollection(db, alias). The collection
// is stored under its real name, so the direct lookup misses. Before the fix the
// canonical entry and its by-id index kept a stale Aliases list that an id-only
// Describe (via the cluster-wide by-id index) would then serve. The proxy must
// resolve the alias to its target and evict it.
func TestMetaCache_DropAliasInvalidatesCanonicalCollectionAndByIDIndex(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var dropped atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var aliases []string
			if !dropped.Load() {
				aliases = []string{"a"}
			}
			if req.GetCollectionID() == 1 || (req.GetDbName() == "db1" && req.GetCollectionName() == "A") {
				return &milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 1,
					DbName:       "db1",
					Schema:       &schemapb.CollectionSchema{Name: "A", Fields: []*schemapb.FieldSchema{}},
					Aliases:      aliases,
					RequestTime:  uint64(reqTime.Add(1)),
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache collection A in the non-default db1 by its real name; its Aliases=[a]
	// must populate the reverse alias index
	info, err := cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, info.aliases)

	cache.mu.RLock()
	hasID := collByIDLive(cache, 1) != nil
	aliasTarget, hasAlias := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	assert.True(t, hasID, "collection must be live in the primary store")
	if assert.True(t, hasAlias, "the collection's alias must be hinted") {
		assert.Equal(t, "A", aliasTarget)
	}

	// DropAlias(a) -> the proxy sees RemoveCollection(db1, "a") with the alias name
	dropped.Store(true)
	cache.RemoveCollection(ctx, "db1", "a")

	cache.mu.RLock()
	hasID = collByIDLive(cache, 1) != nil
	hasA := cachedEntryLocked(cache, "db1", "A") != nil
	cache.mu.RUnlock()
	assert.False(t, hasID, "DropAlias must evict the target's primary entry")
	assert.False(t, hasA, "the target must no longer resolve by name")

	// an id-only Describe now re-queries rootcoord and must get the fresh aliases
	// (without the dropped alias), not the stale cached [a]
	info2, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Empty(t, info2.aliases, "id-only describe must not serve the dropped alias")
}

// TestMetaCache_AlterAliasInvalidatesBothOldAndNewTarget verifies AlterAlias
// evicts BOTH the old target (via the proxy resolving the alias name to it) and
// the new target (via the collection id the rootcoord callback now forwards), so
// neither serves a stale Aliases list afterwards.
func TestMetaCache_AlterAliasInvalidatesBothOldAndNewTarget(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var altered atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			name := req.GetCollectionName()
			if req.GetCollectionID() != 0 {
				id = req.GetCollectionID()
				switch id {
				case 1:
					name = "A"
				case 2:
					name = "B"
				}
			} else if name == "A" {
				id = 1
			} else if name == "B" {
				id = 2
			}
			if id != 1 && id != 2 {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			var aliases []string
			// before alter: alias "a" -> A; after: alias "a" -> B
			if (id == 1 && !altered.Load()) || (id == 2 && altered.Load()) {
				aliases = []string{"a"}
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db1",
				Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  uint64(reqTime.Add(1)),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	_, err = cache.GetCollectionInfo(ctx, "db1", "B", 0)
	assert.NoError(t, err)
	cache.mu.RLock()
	aliasTarget, hasAlias := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	if assert.True(t, hasAlias, "alias of the old target must be reverse-indexed") {
		assert.Equal(t, "A", aliasTarget)
	}

	// AlterAlias(a: A -> B): the proxy sees RemoveCollection(db1, "a") for the old
	// target plus RemoveCollectionsByID(2) for the new target (id forwarded by the
	// rootcoord callback).
	altered.Store(true)
	cache.RemoveCollection(ctx, "db1", "a")
	cache.RemoveCollectionsByID(ctx, 2)

	cache.mu.RLock()
	hasA := collByIDLive(cache, 1) != nil
	hasB := collByIDLive(cache, 2) != nil
	cache.mu.RUnlock()
	assert.False(t, hasA, "old target A must be evicted")
	assert.False(t, hasB, "new target B must be evicted")
	assertMetaCacheByIDConsistent(t, cache)

	// fresh describes: A no longer reports the alias, B now does
	infoA, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Empty(t, infoA.aliases, "old target must no longer report the moved alias")
	infoB, err := cache.GetCollectionInfo(ctx, "", "", 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, infoB.aliases, "new target must report the moved alias")
}

// TestMetaCache_InvalidateCollectionMetaIsAtomic verifies that every O(1)
// mutation carried by one expiration request shares a single critical section.
// The hook pauses after both the alias-resolved target and forwarded id have
// been evicted but before the explicit alias-hint cleanup. A concurrent fill
// must not reach RootCoord until the whole batch releases its locks.
func TestMetaCache_InvalidateCollectionMetaIsAtomic(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rpcEntered := make(chan struct{}, 1)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			rpcEntered <- struct{}{}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 2,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "B", Fields: []*schemapb.FieldSchema{}},
				Aliases:      []string{"a"},
				RequestTime:  101,
			}, nil
		}).Once()

	cache := &MetaCache{
		mixCoord:       rootCoord,
		collections:    map[UniqueID]*collectionInfo{},
		nameIdx:        map[string]map[string]UniqueID{},
		aliasInfo:      map[string]map[string]string{},
		partitionCache: map[string]*partitionInfos{},
	}
	oldTarget := seedCollection(cache, "db", "A", 1)
	oldTarget.aliases = []string{"a"}
	seedCollection(cache, "db", "B", 2)
	cache.setAliasLocked("db", "a", "A")

	midMutation := make(chan struct{})
	continueMutation := make(chan struct{})
	cache.testHookInvalidateCollectionMetaMidMutation = func() {
		close(midMutation)
		<-continueMutation
	}
	defer func() { cache.testHookInvalidateCollectionMetaMidMutation = nil }()

	removedCh := make(chan []string, 1)
	go func() {
		removedCh <- cache.InvalidateCollectionMeta(ctx, "db", "a", 2, true)
	}()
	<-midMutation

	fillDone := make(chan *collectionInfo, 1)
	fillErr := make(chan error, 1)
	go func() {
		info, err := cache.GetCollectionInfo(ctx, "db", "a", 0)
		fillDone <- info
		fillErr <- err
	}()

	select {
	case <-rpcEntered:
		t.Fatal("concurrent fill reached RootCoord inside an O(1) invalidation batch")
	case <-time.After(100 * time.Millisecond):
	}

	close(continueMutation)
	removed := <-removedCh
	assert.ElementsMatch(t, []string{"A", "B"}, removed)

	select {
	case <-rpcEntered:
	case <-time.After(time.Second):
		t.Fatal("concurrent fill did not resume after the invalidation batch released its locks")
	}
	info := <-fillDone
	assert.NoError(t, <-fillErr)
	if assert.NotNil(t, info) {
		assert.Equal(t, UniqueID(2), info.collID)
		assert.Equal(t, []string{"a"}, info.aliases)
	}
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_CreateAliasInvalidatesTargetViaForwardedID verifies CreateAlias
// evicts the target collection even though the proxy's reverse alias index has no
// entry for the brand-new alias (the collection was cached before it existed) --
// it relies on the collection id forwarded by the rootcoord callback.
func TestMetaCache_CreateAliasInvalidatesTargetViaForwardedID(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var created atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var aliases []string
			if created.Load() {
				aliases = []string{"a"}
			}
			if req.GetCollectionID() == 1 || (req.GetDbName() == "db1" && req.GetCollectionName() == "A") {
				return &milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 1,
					DbName:       "db1",
					Schema:       &schemapb.CollectionSchema{Name: "A", Fields: []*schemapb.FieldSchema{}},
					Aliases:      aliases,
					RequestTime:  uint64(reqTime.Add(1)),
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache A with no alias yet -> the reverse index has no entry for "a"
	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	cache.mu.RLock()
	_, hasAliasRev := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	assert.False(t, hasAliasRev, "A had no alias, so the reverse index has no entry for a")

	// CreateAlias(a -> A): the proxy's RemoveCollection(db1, "a") cannot resolve a
	// (not reverse-indexed); eviction relies on RemoveCollectionsByID(1) with the
	// forwarded id.
	created.Store(true)
	cache.RemoveCollection(ctx, "db1", "a")
	cache.RemoveCollectionsByID(ctx, 1)

	cache.mu.RLock()
	hasID := collByIDLive(cache, 1) != nil
	cache.mu.RUnlock()
	assert.False(t, hasID, "CreateAlias must evict the target via the forwarded id")

	// id-only Describe now reports the newly created alias
	info, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, info.aliases, "target must report the newly created alias")
}

// TestMetaCache_DropAliasDefensiveDanglingHintCleanup: under the invariant
// "an alias hint exists only while its target entry lives and declares it"
// (hints are written solely from update()'s declared-aliases loop and cleaned
// at eviction), a dangling hint should be impossible. The dangling branch in
// removeCollectionByAliasLocked is kept as a one-line defense; seed the
// impossible state directly to keep it covered.
func TestMetaCache_DropAliasDefensiveDanglingHintCleanup(t *testing.T) {
	ctx := context.Background()
	mockCoord := mocks.NewMockMixCoordClient(t)
	cache := &MetaCache{
		mixCoord:       mockCoord,
		collections:    map[UniqueID]*collectionInfo{},
		nameIdx:        map[string]map[string]UniqueID{},
		aliasInfo:      map[string]map[string]string{},
		partitionCache: map[string]*partitionInfos{},
	}
	// impossible-by-invariant state: a hint whose target is not in the primary
	cache.setAliasLocked("db1", "a", "foo")

	// DropAlias(a): the target is not in the primary store, so the dangling
	// branch must delete the alias hint itself.
	cache.RemoveCollection(ctx, "db1", "a")

	cache.mu.RLock()
	_, hasRev := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	assert.False(t, hasRev, "the dangling alias hint must be cleaned even when the target is uncached")
}

// TestMetaCache_ConcurrentAlterAliasRefreshEvictsOldTarget is the P2-1 regression:
// under a concurrent AlterAlias(a: A -> B), a Describe of the new target B lands on
// this proxy AFTER the alter commits (so B reports Aliases=[a]) but BEFORE the
// alter's expiration arrives. That Describe overwrites the single-valued alias
// resolution aliasInfo[a] from A to B, while A is still cached holding the stale
// Aliases=[a] -- so the proxy alone cannot resolve the OLD target from the alias
// name. The fix is at the source: rootcoord resolves the pre-alter target under
// its database lock and forwards BOTH ids in the expiration
// (AlterAliasMessageHeader.old_collection_id), so the proxy evicts A and B by id
// regardless of its own alias-resolution state. This test simulates that contract:
// the expiration is RemoveCollection(db, "a") + RemoveCollectionsByID(B) +
// RemoveCollectionsByID(A).
func TestMetaCache_ConcurrentAlterAliasRefreshEvictsOldTarget(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var altered atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			name := req.GetCollectionName()
			if req.GetCollectionID() != 0 {
				id = req.GetCollectionID()
				switch id {
				case 1:
					name = "A"
				case 2:
					name = "B"
				}
			} else if name == "A" {
				id = 1
			} else if name == "B" {
				id = 2
			}
			if id != 1 && id != 2 {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			var aliases []string
			// before alter: alias "a" -> A; after: alias "a" -> B
			if (id == 1 && !altered.Load()) || (id == 2 && altered.Load()) {
				aliases = []string{"a"}
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db1",
				Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  uint64(reqTime.Add(1)),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// (1) cache the OLD target A holding alias "a"
	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)

	// (2) the alter commits at rootcoord, then a concurrent Describe(B) lands here
	// BEFORE the expiration and refreshes B with the moved alias. This overwrites the
	// single-valued reverse index aliasInfo[a] from A to B; A stays cached, stale.
	altered.Store(true)
	_, err = cache.GetCollectionInfo(ctx, "db1", "B", 0)
	assert.NoError(t, err)

	cache.mu.RLock()
	singleVal, hasSingle := cache.aliasInfo["db1"]["a"]
	staleA := collByIDLive(cache, 1)
	freshB := collByIDLive(cache, 2)
	cache.mu.RUnlock()
	// the race state: the single-valued alias resolution lost the old target A
	// (the concurrent Describe of B overwrote it to B), yet BOTH A and B are
	// cached, each holding "a" in its own Aliases list -- name-based resolution
	// alone can no longer reach A, which is why rootcoord must forward its id.
	if assert.True(t, hasSingle) {
		assert.Equal(t, "B", singleVal, "the concurrent refresh overwrote the single-valued alias resolution to the new target")
	}
	if assert.NotNil(t, staleA, "old target A is still cached at this point") {
		assert.Contains(t, staleA.aliases, "a", "old target A still holds the stale alias in its own Aliases list")
	}
	if assert.NotNil(t, freshB, "new target B is cached") {
		assert.Contains(t, freshB.aliases, "a", "new target B holds the refreshed alias")
	}

	// (3) the AlterAlias expiration arrives with the rootcoord-forwarded ids:
	// RemoveCollection(db1, "a") for the alias name, RemoveCollectionsByID(2) for
	// the new target, and RemoveCollectionsByID(1) for the OLD target (from the
	// header's old_collection_id).
	cache.RemoveCollection(ctx, "db1", "a")
	cache.RemoveCollectionsByID(ctx, 2)
	cache.RemoveCollectionsByID(ctx, 1)

	cache.mu.RLock()
	hasA := collByIDLive(cache, 1) != nil
	hasB := collByIDLive(cache, 2) != nil
	cache.mu.RUnlock()
	assert.False(t, hasA, "old target A must be evicted via the forwarded old-target id (P2-1)")
	assert.False(t, hasB, "new target B must be evicted too")
	assertMetaCacheByIDConsistent(t, cache)

	// a fresh id-only Describe of A must no longer report the moved alias
	infoA, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Empty(t, infoA.aliases, "old target must no longer serve the stale moved alias")
}

// TestMetaCache_RemoveDatabaseBlocksStaleResurrection: RemoveDatabase must drain
// an in-flight describe (fillMu) before evicting, so a describe issued BEFORE a
// DropDatabase broadcast cannot write its pre-DDL response back afterwards and
// resurrect an entry of a dropped database — with the id
// index, such a resurrected entry would even serve id-only lookups cluster-wide.
func TestMetaCache_RemoveDatabaseBlocksStaleResurrection(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	var describeCount atomic.Int32
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCount.Add(1)
			// first call parks in flight until released; it is served from a
			// pre-DDL snapshot
			once.Do(func() {
				close(entered)
				<-gate
			})
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  1000,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// a fill is in flight when the DropDatabase broadcast arrives
	fillDone := make(chan struct{})
	go func() {
		defer close(fillDone)
		_, err := cache.GetCollectionInfo(ctx, "db", "foo", 0)
		assert.NoError(t, err)
	}()
	<-entered

	invalDone := make(chan struct{})
	go func() {
		defer close(invalDone)
		cache.RemoveDatabase(ctx, "db")
	}()
	select {
	case <-invalDone:
		t.Fatal("RemoveDatabase must drain the in-flight fill before evicting")
	case <-time.After(100 * time.Millisecond):
	}

	// release: the pre-DDL write-back lands first, the db eviction then cleans it
	close(gate)
	<-fillDone
	<-invalDone

	// assert via the read-path helpers: RemoveDatabase drained the fill first
	// and then synchronously deleted the db's primary entries and hint buckets,
	// so the written-back pre-DDL entry is gone
	cache.mu.RLock()
	hasFoo := cachedEntryLocked(cache, "db", "foo") != nil
	hasID := collByIDLive(cache, 1) != nil
	cache.mu.RUnlock()
	assert.False(t, hasFoo, "a pre-DDL describe must not resurrect the evicted db entry")
	assert.False(t, hasID, "a by-id lookup must not serve the resurrected entry")

	// PRODUCTION-path check: the helpers above re-implement the read-path
	// resolution, so additionally prove the real read path does not serve the
	// dead entry -- a post-DDL lookup must RE-DESCRIBE (count goes up), not hit
	// the cache.
	countBefore := describeCount.Load()
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)
	assert.Greater(t, describeCount.Load(), countBefore,
		"the post-DDL lookup must re-describe instead of serving the generation-dead entry")
	cache.mu.RLock()
	hasFoo = cachedEntryLocked(cache, "db", "foo") != nil
	cache.mu.RUnlock()
	assert.True(t, hasFoo, "a post-DDL describe re-caches")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_SingleflightResultCannotEscapeInvalidation covers the gap
// between a fill callback returning and singleflight removing its completed
// call. The fill lock must remain held through that cleanup; otherwise an
// invalidation can finish in the gap and a later caller can join the old flight,
// receiving pre-DDL metadata directly even though the cache entry was evicted.
func TestMetaCache_SingleflightResultCannotEscapeInvalidation(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var postDDL atomic.Bool
	var describeCount atomic.Int32
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCount.Add(1)
			id := int64(1)
			requestTime := uint64(100)
			if postDDL.Load() {
				id = 2
				requestTime = 200
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:          merr.Success(),
				CollectionID:    id,
				DbName:          "db",
				Schema:          &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:     requestTime,
				UpdateTimestamp: requestTime,
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	beforeSingleflightReturn := make(chan struct{})
	releaseFirstFill := make(chan struct{})
	var firstFill sync.Once
	cache.testHookBeforeSingleflightReturn = func() {
		firstFill.Do(func() {
			close(beforeSingleflightReturn)
			<-releaseFirstFill
		})
	}
	defer func() { cache.testHookBeforeSingleflightReturn = nil }()

	firstFillDone := make(chan *collectionInfo, 1)
	firstFillErr := make(chan error, 1)
	go func() {
		info, err := cache.GetCollectionInfo(ctx, "db", "foo", 0)
		firstFillDone <- info
		firstFillErr <- err
	}()
	<-beforeSingleflightReturn

	// RootCoord has committed the DDL before broadcasting the invalidation.
	postDDL.Store(true)
	invalidationMidpoint := make(chan struct{})
	releaseInvalidation := make(chan struct{})
	cache.testHookInvalidateCollectionMetaMidMutation = func() {
		close(invalidationMidpoint)
		<-releaseInvalidation
	}
	defer func() { cache.testHookInvalidateCollectionMetaMidMutation = nil }()

	invalidationDone := make(chan struct{})
	go func() {
		defer close(invalidationDone)
		cache.InvalidateCollectionMeta(ctx, "db", "foo", 1, false)
	}()

	select {
	case <-invalidationMidpoint:
		close(releaseFirstFill)
		close(releaseInvalidation)
		t.Fatal("invalidation entered while the completed flight was still published")
	case <-time.After(100 * time.Millisecond):
	}

	// Let singleflight publish the first result and remove its key. Only then may
	// invalidation acquire fillMu.Lock and evict the pre-DDL write-back.
	close(releaseFirstFill)
	info := <-firstFillDone
	assert.NoError(t, <-firstFillErr)
	if assert.NotNil(t, info) {
		assert.Equal(t, UniqueID(1), info.collID)
	}

	select {
	case <-invalidationMidpoint:
	case <-time.After(time.Second):
		t.Fatal("invalidation did not resume after the old singleflight completed")
	}

	// A request arriving while invalidation owns fillMu must wait. Once the
	// invalidation finishes, the old flight is gone and this request must issue a
	// fresh describe instead of receiving collection id 1 from singleflight.
	postInvalidationFillDone := make(chan *collectionInfo, 1)
	postInvalidationFillErr := make(chan error, 1)
	go func() {
		info, err := cache.GetCollectionInfo(ctx, "db", "foo", 0)
		postInvalidationFillDone <- info
		postInvalidationFillErr <- err
	}()
	select {
	case info := <-postInvalidationFillDone:
		close(releaseInvalidation)
		var id UniqueID
		if info != nil {
			id = info.collID
		}
		t.Fatalf("fill returned during invalidation with collection id %d", id)
	case <-time.After(100 * time.Millisecond):
	}
	assert.Equal(t, int32(1), describeCount.Load(), "fill must not reach RootCoord during invalidation")

	close(releaseInvalidation)
	<-invalidationDone
	info = <-postInvalidationFillDone
	assert.NoError(t, <-postInvalidationFillErr)
	if assert.NotNil(t, info) {
		assert.Equal(t, UniqueID(2), info.collID)
		assert.Equal(t, uint64(200), info.updateTimestamp)
	}
	assert.Equal(t, int32(2), describeCount.Load(), "post-invalidation lookup must start a fresh flight")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_DropRecreateReusingNameRelinksIDIndex: a drop + recreate reusing
// the collection name replaces the same name-key with a NEW id. The id index
// must unlink the old id and serve the new one (invariant I0 holds for both).
func TestMetaCache_DropRecreateReusingNameRelinksIDIndex(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var collID atomic.Int64
	collID.Store(1)
	var reqTime atomic.Int64
	reqTime.Store(1000)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: collID.Load(),
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  uint64(reqTime.Add(1)),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	id, err := cache.GetCollectionID(ctx, "db", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	// drop (broadcast evicts by id) + recreate under the same name with a new id
	cache.RemoveCollectionsByID(ctx, 1)
	collID.Store(2)

	id, err = cache.GetCollectionID(ctx, "db", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(2), id)

	cache.mu.RLock()
	hasOld := collByIDLive(cache, 1) != nil
	newLive := collByIDLive(cache, 2)
	sameKey := cachedEntryLocked(cache, "db", "foo")
	cache.mu.RUnlock()
	assert.False(t, hasOld, "the old id must not be live after the drop")
	assert.Same(t, sameKey, newLive, "the reused name must resolve to the recreated collection's new id")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_EvictionCleansEverythingSynchronously: there is NO background
// GC -- an eviction must clean everything the entry owns right there, all O(1)
// from the entry in hand: its name hint, its alias hints, and its partition
// list. Living entries and their satellites must be untouched.
func TestMetaCache_EvictionCleansEverythingSynchronously(t *testing.T) {
	cache := &MetaCache{
		collections:    map[UniqueID]*collectionInfo{},
		nameIdx:        map[string]map[string]UniqueID{},
		aliasInfo:      map[string]map[string]string{},
		dbInfo:         map[string]*databaseInfo{},
		partitionCache: map[string]*partitionInfos{},
	}
	doomed := seedCollection(cache, "db1", "foo", 1)
	doomed.aliases = []string{"a"} // as a real fill would record it
	seedCollection(cache, "db2", "bar", 2)
	cache.setAliasLocked("db1", "a", "foo")
	cache.partitionCache[buildPartitionCacheKey(1)] = parsePartitionsInfo([]*partitionInfo{{name: "p1", partitionID: 11}}, false)
	cache.partitionCache[buildPartitionCacheKey(2)] = parsePartitionsInfo([]*partitionInfo{{name: "p1", partitionID: 21}}, false)

	cache.RemoveCollectionsByID(context.Background(), 1)

	cache.mu.RLock()
	_, hasEntry := cache.collections[1]
	_, hasNameHint := cache.nameIdx["db1"]["foo"]
	_, hasAliasHint := cache.aliasInfo["db1"]["a"]
	_, hasLive := cache.collections[2]
	liveName := cachedEntryLocked(cache, "db2", "bar")
	cache.mu.RUnlock()
	assert.False(t, hasEntry, "primary entry deleted")
	assert.False(t, hasNameHint, "the entry's name hint is cleaned at eviction")
	assert.False(t, hasAliasHint, "the entry's alias hints are cleaned at eviction")
	assert.True(t, hasLive, "living entries untouched")
	assert.NotNil(t, liveName, "living name hints untouched")

	_, okDead := cache.partitionCache[buildPartitionCacheKey(1)]
	_, okLive := cache.partitionCache[buildPartitionCacheKey(2)]
	assert.False(t, okDead, "the entry's partition list is reclaimed at eviction")
	assert.True(t, okLive, "living collections' partition entries untouched")
}

// TestMetaCache_DroppedCascadeAliasCannotResurrectAfterRecreate: dropping a
// collection cascade-drops its aliases at rootcoord, but the DropCollection
// broadcast carries no per-alias expiration. The eviction must drop the
// entry's own alias hints; otherwise, after a same-name recreate (new id), the
// stale hint would VALIDATE again (alias -> name -> new id) and permanently
// resurrect the deleted alias for RBAC and routing.
func TestMetaCache_DroppedCascadeAliasCannotResurrectAfterRecreate(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var dropped atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			id := int64(1)
			var aliases []string
			if dropped.Load() {
				id = 2 // recreated under the same name: new id, no aliases
			} else {
				aliases = []string{"a"}
			}
			if req.GetCollectionID() == 0 && req.GetCollectionName() != "foo" {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  100,
			}, nil
		})
	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache foo (id 1) holding alias "a" -> the hint a->foo exists
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)
	cache.mu.RLock()
	_, hasHint := cache.aliasInfo["db"]["a"]
	cache.mu.RUnlock()
	assert.True(t, hasHint)

	// drop foo (cascade-drops alias "a" at rootcoord; broadcast carries only
	// the collection), then recreate foo under a NEW id before any GC ran
	dropped.Store(true)
	cache.RemoveCollectionsByID(ctx, 1)
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)

	// the dead alias must NOT resolve through the stale hint to the new foo
	cache.mu.RLock()
	_, resurrected := cache.getCollection("db", "a", 0)
	cache.mu.RUnlock()
	assert.False(t, resurrected, "a cascade-dropped alias must not resurrect after a same-name recreate")
	got, err := cache.ResolveCollectionAlias(ctx, "db", "a")
	assert.NoError(t, err)
	assert.Equal(t, "a", got, "resolution must fall through to rootcoord, which reports the alias gone")
}

// TestMetaCache_OldRootcoordAliasScanFallbackEvictsGhostHolder covers the
// upgrade-window case with no healing event: under an old rootcoord (alias-DDL
// broadcasts carry no target ids), a concurrent describe re-points the alias
// hint to the new target before the AlterAlias broadcast arrives, so name-based
// resolution reaches only the new target and the OLD one keeps a ghost alias in
// its Aliases list indefinitely (a stable, name-addressed collection never
// re-fills). The id-fingerprinted fallback (CollectionID==0 in the broadcast)
// scans for cached holders and evicts them.
func TestMetaCache_OldRootcoordAliasScanFallbackEvictsGhostHolder(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var altered atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			name := req.GetCollectionName()
			if req.GetCollectionID() != 0 {
				id = req.GetCollectionID()
				if id == 1 {
					name = "A"
				} else {
					name = "B"
				}
			} else if name == "A" {
				id = 1
			} else if name == "B" {
				id = 2
			}
			if id != 1 && id != 2 {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			var aliases []string
			if (id == 1 && !altered.Load()) || (id == 2 && altered.Load()) {
				aliases = []string{"a"}
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db1",
				Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  100,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// old target A cached holding "a"; then the alter commits and a concurrent
	// Describe(B) re-points the hint to B while A stays cached with the ghost
	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	altered.Store(true)
	_, err = cache.GetCollectionInfo(ctx, "db1", "B", 0)
	assert.NoError(t, err)

	// old-rootcoord broadcast: alias name only, no ids. Name-based resolution
	// evicts B (the hint's current target); the fallback scan must evict A.
	cache.RemoveCollection(ctx, "db1", "a")
	cache.RemoveAliasHolders(ctx, "db1", "a")
	cache.RemoveAlias(ctx, "db1", "a")

	cache.mu.RLock()
	ghost := collByIDLive(cache, 1)
	cache.mu.RUnlock()
	assert.Nil(t, ghost, "the fallback scan must evict the ghost holder the hint could no longer reach")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_RemoveAliasHoldersEvictsAllHoldersAndRespectsDB verifies the
// old-rootcoord scan fallback across MULTIPLE holders and its db scoping: the
// linear scan must evict EVERY cached entry declaring the alias (not stop at the
// first), and must never touch a same-named alias holder in another database.
func TestMetaCache_RemoveAliasHoldersEvictsAllHoldersAndRespectsDB(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// A(1) and B(2) both live in db1 and both declare alias "a" (the ghost state:
	// a concurrent describe re-pointed the hint, leaving two holders). C(3) in db2
	// also declares "a" but must be spared by the db-scoped scan.
	type ent struct {
		id int64
		db string
	}
	byName := map[string]ent{
		"A": {1, "db1"},
		"B": {2, "db1"},
		"C": {3, "db2"},
	}
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			e, ok := byName[req.GetCollectionName()]
			if !ok {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: e.id,
				DbName:       e.db,
				Schema:       &schemapb.CollectionSchema{Name: req.GetCollectionName(), Fields: []*schemapb.FieldSchema{}},
				Aliases:      []string{"a"},
				RequestTime:  100,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	_, err = cache.GetCollectionInfo(ctx, "db1", "B", 0)
	assert.NoError(t, err)
	_, err = cache.GetCollectionInfo(ctx, "db2", "C", 0)
	assert.NoError(t, err)

	// old-rootcoord fallback for db1: must evict BOTH db1 holders, not just one,
	// and must not touch the same-named alias holder in db2.
	cache.RemoveAliasHolders(ctx, "db1", "a")

	cache.mu.RLock()
	a := collByIDLive(cache, 1)
	b := collByIDLive(cache, 2)
	c := collByIDLive(cache, 3)
	cache.mu.RUnlock()
	assert.Nil(t, a, "db1 holder A must be evicted")
	assert.Nil(t, b, "db1 holder B must be evicted (the scan must reach every holder, not stop at one)")
	assert.NotNil(t, c, "db2 holder C must be spared (RemoveAliasHolders is db-scoped)")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_RemoveDatabaseEvictsAllCollectionsAcrossBatch exercises the
// database walk over a db holding MORE than one removeBatch (1024) of distinct
// collections: the completeness invariant (the db's nameIdx bucket reaches every
// live entry) and the batch-boundary loop must evict all of them.
func TestMetaCache_RemoveDatabaseEvictsAllCollectionsAcrossBatch(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	const n = 1025
	byName := make(map[string]int64, n)
	for i := 1; i <= n; i++ {
		byName[fmt.Sprintf("c%d", i)] = int64(i)
	}
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			name := req.GetCollectionName()
			id, ok := byName[name]
			if !ok {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(name))}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
				RequestTime:  100,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	for i := 1; i <= n; i++ {
		_, err := cache.GetCollectionInfo(ctx, "db", fmt.Sprintf("c%d", i), 0)
		assert.NoError(t, err)
	}

	cache.RemoveDatabase(ctx, "db")

	cache.mu.RLock()
	_, bucketExists := cache.nameIdx["db"]
	live := 0
	for i := int64(1); i <= n; i++ {
		if collByIDLive(cache, i) != nil {
			live++
		}
	}
	cache.mu.RUnlock()
	assert.False(t, bucketExists, "the db's name-hint bucket must be dropped")
	assert.Equal(t, 0, live, "every collection of the db must be evicted across the batch boundary")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_FillPreservesCallerDeadlineAndOrdersInvalidation verifies that
// MetaCache does not shorten the API deadline while still ordering a cache fill
// before a concurrent invalidation. The invalidation must wait until the caller
// cancels the fill; there is intentionally no MetaCache-specific timeout.
func TestMetaCache_FillPreservesCallerDeadlineAndOrdersInvalidation(t *testing.T) {
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	entered := make(chan struct{}, 1)
	seenDeadline := make(chan time.Time, 1)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			deadline, ok := ctx.Deadline()
			assert.True(t, ok, "the caller deadline must reach the coordinator")
			seenDeadline <- deadline
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	callerDeadline := time.Now().Add(10 * time.Second)
	callerCtx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	fillDone := make(chan error, 1)
	go func() {
		_, err := cache.GetCollectionInfo(callerCtx, "db", "foo", 0)
		fillDone <- err
	}()
	<-entered
	assert.WithinDuration(t, callerDeadline, <-seenDeadline, 100*time.Millisecond,
		"MetaCache must not replace the caller deadline with a shorter internal deadline")

	invalStarted := make(chan struct{})
	invalDone := make(chan struct{})
	go func() {
		close(invalStarted)
		cache.RemoveDatabase(context.Background(), "db")
		close(invalDone)
	}()
	<-invalStarted

	select {
	case <-invalDone:
		t.Fatal("invalidation completed before the in-flight fill was canceled")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-fillDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("the fill did not return after its caller was canceled")
	}

	select {
	case <-invalDone:
	case <-time.After(time.Second):
		t.Fatal("the invalidation did not proceed after the fill released fillMu")
	}
}

// TestMetaCache_PartitionDDLThenDropCannotResurrectAlias: RemovePartition must
// evict through the unified routine. A raw primary delete would leave the
// entry's alias hints behind; the subsequent DropCollection's cleanup then
// no-ops (entry already gone), and after a same-name recreate the surviving
// hint (alias -> name -> NEW id) would resurrect the cascade-dropped alias for
// routing and RBAC.
func TestMetaCache_PartitionDDLThenDropCannotResurrectAlias(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var dropped atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			id := int64(1)
			var aliases []string
			if dropped.Load() {
				id = 2 // recreated under the same name: new id, no aliases
			} else {
				aliases = []string{"a"}
			}
			if req.GetCollectionID() == 0 && req.GetCollectionName() != "foo" {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  100,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache foo (id 1) holding alias "a"
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)

	// partition DDL evicts the entry; then the drop broadcast arrives with NO
	// intervening access (so the drop's own cleanup finds no entry)
	cache.RemovePartition(ctx, "db", 1, "foo", "p1")
	dropped.Store(true)
	cache.RemoveCollectionsByID(ctx, 1)

	// same-name recreate under a new id
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)

	// the cascade-dropped alias must NOT resolve to the recreated collection
	cache.mu.RLock()
	_, resurrected := cache.getCollection("db", "a", 0)
	cache.mu.RUnlock()
	assert.False(t, resurrected, "partition DDL must not strand alias hints for a later resurrection")
}

// TestMetaCache_RemoveDatabaseRacingFillLeavesNoDanglingAliases stress-tests a
// defensive live-database race: if a coordinator still serves collections while
// RemoveDatabase's post-drain walk runs, fills may rebuild entries WITH fresh
// hints. The walk must evict those rebuilds through the unified routine, so no
// alias hint dangles afterwards.
func TestMetaCache_RemoveDatabaseRacingFillLeavesNoDanglingAliases(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				Aliases:      []string{"a"},
				RequestTime:  100,
			}, nil
		}).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	for i := 0; i < 50; i++ {
		_, err := cache.GetCollectionInfo(ctx, "db", "foo", 0)
		assert.NoError(t, err)
		done := make(chan struct{})
		go func() {
			defer close(done)
			// concurrent fill racing the post-drain walk
			_, _ = cache.GetCollectionInfo(ctx, "db", "foo", 0)
		}()
		cache.RemoveDatabase(ctx, "db")
		<-done

		// invariant: no dangling alias hint -- if the hint survives, its full
		// chain (alias -> name -> live entry declaring it) must hold
		cache.mu.RLock()
		if target, ok := cache.aliasInfo["db"]["a"]; ok {
			entry := cachedEntryLocked(cache, "db", target)
			if assert.NotNil(t, entry, "iteration %d: alias hint dangles -- target %q not live", i, target) {
				assert.Contains(t, entry.aliases, "a", "iteration %d: surviving hint must be declared by its entry", i)
			}
		}
		cache.mu.RUnlock()
	}
}

// TestMetaCache_RemoveDatabaseWalkEvictsMidWindowFill deterministically hits
// the window the hammer test above can only hit probabilistically: AFTER
// RemoveDatabase dropped the hint buckets (fill lock released) and BEFORE its
// batched primary walk. The hook triggers a fill exactly there; it rebuilds
// the entry WITH fresh name/alias hints, and the walk must evict it through
// the unified routine -- a raw primary delete would strand the fresh hints
// (red-green verified by negative control).
func TestMetaCache_RemoveDatabaseWalkEvictsMidWindowFill(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
		Aliases:      []string{"a"},
		RequestTime:  100,
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// prime the cache so the walk has a collected id
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)

	filled := false
	cache.testHookRemoveDatabaseMidWindow = func() {
		// exact mid-window fill: buckets are gone, walk has not run yet
		_, err := cache.GetCollectionInfo(ctx, "db", "foo", 0)
		assert.NoError(t, err)
		// the fill rebuilt the entry WITH fresh hints
		cache.mu.RLock()
		_, hasHint := cache.aliasInfo["db"]["a"]
		_, hasEntry := cache.collections[1]
		cache.mu.RUnlock()
		assert.True(t, hasEntry, "mid-window fill must have rebuilt the entry")
		assert.True(t, hasHint, "mid-window fill must have rebuilt the alias hint")
		filled = true
	}
	defer func() { cache.testHookRemoveDatabaseMidWindow = nil }()

	cache.RemoveDatabase(ctx, "db")
	assert.True(t, filled, "the hook must have run")

	// the walk must have evicted the mid-window rebuild TOGETHER with its hints
	cache.mu.RLock()
	_, hasEntry := cache.collections[1]
	_, hasHint := cache.aliasInfo["db"]["a"]
	_, hasName := cache.nameIdx["db"]["foo"]
	cache.mu.RUnlock()
	assert.False(t, hasEntry, "the walk must evict the mid-window rebuilt entry")
	assert.False(t, hasHint, "the walk must clean the rebuilt alias hint -- a raw delete would strand it")
	assert.False(t, hasName, "the walk must clean the rebuilt name hint")
}

// TestMetaCache_ConsistencyCheckerCatchesGhostAlias proves the checker itself
// catches the exact "ghost" the reviewer flagged: two live entries declare the
// same alias while aliasInfo points at only one of them. Pointing-at-a-live-
// entry alone would pass; exact ownership must not.
func TestMetaCache_ConsistencyCheckerCatchesGhostAlias(t *testing.T) {
	cache := &MetaCache{
		collections: map[UniqueID]*collectionInfo{},
		nameIdx:     map[string]map[string]UniqueID{},
		aliasInfo:   map[string]map[string]string{},
	}
	a := seedCollection(cache, "db", "A", 1)
	b := seedCollection(cache, "db", "B", 2)
	a.aliases = []string{"a"}            // A declares "a" (ghost -- stale snapshot)
	b.aliases = []string{"a"}            // B declares "a" (current target)
	cache.setAliasLocked("db", "a", "B") // aliasInfo resolves "a" to B only

	cache.mu.RLock()
	v := metaCacheConsistencyViolations(cache)
	cache.mu.RUnlock()
	found := false
	for _, msg := range v {
		if strings.Contains(msg, "ghost") && strings.Contains(msg, "id 1") {
			found = true
		}
	}
	assert.Truef(t, found, "checker must flag A's ghost declaration of \"a\"; violations: %v", v)

	// after A is evicted (its declaration gone), the state is consistent
	delete(cache.collections, 1)
	delete(cache.nameIdx["db"], "A")
	cache.mu.RLock()
	v = metaCacheConsistencyViolations(cache)
	cache.mu.RUnlock()
	assert.Empty(t, v, "with only B declaring \"a\", the cache is consistent")
}

// TestMetaCache_PartitionFillDoesNotAttachNewCollectionToOldID: the partition
// fill resolves the collection id first, then must describe/show BY THAT ID.
// If a same-name drop+recreate happens between resolve and fill, a by-name
// describe would return the NEW collection's partitions and cache them under
// the OLD id (wrong partition context, uncleanable with no GC). By id, the
// stale old id returns not-found and the fill fails (caller re-resolves)
// instead of caching a mismatch.
func TestMetaCache_PartitionFillDoesNotAttachNewCollectionToOldID(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// id 1 was dropped and "foo" recreated as id 2; the proxy's collection cache
	// still holds the stale foo->1 mapping, so the partition fill resolves id 1.
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			id := req.GetCollectionID()
			if id == 0 { // by-name resolve -> the CURRENT collection (id 2)
				id = 2
			}
			if id == 1 { // by-id describe of the dropped old id -> not found
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound("foo"))}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  100,
			}, nil
		})
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			// id 2's partitions -- must NOT be cached under id 1
			return &milvuspb.ShowPartitionsResponse{
				Status: merr.Success(), PartitionIDs: []int64{201}, PartitionNames: []string{"p_new"},
				CreatedTimestamps: []uint64{100}, CreatedUtcTimestamps: []uint64{100},
			}, nil
		}).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// seed the stale collection-cache entry foo->id 1 (as if cached before the drop)
	cache.mu.Lock()
	seedCollection(cache, "db", "foo", 1)
	cache.mu.Unlock()

	// partition fill resolves id 1, describes BY id 1 -> not-found -> error;
	// NOTHING is cached under id 1.
	_, err = cache.GetPartitionInfos(ctx, "db", "foo")
	assert.Error(t, err, "the fill must fail rather than attach id 2's partitions to id 1")

	deadKey := buildPartitionCacheKey(1)
	cache.mu.RLock()
	_, ok := cache.partitionCache[deadKey]
	cache.mu.RUnlock()
	assert.False(t, ok, "no partition entry may be cached under the stale old id")
}

// TestMetaCache_PartitionFillSkipsWriteWhenPrimaryEvictedMidFill: the partition
// fill resolves the collection id BEFORE taking fillMu.RLock; an invalidation in
// that window evicts the primary (and its partition list). The fill still
// describes/shows the collection successfully, but must NOT cache the list under
// the now-owner-less id -- otherwise a later Create/DropPartition invalidation
// (which routes through evictCollectionEntryLocked, a no-op when the primary is
// absent) cannot clean it, and it would be re-adopted when the primary re-fills.
func TestMetaCache_PartitionFillSkipsWriteWhenPrimaryEvictedMidFill(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var cache *MetaCache
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			// The partition fill describes BY ID (empty name). At that exact
			// moment simulate a concurrent invalidation evicting the primary.
			if req.GetCollectionName() == "" && req.GetCollectionID() == 1 {
				cache.mu.Lock()
				delete(cache.collections, 1)
				cache.mu.Unlock()
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  1000,
			}, nil
		})
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status: merr.Success(), PartitionIDs: []int64{100}, PartitionNames: []string{"par1"},
		CreatedTimestamps: []uint64{1000}, CreatedUtcTimestamps: []uint64{1000},
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// the caller still gets the correct list...
	info, err := cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// ...but it must NOT be cached under the now-owner-less id 1.
	cache.mu.RLock()
	_, cached := cache.partitionCache[buildPartitionCacheKey(1)]
	_, primary := cache.collections[1]
	cache.mu.RUnlock()
	assert.False(t, primary, "primary was evicted mid-fill")
	assert.False(t, cached, "an owner-less partition list must not be cached")
}

// TestMetaCache_RemovePartitionEvictsOwnerlessPartitionList: a partition DDL
// must drop the collection's partition list even when the primary entry is
// already gone -- removeCollectionByID's evictCollectionEntryLocked no-ops on an
// absent primary, so RemovePartition stales the id's partition key directly.
func TestMetaCache_RemovePartitionEvictsOwnerlessPartitionList(t *testing.T) {
	cache := &MetaCache{
		collections:    map[UniqueID]*collectionInfo{},
		nameIdx:        map[string]map[string]UniqueID{},
		aliasInfo:      map[string]map[string]string{},
		partitionCache: map[string]*partitionInfos{},
	}
	// an owner-less partition list: cached under id 1 with NO primary entry.
	cache.partitionCache[buildPartitionCacheKey(1)] = parsePartitionsInfo(
		[]*partitionInfo{{name: "p_old", partitionID: 11}}, false)

	cache.RemovePartition(context.Background(), "db", 1, "foo", "p_old")

	cache.mu.RLock()
	_, cached := cache.partitionCache[buildPartitionCacheKey(1)]
	cache.mu.RUnlock()
	assert.False(t, cached, "RemovePartition must evict the partition list even when the primary is absent")
}

func TestMetaCache_Close(t *testing.T) {
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)

	cache.Close()
	cache.Close()
}
