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

package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	rand.Seed(time.Now().UnixNano())
	code := m.Run()
	os.Exit(code)
}

func initStreamingSystemAndCore(t *testing.T) *Core {
	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10)
	catalogKV := etcdkv.NewEtcdKV(kv, path)

	ss, err := rootcoord.NewSuffixSnapshot(catalogKV, rootcoord.SnapshotsSep, path, rootcoord.SnapshotPrefix)
	require.NoError(t, err)
	testDB := newNameDb()
	collID2Meta := make(map[typeutil.UniqueID]*model.Collection)
	core := newTestCore(withHealthyCode(),
		withMeta(&MetaTable{
			catalog:     rootcoord.NewCatalog(catalogKV, ss),
			names:       testDB,
			aliases:     newNameDb(),
			dbName2Meta: make(map[string]*model.Database),
			collID2Meta: collID2Meta,
		}),
		withValidMixCoord(),
		withValidProxyManager(),
		withValidIDAllocator(),
		withBroker(newValidMockBroker()),
	)
	registry.ResetRegistration()
	RegisterDDLCallbacks(core)
	// TODO: we should merge all coordinator code into one package unit,
	// so these mock code can be replaced with the real code.
	registry.RegisterDropIndexV2AckCallback(func(ctx context.Context, result message.BroadcastResultDropIndexMessageV2) error {
		return nil
	})

	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
		results := make(map[string]*message.AppendResult)
		for _, vchannel := range msg.BroadcastHeader().VChannels {
			results[vchannel] = &message.AppendResult{
				MessageID:              rmq.NewRmqID(1),
				TimeTick:               tsoutil.ComposeTSByTime(time.Now(), 0),
				LastConfirmedMessageID: rmq.NewRmqID(1),
			}
		}
		registry.CallMessageAckCallback(context.Background(), msg, results)
		return &types.BroadcastAppendResult{}, nil
	}).Maybe()
	bapi.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(bapi, nil).Maybe()
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).Return(bapi, nil).Maybe()
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bapi, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.ResetBroadcaster()
	broadcast.Register(mb)

	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().AllocVirtualChannels(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, param balancer.AllocVChannelParam) ([]string, error) {
		vchannels := make([]string, 0, param.Num)
		for i := 0; i < param.Num; i++ {
			vchannels = append(vchannels, funcutil.GetVirtualChannel(fmt.Sprintf("%s-rootcoord-dml_%d_100v0", path, i), param.CollectionID, i))
		}
		return vchannels, nil
	}).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, callback balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})
	return core
}

func TestRootCoord_CreateDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})
}

func TestRootCoord_DropDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})
}

func TestRootCoord_ListDatabases(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_AlterDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})
}

func TestRootCoord_CreateCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreatePartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropPartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreateAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_AlterAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DescribeAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("input alias is empty", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		meta := newMockMetaTable()
		meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
			return "", nil
		}
		c.meta = meta
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, int32(1101), resp.GetStatus().GetCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		meta := newMockMetaTable()
		meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
			return "", nil
		}
		c.meta = meta
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ListAliases(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		meta := newMockMetaTable()
		meta.ListAliasesFunc = func(ctx context.Context, dbName, collectionName string, ts Timestamp) ([]string, error) {
			return nil, nil
		}
		c.meta = meta
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_DescribeCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_HasCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowCollections(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowCollectionIDs(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("test failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		c.meta = meta

		ctx := context.Background()

		// specify db names
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, typeutil.MaxTimestamp).Return(nil, errors.New("mock err"))
		resp, err := c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			DbNames:          []string{"db1"},
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// not specify db names
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))
		resp, err = c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// list collections failed
		meta.ExpectedCalls = nil
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(
			[]*model.Database{model.NewDatabase(rand.Int63(), "db1", etcdpb.DatabaseState_DatabaseCreated, nil)}, nil)
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, typeutil.MaxTimestamp, false).Return(nil, errors.New("mock err"))
		resp, err = c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, typeutil.MaxTimestamp, false).Return([]*model.Collection{}, nil)
		c.meta = meta

		ctx := context.Background()

		// specify db names
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, typeutil.MaxTimestamp).Return(
			model.NewDatabase(rand.Int63(), "db1", etcdpb.DatabaseState_DatabaseCreated, nil), nil)
		resp, err := c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			DbNames:          []string{"db1"},
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// not specify db names
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(
			[]*model.Database{model.NewDatabase(rand.Int63(), "db1", etcdpb.DatabaseState_DatabaseCreated, nil)}, nil)
		resp, err = c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_HasPartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowPartitions(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_AllocTimestamp(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate ts", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidTsoAllocator())
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		alloc := newMockTsoAllocator()
		count := uint32(10)
		ts := Timestamp(100)
		alloc.GenerateTSOF = func(count uint32) (uint64, error) {
			// end ts
			return ts, nil
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTsoAllocator(alloc))
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{Count: count})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		// begin ts
		assert.Equal(t, ts-uint64(count)+1, resp.GetTimestamp())
		assert.Equal(t, count, resp.GetCount())
	})

	t.Run("block timestamp", func(t *testing.T) {
		alloc := newMockTsoAllocator()
		count := uint32(10)
		current := time.Now()
		ts := tsoutil.ComposeTSByTime(current.Add(time.Second), 1)
		alloc.GenerateTSOF = func(count uint32) (uint64, error) {
			// end ts
			return ts, nil
		}
		alloc.GetLastSavedTimeF = func() time.Time {
			return current
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTsoAllocator(alloc))
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{
			Count:          count,
			BlockTimestamp: tsoutil.ComposeTSByTime(current.Add(time.Second), 0),
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		// begin ts
		assert.Equal(t, ts-uint64(count)+1, resp.GetTimestamp())
		assert.Equal(t, count, resp.GetCount())
	})
}

func TestRootCoord_AllocID(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate id", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidIDAllocator())
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		alloc := newMockIDAllocator()
		id := UniqueID(100)
		alloc.AllocF = func(count uint32) (UniqueID, UniqueID, error) {
			return id, id + int64(count), nil
		}
		count := uint32(10)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withIDAllocator(alloc))
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{Count: count})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, id, resp.GetID())
		assert.Equal(t, count, resp.GetCount())
	})
}

func TestRootCoord_UpdateChannelTimeTick(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("invalid msg type", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("invalid msg", func(t *testing.T) {
		defer cleanTestEnv()

		ticker := newRocksMqTtSynchronizer()

		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTtSynchronizer(ticker))

		// the length of channel names & timestamps mismatch.
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_TimeTick,
			},
			ChannelNames: []string{funcutil.GenRandomStr()},
			Timestamps:   []uint64{},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		source := int64(20220824)
		ts := Timestamp(100)
		defaultTs := Timestamp(101)

		ticker := newRocksMqTtSynchronizer()
		ticker.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: source}})

		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTtSynchronizer(ticker))

		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				SourceID: source,
				MsgType:  commonpb.MsgType_TimeTick,
			},
			ChannelNames:     []string{funcutil.GenRandomStr()},
			Timestamps:       []uint64{ts},
			DefaultTimestamp: defaultTs,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_InvalidateCollectionMetaCache(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to invalidate cache", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidProxyManager())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withValidProxyManager())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_RenameCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_ListPolicy(t *testing.T) {
	t.Run("expand privilege groups", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		ctx := context.Background()

		meta.EXPECT().ListPolicy(ctx, util.DefaultTenant).Return([]*milvuspb.GrantEntity{
			{
				ObjectName: "*",
				Object: &milvuspb.ObjectEntity{
					Name: "Global",
				},
				Role:    &milvuspb.RoleEntity{Name: "role"},
				Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "CollectionAdmin"}},
			},
		}, nil)

		meta.EXPECT().ListPrivilegeGroups(ctx).Return([]*milvuspb.PrivilegeGroupInfo{}, nil)

		meta.EXPECT().ListUserRole(ctx, util.DefaultTenant).Return([]string{}, nil)

		resp, err := c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.Equal(t, len(Params.RbacConfig.GetDefaultPrivilegeGroup("CollectionAdmin").Privileges), len(resp.PolicyInfos))
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
}

func TestRootCoord_ShowConfigurations(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		paramtable.Init()

		pattern := "rootcoord.Port"
		req := &internalpb.ShowConfigurationsRequest{
			Base: &commonpb.MsgBase{
				MsgID: rand.Int63(),
			},
			Pattern: pattern,
		}

		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 1, len(resp.GetConfiguations()))
		assert.Equal(t, "rootcoord.port", resp.GetConfiguations()[0].Key)
	})
}

func TestRootCoord_GetMetrics(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to parse metric type", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "invalid request",
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("unsupported metric type", func(t *testing.T) {
		// unsupported metric type
		unsupportedMetricType := "unsupported"
		req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("get system info metrics from cache", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		c.metricsCacheManager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{
			Status:        merr.Success(),
			Response:      "cached response",
			ComponentName: "cached component",
		})
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("get system info metrics, cache miss", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		c.metricsCacheManager.InvalidateSystemInfoMetrics()
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("get system info metrics", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		ret, err := c.getSystemInfoMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEmpty(t, ret)
	})
}

func TestCore_Rbac(t *testing.T) {
	ctx := context.Background()
	c := &Core{
		ctx: ctx,
	}

	// not healthy.
	c.UpdateStateCode(commonpb.StateCode_Abnormal)

	{
		resp, err := c.CreateCredential(ctx, &internalpb.CredentialInfo{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	}

	{
		resp, err := c.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	}

	{
		resp, err := c.UpdateCredential(ctx, &internalpb.CredentialInfo{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	}

	{
		resp, err := c.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.CreateRole(ctx, &milvuspb.CreateRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.DropRole(ctx, &milvuspb.DropRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.SelectUser(ctx, &milvuspb.SelectUserRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}
}

func TestCore_sendMinDdlTsAsTt(t *testing.T) {
	ticker := newRocksMqTtSynchronizer()
	ddlManager := newMockDdlTsLockManager()
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	sched := newMockScheduler()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	c := newTestCore(
		withTtSynchronizer(ticker),
		withDdlTsLockManager(ddlManager),
		withScheduler(sched))

	c.UpdateStateCode(commonpb.StateCode_Healthy)

	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")
	c.sendMinDdlTsAsTt() // disable ts msg
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")

	c.sendMinDdlTsAsTt() // no session.
	ticker.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: TestRootCoordID}})
	c.sendMinDdlTsAsTt()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.ZeroTimestamp
	}
	c.sendMinDdlTsAsTt() // zero ts
	sched.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.MaxTimestamp
	}
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.MaxTimestamp
	}
	c.sendMinDdlTsAsTt()
}

func TestCore_startTimeTickLoop(t *testing.T) {
	ticker := newRocksMqTtSynchronizer()
	ticker.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: TestRootCoordID}})
	ddlManager := newMockDdlTsLockManager()
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	sched := newMockScheduler()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	c := newTestCore(
		withTtSynchronizer(ticker),
		withDdlTsLockManager(ddlManager),
		withScheduler(sched))
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
	c.wg.Add(1)
	c.UpdateStateCode(commonpb.StateCode_Initializing)
	go c.startTimeTickLoop()

	time.Sleep(time.Millisecond * 4)
	cancel()
	c.wg.Wait()
}

// make sure the main functions work well when EnableActiveStandby=true
// func TestRootcoord_EnableActiveStandby(t *testing.T) {
// 	randVal := rand.Int()
// 	paramtable.Init()
// 	registry.ResetRegistration()
// 	Params.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
// 	// Need to reset global etcd to follow new path
// 	kvfactory.CloseEtcdClient()
// 	paramtable.Get().Save(Params.RootCoordCfg.EnableActiveStandby.Key, "true")
// 	defer paramtable.Get().Reset(Params.RootCoordCfg.EnableActiveStandby.Key)
// 	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
// 	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordTimeTick.Key)
// 	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
// 	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordStatistics.Key)
// 	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))
// 	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordDml.Key)

// 	ctx := context.Background()
// 	coreFactory := dependency.NewDefaultFactory(true)
// 	etcdCli, err := etcd.GetEtcdClient(
// 		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
// 		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
// 		Params.EtcdCfg.Endpoints.GetAsStrings(),
// 		Params.EtcdCfg.EtcdTLSCert.GetValue(),
// 		Params.EtcdCfg.EtcdTLSKey.GetValue(),
// 		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
// 		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
// 	assert.NoError(t, err)
// 	defer etcdCli.Close()
// 	core, err := NewCore(ctx, coreFactory)
// 	core.etcdCli = etcdCli
// 	assert.NoError(t, err)
// 	core.SetTiKVClient(tikv.SetupLocalTxn())

// 	err = core.Init()
// 	assert.NoError(t, err)
// 	assert.Equal(t, commonpb.StateCode_StandBy, core.GetStateCode())
// 	err = core.Register()
// 	assert.NoError(t, err)
// 	err = core.Start()
// 	assert.NoError(t, err)

// 	assert.Eventually(t, func() bool {
// 		return core.GetStateCode() == commonpb.StateCode_Healthy
// 	}, time.Second*5, time.Millisecond*200)
// 	resp, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
// 		Base: &commonpb.MsgBase{
// 			MsgType:   commonpb.MsgType_DescribeCollection,
// 			MsgID:     0,
// 			Timestamp: 0,
// 			SourceID:  paramtable.GetNodeID(),
// 		},
// 		CollectionName: "unexist",
// 	})
// 	assert.NoError(t, err)
// 	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
// 	err = core.Stop()
// 	assert.NoError(t, err)
// }

// make sure the main functions work well when EnableActiveStandby=false
func TestRootCoord_AlterCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("set_dynamic_field_bad_request", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
			Properties: []*commonpb.KeyValuePair{
				{Key: common.EnableDynamicSchemaKey, Value: "abc"},
			},
		})
		assert.Error(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("set_dynamic_field_ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
			Properties: []*commonpb.KeyValuePair{
				{Key: common.EnableDynamicSchemaKey, Value: "true"},
			},
		})
		assert.NoError(t, merr.CheckRPCCall(resp, err))
	})
}

func TestRootCoord_AddCollectionFunction(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AddCollectionFunction(ctx, &milvuspb.AddCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AddCollectionFunction(ctx, &milvuspb.AddCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.AddCollectionFunction(ctx, &milvuspb.AddCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AddCollectionFunction(ctx, &milvuspb.AddCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropCollectionFunction(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.DropCollectionFunction(ctx, &milvuspb.DropCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("drop task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropCollectionFunction(ctx, &milvuspb.DropCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropCollectionFunction(ctx, &milvuspb.DropCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DropCollectionFunction(ctx, &milvuspb.DropCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_AlterCollectionFunction(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AlterCollectionFunction(ctx, &milvuspb.AlterCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("drop task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollectionFunction(ctx, &milvuspb.AlterCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollectionFunction(ctx, &milvuspb.AlterCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollectionFunction(ctx, &milvuspb.AlterCollectionFunctionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CheckHealth(t *testing.T) {
	// getQueryCoordMetricsFunc := func(tt typeutil.Timestamp) (*milvuspb.GetMetricsResponse, error) {
	// 	clusterTopology := metricsinfo.QueryClusterTopology{
	// 		ConnectedNodes: []metricsinfo.QueryNodeInfos{
	// 			{
	// 				QuotaMetrics: &metricsinfo.QueryNodeQuotaMetrics{
	// 					Fgm: metricsinfo.FlowGraphMetric{
	// 						MinFlowGraphChannel: "ch1",
	// 						MinFlowGraphTt:      tt,
	// 						NumFlowGraph:        1,
	// 					},
	// 				},
	// 			},
	// 		},
	// 	}

	// 	resp, _ := metricsinfo.MarshalTopology(metricsinfo.QueryCoordTopology{Cluster: clusterTopology})
	// 	return &milvuspb.GetMetricsResponse{
	// 		Status:        merr.Success(),
	// 		Response:      resp,
	// 		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, 0),
	// 	}, nil
	// }

	// getDataCoordMetricsFunc := func(tt typeutil.Timestamp) (*milvuspb.GetMetricsResponse, error) {
	// 	clusterTopology := metricsinfo.DataClusterTopology{
	// 		ConnectedDataNodes: []metricsinfo.DataNodeInfos{
	// 			{
	// 				QuotaMetrics: &metricsinfo.DataNodeQuotaMetrics{
	// 					Fgm: metricsinfo.FlowGraphMetric{
	// 						MinFlowGraphChannel: "ch1",
	// 						MinFlowGraphTt:      tt,
	// 						NumFlowGraph:        1,
	// 					},
	// 				},
	// 			},
	// 		},
	// 	}

	// 	resp, _ := metricsinfo.MarshalTopology(metricsinfo.DataCoordTopology{Cluster: clusterTopology})
	// 	return &milvuspb.GetMetricsResponse{
	// 		Status:        merr.Success(),
	// 		Response:      resp,
	// 		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, 0),
	// 	}, nil
	// }

	// querynodeTT := tsoutil.ComposeTSByTime(time.Now().Add(-1*time.Minute), 0)
	// datanodeTT := tsoutil.ComposeTSByTime(time.Now().Add(-2*time.Minute), 0)

	// dcClient := mocks.NewMixCoord(t)
	// dcClient.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(getDataCoordMetricsFunc(datanodeTT))
	// dcClient.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(getQueryCoordMetricsFunc(querynodeTT))

	// errDataCoordClient := mocks.NewMixCoord(t)
	// errDataCoordClient.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	// errDataCoordClient.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

	// t.Run("not healthy", func(t *testing.T) {
	// 	ctx := context.Background()
	// 	c := newTestCore(withAbnormalCode())
	// 	resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 	assert.NoError(t, err)
	// 	assert.Equal(t, false, resp.IsHealthy)
	// 	assert.NotEmpty(t, resp.Reasons)
	// })

	// t.Run("ok with disabled tt lag configuration", func(t *testing.T) {
	// 	v := Params.QuotaConfig.MaxTimeTickDelay.GetValue()
	// 	Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "-1")
	// 	defer Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, v)

	// 	c := newTestCore(withHealthyCode(), withValidProxyManager())
	// 	ctx := context.Background()
	// 	resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 	assert.NoError(t, err)
	// 	assert.Equal(t, true, resp.IsHealthy)
	// 	assert.Empty(t, resp.Reasons)
	// })

	// t.Run("proxy health check fail with invalid proxy", func(t *testing.T) {
	// 	v := Params.QuotaConfig.MaxTimeTickDelay.GetValue()
	// 	Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "6000")
	// 	defer Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, v)

	// 	c := newTestCore(withHealthyCode(), withInvalidProxyManager(), withMixCoord(dcClient))

	// 	ctx := context.Background()
	// 	resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 	assert.NoError(t, err)
	// 	assert.Equal(t, false, resp.IsHealthy)
	// 	assert.NotEmpty(t, resp.Reasons)
	// })

	// t.Run("proxy health check fail with get metrics error", func(t *testing.T) {
	// 	v := Params.QuotaConfig.MaxTimeTickDelay.GetValue()
	// 	Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "6000")
	// 	defer Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, v)

	// 	{
	// 		c := newTestCore(withHealthyCode(),
	// 			withValidProxyManager(), withMixCoord(dcClient))

	// 		ctx := context.Background()
	// 		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 		assert.NoError(t, err)
	// 		assert.Equal(t, false, resp.IsHealthy)
	// 		assert.NotEmpty(t, resp.Reasons)
	// 	}

	// 	{
	// 		c := newTestCore(withHealthyCode(),
	// 			withValidProxyManager(), withMixCoord(errDataCoordClient))

	// 		ctx := context.Background()
	// 		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 		assert.NoError(t, err)
	// 		assert.Equal(t, false, resp.IsHealthy)
	// 		assert.NotEmpty(t, resp.Reasons)
	// 	}
	// })

	// t.Run("ok with tt lag exceeded", func(t *testing.T) {
	// 	v := Params.QuotaConfig.MaxTimeTickDelay.GetValue()
	// 	Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "90")
	// 	defer Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, v)

	// 	c := newTestCore(withHealthyCode(),
	// 		withValidProxyManager(), withMixCoord(dcClient))
	// 	ctx := context.Background()
	// 	resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 	assert.NoError(t, err)
	// 	assert.Equal(t, false, resp.IsHealthy)
	// 	assert.NotEmpty(t, resp.Reasons)
	// })

	// t.Run("ok with tt lag checking", func(t *testing.T) {
	// 	v := Params.QuotaConfig.MaxTimeTickDelay.GetValue()
	// 	Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "600")
	// 	defer Params.Save(Params.QuotaConfig.MaxTimeTickDelay.Key, v)

	// 	c := newTestCore(withHealthyCode(),
	// 		withValidProxyManager(), withMixCoord(dcClient))
	// 	ctx := context.Background()
	// 	resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	// 	assert.NoError(t, err)
	// 	assert.Equal(t, true, resp.IsHealthy)
	// 	assert.Empty(t, resp.Reasons)
	// })
}

func TestRootCoord_DescribeDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})

	t.Run("add task failed", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})

	t.Run("execute task failed", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})

	t.Run("run ok", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.NoError(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})
}

func TestCore_Stop(t *testing.T) {
	t.Run("abnormal stop before component is ready", func(t *testing.T) {
		c := &Core{}
		err := c.Stop()
		assert.NoError(t, err)
		code := c.GetStateCode()
		assert.Equal(t, commonpb.StateCode_Abnormal, code)
	})

	t.Run("normal case", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		c.ctx, c.cancel = context.WithCancel(context.Background())
		err := c.Stop()
		assert.NoError(t, err)
		code := c.GetStateCode()
		assert.Equal(t, commonpb.StateCode_Abnormal, code)
	})
}

func TestCore_InitRBAC(t *testing.T) {
	paramtable.Init()
	t.Run("init default role and public role privilege", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		meta.EXPECT().CreateRole(mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
		meta.EXPECT().OperatePrivilege(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(3)

		Params.Save(Params.RoleCfg.Enabled.Key, "false")
		Params.Save(Params.CommonCfg.EnablePublicPrivilege.Key, "true")

		defer func() {
			Params.Reset(Params.RoleCfg.Enabled.Key)
			Params.Reset(Params.CommonCfg.EnablePublicPrivilege.Key)
		}()

		err := c.initRbac(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("not init public role privilege and init default privilege", func(t *testing.T) {
		builtinRoles := `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		meta.EXPECT().CreateRole(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(3)
		meta.EXPECT().OperatePrivilege(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		Params.Save(Params.RoleCfg.Enabled.Key, "true")
		Params.Save(Params.RoleCfg.Roles.Key, builtinRoles)
		Params.Save(Params.CommonCfg.EnablePublicPrivilege.Key, "false")

		defer func() {
			Params.Reset(Params.RoleCfg.Enabled.Key)
			Params.Reset(Params.RoleCfg.Roles.Key)
			Params.Reset(Params.CommonCfg.EnablePublicPrivilege.Key)
		}()

		err := c.initRbac(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("compact proxy setting", func(t *testing.T) {
		builtinRoles := `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		meta.EXPECT().CreateRole(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(3)
		meta.EXPECT().OperatePrivilege(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		Params.Save(Params.RoleCfg.Enabled.Key, "true")
		Params.Save(Params.RoleCfg.Roles.Key, builtinRoles)
		Params.Save("proxy.enablePublicPrivilege", "false")

		defer func() {
			Params.Reset(Params.RoleCfg.Enabled.Key)
			Params.Reset(Params.RoleCfg.Roles.Key)
			Params.Reset("proxy.enablePublicPrivilege")
		}()

		err := c.initRbac(context.TODO())
		assert.NoError(t, err)
	})
}

func TestCore_BackupRBAC(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))

	meta.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(&milvuspb.RBACMeta{}, nil)
	resp, err := c.BackupRBAC(context.Background(), &milvuspb.BackupRBACMetaRequest{})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(resp.GetStatus()))

	meta.ExpectedCalls = nil
	meta.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
	resp, err = c.BackupRBAC(context.Background(), &milvuspb.BackupRBACMetaRequest{})
	assert.NoError(t, err)
	assert.False(t, merr.Ok(resp.GetStatus()))
}

func TestCore_getMetastorePrivilegeName(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))

	priv, err := c.getMetastorePrivilegeName(context.Background(), util.AnyWord)
	assert.NoError(t, err)
	assert.Equal(t, priv, util.AnyWord)

	meta.EXPECT().IsCustomPrivilegeGroup(mock.Anything, "unknown").Return(false, nil)
	_, err = c.getMetastorePrivilegeName(context.Background(), "unknown")
	assert.Equal(t, err.Error(), "not found the privilege name [unknown] from metastore")
}

func TestCore_expandPrivilegeGroup(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))

	grants := []*milvuspb.GrantEntity{
		{
			ObjectName: "*",
			Object: &milvuspb.ObjectEntity{
				Name: "Global",
			},
			Role:    &milvuspb.RoleEntity{Name: "role"},
			Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "*"}},
		},
	}
	groups := map[string][]*milvuspb.PrivilegeEntity{}
	expandGrants, err := c.expandPrivilegeGroups(context.Background(), grants, groups)
	assert.NoError(t, err)
	assert.Equal(t, len(expandGrants), len(grants))
	assert.Equal(t, expandGrants[0].Grantor.Privilege.Name, grants[0].Grantor.Privilege.Name)
}

type RootCoordSuite struct {
	suite.Suite
}

func (s *RootCoordSuite) TestRestore() {
	meta := mockrootcoord.NewIMetaTable(s.T())

	meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).
		Return([]*model.Database{
			{Name: "available_colls_db"},
			{Name: "not_available_colls_db"},
		}, nil)

	meta.EXPECT().ListCollections(mock.Anything, "available_colls_db", mock.Anything, false).
		Return([]*model.Collection{
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionCreated, // available collection
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped}, // ignored
				},
			},
		}, nil)
	meta.EXPECT().ListCollections(mock.Anything, "not_available_colls_db", mock.Anything, false).
		Return([]*model.Collection{
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionDropping, // not available collection
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped},
				},
			},
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionCreating, // not available collection
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped},
				},
			},
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionDropped, // ignored
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped},
				},
			},
		}, nil)

	// ticker := newTickerWithMockNormalStream()
	tsoAllocator := newMockTsoAllocator()
	tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
		return 100, nil
	}
	core := newTestCore(
		withTsoAllocator(tsoAllocator),
		withMeta(meta))
	core.restore(context.Background())
}

func TestRootCoordSuite(t *testing.T) {
	suite.Run(t, new(RootCoordSuite))
}
