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
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type objectCountTestKV struct {
	kv.MetaKv
	failKey string
}

func (kv *objectCountTestKV) Save(ctx context.Context, key, value string) error {
	if key == kv.failKey {
		return errors.New("injected KV save failure")
	}
	return kv.MetaKv.Save(ctx, key, value)
}

func TestMetaTable_ObjectCounts(t *testing.T) {
	ctx := context.Background()
	client, _ := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, funcutil.RandomString(10)+"/meta")
	t.Cleanup(func() {
		require.NoError(t, store.RemoveWithPrefix(ctx, ""))
	})
	kv := &objectCountTestKV{MetaKv: store}
	catalog := catalogkv.NewCatalog(kv)
	allocator := mocktso.NewAllocator(t)
	allocator.EXPECT().GenerateTSO(mock.Anything).Return(1000, nil).Once()
	loadMeta := func() *MetaTable {
		channel.ResetStaticPChannelStatsManager()
		mt, err := NewMetaTable(ctx, catalog, allocator)
		require.NoError(t, err)
		return mt
	}
	databases := metrics.RootCoordNumOfDatabases
	groups := metrics.RootCoordNumOfPrivilegeGroups
	oldDatabases, oldGroups := testutil.ToFloat64(databases), testutil.ToFloat64(groups)
	t.Cleanup(func() {
		databases.Set(oldDatabases)
		groups.Set(oldGroups)
		channel.ResetStaticPChannelStatsManager()
	})

	mt := loadMeta()
	require.Equal(t, 1.0, testutil.ToFloat64(databases)) // default database
	require.Zero(t, testutil.ToFloat64(groups))          // built-in groups are excluded

	db := model.NewDatabase(42, "analytics", pb.DatabaseState_DatabaseCreated, nil)
	kv.failKey = catalogkv.BuildDatabaseKey(db.ID)
	require.Error(t, mt.CreateDatabase(ctx, db, 1001))
	require.Equal(t, 1.0, testutil.ToFloat64(databases))
	kv.failKey = ""
	for range 2 {
		require.NoError(t, mt.CreateDatabase(ctx, db, 1001))
		require.Equal(t, 2.0, testutil.ToFloat64(databases))
		require.NoError(t, mt.CreatePrivilegeGroup(ctx, "readers"))
		require.Equal(t, 1.0, testutil.ToFloat64(groups))
	}

	backup := &milvuspb.RBACMeta{PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{
		{GroupName: "writers"}, {GroupName: "admins"},
	}}
	kv.failKey = catalogkv.BuildPrivilegeGroupkey("admins")
	require.Error(t, mt.RestoreRBAC(ctx, util.DefaultTenant, backup))
	require.Equal(t, 2.0, testutil.ToFloat64(groups)) // readers and writers are persisted
	kv.failKey = ""
	for range 2 {
		require.NoError(t, mt.RestoreRBAC(ctx, util.DefaultTenant, backup))
		require.Equal(t, 3.0, testutil.ToFloat64(groups))
	}

	databases.Set(0)
	groups.Set(0)
	mt = loadMeta()
	require.Equal(t, 2.0, testutil.ToFloat64(databases))
	require.Equal(t, 3.0, testutil.ToFloat64(groups))
	for range 2 {
		require.NoError(t, mt.DropDatabase(ctx, db.Name, 1002))
		require.Equal(t, 1.0, testutil.ToFloat64(databases))
		require.NoError(t, mt.DropPrivilegeGroup(ctx, "readers"))
		require.Equal(t, 2.0, testutil.ToFloat64(groups))
	}
}

func TestMetaTable_PrivilegeGroupCountReadFailure(t *testing.T) {
	ctx := context.Background()
	catalog := mocks.NewRootCoordCatalog(t)
	mt := &MetaTable{catalog: catalog}
	gauge := metrics.RootCoordNumOfPrivilegeGroups
	previous := testutil.ToFloat64(gauge)
	t.Cleanup(func() { gauge.Set(previous) })
	gauge.Set(0)

	catalog.EXPECT().SavePrivilegeGroup(ctx, mock.Anything).Return(nil).Once()
	catalog.EXPECT().ListPrivilegeGroups(ctx).Return(nil, errors.New("injected KV read failure")).Once()
	require.NoError(t, mt.CreatePrivilegeGroup(ctx, "readers"))
	require.Zero(t, testutil.ToFloat64(gauge))

	catalog.EXPECT().ListPrivilegeGroups(ctx).Return([]*milvuspb.PrivilegeGroupInfo{{GroupName: "readers"}}, nil).Once()
	_, err := mt.ListPrivilegeGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(gauge))
}
