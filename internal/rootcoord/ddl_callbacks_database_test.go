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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDDLCallbacksDatabaseDDL(t *testing.T) {
	initStreamingSystem()

	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10)
	catalogKV := etcdkv.NewEtcdKV(kv, path)

	ss, err := rootcoord.NewSuffixSnapshot(catalogKV, rootcoord.SnapshotsSep, path, rootcoord.SnapshotPrefix)
	require.NoError(t, err)
	core := newTestCore(withHealthyCode(),
		withMeta(&MetaTable{
			catalog:     rootcoord.NewCatalog(catalogKV, ss),
			names:       newNameDb(),
			aliases:     newNameDb(),
			dbName2Meta: make(map[string]*model.Database),
		}),
		withValidProxyManager(),
		withValidIDAllocator(),
	)
	registry.ResetRegistration()
	RegisterDDLCallbacks(core)

	// Create a new database
	status, err := core.CreateDatabase(context.Background(), &milvuspb.CreateDatabaseRequest{
		DbName: "test",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	db, err := core.meta.GetDatabaseByName(context.Background(), "test", typeutil.MaxTimestamp)
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Equal(t, db.Name, "test")
	require.Empty(t, db.Properties)

	// Alter a database to add properties
	status, err = core.AlterDatabase(context.Background(), &rootcoordpb.AlterDatabaseRequest{
		DbName: "test",
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   "key",
				Value: "value",
			},
			{
				Key:   "key2",
				Value: "value2",
			},
		},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	db, err = core.meta.GetDatabaseByName(context.Background(), "test", typeutil.MaxTimestamp)
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Equal(t, db.Name, "test")
	require.Len(t, db.Properties, 2)

	// Drop a property
	status, err = core.AlterDatabase(context.Background(), &rootcoordpb.AlterDatabaseRequest{
		DbName:     "test",
		DeleteKeys: []string{"key"},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	db, err = core.meta.GetDatabaseByName(context.Background(), "test", typeutil.MaxTimestamp)
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Equal(t, db.Name, "test")
	require.Len(t, db.Properties, 1)

	// Drop a database
	status, err = core.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{
		DbName: "test",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	db, err = core.meta.GetDatabaseByName(context.Background(), "test", typeutil.MaxTimestamp)
	require.Error(t, err)
	require.Nil(t, db)
}
